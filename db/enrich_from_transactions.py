#!/usr/bin/env python3
"""
enrich_from_transactions.py  v2
================================
從 transactions.db (591 API) 補充 land_data.db 所有缺失欄位。

匹配策略（依優先順序）：
  1. 正規化全址匹配（去城市前綴、全半形統一）
  2. 日期 + 總價匹配（解決地址格式不同的問題）
  3. 去樓層基礎地址匹配（同棟不同樓共享 lat/lng/community）

補充欄位（只補空白，不覆蓋現有）：
  lat, lng, community_name, building_type, main_use,
  has_management, rooms, halls, bathrooms, building_area,
  unit_price, transaction_type, floor_level, total_floors, note
"""

import sqlite3
import re
import json
import os
import time

DB_LAND  = os.path.join(os.path.dirname(__file__), 'land_data.db')
DB_TRANS = os.path.join(os.path.dirname(__file__), 'transactions.db')

# ─── 地址正規化 ──────────────────────────────────────────────────────────────

CHINESE_FLOOR = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '十': 10, '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
    '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
    '二十一': 21, '二十二': 22, '二十三': 23, '二十四': 24, '二十五': 25,
    '二十六': 26, '二十七': 27, '二十八': 28, '二十九': 29, '三十': 30,
    '地下一': -1, '地下二': -2, '地下三': -3,
}

def norm_addr(addr):
    """全形→半形、臺→台、中文樓層→數字"""
    result = []
    for ch in (addr or ''):
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(ch)
    addr = ''.join(result).replace('臺', '台').replace(' ', '')
    addr = re.sub(
        r'(地下[一二三]|二十[一二三四五六七八九]|三十|二十|十[一二三四五六七八九]|[一二三四五六七八九十])(樓|層)',
        lambda m: str(CHINESE_FLOOR.get(m.group(1), m.group(1))) + m.group(2),
        addr
    )
    return addr

CITY_PREFIXES = [
    '台北市', '新北市', '桃園市', '台中市', '台南市', '高雄市',
    '基隆市', '新竹市', '嘉義市', '新竹縣', '苗栗縣', '彰化縣',
    '南投縣', '雲林縣', '嘉義縣', '屏東縣', '宜蘭縣', '花蓮縣',
    '台東縣', '澎湖縣', '金門縣', '連江縣', '桃園縣', '台北縣',
    '台中縣', '台南縣', '高雄縣',
]

def strip_city(addr):
    for prefix in CITY_PREFIXES:
        if addr.startswith(prefix):
            return addr[len(prefix):]
    return addr

def strip_floor(addr):
    """去除尾端樓層資訊，取得建物基礎地址"""
    addr = re.sub(r'(-\d+|地下\d+|\d+)樓[之\d]*$', '', addr)
    addr = addr.rstrip('之号號 ')
    return addr

def parse_price(val):
    if not val:
        return None
    try:
        return int(str(val).replace(',', '').replace(' ', ''))
    except Exception:
        return None

def normalize_date(date_str):
    """101/01/05 → 1010105, 1010105 → 1010105"""
    if not date_str:
        return ''
    return date_str.replace('/', '')

def parse_floor_info(floor_str):
    """'九層/十五層' → (floor_level, total_floors)"""
    if not floor_str:
        return '', ''
    parts = floor_str.split('/')
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return floor_str.strip(), ''


# ─── 資料結構 ────────────────────────────────────────────────────────────────

ENRICH_FIELDS = [
    'lat', 'lng', 'community', 'building_type', 'main_use',
    'has_management', 'rooms', 'halls', 'bathrooms', 'building_area',
    'unit_price', 'transaction_type', 'floor_level', 'total_floors', 'note',
]

def make_edata(lat=None, lng=None, community='', building_type='',
               main_use='', has_management='', rooms=None, halls=None,
               bathrooms=None, building_area=None, unit_price=None,
               transaction_type='', floor_level='', total_floors='', note=''):
    return {
        'lat': lat, 'lng': lng, 'community': community,
        'building_type': building_type, 'main_use': main_use,
        'has_management': has_management, 'rooms': rooms, 'halls': halls,
        'bathrooms': bathrooms, 'building_area': building_area,
        'unit_price': unit_price, 'transaction_type': transaction_type,
        'floor_level': floor_level, 'total_floors': total_floors,
        'note': note,
    }

def richness(d):
    score = 0
    if d.get('lat') and d['lat'] != 0: score += 3
    if d.get('community'): score += 3
    if d.get('rooms') is not None: score += 1
    if d.get('halls') is not None: score += 1
    if d.get('bathrooms') is not None: score += 1
    if d.get('building_area'): score += 1
    if d.get('building_type'): score += 1
    if d.get('main_use'): score += 1
    if d.get('has_management'): score += 1
    if d.get('transaction_type'): score += 1
    return score

def merge_into(target, source):
    """從 source 補充 target 缺失的欄位"""
    for f in ENRICH_FIELDS:
        tv = target.get(f)
        if tv is None or tv == '' or tv == 0:
            sv = source.get(f)
            if sv is not None and sv != '' and sv != 0:
                target[f] = sv


def parse_transaction_row(row):
    """從 transactions.db 一列解析出 edata dict + keys"""
    addr_raw, lat, lon, community, build_type, date_str, floor_col, area, tp, up, rj_text = row

    lat = lat if (lat and lat != 0) else None
    lng = lon if (lon and lon != 0) else None
    community = (community or '').strip()

    j = {}
    if rj_text:
        try:
            j = json.loads(rj_text)
        except Exception:
            pass

    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    rooms = halls = bathrooms = None
    try:
        rooms = int(j['j']) if j.get('j', '') != '' else None
    except Exception:
        pass
    try:
        halls = int(j['k']) if j.get('k', '') != '' else None
    except Exception:
        pass
    try:
        bathrooms = int(j['l']) if j.get('l', '') != '' else None
    except Exception:
        pass

    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''
    transaction_type = j.get('t', '') or ''
    note = j.get('note', '') or ''
    building_type = build_type or j.get('b', '') or ''

    building_area = None
    try:
        v = area or j.get('s', '')
        if v:
            building_area = float(str(v).replace(',', ''))
    except Exception:
        pass

    unit_price = None
    try:
        v = j.get('cp', '')
        if v:
            unit_price = float(str(v).replace(',', ''))
    except Exception:
        pass

    total_price = parse_price(j.get('tp'))

    clean = addr_raw.split('#', 1)[1] if (addr_raw and '#' in addr_raw) else (addr_raw or '')
    norm = strip_city(norm_addr(clean))
    base = strip_floor(norm)
    date_norm = normalize_date(date_str)

    edata = make_edata(
        lat=lat, lng=lng, community=community, building_type=building_type,
        main_use=main_use, has_management=has_management,
        rooms=rooms, halls=halls, bathrooms=bathrooms,
        building_area=building_area, unit_price=unit_price,
        transaction_type=transaction_type, floor_level=floor_level,
        total_floors=total_floors, note=note,
    )
    return edata, norm, base, date_norm, total_price


# ─── 主邏輯 ───────────────────────────────────────────────────────────────────

def build_trans_maps():
    """從 transactions.db 建立三種映射表"""
    print("Step 1: 讀取 transactions.db 建立映射表...", flush=True)
    conn = sqlite3.connect(DB_TRANS)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    cur.execute("""
        SELECT address, lat, lon, community, build_type, date_str, floor, area,
               total_price, unit_price, raw_json
        FROM transactions
        WHERE address IS NOT NULL AND address != '' AND address != '#'
    """)

    full_map = {}       # norm 完整地址 → edata
    date_price_map = {} # (date, total_price) → edata
    base_map = {}       # norm 基礎地址（去樓層）→ edata

    count = 0
    for row in cur:
        try:
            edata, norm, base, date_norm, total_price = parse_transaction_row(row)
        except Exception:
            continue

        if not norm:
            continue

        # 1. 全址映射
        if norm not in full_map:
            full_map[norm] = edata
        elif richness(edata) > richness(full_map[norm]):
            merge_into(edata, full_map[norm])
            full_map[norm] = edata
        else:
            merge_into(full_map[norm], edata)

        # 2. 日期+總價映射
        if date_norm and total_price and total_price > 0:
            key = (date_norm, total_price)
            if key not in date_price_map:
                date_price_map[key] = edata
            elif richness(edata) > richness(date_price_map[key]):
                merge_into(edata, date_price_map[key])
                date_price_map[key] = edata
            else:
                merge_into(date_price_map[key], edata)

        # 3. 基礎地址映射（去樓層）
        if base and base != norm:
            if base not in base_map:
                base_map[base] = edata
            elif richness(edata) > richness(base_map[base]):
                merge_into(edata, base_map[base])
                base_map[base] = edata
            else:
                merge_into(base_map[base], edata)

        count += 1
        if count % 500_000 == 0:
            print(f"  已讀取 {count:,} 筆...", flush=True)

    conn.close()
    print(f"  完成: 共 {count:,} 筆")
    print(f"  full_map:       {len(full_map):,} 個地址")
    print(f"  date_price_map: {len(date_price_map):,} 個日期+總價")
    print(f"  base_map:       {len(base_map):,} 個基礎地址")
    return full_map, date_price_map, base_map


# 需要補充的欄位：(land_data 欄位名, edata key, 空值判斷)
FIELD_MAP = [
    ('lat',              'lat',              lambda v: v is None or v == 0),
    ('lng',              'lng',              lambda v: v is None or v == 0),
    ('community_name',   'community',        lambda v: not v),
    ('building_type',    'building_type',    lambda v: not v),
    ('main_use',         'main_use',         lambda v: not v),
    ('has_management',   'has_management',   lambda v: not v),
    ('rooms',            'rooms',            lambda v: v is None),
    ('halls',            'halls',            lambda v: v is None),
    ('bathrooms',        'bathrooms',        lambda v: v is None),
    ('building_area',    'building_area',    lambda v: v is None or v == 0),
    ('unit_price',       'unit_price',       lambda v: v is None or v == 0),
    ('transaction_type', 'transaction_type', lambda v: not v),
    ('floor_level',      'floor_level',      lambda v: not v),
    ('total_floors',     'total_floors',     lambda v: not v),
    ('note',             'note',             lambda v: not v),
]


def enrich_land_data(full_map, date_price_map, base_map):
    """更新 land_data.db 缺失欄位"""
    print("\nStep 2: 更新 land_data.db 缺失欄位...", flush=True)

    # 讀寫分離：read_conn 唯讀（避免 SELECT cursor 被 WAL checkpoint 干擾）
    read_conn = sqlite3.connect(f'file:{DB_LAND}?mode=ro', uri=True)
    read_conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    read_conn.execute('PRAGMA cache_size=-131072')

    write_conn = sqlite3.connect(DB_LAND)
    write_conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    write_conn.execute('PRAGMA journal_mode=WAL')
    write_conn.execute('PRAGMA synchronous=NORMAL')
    write_conn.execute('PRAGMA cache_size=-65536')
    write_conn.execute('PRAGMA wal_autocheckpoint=0')  # 停用自動 checkpoint，手動控制

    cols = ', '.join(['rowid', 'id', 'address', 'transaction_date', 'total_price'] +
                     [f[0] for f in FIELD_MAP])

    # 先取總數
    cur2 = read_conn.cursor()
    cur2.execute("""
        SELECT COUNT(*) FROM land_transaction
        WHERE (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
          AND address LIKE '%號%'
    """)
    total = cur2.fetchone()[0]
    print(f"  候選記錄: {total:,}", flush=True)

    updated_full  = 0
    updated_dp    = 0
    updated_base  = 0
    not_found     = 0
    already_full  = 0
    batch         = []
    BATCH_SIZE    = 10_000
    CHUNK_SIZE    = 50_000    # rowid-based 分頁
    t0            = time.time()
    global_i      = 0

    # 取候選記錄的最大 rowid
    cur_max = read_conn.cursor()
    cur_max.execute("""
        SELECT MAX(rowid) FROM land_transaction
        WHERE (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
          AND address LIKE '%號%'
    """)
    max_rowid = cur_max.fetchone()[0] or 0

    last_rowid = 0
    while last_rowid <= max_rowid:
        try:
            cur = read_conn.cursor()
            cur.execute(f"""
                SELECT {cols}
                FROM land_transaction
                WHERE rowid > {last_rowid}
                  AND (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
                  AND address LIKE '%號%'
                ORDER BY rowid
                LIMIT {CHUNK_SIZE}
            """)
            chunk = cur.fetchall()
        except sqlite3.DatabaseError as e:
            # 跳過此段損壞頁面，往後推進
            print(f"\n  [WARN] rowid>{last_rowid:,} 讀取失敗 ({e})，跳過 {CHUNK_SIZE} 筆", flush=True)
            last_rowid += CHUNK_SIZE
            continue

        if not chunk:
            break

        # 更新 last_rowid 為 chunk 最後一筆的 rowid，確保正確推進
        last_rowid = chunk[-1][0]

        for row in chunk:
            global_i += 1
            row_rowid    = row[0]
            row_id       = row[1]
            address      = row[2]
            trans_date   = row[3]
            land_total_price = row[4]

            current_values = {}
            for j_idx, (db_col, _, is_empty) in enumerate(FIELD_MAP):
                current_values[db_col] = row[5 + j_idx]

            needs_enrich = any(
                is_empty(current_values[db_col])
                for db_col, _, is_empty in FIELD_MAP
            )
            if not needs_enrich:
                already_full += 1
                continue

            norm = strip_city(norm_addr(address))
            base = strip_floor(norm)
            date_norm = normalize_date(trans_date)

            # 嘗試三種匹配
            match = None
            match_type = None

            # 1. 全址匹配
            if norm in full_map:
                match = full_map[norm].copy()
                match_type = 'full'

            # 2. 日期+總價匹配
            if date_norm and land_total_price and land_total_price > 0:
                dp_key = (date_norm, land_total_price)
                dp_match = date_price_map.get(dp_key)
                if dp_match:
                    if match is None:
                        match = dp_match.copy()
                        match_type = 'date_price'
                    else:
                        merge_into(match, dp_match)

            # 3. 基礎地址匹配（去樓層）
            if base and base != norm and base in base_map:
                base_match = base_map[base]
                if match is None:
                    match = base_match.copy()
                    match_type = 'base'
                else:
                    merge_into(match, base_match)

            if match is None:
                not_found += 1
                continue

            # 計算需要更新的欄位
            updates = {}
            for db_col, edata_key, is_empty in FIELD_MAP:
                if is_empty(current_values[db_col]):
                    new_val = match.get(edata_key)
                    if new_val is not None and new_val != '' and new_val != 0:
                        updates[db_col] = new_val

            if not updates:
                not_found += 1
                continue

            batch.append((updates, row_id))
            if match_type == 'full':
                updated_full += 1
            elif match_type == 'date_price':
                updated_dp += 1
            else:
                updated_base += 1

            if len(batch) >= BATCH_SIZE:
                _flush_batch(write_conn, batch)
                batch.clear()
                total_updated = updated_full + updated_dp + updated_base
                # 每 100k 筆做一次 WAL checkpoint，防止 WAL 無限膨脹
                if total_updated % 100_000 == 0:
                    write_conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
                elapsed = time.time() - t0
                print(f"\r  進度: {global_i:,}/{total:,} | 更新: {total_updated:,} "
                      f"(全址:{updated_full:,} 日期價格:{updated_dp:,} 基礎:{updated_base:,}) "
                      f"({elapsed:.0f}s)",
                      end='', flush=True)

    if batch:
        _flush_batch(write_conn, batch)

    write_conn.execute('PRAGMA wal_checkpoint(FULL)')
    write_conn.commit()
    write_conn.close()
    read_conn.close()

    elapsed = time.time() - t0
    total_updated = updated_full + updated_dp + updated_base
    print(f"\n\n✅ 補充完成")
    print(f"   候選記錄:     {total:,}")
    print(f"   已有完整資料: {already_full:,}")
    print(f"   成功更新:     {total_updated:,}")
    print(f"     全址匹配:     {updated_full:,}")
    print(f"     日期+總價:    {updated_dp:,}")
    print(f"     基礎地址:     {updated_base:,}")
    print(f"   未找匹配:     {not_found:,}")
    print(f"   耗時: {elapsed:.1f}s")


def _flush_batch(conn, batch):
    """批次更新"""
    cur = conn.cursor()
    for updates, row_id in batch:
        set_clauses = []
        values = []
        for col, val in updates.items():
            set_clauses.append(f"{col} = ?")
            values.append(val)
        values.append(row_id)
        sql = f"UPDATE land_transaction SET {', '.join(set_clauses)} WHERE id = ?"
        cur.execute(sql, values)
    conn.commit()


def verify(t0_total):
    print("\nStep 3: 驗證結果...", flush=True)
    conn = sqlite3.connect(DB_LAND)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM land_transaction")
    n_total = cur.fetchone()[0]

    stats = []
    for db_col, _, is_empty in FIELD_MAP:
        if db_col in ('lat', 'lng', 'building_area', 'unit_price'):
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL AND {db_col} != 0")
        elif db_col in ('rooms', 'halls', 'bathrooms'):
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL")
        else:
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL AND {db_col} != ''")
        cnt = cur.fetchone()[0]
        pct = cnt / n_total * 100
        stats.append((db_col, cnt, pct))

    conn.close()

    print(f"\n📊 land_data.db 最終統計 (總筆數: {n_total:,})")
    print(f"{'欄位':<20} {'有值筆數':>12} {'覆蓋率':>8}")
    print(f"{'─'*20} {'─'*12} {'─'*8}")
    for col, cnt, pct in stats:
        print(f"{col:<20} {cnt:>12,} {pct:>7.1f}%")
    print(f"\n總耗時: {time.time()-t0_total:.1f}s")


if __name__ == '__main__':
    t0 = time.time()
    full_map, date_price_map, base_map = build_trans_maps()
    enrich_land_data(full_map, date_price_map, base_map)
    verify(t0)
