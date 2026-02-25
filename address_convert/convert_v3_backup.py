#!/usr/bin/env python3
"""
台灣不動產實價登錄資料轉換腳本 v3

整合 CSV (ALL_lvr_land_a.csv) 與 API DB (transactions_all_original.db)
兩種資料來源，輸出統一格式的 land_data.db。

支援三種資料來源模式:
  csv  : 僅從政府 CSV 匯入
  api  : 僅從 LVR API DB 匯入
  both : CSV + API 合併——先匯入 CSV，再將 API 資料配對/補充/新增

合併策略 (--source both):
  Phase A — 去重插入: 掃描 API DB，用 (日期+地址) 或 (日期+總價) 判斷
            是否已存在於 CSV 資料中。不存在則新增，資料缺損則丟棄。
  Phase B — Enrich:   用 API 的 lat/lng、社區名、房型等欄位補充既有
            CSV 記錄的遺漏值。三層匹配: 全址→日期+總價→去樓層基礎地址。
  Phase C — 社區回填: 從 API 社區對應表回填 community_name。

用法:
  python3 convert.py                          # 預設: both
  python3 convert.py --source csv             # 僅 CSV
  python3 convert.py --source api             # 僅 API
  python3 convert.py --source both            # CSV + API 合併
  python3 convert.py --csv-input a.csv --api-input t.db --output out.db
"""

import csv
import json
import sqlite3
import os
import sys
import argparse
import re
import time

# ── 共用模組 ──────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from address_utils import (
    normalize_address,
    parse_address,
    chinese_numeral_to_int,
    fullwidth_to_halfwidth,
    CITY_CODE_MAP,
    AMBIGUOUS_DISTRICTS,
    DISTRICT_CITY_MAP,
)

# 向後相容別名 (供 test_convert.py 等使用)
normalize_address_numbers = normalize_address


# ============================================================
# 安全數值轉換
# ============================================================

def safe_int(val, default=None):
    if val is None or val == '':
        return default
    try:
        return int(float(str(val).replace(',', '').replace(' ', '')))
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None):
    if val is None or val == '':
        return default
    try:
        return float(str(val).replace(',', ''))
    except (ValueError, TypeError):
        return default


def parse_price(val):
    """'39,380,000' → int"""
    if not val:
        return None
    try:
        return int(str(val).replace(',', '').replace(' ', ''))
    except Exception:
        return None


# ============================================================
# API 資料解析工具
# ============================================================

# 中文樓層 → 數字（用於解析 raw_json 的 'f' 欄位）
CHINESE_FLOOR = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7,
    '八': 8, '九': 9, '十': 10, '十一': 11, '十二': 12, '十三': 13,
    '十四': 14, '十五': 15, '十六': 16, '十七': 17, '十八': 18,
    '十九': 19, '二十': 20, '二十一': 21, '二十二': 22, '二十三': 23,
    '二十四': 24, '二十五': 25, '二十六': 26, '二十七': 27,
    '二十八': 28, '二十九': 29, '三十': 30,
    '地下一': -1, '地下二': -2, '地下三': -3,
}


def parse_floor_info(floor_str):
    """解析樓層欄位: '九層/十五層' → ('9', '15')"""
    if not floor_str:
        return '', ''
    parts = floor_str.split('/')
    if len(parts) == 2:
        fl, tf = parts[0].strip(), parts[1].strip()
        for s, attr in ((fl, 'fl'), (tf, 'tf')):
            stripped = s.rstrip('層')
            if stripped in CHINESE_FLOOR:
                if attr == 'fl':
                    fl = str(CHINESE_FLOOR[stripped])
                else:
                    tf = str(CHINESE_FLOOR[stripped])
        return fl, tf
    return floor_str.strip(), ''


def normalize_date(date_str):
    """'101/01/09' → '1010109'"""
    if not date_str:
        return ''
    return date_str.replace('/', '')


def clean_trans_addr(addr_raw):
    """取 transactions.db 地址 '#' 後半部的乾淨地址"""
    if addr_raw and '#' in addr_raw:
        return addr_raw.split('#', 1)[1]
    return addr_raw or ''


def norm_addr_simple(addr):
    """簡單正規化: 全形→半形、臺→台、去空白"""
    return fullwidth_to_halfwidth(addr or '').replace('臺', '台').replace(' ', '')


def strip_city(addr):
    """移除地址開頭的縣市名"""
    for city in CITY_CODE_MAP.values():
        if addr.startswith(city):
            return addr[len(city):]
    for old in ('台北縣', '桃園縣', '台中縣', '台南縣', '高雄縣'):
        if addr.startswith(old):
            return addr[len(old):]
    return addr


def strip_floor(addr):
    """去除尾端樓層資訊，取得建物基礎地址"""
    addr = re.sub(r'(-\d+|地下\d+|\d+)樓[之\d]*$', '', addr)
    return addr.rstrip('之号號 ')


# ============================================================
# 表結構 / 索引 / FTS
# ============================================================

def create_tables(cursor):
    """建立 SQLite 資料表"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS land_transaction (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_district    TEXT,
            transaction_type TEXT,
            address         TEXT,
            land_area       REAL,
            urban_zone      TEXT,
            non_urban_zone  TEXT,
            non_urban_use   TEXT,
            transaction_date TEXT,
            transaction_count TEXT,
            floor_level     TEXT,
            total_floors    TEXT,
            building_type   TEXT,
            main_use        TEXT,
            main_material   TEXT,
            build_date      TEXT,
            building_area   REAL,
            rooms           INTEGER,
            halls           INTEGER,
            bathrooms       INTEGER,
            partitioned     TEXT,
            has_management  TEXT,
            total_price     INTEGER,
            unit_price      REAL,
            parking_type    TEXT,
            parking_area    REAL,
            parking_price   INTEGER,
            note            TEXT,
            serial_no       TEXT,
            main_area       REAL,
            attached_area   REAL,
            balcony_area    REAL,
            elevator        TEXT,
            transfer_no     TEXT,
            county_city     TEXT,
            district        TEXT,
            village         TEXT,
            street          TEXT,
            lane            TEXT,
            alley           TEXT,
            number          TEXT,
            floor           TEXT,
            sub_number      TEXT,
            community_name  TEXT,
            lat             REAL,
            lng             REAL
        )
    ''')


def create_indexes(cursor):
    """建立搜尋索引"""
    print('  📇 建立索引...')
    indexes = [
        ('idx_county_city', 'county_city'),
        ('idx_district', 'district'),
        ('idx_street', 'street'),
        ('idx_lane', 'lane'),
        ('idx_number', 'number'),
        ('idx_floor', 'floor'),
        ('idx_date', 'transaction_date'),
        ('idx_price', 'total_price'),
        ('idx_serial', 'serial_no'),
    ]
    for name, col in indexes:
        cursor.execute(f'CREATE INDEX IF NOT EXISTS {name} ON land_transaction({col})')
    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_addr_combo
        ON land_transaction(county_city, district, street, lane, number)''')


def create_fts(cursor):
    """建立 FTS5 全文檢索表"""
    print('  🔍 建立 FTS5 全文檢索...')
    cursor.execute('DROP TABLE IF EXISTS address_fts')
    cursor.execute('''
        CREATE VIRTUAL TABLE address_fts USING fts5(
            address,
            content='land_transaction',
            content_rowid='id',
            tokenize='unicode61'
        )
    ''')
    cursor.execute('''
        INSERT INTO address_fts(rowid, address)
        SELECT id, address FROM land_transaction WHERE address != ''
    ''')


# 45 欄位 INSERT 語句
INSERT_SQL = '''INSERT INTO land_transaction (
    raw_district, transaction_type, address, land_area,
    urban_zone, non_urban_zone, non_urban_use,
    transaction_date, transaction_count, floor_level, total_floors,
    building_type, main_use, main_material, build_date,
    building_area, rooms, halls, bathrooms, partitioned,
    has_management, total_price, unit_price,
    parking_type, parking_area, parking_price,
    note, serial_no, main_area, attached_area, balcony_area,
    elevator, transfer_no,
    county_city, district, village, street, lane, alley,
    number, floor, sub_number,
    community_name, lat, lng
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''


# ============================================================
# Phase 1: 從 CSV 載入
# ============================================================

def load_csv(conn, csv_path):
    """從 ALL_lvr_land_a.csv 載入資料到 land_data.db"""
    print(f'\n📄 [CSV] 載入: {csv_path}')
    cursor = conn.cursor()

    batch = []
    batch_size = 10000
    total = 0
    parsed_ok = 0
    t0 = time.time()

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)  # 中文標頭
        next(reader, None)  # 英文標頭

        for row in reader:
            total += 1
            while len(row) < 33:
                row.append('')

            raw_address = row[2]
            parsed = parse_address(raw_address, row[0])
            if parsed['street']:
                parsed_ok += 1

            values = (
                row[0],                          # raw_district
                row[1],                          # transaction_type
                row[2],                          # address
                safe_float(row[3]),              # land_area
                row[4],                          # urban_zone
                row[5],                          # non_urban_zone
                row[6],                          # non_urban_use
                row[7],                          # transaction_date
                row[8],                          # transaction_count
                row[9],                          # floor_level
                row[10],                         # total_floors
                row[11],                         # building_type
                row[12],                         # main_use
                row[13],                         # main_material
                row[14],                         # build_date
                safe_float(row[15]),             # building_area
                safe_int(row[16]),               # rooms
                safe_int(row[17]),               # halls
                safe_int(row[18]),               # bathrooms
                row[19],                         # partitioned
                row[20],                         # has_management
                safe_int(row[21]),               # total_price
                safe_float(row[22]),             # unit_price
                row[23],                         # parking_type
                safe_float(row[24]),             # parking_area
                safe_int(row[25]),               # parking_price
                row[26],                         # note
                row[27],                         # serial_no
                safe_float(row[28]),             # main_area
                safe_float(row[29]),             # attached_area
                safe_float(row[30]),             # balcony_area
                row[31],                         # elevator
                row[32],                         # transfer_no
                parsed['county_city'],
                parsed['district'],
                parsed['village'],
                parsed['street'],
                parsed['lane'],
                parsed['alley'],
                parsed['number'],
                parsed['floor'],
                parsed['sub_number'],
                None, None, None,                # community_name, lat, lng
            )
            batch.append(values)

            if len(batch) >= batch_size:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                elapsed = time.time() - t0
                rate = total / elapsed if elapsed > 0 else 0
                print(f'\r  ⏳ 已處理 {total:,} 筆 ({rate:,.0f} 筆/秒)',
                      end='', flush=True)
                batch = []

    if batch:
        cursor.executemany(INSERT_SQL, batch)
        conn.commit()

    elapsed = time.time() - t0
    pct = parsed_ok / total * 100 if total else 0
    print(f'\n  ✅ CSV 載入完成: {total:,} 筆, '
          f'地址解析率 {pct:.1f}%, {elapsed:.1f}s')
    return total


# ============================================================
# Phase 2: 從 API DB 載入 (api-only 模式)
# ============================================================

def _parse_api_record(row):
    """
    將 transactions.db 一列 → INSERT 用 45-tuple。
    使用 parse_address + city_hint 做完整結構化解析。
    回傳 None 表示資料缺損應丟棄。
    """
    _id, city_code, town, addr_raw, build_type, community, \
        date_str, floor_col, area_col, tp_raw, up_raw, \
        lat, lon, sq, rj_text = row

    j = {}
    if rj_text:
        try:
            j = json.loads(rj_text)
        except Exception:
            pass

    # 地址清洗
    addr_clean = clean_trans_addr(addr_raw)
    if not addr_clean or '號' not in addr_clean:
        return None

    # 用 city_code 取得 city_hint → 精確消歧
    city_hint = CITY_CODE_MAP.get(city_code, '')
    parsed = parse_address(addr_clean, city_hint=city_hint)

    # 日期
    transaction_date = normalize_date(date_str)

    # 樓層
    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    # JSON 欄位
    transaction_type = j.get('t', '') or ''
    rooms = safe_int(j.get('j'))
    halls = safe_int(j.get('k'))
    bathrooms = safe_int(j.get('l'))
    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''
    building_type_j = build_type or j.get('b', '') or ''
    note = j.get('note', '') or ''

    total_price = parse_price(tp_raw) or parse_price(j.get('tp'))
    unit_price = safe_float(up_raw) or safe_float(j.get('cp'))
    building_area = safe_float(area_col) or safe_float(j.get('s'))

    serial_no = f'api_{sq}' if sq else None

    lat_val = lat if (lat and lat != 0) else None
    lng_val = lon if (lon and lon != 0) else None
    if not lat_val and j.get('lat'):
        lat_val = j['lat']
    if not lng_val and j.get('lon'):
        lng_val = j['lon']

    floor_parsed = parsed['floor']
    if not floor_parsed and floor_level:
        try:
            int(floor_level)
            floor_parsed = floor_level
        except ValueError:
            pass

    return (
        parsed.get('district') or town or '',  # raw_district
        transaction_type,                       # transaction_type
        addr_clean,                             # address
        None,                                   # land_area
        '', '', '',                             # urban / non-urban zones
        transaction_date,                       # transaction_date
        '',                                     # transaction_count
        floor_level,                            # floor_level
        total_floors,                           # total_floors
        building_type_j,                        # building_type
        main_use,                               # main_use
        '',                                     # main_material
        '',                                     # build_date
        building_area,                          # building_area
        rooms,                                  # rooms
        halls,                                  # halls
        bathrooms,                              # bathrooms
        '',                                     # partitioned
        has_management,                         # has_management
        total_price,                            # total_price
        unit_price,                             # unit_price
        '',                                     # parking_type
        None,                                   # parking_area
        None,                                   # parking_price
        note,                                   # note
        serial_no,                              # serial_no
        None,                                   # main_area
        None,                                   # attached_area
        None,                                   # balcony_area
        '',                                     # elevator
        '',                                     # transfer_no
        parsed['county_city'],                  # county_city
        parsed['district'],                     # district
        parsed['village'],                      # village
        parsed['street'],                       # street
        parsed['lane'],                         # lane
        parsed['alley'],                        # alley
        parsed['number'],                       # number
        floor_parsed,                           # floor
        parsed['sub_number'],                   # sub_number
        community or '',                        # community_name
        lat_val,                                # lat
        lng_val,                                # lng
    )


def load_api(conn, api_db_path):
    """從 transactions_all_original.db 載入資料到 land_data.db (api-only)"""
    print(f'\n🌐 [API] 載入: {api_db_path}')
    cursor = conn.cursor()

    conn_t = sqlite3.connect(api_db_path)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
    ct = conn_t.cursor()
    ct.execute(
        'SELECT id, city, town, address, build_type, community, date_str, '
        'floor, area, total_price, unit_price, lat, lon, sq, raw_json '
        'FROM transactions'
    )

    batch = []
    batch_size = 10000
    total = inserted = skipped = 0
    t0 = time.time()

    for row in ct:
        total += 1
        try:
            rec = _parse_api_record(row)
        except Exception:
            skipped += 1
            continue
        if rec is None:
            skipped += 1
            continue

        batch.append(rec)
        inserted += 1

        if len(batch) >= batch_size:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            print(f'\r  ⏳ 掃描 {total:,} | 插入 {inserted:,} | '
                  f'略過 {skipped:,} ({rate:,.0f}/s)',
                  end='', flush=True)
            batch = []

    if batch:
        cursor.executemany(INSERT_SQL, batch)
        conn.commit()

    conn_t.close()
    elapsed = time.time() - t0
    print(f'\n  ✅ API 載入完成: 掃描 {total:,}, '
          f'插入 {inserted:,}, 略過 {skipped:,}, {elapsed:.1f}s')
    return inserted


# ============================================================
# Phase 3: 合併模式 — CSV 為主, API 配對/補充/新增
# ============================================================

# Enrich 用欄位定義: (land_data 欄位, edata key, 判空函數)
ENRICH_FIELDS = [
    ('lat',              'lat',              lambda v: v is None or v == 0),
    ('lng',              'lng',              lambda v: v is None or v == 0),
    ('community_name',   'community',        lambda v: not v),
    ('county_city',      'county_city',      lambda v: not v),
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


def _build_csv_keys(cursor):
    """從已載入的 CSV 資料建立去重用的 key set"""
    print('  建立去重鍵值...', flush=True)
    cursor.execute(
        'SELECT transaction_date, address, total_price '
        'FROM land_transaction WHERE address LIKE "%號%"'
    )
    addr_keys = set()
    price_keys = set()
    for date, addr, price in cursor.fetchall():
        d = (date or '').replace('/', '')[:7]
        a = strip_city(norm_addr_simple(addr or ''))
        addr_keys.add((d, a))
        p = parse_price(price)
        if p:
            price_keys.add((d, p))
    print(f'    date+addr keys: {len(addr_keys):,}, '
          f'date+price keys: {len(price_keys):,}')
    return addr_keys, price_keys


def _richness(d):
    """計算 edata 的「資料豐富度」分數"""
    s = 0
    if d.get('lat') and d['lat'] != 0:
        s += 3
    if d.get('community'):
        s += 3
    for k in ('rooms', 'halls', 'bathrooms', 'building_area',
              'building_type', 'main_use', 'has_management',
              'transaction_type'):
        if d.get(k) not in (None, '', 0):
            s += 1
    return s


def _merge_edata(target, source):
    """從 source 補充 target 中的空欄位"""
    for f in target:
        tv = target.get(f)
        if tv is None or tv == '' or tv == 0:
            sv = source.get(f)
            if sv is not None and sv != '' and sv != 0:
                target[f] = sv


def _build_enrich_maps(api_db_path):
    """
    從 API DB 建立三種映射表供 enrich 使用:
      full_map       : 正規化完整地址 → edata
      date_price_map : (日期, 總價) → edata
      base_map       : 去樓層基礎地址 → edata
    """
    print('  建立 API 映射表...', flush=True)
    conn = sqlite3.connect(api_db_path)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    cur.execute("""
        SELECT city, address, lat, lon, community, build_type,
               date_str, floor, area, total_price, unit_price, raw_json
        FROM transactions
        WHERE address IS NOT NULL AND address != '' AND address != '#'
    """)

    full_map = {}
    date_price_map = {}
    base_map = {}
    count = 0

    for row in cur:
        city_code, addr_raw = row[0], row[1]
        lat, lon, community = row[2], row[3], (row[4] or '').strip()
        build_type, date_str, floor_col = row[5], row[6], row[7]
        area_col, tp_raw, up_raw, rj_text = row[8], row[9], row[10], row[11]

        j = {}
        if rj_text:
            try:
                j = json.loads(rj_text)
            except Exception:
                pass

        floor_json = j.get('f', '') or floor_col or ''
        floor_level, total_floors = parse_floor_info(floor_json)

        edata = {
            'lat': lat if (lat and lat != 0) else None,
            'lng': lon if (lon and lon != 0) else None,
            'community': community,
            'county_city': CITY_CODE_MAP.get(city_code, ''),
            'building_type': build_type or j.get('b', '') or '',
            'main_use': j.get('pu', '') or j.get('AA11', '') or '',
            'has_management': j.get('m', '') or '',
            'rooms': safe_int(j.get('j')),
            'halls': safe_int(j.get('k')),
            'bathrooms': safe_int(j.get('l')),
            'building_area': safe_float(area_col) or safe_float(j.get('s')),
            'unit_price': safe_float(up_raw) or safe_float(j.get('cp')),
            'transaction_type': j.get('t', '') or '',
            'floor_level': floor_level,
            'total_floors': total_floors,
            'note': j.get('note', '') or '',
        }

        clean = clean_trans_addr(addr_raw)
        norm = strip_city(norm_addr_simple(clean))
        if not norm:
            continue
        base = strip_floor(norm)
        date_norm = normalize_date(date_str)
        total_price = parse_price(tp_raw) or parse_price(j.get('tp'))

        # 全址映射 (取 richness 較高者)
        if norm not in full_map or _richness(edata) > _richness(full_map[norm]):
            if norm in full_map:
                _merge_edata(edata, full_map[norm])
            full_map[norm] = edata
        else:
            _merge_edata(full_map[norm], edata)

        # 日期+總價映射
        if date_norm and total_price and total_price > 0:
            key = (date_norm, total_price)
            if key not in date_price_map or \
               _richness(edata) > _richness(date_price_map[key]):
                if key in date_price_map:
                    _merge_edata(edata, date_price_map[key])
                date_price_map[key] = edata
            else:
                _merge_edata(date_price_map[key], edata)

        # 基礎地址 (去樓層)
        if base and base != norm:
            if base not in base_map or \
               _richness(edata) > _richness(base_map[base]):
                if base in base_map:
                    _merge_edata(edata, base_map[base])
                base_map[base] = edata
            else:
                _merge_edata(base_map[base], edata)

        count += 1
        if count % 500_000 == 0:
            print(f'    已讀取 {count:,} 筆...', flush=True)

    conn.close()
    print(f'    完成: {count:,} 筆')
    print(f'    full_map: {len(full_map):,}, '
          f'date_price: {len(date_price_map):,}, '
          f'base: {len(base_map):,}')
    return full_map, date_price_map, base_map


def _flush_updates(conn, batch):
    """批次執行 UPDATE"""
    cur = conn.cursor()
    for updates, row_id in batch:
        set_clauses = ', '.join(f'{col} = ?' for col in updates)
        values = list(updates.values()) + [row_id]
        cur.execute(
            f'UPDATE land_transaction SET {set_clauses} WHERE id = ?',
            values
        )
    conn.commit()


def _backfill_community(conn, api_db_path):
    """
    從 API DB 回填 community_name。
    建立 (county_city, district, road+號) → community 映射，
    再批次 UPDATE land_data.db。
    """
    print('  回填社區名...', flush=True)
    conn_t = sqlite3.connect(api_db_path)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
    rows = conn_t.execute(
        "SELECT city, address, community FROM transactions "
        "WHERE community != '' AND community IS NOT NULL AND address != ''"
    ).fetchall()
    conn_t.close()

    mapping = {}
    for city_code, addr_raw, community in rows:
        addr = norm_addr_simple(clean_trans_addr(addr_raw))
        short = strip_city(addr)
        m = re.match(r'^(.{1,4}?[區鎮鄉市])', short)
        if not m:
            continue
        district = m.group(1)
        rest = short[len(district):] if short.startswith(district) else short
        pos = rest.find('號')
        if pos < 0:
            continue
        road_number = rest[:pos + 1]
        county_city = CITY_CODE_MAP.get(city_code, '')
        key = (county_city, district, road_number)
        if key not in mapping:
            mapping[key] = {}
        mapping[key][community] = mapping[key].get(community, 0) + 1

    comm_map = {k: max(v, key=v.get) for k, v in mapping.items()}
    print(f'    社區映射: {len(comm_map):,} 個門牌', flush=True)

    updated = 0
    conn.execute('BEGIN')
    for i, ((county_city, district, road_number), community) in \
            enumerate(comm_map.items()):
        pattern = f'%{district}{road_number}%'
        cur = conn.execute(
            "UPDATE land_transaction SET community_name = ? "
            "WHERE district = ? AND address LIKE ? "
            "AND (community_name IS NULL OR community_name = '')",
            (community, district, pattern)
        )
        updated += cur.rowcount
        if (i + 1) % 500 == 0:
            conn.execute('COMMIT')
            conn.execute('BEGIN')
    conn.execute('COMMIT')
    return updated


def merge_api(conn, api_db_path):
    """
    合併 API 資料到已有 CSV 資料的 land_data.db。

    Phase A: 掃描 API DB，以 (日期+地址) 或 (日期+總價) 去重，
             不存在則新增，資料缺損則丟棄。
    Phase B: 建立 API 映射表，enrich 既有 CSV 記錄的缺失欄位。
    Phase C: 回填社區名。
    """
    print(f'\n🔗 [合併] 將 API 資料整合到 CSV 資料...')
    cursor = conn.cursor()
    t0 = time.time()

    # ── Phase A: 插入 API 獨有記錄 ───────────────────────────────────
    addr_keys, price_keys = _build_csv_keys(cursor)

    conn_t = sqlite3.connect(api_db_path)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
    ct = conn_t.cursor()
    ct.execute(
        'SELECT id, city, town, address, build_type, community, date_str, '
        'floor, area, total_price, unit_price, lat, lon, sq, raw_json '
        'FROM transactions'
    )

    batch = []
    total = inserted = dup_addr = dup_price = discarded = 0

    for row in ct:
        total += 1
        date_str = row[6] or ''
        addr_raw = row[3] or ''
        tp_raw = row[9]

        d = normalize_date(date_str)[:7]
        addr_clean = clean_trans_addr(addr_raw)
        addr_norm = strip_city(norm_addr_simple(addr_clean))

        # 去重檢查
        if (d, addr_norm) in addr_keys:
            dup_addr += 1
            continue
        price = parse_price(tp_raw)
        if price and (d, price) in price_keys:
            dup_price += 1
            continue

        try:
            rec = _parse_api_record(row)
        except Exception:
            discarded += 1
            continue
        if rec is None:
            discarded += 1
            continue

        batch.append(rec)
        inserted += 1

        # 更新 key set 防止後續重複
        addr_keys.add((d, addr_norm))
        if price:
            price_keys.add((d, price))

        if len(batch) >= 10000:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()
            elapsed = time.time() - t0
            print(f'\r  [A 插入] 掃描 {total:,} | 新增 {inserted:,} | '
                  f'重複(地址) {dup_addr:,} | 重複(價格) {dup_price:,} | '
                  f'丟棄 {discarded:,} ({elapsed:.0f}s)',
                  end='', flush=True)
            batch = []

    if batch:
        cursor.executemany(INSERT_SQL, batch)
        conn.commit()

    conn_t.close()
    elapsed_a = time.time() - t0
    print(f'\n  ✅ Phase A 完成: 新增 {inserted:,}, '
          f'重複跳過 {dup_addr + dup_price:,}, '
          f'丟棄 {discarded:,}, {elapsed_a:.1f}s')

    # ── Phase B: Enrich 既有 CSV 記錄 ───────────────────────────────
    t1 = time.time()
    full_map, date_price_map, base_map = _build_enrich_maps(api_db_path)

    print('  更新 CSV 記錄的缺失欄位...', flush=True)
    enrich_cols = ', '.join(
        ['id', 'address', 'transaction_date', 'total_price'] +
        [f[0] for f in ENRICH_FIELDS]
    )
    # 只處理非 API 來源的記錄
    cursor.execute(f"""
        SELECT {enrich_cols} FROM land_transaction
        WHERE (serial_no NOT LIKE 'api_%' OR serial_no IS NULL)
          AND address LIKE '%號%'
    """)

    updated_full = updated_dp = updated_base = 0
    not_found = already_ok = 0
    update_batch = []
    BATCH = 5000

    for row in cursor.fetchall():
        row_id = row[0]
        address = row[1]
        trans_date = row[2]
        land_price = row[3]

        # 當前欄位值
        current = {}
        for idx, (db_col, _, _) in enumerate(ENRICH_FIELDS):
            current[db_col] = row[4 + idx]

        needs = any(
            is_empty(current[db_col])
            for db_col, _, is_empty in ENRICH_FIELDS
        )
        if not needs:
            already_ok += 1
            continue

        # 三層匹配
        norm = strip_city(norm_addr_simple(address or ''))
        base = strip_floor(norm)
        d = normalize_date(trans_date)

        match = None
        match_type = None

        if norm in full_map:
            match = full_map[norm].copy()
            match_type = 'full'

        if d and land_price and land_price > 0:
            dp = date_price_map.get((d, land_price))
            if dp:
                if match is None:
                    match = dp.copy()
                    match_type = 'date_price'
                else:
                    _merge_edata(match, dp)

        if base and base != norm and base in base_map:
            bm = base_map[base]
            if match is None:
                match = bm.copy()
                match_type = 'base'
            else:
                _merge_edata(match, bm)

        if match is None:
            not_found += 1
            continue

        # 僅更新空欄位
        updates = {}
        for db_col, edata_key, is_empty in ENRICH_FIELDS:
            if is_empty(current[db_col]):
                new_val = match.get(edata_key)
                if new_val is not None and new_val != '' and new_val != 0:
                    updates[db_col] = new_val

        if not updates:
            not_found += 1
            continue

        update_batch.append((updates, row_id))
        if match_type == 'full':
            updated_full += 1
        elif match_type == 'date_price':
            updated_dp += 1
        else:
            updated_base += 1

        if len(update_batch) >= BATCH:
            _flush_updates(conn, update_batch)
            update_batch = []

    if update_batch:
        _flush_updates(conn, update_batch)

    total_updated = updated_full + updated_dp + updated_base
    elapsed_b = time.time() - t1
    print(f'  ✅ Phase B 完成: enrich {total_updated:,} 筆 '
          f'(全址:{updated_full:,} 日期+價格:{updated_dp:,} '
          f'基礎:{updated_base:,}), {elapsed_b:.1f}s')

    # ── Phase C: 回填社區名 ──────────────────────────────────────
    t2 = time.time()
    bf_count = _backfill_community(conn, api_db_path)
    elapsed_c = time.time() - t2
    print(f'  ✅ Phase C 完成: 社區名回填 {bf_count:,} 筆, {elapsed_c:.1f}s')

    total_elapsed = time.time() - t0
    print(f'\n  🔗 合併總計: 新增 {inserted:,}, '
          f'enriched {total_updated:,}, '
          f'社區回填 {bf_count:,}, 總耗時 {total_elapsed:.1f}s')


# ============================================================
# 主要流程
# ============================================================

def convert(source, csv_path=None, api_path=None, output_path=None):
    """
    主要轉換流程。

    Args:
        source: 'csv', 'api', 或 'both'
        csv_path:    CSV 輸入路徑
        api_path:    API DB 輸入路徑
        output_path: SQLite 輸出路徑
    """
    print(f'\n{"=" * 60}')
    print(f'  資料來源模式: {source}')
    if csv_path and source in ('csv', 'both'):
        print(f'  CSV:          {csv_path}')
    if api_path and source in ('api', 'both'):
        print(f'  API DB:       {api_path}')
    print(f'  輸出:         {output_path}')
    print(f'{"=" * 60}\n')

    if os.path.exists(output_path):
        os.remove(output_path)
        print('  (已刪除舊資料庫)')

    conn = sqlite3.connect(output_path)
    cursor = conn.cursor()

    # 效能設定
    cursor.execute('PRAGMA page_size=4096')
    cursor.execute('PRAGMA journal_mode=WAL')
    cursor.execute('PRAGMA synchronous=NORMAL')
    cursor.execute('PRAGMA cache_size=-200000')
    cursor.execute('PRAGMA temp_store=MEMORY')

    create_tables(cursor)
    conn.commit()

    t0 = time.time()

    if source == 'csv':
        load_csv(conn, csv_path)
    elif source == 'api':
        load_api(conn, api_path)
    elif source == 'both':
        load_csv(conn, csv_path)
        merge_api(conn, api_path)

    # 建立索引和 FTS
    cursor = conn.cursor()
    create_indexes(cursor)
    conn.commit()
    create_fts(cursor)
    conn.commit()
    cursor.close()

    # ANALYZE
    print('\n  📊 更新統計資訊...')
    conn.execute('ANALYZE')
    conn.commit()

    # VACUUM 壓縮
    print('  🗜  壓縮資料庫 (VACUUM)...')
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.commit()
    conn.execute('VACUUM')
    conn.execute('PRAGMA journal_mode=WAL')
    conn.commit()

    # 最終統計
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM land_transaction')
    total = cur.fetchone()[0]
    cur.execute(
        'SELECT COUNT(*) FROM land_transaction '
        'WHERE county_city IS NOT NULL AND county_city != ""'
    )
    has_city = cur.fetchone()[0]
    cur.execute(
        'SELECT COUNT(*) FROM land_transaction '
        'WHERE lat IS NOT NULL AND lat != 0'
    )
    has_geo = cur.fetchone()[0]
    cur.execute(
        'SELECT COUNT(*) FROM land_transaction '
        'WHERE community_name IS NOT NULL AND community_name != ""'
    )
    has_comm = cur.fetchone()[0]
    cur.execute(
        'SELECT COUNT(*) FROM land_transaction '
        'WHERE street IS NOT NULL AND street != ""'
    )
    has_street = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - t0
    db_size = os.path.getsize(output_path) / 1024 / 1024

    def pct(n):
        return n / total * 100 if total else 0

    print(f'\n🎉 轉換完成!')
    print(f'  總筆數:         {total:,}')
    print(f'  有縣市名:       {has_city:,} ({pct(has_city):.1f}%)')
    print(f'  地址解析成功:   {has_street:,} ({pct(has_street):.1f}%)')
    print(f'  有經緯度:       {has_geo:,} ({pct(has_geo):.1f}%)')
    print(f'  有社區名:       {has_comm:,} ({pct(has_comm):.1f}%)')
    print(f'  耗時:           {elapsed:.1f} 秒')
    print(f'  資料庫大小:     {db_size:.1f} MB')


def main():
    parser = argparse.ArgumentParser(
        description='台灣實價登錄資料轉換 v3 — 支援 CSV / API / 合併',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""範例:
  python3 convert.py                              # 預設: both
  python3 convert.py --source csv                 # 僅 CSV
  python3 convert.py --source api                 # 僅 API DB
  python3 convert.py --source both                # CSV + API 合併
  python3 convert.py --csv-input a.csv --api-input t.db -o out.db
        """
    )
    parser.add_argument('--source', '-s',
                        choices=['csv', 'api', 'both'], default='both',
                        help='資料來源模式 (預設: both)')
    parser.add_argument('--csv-input', default=None,
                        help='CSV 輸入路徑 (預設: ../db/ALL_lvr_land_a.csv)')
    parser.add_argument('--api-input', default=None,
                        help='API DB 路徑 (預設: ../db/transactions_all_original.db)')
    parser.add_argument('--output', '-o', default=None,
                        help='SQLite 輸出路徑 (預設: ../db/land_data.db)')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)

    csv_path = args.csv_input or os.path.join(
        project_dir, 'db', 'ALL_lvr_land_a.csv')
    api_path = args.api_input or os.path.join(
        project_dir, 'db', 'transactions_all_original.db')
    output_path = args.output or os.path.join(
        project_dir, 'db', 'land_data.db')

    # 驗證輸入來源
    if args.source in ('csv', 'both'):
        if not os.path.exists(csv_path):
            print(f'❌ 找不到 CSV 檔案: {csv_path}')
            sys.exit(1)
    if args.source in ('api', 'both'):
        if not os.path.exists(api_path):
            print(f'❌ 找不到 API DB: {api_path}')
            sys.exit(1)

    convert(args.source, csv_path, api_path, output_path)


if __name__ == '__main__':
    main()
