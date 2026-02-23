#!/usr/bin/env python3
"""
backfill_community.py
=====================
從 transactions.db 的 community 欄位，回填到 land_data.db 的 community_name。

邏輯：
  1. 從 transactions.db 取出所有有 community 的記錄
  2. 取 # 後面的乾淨地址，去掉樓層資訊得到「門牌無樓」
  3. 按 (district, 門牌無樓) 分組 → community 名稱
  4. 用 UPDATE … WHERE district = ? AND address LIKE '%門牌無樓%' 回填 land_data.db

用法：
    python3 backfill_community.py          # 預設路徑
    python3 backfill_community.py --dry-run # 只統計不寫入
"""

import sqlite3
import re
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LAND_DB = os.path.join(SCRIPT_DIR, 'land_data.db')
TRANS_DB = os.path.join(SCRIPT_DIR, 'transactions.db')

# ── 城市代碼 → 地址前綴 (transactions.db 用字母代碼) ──────────────────────────
CITY_CODE_MAP = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市',
    'E': '高雄市', 'F': '新北市', 'G': '宜蘭縣', 'H': '桃園市',
    'I': '嘉義市', 'J': '新竹縣', 'K': '苗栗縣', 'M': '南投縣',
    'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣', 'Q': '嘉義縣',
    'T': '屏東縣', 'U': '花蓮縣', 'V': '台東縣', 'W': '金門縣',
    'X': '澎湖縣', 'Z': '連江縣',
}


def fullwidth_to_halfwidth(s):
    """全形英數→半形"""
    result = []
    for ch in s:
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(ch)
    return ''.join(result)


def normalize_addr(addr):
    """統一全形→半形、臺→台、去空白"""
    addr = fullwidth_to_halfwidth(addr)
    addr = addr.replace('臺', '台').replace(' ', '').replace('　', '')
    return addr


def strip_floor(addr):
    """去掉地址末尾的樓層資訊，保留門牌號。
    例: '中正區汀州路一段76號二十樓' → '中正區汀州路一段76號'
        '中正區汀州路一段76號二十樓之3' → '中正區汀州路一段76號'
        '信義區松仁路53之9號14樓'  → '信義區松仁路53之9號'
    """
    # 先嘗試: …號XX樓… → …號
    m = re.search(r'號', addr)
    if m:
        pos = m.end()
        # 號 後面如果是樓層資訊就截掉
        rest = addr[pos:]
        if re.match(r'^[一二三四五六七八九十百零\d]+樓', rest) or \
           re.match(r'^(地下)?[一二三四五六七八九十百零\d]+[層F]', rest):
            return addr[:pos]
        # 號後面是空的也行
        if not rest.strip():
            return addr[:pos]
    return addr


def extract_district(addr):
    """從地址中提取行政區"""
    # 先去掉可能的縣市前綴
    for prefix_len in (3, 2):  # 台北市(3), or shorter
        pref = addr[:prefix_len]
        if pref.endswith(('市', '縣')):
            addr = addr[prefix_len:]
            break

    # 抓行政區 (X區, XX區, XXX區, X鎮, X鄉, X市)
    m = re.match(r'^(.{1,4}?[區鎮鄉市])', addr)
    if m:
        return m.group(1)
    return ''


def build_community_map():
    """從 transactions.db 建立 (district, 門牌無樓) → community 映射"""
    print('讀取 transactions.db ...', flush=True)
    conn = sqlite3.connect(TRANS_DB)
    rows = conn.execute(
        "SELECT city, address, community FROM transactions "
        "WHERE community != '' AND community IS NOT NULL AND address != ''"
    ).fetchall()
    conn.close()
    print(f'  有 community 的記錄: {len(rows):,}', flush=True)

    # (county_city, district, base_addr_no_district) → community
    # base_addr_no_district = 路段巷弄號 (不含區、不含樓)
    mapping = {}

    for city_code, addr_raw, community in rows:
        # 取 # 後半（乾淨地址）
        if '#' in addr_raw:
            addr = addr_raw.split('#', 1)[1]
        else:
            addr = addr_raw

        addr = normalize_addr(addr)
        district = extract_district(addr)
        if not district:
            continue

        # 去掉 district 前綴
        if addr.startswith(district):
            road_number = addr[len(district):]
        else:
            # 可能有縣市前綴
            for prefix_len in (3, 2):
                if addr[prefix_len:].startswith(district):
                    road_number = addr[prefix_len + len(district):]
                    break
            else:
                road_number = addr

        # 去掉樓層
        road_number = strip_floor(road_number)
        if not road_number or '號' not in road_number:
            continue

        county_city = CITY_CODE_MAP.get(city_code, '')
        key = (county_city, district, road_number)

        # 如果同一個門牌有多個社區名 (罕見)，取最常出現的
        if key not in mapping:
            mapping[key] = {}
        mapping[key][community] = mapping[key].get(community, 0) + 1

    # 取每個 key 出現次數最多的 community
    result = {}
    for key, comm_counts in mapping.items():
        best = max(comm_counts, key=comm_counts.get)
        result[key] = best

    print(f'  不重複 (county_city, district, road+number) 組合: {len(result):,}', flush=True)
    return result


def backfill(dry_run=False):
    t0 = time.time()
    comm_map = build_community_map()

    print(f'\n連接 land_data.db ...', flush=True)
    land = sqlite3.connect(LAND_DB)
    land.execute('PRAGMA journal_mode=WAL')
    land.execute('PRAGMA synchronous=NORMAL')
    land.execute('PRAGMA cache_size=-200000')  # 200MB cache

    # 確認有 district 索引
    try:
        land.execute('CREATE INDEX IF NOT EXISTS idx_lt_district ON land_transaction(district)')
    except Exception:
        pass

    updated_total = 0
    skipped = 0
    batch_size = 500
    batch_count = 0
    total_keys = len(comm_map)

    print(f'開始回填 community_name ({total_keys:,} 個門牌) ...', flush=True)

    land.execute('BEGIN')

    for i, ((county_city, district, road_number), community) in enumerate(comm_map.items()):
        # 地址模式：land_data.db 的 address 通常是 "台北市中正區汀州路一段76號二十樓"
        # 我們要匹配 district + road_number 的部分
        # 用 LIKE pattern: address LIKE '%{district}{road_number}%'
        pattern = f'%{district}{road_number}%'

        if dry_run:
            count = land.execute(
                "SELECT COUNT(*) FROM land_transaction "
                "WHERE district = ? AND address LIKE ? "
                "AND (community_name IS NULL OR community_name = '')",
                (district, pattern)
            ).fetchone()[0]
            updated_total += count
        else:
            cursor = land.execute(
                "UPDATE land_transaction SET community_name = ? "
                "WHERE district = ? AND address LIKE ? "
                "AND (community_name IS NULL OR community_name = '')",
                (community, district, pattern)
            )
            updated_total += cursor.rowcount

        batch_count += 1
        if batch_count >= batch_size:
            if not dry_run:
                land.execute('COMMIT')
                land.execute('BEGIN')
            batch_count = 0
            elapsed = time.time() - t0
            pct = (i + 1) / total_keys * 100
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total_keys - i - 1) / rate if rate > 0 else 0
            print(f'  [{pct:5.1f}%] {i+1:,}/{total_keys:,} 已更新 {updated_total:,} 筆 '
                  f'({elapsed:.0f}s, ETA {eta:.0f}s)', flush=True)

    if not dry_run:
        land.execute('COMMIT')

    elapsed = time.time() - t0
    print(f'\n{"[DRY RUN] 預計" if dry_run else "已"}更新 {updated_total:,} 筆 community_name', flush=True)
    print(f'耗時 {elapsed:.1f} 秒', flush=True)

    # 統計結果
    stats = land.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN community_name IS NOT NULL AND community_name != '' THEN 1 ELSE 0 END) as has_comm "
        "FROM land_transaction"
    ).fetchone()
    print(f'\nland_data.db 統計:')
    print(f'  總筆數: {stats[0]:,}')
    print(f'  有 community_name: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)')

    land.close()


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    backfill(dry_run=dry_run)
