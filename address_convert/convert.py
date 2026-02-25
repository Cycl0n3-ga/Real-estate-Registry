#!/usr/bin/env python3
"""
台灣不動產實價登錄 CSV → SQLite 轉換腳本 v2

效能優化:
  - 數值欄位使用 INTEGER/REAL 而非 TEXT
  - 建立 FTS5 全文檢索 (地址搜尋 <1秒)
  - 精簡索引策略 (解析後的地址欄位 + 複合索引)
  - page_size=4096, WAL mode
  - 解析後的地址欄位獨立存儲, 加速搜尋

用法: python3 convert.py [--input <csv>] [--output <sqlite>]
"""

import csv
import sqlite3
import os
import sys
import argparse
import time

# ── 共用模組 ──────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from address_utils import (
    normalize_address,
    parse_address,
    chinese_numeral_to_int,
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
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None):
    if val is None or val == '':
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ============================================================
# 主程式
# ============================================================

def create_tables(cursor):
    """建立優化的 SQLite 資料表"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS land_transaction (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            -- 原始欄位 (精簡命名, 數值型態)
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
            -- 解析後地址欄位
            county_city     TEXT,
            district        TEXT,
            village         TEXT,
            street          TEXT,
            lane            TEXT,
            alley           TEXT,
            number          TEXT,
            floor           TEXT,
            sub_number      TEXT,
            -- 預留欄位
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

    # 複合索引: 常用搜尋組合
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


def convert(input_path, output_path):
    """主要轉換流程"""
    print(f'📂 輸入: {input_path}')
    print(f'💾 輸出: {output_path}')

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

    insert_sql = '''INSERT INTO land_transaction (
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
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''

    batch = []
    batch_size = 10000
    total = 0
    parsed_ok = 0
    t0 = time.time()

    with open(input_path, 'r', encoding='utf-8-sig') as f:
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
                None, None, None,                # 社區名, lat, lng
            )
            batch.append(values)

            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                elapsed = time.time() - t0
                rate = total / elapsed if elapsed > 0 else 0
                print(f'\r  ⏳ 已處理 {total:,} 筆 ({rate:,.0f} 筆/秒)', end='', flush=True)
                batch = []

    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()

    print(f'\n  ✅ 資料載入完成: {total:,} 筆')

    # 建立索引和 FTS
    create_indexes(cursor)
    conn.commit()
    create_fts(cursor)
    conn.commit()

    # 關閉 cursor 以便 VACUUM
    cursor.close()

    # ANALYZE 統計
    print('  📊 更新統計資訊...')
    conn.execute('ANALYZE')
    conn.commit()

    # VACUUM 壓縮
    print('  🗜  壓縮資料庫 (VACUUM)...')
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.commit()
    conn.execute('VACUUM')
    conn.execute('PRAGMA journal_mode=WAL')
    conn.commit()

    elapsed = time.time() - t0
    db_size = os.path.getsize(output_path) / 1024 / 1024
    print(f'\n🎉 完成!')
    print(f'  總筆數:        {total:,}')
    print(f'  地址解析成功:  {parsed_ok:,} ({parsed_ok/total*100:.1f}%)')
    print(f'  耗時:          {elapsed:.1f} 秒')
    print(f'  資料庫大小:    {db_size:.1f} MB')

    conn.close()


def main():
    parser = argparse.ArgumentParser(description='台灣實價登錄 CSV → SQLite 轉換 (v2)')
    parser.add_argument('--input', '-i', default=None,
                        help='CSV 輸入路徑 (預設: db/ALL_lvr_land_a.csv)')
    parser.add_argument('--output', '-o', default=None,
                        help='SQLite 輸出路徑 (預設: db/land_data.db)')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)

    input_path = args.input or os.path.join(project_dir, 'db', 'ALL_lvr_land_a.csv')
    output_path = args.output or os.path.join(project_dir, 'db', 'land_data.db')

    if not os.path.exists(input_path):
        print(f'❌ 找不到輸入檔案: {input_path}')
        sys.exit(1)

    convert(input_path, output_path)


if __name__ == '__main__':
    main()
