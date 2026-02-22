#!/usr/bin/env python3
"""
csv_to_sqlite.py
================
將 ALL_lvr_land_a.csv 轉換成 SQLite 資料庫並存到 land_reg/ 資料夾。

用法：
    python3 csv_to_sqlite.py [CSV路徑]  [DB輸出路徑]

預設：
    CSV: ../ALL_lvr_land_a.csv  (相對於此腳本所在目錄)
    DB : ../db/land_a.db
"""

import csv
import sqlite3
import sys
import os
import time

# ── 路徑設定 ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(SCRIPT_DIR, '..', 'db', 'ALL_lvr_land_a.csv')
DEFAULT_DB  = os.path.join(SCRIPT_DIR, '..', 'db', 'land_a.db')

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CSV
DB_PATH  = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB

# ── 欄位對應（去除 BOM、統一命名）─────────────────────────────────────────────
COLUMN_MAP = {
    '\ufeff鄉鎮市區': 'district',
    '鄉鎮市區':       'district',
    '交易標的':       'transaction_type',
    '土地位置建物門牌': 'address',
    '土地移轉總面積平方公尺': 'land_area_sqm',
    '都市土地使用分區': 'urban_zone',
    '非都市土地使用分區': 'non_urban_zone',
    '非都市土地使用編定': 'non_urban_designation',
    '交易年月日':     'transaction_date',
    '交易筆棟數':     'transaction_units',
    '移轉層次':       'floor_level',
    '總樓層數':       'total_floors',
    '建物型態':       'building_type',
    '主要用途':       'main_use',
    '主要建材':       'main_material',
    '建築完成年月':   'completion_date',
    '建物移轉總面積平方公尺': 'building_area_sqm',
    '建物現況格局-房': 'rooms',
    '建物現況格局-廳': 'halls',
    '建物現況格局-衛': 'bathrooms',
    '建物現況格局-隔間': 'partitioned',
    '有無管理組織':   'has_management',
    '總價元':         'total_price',
    '單價元平方公尺': 'unit_price',
    '車位類別':       'parking_type',
    '車位移轉總面積(平方公尺)': 'parking_area_sqm',
    '車位總價元':     'parking_price',
    '備註':           'note',
    '編號':           'serial_no',
    '主建物面積':     'main_building_area',
    '附屬建物面積':   'attached_area',
    '陽台面積':       'balcony_area',
    '電梯':           'elevator',
    '移轉編號':       'transfer_no',
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    district         TEXT,        -- 鄉鎮市區
    transaction_type TEXT,        -- 交易標的
    address          TEXT,        -- 土地位置建物門牌
    land_area_sqm    REAL,        -- 土地移轉總面積平方公尺
    urban_zone       TEXT,        -- 都市土地使用分區
    non_urban_zone   TEXT,        -- 非都市土地使用分區
    non_urban_designation TEXT,   -- 非都市土地使用編定
    transaction_date TEXT,        -- 交易年月日 (民國年YYYYMMDD)
    transaction_units TEXT,       -- 交易筆棟數
    floor_level      TEXT,        -- 移轉層次
    total_floors     TEXT,        -- 總樓層數
    building_type    TEXT,        -- 建物型態
    main_use         TEXT,        -- 主要用途
    main_material    TEXT,        -- 主要建材
    completion_date  TEXT,        -- 建築完成年月
    building_area_sqm REAL,       -- 建物移轉總面積
    rooms            INTEGER,     -- 房
    halls            INTEGER,     -- 廳
    bathrooms        INTEGER,     -- 衛
    partitioned      TEXT,        -- 隔間
    has_management   TEXT,        -- 有無管理組織
    total_price      INTEGER,     -- 總價元
    unit_price       REAL,        -- 單價元/平方公尺
    parking_type     TEXT,        -- 車位類別
    parking_area_sqm REAL,        -- 車位面積
    parking_price    INTEGER,     -- 車位總價元
    note             TEXT,        -- 備註
    serial_no        TEXT,        -- 編號
    main_building_area REAL,      -- 主建物面積
    attached_area    REAL,        -- 附屬建物面積
    balcony_area     REAL,        -- 陽台面積
    elevator         TEXT,        -- 電梯
    transfer_no      TEXT         -- 移轉編號
);
"""

CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_address  ON transactions(address);",
    "CREATE INDEX IF NOT EXISTS idx_district ON transactions(district);",
    "CREATE INDEX IF NOT EXISTS idx_date     ON transactions(transaction_date);",
    "CREATE INDEX IF NOT EXISTS idx_price    ON transactions(total_price);",
    "CREATE INDEX IF NOT EXISTS idx_type     ON transactions(transaction_type);",
]

def safe_real(val):
    try:
        return float(val) if val and val.strip() else None
    except:
        return None

def safe_int(val):
    try:
        return int(float(val)) if val and val.strip() else None
    except:
        return None

def convert():
    csv_path = os.path.abspath(CSV_PATH)
    db_path  = os.path.abspath(DB_PATH)

    if not os.path.exists(csv_path):
        print(f"❌ 找不到 CSV 檔案: {csv_path}")
        sys.exit(1)

    csv_size_mb = os.path.getsize(csv_path) / 1024 / 1024
    print(f"📂 CSV 來源 : {csv_path} ({csv_size_mb:.1f} MB)")
    print(f"💾 SQLite 輸出: {db_path}")

    if os.path.exists(db_path):
        ans = input(f"⚠️  資料庫已存在，要重建嗎？(y/N) ").strip().lower()
        if ans != 'y':
            print("已取消。")
            sys.exit(0)
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")   # 64 MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    t0 = time.time()
    BATCH = 50_000
    total_inserted = 0
    total_skipped  = 0

    with open(csv_path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        # 建立原始欄位名 → 英文欄位名的映射
        raw_cols = reader.fieldnames or []
        col_rename = {}
        for raw in raw_cols:
            key = raw.strip()
            if key in COLUMN_MAP:
                col_rename[raw] = COLUMN_MAP[key]
        print(f"📋 識別到 {len(col_rename)}/{len(raw_cols)} 個欄位")

        insert_sql = """
            INSERT INTO transactions (
                district, transaction_type, address,
                land_area_sqm, urban_zone, non_urban_zone, non_urban_designation,
                transaction_date, transaction_units, floor_level, total_floors,
                building_type, main_use, main_material, completion_date,
                building_area_sqm, rooms, halls, bathrooms, partitioned,
                has_management, total_price, unit_price,
                parking_type, parking_area_sqm, parking_price,
                note, serial_no, main_building_area, attached_area,
                balcony_area, elevator, transfer_no
            ) VALUES (
                ?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,?,
                ?,?,?,  ?,?,?,  ?,?,?,?,  ?,?,?
            )
        """

        batch = []
        for row in reader:
            # 跳過英文表頭列（第2列）
            district = row.get('\ufeff鄉鎮市區') or row.get('鄉鎮市區', '')
            if district.strip() in ('The villages', 'the villages', ''):
                total_skipped += 1
                continue

            def g(key):
                return (row.get(key) or '').strip()

            rec = (
                g('鄉鎮市區') or g('\ufeff鄉鎮市區'),
                g('交易標的'),
                g('土地位置建物門牌'),
                safe_real(g('土地移轉總面積平方公尺')),
                g('都市土地使用分區'),
                g('非都市土地使用分區'),
                g('非都市土地使用編定'),
                g('交易年月日'),
                g('交易筆棟數'),
                g('移轉層次'),
                g('總樓層數'),
                g('建物型態'),
                g('主要用途'),
                g('主要建材'),
                g('建築完成年月'),
                safe_real(g('建物移轉總面積平方公尺')),
                safe_int(g('建物現況格局-房')),
                safe_int(g('建物現況格局-廳')),
                safe_int(g('建物現況格局-衛')),
                g('建物現況格局-隔間'),
                g('有無管理組織'),
                safe_int(g('總價元')),
                safe_real(g('單價元平方公尺')),
                g('車位類別'),
                safe_real(g('車位移轉總面積(平方公尺)')),
                safe_int(g('車位總價元')),
                g('備註'),
                g('編號'),
                safe_real(g('主建物面積')),
                safe_real(g('附屬建物面積')),
                safe_real(g('陽台面積')),
                g('電梯'),
                g('移轉編號'),
            )
            batch.append(rec)

            if len(batch) >= BATCH:
                conn.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
                elapsed = time.time() - t0
                rate = total_inserted / elapsed
                print(f"  已匯入 {total_inserted:,} 筆 | 速率 {rate:,.0f} 筆/秒", end='\r')
                batch.clear()

        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)

    elapsed = time.time() - t0
    print(f"\n✅ 資料匯入完成！共 {total_inserted:,} 筆，耗時 {elapsed:.1f} 秒")
    print(f"   跳過 {total_skipped} 列（空行/表頭）")

    # 建立索引
    print("\n🔍 建立索引中...")
    t1 = time.time()
    for sql in CREATE_INDEX_SQL:
        idx_name = sql.split('idx_')[1].split(' ')[0]
        print(f"  建立 idx_{idx_name}...", end=' ')
        conn.execute(sql)
        conn.commit()
        print("✓")
    print(f"   索引建立完成，耗時 {time.time()-t1:.1f} 秒")

    # 顯示統計
    count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    db_size_mb = os.path.getsize(db_path) / 1024 / 1024
    print(f"\n📊 資料庫統計")
    print(f"   總筆數: {count:,}")
    print(f"   檔案大小: {db_size_mb:.1f} MB")
    print(f"   路徑: {db_path}")

    conn.close()

if __name__ == '__main__':
    convert()
