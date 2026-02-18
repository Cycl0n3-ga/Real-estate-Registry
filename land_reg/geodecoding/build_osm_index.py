#!/usr/bin/env python3
"""
build_osm_index.py - 從 Overpass API 批次下載台灣全境門牌坐標
================================================================

從 OpenStreetMap Overpass API 下載台灣全境的 addr:housenumber 地址節點，
建立本地 SQLite 索引，供 geocoder.py 的 OSMIndexProvider 使用。

資料規模（2024）：
  台灣全境 addr:housenumber 節點：約 909 萬筆
  SQLite 索引大小：約 600-900 MB
  下載時間：約 15-25 分鐘（按縣市分批）

使用方式:
    python3 build_osm_index.py                    # 下載所有縣市
    python3 build_osm_index.py --cities 臺北市,新北市  # 只下載指定縣市
    python3 build_osm_index.py --status            # 查看下載進度
    python3 build_osm_index.py --resume            # 繼續未完成的下載
    python3 build_osm_index.py --test              # 測試查詢功能
"""

import sqlite3
import urllib.request
import urllib.parse
import csv
import io
import time
import sys
import os
import logging
import argparse
import re
from pathlib import Path
from typing import Optional, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# =====================================================================
# 台灣 22 縣市清單（Overpass area 查詢用名稱）
# =====================================================================
TAIWAN_CITIES = [
    # 直轄市（節點較多，排前面）
    ("臺北市",   "臺北市",   "4"),
    ("新北市",   "新北市",   "4"),
    ("桃園市",   "桃園市",   "4"),
    ("臺中市",   "臺中市",   "4"),
    ("臺南市",   "臺南市",   "4"),
    ("高雄市",   "高雄市",   "4"),
    # 省轄市
    ("基隆市",   "基隆市",   "4"),
    ("新竹市",   "新竹市",   "4"),
    ("嘉義市",   "嘉義市",   "4"),
    # 縣
    ("新竹縣",   "新竹縣",   "4"),
    ("苗栗縣",   "苗栗縣",   "4"),
    ("彰化縣",   "彰化縣",   "4"),
    ("南投縣",   "南投縣",   "4"),
    ("雲林縣",   "雲林縣",   "4"),
    ("嘉義縣",   "嘉義縣",   "4"),
    ("屏東縣",   "屏東縣",   "4"),
    ("宜蘭縣",   "宜蘭縣",   "4"),
    ("花蓮縣",   "花蓮縣",   "4"),
    ("臺東縣",   "臺東縣",   "4"),
    ("澎湖縣",   "澎湖縣",   "4"),
    ("金門縣",   "金門縣",   "4"),
    ("連江縣",   "連江縣",   "4"),
]

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DEFAULT_DB = Path(__file__).parent.parent.parent / "db" / "osm_addresses.db"

# 全形→半形數字
_FW2HW = str.maketrans('０１２３４５６７８９', '0123456789')


def normalize_housenumber(num: str) -> str:
    """正規化門牌號碼：全形→半形，去除「號」字，統一 126-5 → 126之5"""
    if not num:
        return ''
    num = num.translate(_FW2HW).strip()
    num = num.rstrip('號')
    # 統一連字號格式：「126-5」→「126之5」（台灣門牌號 X之Y 的 OSM 常見寫法）
    num = re.sub(r'^(\d+)-(\d+)$', r'\1之\2', num)
    return num


def normalize_city(city: str) -> str:
    """統一台/臺"""
    return city.replace('台北市', '臺北市').replace('台中市', '臺中市') \
               .replace('台南市', '臺南市').replace('台東', '臺東') \
               .replace('台灣', '臺灣')


# =====================================================================
# OSM 地址索引資料庫
# =====================================================================
class OSMAddressDB:
    """
    SQLite 本地門牌座標索引

    表結構:
        osm_addresses(
            id INTEGER PRIMARY KEY,
            city TEXT,         -- 縣市 (addr:city)
            district TEXT,     -- 鄉鎮市區 (addr:district)
            street TEXT,       -- 路段 (addr:street)
            housenumber TEXT,  -- 門牌號 (addr:housenumber，不含「號」)
            lat REAL,          -- 緯度
            lng REAL           -- 經度
        )
        
        download_progress(
            city TEXT PRIMARY KEY,
            status TEXT,       -- pending/done/error
            node_count INTEGER,
            downloaded_at TIMESTAMP
        )
    """

    def __init__(self, db_path: str = str(DEFAULT_DB)):
        self.db_path = str(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS osm_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                district TEXT DEFAULT '',
                street TEXT NOT NULL,
                housenumber TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS download_progress (
                city TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                node_count INTEGER DEFAULT 0,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 只在全量下載完成後才建索引（插入前不建索引，速度更快）
        con.commit()
        con.close()

    def create_indexes(self):
        """建立查詢索引（下載完成後執行）"""
        logger.info("建立查詢索引中...")
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL")
        # 主查詢索引：(street, housenumber)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_osm_street_num
            ON osm_addresses(street, housenumber)
        """)
        # 包含 district 的索引用於消歧義
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_osm_district_street
            ON osm_addresses(district, street, housenumber)
        """)
        con.commit()
        con.close()
        logger.info("索引建立完成")

    def insert_batch(self, records: List[Tuple]):
        """批次寫入地址節點"""
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.executemany(
            "INSERT INTO osm_addresses (city, district, street, housenumber, lat, lng) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            records
        )
        con.commit()
        con.close()

    def mark_city_done(self, city: str, node_count: int):
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT OR REPLACE INTO download_progress (city, status, node_count) VALUES (?, 'done', ?)",
            (city, node_count)
        )
        con.commit()
        con.close()

    def mark_city_pending(self, city: str):
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT OR REPLACE INTO download_progress (city, status, node_count) VALUES (?, 'pending', 0)",
            (city,)
        )
        con.commit()
        con.close()

    def get_done_cities(self) -> set:
        """取得已下載完成的縣市"""
        con = sqlite3.connect(self.db_path)
        cur = con.execute("SELECT city FROM download_progress WHERE status='done'")
        cities = {row[0] for row in cur}
        con.close()
        return cities

    def get_status(self) -> dict:
        """取得所有縣市下載狀態"""
        con = sqlite3.connect(self.db_path)
        cur = con.execute("SELECT city, status, node_count, downloaded_at FROM download_progress ORDER BY city")
        rows = {row[0]: {'status': row[1], 'count': row[2], 'at': row[3]} for row in cur}
        total = con.execute("SELECT COUNT(*) FROM osm_addresses").fetchone()[0]
        con.close()
        return {'cities': rows, 'total_nodes': total}

    def query(self, street: str, housenumber: str, district: str = '') -> Optional[Tuple]:
        """
        查詢地址座標

        Returns: (lat, lng) 或 None
        """
        con = sqlite3.connect(self.db_path)
        # 先嘗試含 district 的精確查詢
        if district:
            cur = con.execute(
                "SELECT lat, lng FROM osm_addresses "
                "WHERE district=? AND street=? AND housenumber=? LIMIT 1",
                (district, street, housenumber)
            )
            row = cur.fetchone()
            if row:
                con.close()
                return row
        # 再試不含 district（跨縣市同名路段）
        cur = con.execute(
            "SELECT lat, lng FROM osm_addresses "
            "WHERE street=? AND housenumber=? LIMIT 1",
            (street, housenumber)
        )
        row = cur.fetchone()
        con.close()
        return row

    def delete_city(self, city: str):
        """刪除某縣市的資料（重新下載用）"""
        con = sqlite3.connect(self.db_path)
        con.execute("DELETE FROM osm_addresses WHERE city=?", (city,))
        con.execute("DELETE FROM download_progress WHERE city=?", (city,))
        con.commit()
        con.close()


# =====================================================================
# Overpass API 下載器
# =====================================================================
class OverpassDownloader:
    def __init__(self, db: OSMAddressDB):
        self.db = db

    def _build_query(self, city_name: str, admin_level: str) -> str:
        return f"""
[out:csv(::lat, ::lon, "addr:city", "addr:district", "addr:street", "addr:housenumber"; true; ",")][timeout:300];
area["name"="{city_name}"]["admin_level"="{admin_level}"]->.city;
(
  node["addr:housenumber"]["addr:street"](area.city);
);
out;
"""

    def download_city(self, city_key: str, city_name: str, admin_level: str,
                      max_retries: int = 3) -> int:
        """下載一個縣市的所有門牌節點，回傳節點數"""
        query = self._build_query(city_name, admin_level)
        data_bytes = urllib.parse.urlencode({"data": query}).encode()

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"  下載 {city_key}（第 {attempt} 次）...")
                req = urllib.request.Request(
                    OVERPASS_URL, data=data_bytes,
                    headers={"User-Agent": "TaiwanLandGeocoder/2.0 (build_osm_index)"}
                )
                t0 = time.time()
                with urllib.request.urlopen(req, timeout=360) as r:
                    raw = r.read()
                elapsed = time.time() - t0

                # 解析 CSV
                records = self._parse_csv(raw, city_key)
                logger.info(f"  {city_key}: {len(records):,} 節點，{len(raw)/1024/1024:.1f}MB，{elapsed:.0f}s")

                # 寫入 DB
                if records:
                    self.db.insert_batch(records)
                self.db.mark_city_done(city_key, len(records))
                return len(records)

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = 60 * attempt
                    logger.warning(f"  速率限制，等待 {wait}s 後重試...")
                    time.sleep(wait)
                elif e.code == 504:
                    wait = 30 * attempt
                    logger.warning(f"  Gateway Timeout，等待 {wait}s 後重試...")
                    time.sleep(wait)
                else:
                    logger.error(f"  HTTP {e.code}: {e}")
                    if attempt >= max_retries:
                        raise

            except Exception as e:
                logger.error(f"  下載失敗: {e}")
                if attempt >= max_retries:
                    raise
                time.sleep(30 * attempt)

        return 0

    def _parse_csv(self, raw: bytes, city_key: str) -> List[Tuple]:
        """
        解析 Overpass CSV 輸出

        CSV 格式: @lat,@lon,addr:city,addr:district,addr:street,addr:housenumber
        """
        records = []
        try:
            text = raw.decode('utf-8')
            reader = csv.reader(io.StringIO(text))
            next(reader, None)  # 跳過標頭行

            for row in reader:
                if len(row) < 6:
                    continue
                try:
                    lat = float(row[0])
                    lng = float(row[1])
                    city = normalize_city(row[2].strip()) or city_key
                    district = row[3].strip()
                    street = row[4].strip()
                    housenumber = normalize_housenumber(row[5].strip())

                    # 基本驗證
                    if not street or not housenumber:
                        continue
                    if not (20 < lat < 27 and 118 < lng < 123):
                        continue  # 台灣範圍外

                    records.append((city, district, street, housenumber, lat, lng))
                except (ValueError, IndexError):
                    continue
        except Exception as e:
            logger.error(f"CSV 解析錯誤: {e}")

        return records


# =====================================================================
# 主程式
# =====================================================================
def cmd_download(args, db: OSMAddressDB):
    """執行下載"""
    downloader = OverpassDownloader(db)
    done_cities = db.get_done_cities()

    # 決定要下載哪些縣市
    if args.cities:
        target_cities = [c.strip() for c in args.cities.split(',')]
        city_list = [(k, n, l) for k, n, l in TAIWAN_CITIES if k in target_cities]
    else:
        city_list = TAIWAN_CITIES

    # 過濾已完成（除非 --force）
    if not getattr(args, 'force', False):
        pending = [(k, n, l) for k, n, l in city_list if k not in done_cities]
    else:
        pending = city_list
        for k, _, _ in city_list:
            db.delete_city(k)

    if not pending:
        logger.info("所有縣市已下載完成！使用 --force 重新下載。")
        return

    logger.info(f"準備下載 {len(pending)} 個縣市...")
    total_nodes = 0
    t_start = time.time()

    for i, (city_key, city_name, admin_level) in enumerate(pending, 1):
        logger.info(f"[{i}/{len(pending)}] 開始下載 {city_key}...")
        try:
            n = downloader.download_city(city_key, city_name, admin_level)
            total_nodes += n
            # Overpass 禮貌等待（避免 429）
            if i < len(pending):
                time.sleep(10)
        except Exception as e:
            logger.error(f"  {city_key} 下載失敗: {e}，跳過繼續...")
            time.sleep(30)

    elapsed = time.time() - t_start
    logger.info(f"\n下載完成！總計 {total_nodes:,} 個節點，耗時 {elapsed/60:.1f} 分鐘")

    # 下載完成後建立索引
    logger.info("建立查詢索引...")
    db.create_indexes()

    # 顯示最終統計
    status = db.get_status()
    logger.info(f"資料庫總計：{status['total_nodes']:,} 個地址節點")


def cmd_status(db: OSMAddressDB):
    """顯示下載進度"""
    status = db.get_status()
    print(f"\n{'縣市':<8} {'狀態':<8} {'節點數':>12}  下載時間")
    print("-" * 50)

    all_cities = {k: (n, l) for k, n, l in TAIWAN_CITIES}
    done_set = set()

    for city, info in status['cities'].items():
        icon = "✓" if info['status'] == 'done' else "✗"
        print(f"  {icon} {city:<8} {info['status']:<6} {info['count']:>12,}  {info['at'] or ''}")
        if info['status'] == 'done':
            done_set.add(city)

    pending = [k for k, _, _ in TAIWAN_CITIES if k not in done_set]
    if pending:
        print(f"\n未下載：{', '.join(pending)}")

    print(f"\n資料庫總計：{status['total_nodes']:,} 個節點")
    db_size = os.path.getsize(db.db_path) / 1024 / 1024
    print(f"DB 檔案大小：{db_size:.1f} MB")


def cmd_test(db: OSMAddressDB):
    """測試查詢功能"""
    test_cases = [
        ("大安區", "和平東路三段", "168",  "臺北市大安區和平東路三段168號"),
        ("大安區", "和平東路三段", "410",  "臺北市大安區和平東路三段410號（捷運站）"),
        ("大安區", "復興南路二段", "235",  "臺北市大安區復興南路二段235號"),
        ("中正區", "重慶南路一段", "122",  "臺北市中正區重慶南路一段122號"),
        ("信義區", "松仁路", "100",        "臺北市信義區松仁路100號"),
        ("板橋區", "文化路一段", "100",    "新北市板橋區文化路一段100號"),
    ]

    print("\n查詢測試:")
    print(f"{'地址':<30} {'結果':<20} {'座標'}")
    print("-" * 70)

    found = 0
    for district, street, num, label in test_cases:
        result = db.query(street, num, district)
        if result:
            lat, lng = result
            print(f"  {label:<30}  ✓ 精確門牌  ({lat:.6f}, {lng:.6f})")
            found += 1
        else:
            print(f"  {label:<30}  ✗ 未找到")

    print(f"\n找到 {found}/{len(test_cases)} 筆")

    # 資料庫統計
    con = sqlite3.connect(db.db_path)
    total = con.execute("SELECT COUNT(*) FROM osm_addresses").fetchone()[0]
    cities = con.execute("SELECT city, COUNT(*) FROM osm_addresses GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5").fetchall()
    con.close()
    print(f"\n資料庫統計：")
    print(f"  總節點數：{total:,}")
    print(f"  各縣市節點數 (前5)：")
    for c, n in cities:
        print(f"    {c}: {n:,}")


def cmd_index(db: OSMAddressDB):
    """（重新）建立查詢索引"""
    db.create_indexes()


def main():
    parser = argparse.ArgumentParser(
        description="建立 OSM 台灣門牌座標本地索引",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  python3 build_osm_index.py                     # 下載所有縣市
  python3 build_osm_index.py --cities 臺北市,新北市  # 只下載指定縣市
  python3 build_osm_index.py --status             # 查看進度
  python3 build_osm_index.py --resume             # 繼續下載未完成的縣市
  python3 build_osm_index.py --force --cities 臺北市  # 強制重新下載
  python3 build_osm_index.py --test               # 測試查詢
  python3 build_osm_index.py --index              # 重建查詢索引
        """
    )
    parser.add_argument('--db', default=str(DEFAULT_DB), help=f'SQLite 資料庫路徑（預設：{DEFAULT_DB}）')
    parser.add_argument('--cities', help='指定下載的縣市，用逗號分隔（如：臺北市,新北市）')
    parser.add_argument('--status', action='store_true', help='顯示下載進度')
    parser.add_argument('--resume', action='store_true', help='繼續未完成的下載（預設行為）')
    parser.add_argument('--force', action='store_true', help='強制重新下載（覆蓋既有資料）')
    parser.add_argument('--test', action='store_true', help='測試查詢功能')
    parser.add_argument('--index', action='store_true', help='（重新）建立查詢索引')
    args = parser.parse_args()

    db = OSMAddressDB(args.db)
    logger.info(f"使用資料庫：{db.db_path}")

    if args.status:
        cmd_status(db)
    elif args.test:
        cmd_test(db)
    elif args.index:
        cmd_index(db)
    else:
        # 預設：下載（支援 --cities, --force, --resume）
        cmd_download(args, db)


if __name__ == '__main__':
    main()
