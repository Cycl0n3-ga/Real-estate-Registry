#!/usr/bin/env python3
"""
batch_geocode.py - 批次地理編碼 land_a.db 所有地址
====================================================

將 land_a.db 中的交易地址批次轉換為經緯度座標，
並可選擇將結果寫回資料庫或匯出 CSV。

用法:
    # 查看目前進度
    python3 batch_geocode.py --status

    # 路段級批次處理（最快，推薦第一步）
    python3 batch_geocode.py --strategy road

    # 限制處理數量（測試用）
    python3 batch_geocode.py --strategy road --limit 1000

    # 處理特定區域
    python3 batch_geocode.py --strategy road --district 松山區

    # 精確地址級處理（較慢但精確）
    python3 batch_geocode.py --strategy exact --limit 5000

    # 將結果寫回 land_a.db（新增 lat/lng 欄位）
    python3 batch_geocode.py --write-back

    # 匯出已 geocode 的結果為 CSV
    python3 batch_geocode.py --export geocoded_addresses.csv

    # 匯入既有的 JSON 快取
    python3 batch_geocode.py --import-cache ../../geocode_cache.json

    # 使用本地 Nominatim（速度飛升）
    python3 batch_geocode.py --strategy road --nominatim-url http://localhost:8080/search

環境需求:
    pip install tqdm  (選用，顯示進度條)
"""

import sqlite3
import os
import sys
import json
import csv
import time
import argparse
import logging
from pathlib import Path
from collections import defaultdict

# 加入模組路徑
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from geocoder import (
    TaiwanGeocoder, GeoCache, AddressNormalizer,
    DISTRICT_TO_CITY
)

logger = logging.getLogger(__name__)

# land_a.db 路徑
DEFAULT_DB = os.path.join(SCRIPT_DIR, '..', '..', 'db', 'land_a.db')


class LandDBProcessor:
    """
    land_a.db 批次地理編碼處理器

    工作流程:
    1. 從 land_a.db 讀取不同地址
    2. 正規化 + 快取查詢
    3. API 查詢未快取的地址
    4. 儲存結果到快取
    5. （可選）寫回 land_a.db
    """

    def __init__(self, db_path: str = None, geocoder: TaiwanGeocoder = None):
        self.db_path = db_path or DEFAULT_DB
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"找不到資料庫: {self.db_path}")

        self.geocoder = geocoder or TaiwanGeocoder()
        self.normalizer = AddressNormalizer()

    def get_status(self) -> dict:
        """取得目前狀態統計"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()

        stats = {}

        # 總筆數
        stats['total_rows'] = cur.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]

        # 有效地址數
        stats['valid_addresses'] = cur.execute("""
            SELECT COUNT(DISTINCT address) FROM transactions
            WHERE address IS NOT NULL AND address != ''
              AND (address LIKE '%路%' OR address LIKE '%街%' OR address LIKE '%大道%')
              AND address LIKE '%號%'
              AND address NOT LIKE '%地號%'
        """).fetchone()[0]

        # 檢查是否已有 lat/lng 欄位
        cols = [row[1] for row in cur.execute("PRAGMA table_info(transactions)").fetchall()]
        stats['has_geocode_columns'] = 'lat' in cols and 'lng' in cols

        if stats['has_geocode_columns']:
            stats['geocoded_rows'] = cur.execute(
                "SELECT COUNT(*) FROM transactions WHERE lat IS NOT NULL AND lng IS NOT NULL"
            ).fetchone()[0]
        else:
            stats['geocoded_rows'] = 0

        # 區域分布
        stats['districts'] = dict(cur.execute("""
            SELECT district, COUNT(DISTINCT address)
            FROM transactions
            WHERE address IS NOT NULL AND address LIKE '%號%'
              AND address NOT LIKE '%地號%'
            GROUP BY district
            ORDER BY COUNT(DISTINCT address) DESC
            LIMIT 20
        """).fetchall())

        # 快取統計
        stats['cache'] = self.geocoder.stats()

        con.close()
        return stats

    def get_unique_addresses(self, district: str = None,
                              limit: int = None) -> list:
        """
        取得不同的有效地址列表

        Returns: [(district, address), ...]
        """
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()

        query = """
            SELECT DISTINCT district, address
            FROM transactions
            WHERE address IS NOT NULL AND address != ''
              AND (address LIKE '%路%' OR address LIKE '%街%' OR address LIKE '%大道%')
              AND address LIKE '%號%'
              AND address NOT LIKE '%地號%'
        """
        params = []

        if district:
            query += " AND district = ?"
            params.append(district)

        query += " ORDER BY district, address"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        results = cur.execute(query, params).fetchall()
        con.close()

        return [(row[0], row[1]) for row in results]

    def get_unique_roads(self, district: str = None) -> list:
        """
        取得不同路段列表

        Returns: [(district, road_name, address_count), ...]
        """
        addresses = self.get_unique_addresses(district)
        road_counts = defaultdict(lambda: {'count': 0, 'district': ''})

        for dist, addr in addresses:
            full = self.normalizer.build_full_address(addr, dist)
            if not full:
                continue
            road = self.normalizer.extract_road(full)
            if road:
                city_prefix = ''
                if dist in DISTRICT_TO_CITY:
                    city_prefix = DISTRICT_TO_CITY[dist] + dist
                elif dist:
                    city_prefix = dist
                road_key = f"{city_prefix}{road}"
                road_counts[road_key]['count'] += 1
                road_counts[road_key]['district'] = dist

        result = [
            (v['district'], road_key, v['count'])
            for road_key, v in road_counts.items()
        ]
        result.sort(key=lambda x: -x[2])
        return result

    def add_geocode_columns(self):
        """在 land_a.db 新增 lat/lng/geocode_level 欄位"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()

        cols = [row[1] for row in cur.execute("PRAGMA table_info(transactions)").fetchall()]

        added = []
        if 'lat' not in cols:
            cur.execute("ALTER TABLE transactions ADD COLUMN lat REAL")
            added.append('lat')
        if 'lng' not in cols:
            cur.execute("ALTER TABLE transactions ADD COLUMN lng REAL")
            added.append('lng')
        if 'geocode_level' not in cols:
            cur.execute("ALTER TABLE transactions ADD COLUMN geocode_level TEXT")
            added.append('geocode_level')

        if added:
            # 建立索引加速查詢
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_latlng
                ON transactions(lat, lng)
                WHERE lat IS NOT NULL
            """)
            con.commit()
            print(f"✅ 新增欄位: {', '.join(added)}")
        else:
            print(f"ℹ️  欄位已存在，無需新增")

        con.close()

    def write_back(self, progress: bool = True):
        """
        將快取的 geocode 結果寫回 land_a.db

        策略：
        1. 用正規化後的基本地址比對快取
        2. 比對不到的用路段級座標
        """
        # 確保欄位存在
        self.add_geocode_columns()

        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL")  # 加速寫入
        cur = con.cursor()

        # 讀取所有需要 geocode 的 row
        rows = cur.execute("""
            SELECT id, district, address FROM transactions
            WHERE address IS NOT NULL AND address != ''
              AND (address LIKE '%路%' OR address LIKE '%街%' OR address LIKE '%大道%')
              AND address LIKE '%號%'
              AND address NOT LIKE '%地號%'
              AND lat IS NULL
        """).fetchall()

        total = len(rows)
        if total == 0:
            print("ℹ️  所有有效地址已完成 geocode")
            con.close()
            return

        if progress:
            print(f"\n📝 寫回 land_a.db ({total:,} 筆待處理)")

        # 先收集所有需要查詢的 key
        lookup_keys = set()
        row_to_keys = {}

        for row_id, district, address in rows:
            full_addr = self.normalizer.build_full_address(address, district)
            if not full_addr:
                continue
            base_addr = self.normalizer.extract_base_address(full_addr)
            if not base_addr:
                base_addr = full_addr

            # 路段 key
            road = self.normalizer.extract_road(full_addr)
            road_key = None
            if road:
                city_prefix = ''
                if district in DISTRICT_TO_CITY:
                    city_prefix = DISTRICT_TO_CITY[district] + district
                elif district:
                    city_prefix = district
                road_key = f"{city_prefix}{road}"
                lookup_keys.add(road_key)

            lookup_keys.add(base_addr)
            row_to_keys[row_id] = (base_addr, road_key)

        # 批次查快取
        if progress:
            print(f"   查詢快取 ({len(lookup_keys):,} 個 key)...")

        all_cached = self.geocoder.cache.get_batch(list(lookup_keys))

        # 批次更新
        updates = []
        matched = 0

        for row_id, (base_addr, road_key) in row_to_keys.items():
            lat = lng = level = None

            # 精確匹配
            if base_addr in all_cached:
                c = all_cached[base_addr]
                lat, lng, level = c['lat'], c['lng'], c.get('level', 'exact')
            # 路段匹配
            elif road_key and road_key in all_cached:
                c = all_cached[road_key]
                lat, lng, level = c['lat'], c['lng'], 'road'

            if lat is not None:
                updates.append((lat, lng, level, row_id))
                matched += 1

        if updates:
            if progress:
                print(f"   更新 {len(updates):,} 筆...")

            # 分批更新避免記憶體爆炸
            batch_size = 10000
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i+batch_size]
                cur.executemany(
                    "UPDATE transactions SET lat=?, lng=?, geocode_level=? WHERE id=?",
                    batch
                )
                con.commit()
                if progress:
                    done = min(i + batch_size, len(updates))
                    print(f"   已寫入: {done:,}/{len(updates):,}")

        if progress:
            print(f"\n✅ 寫回完成: {matched:,}/{total:,} 筆已更新")

        con.close()

    def upgrade_road_to_exact(self, progress: bool = True, dry_run: bool = False):
        """
        將已寫入的路段級座標（road）升級為精確門牌級（exact）

        使用 OSM 本地索引重新查詢，只更新能找到精確座標的記錄。
        適用於建立 OSM 索引後，重新刷新舊的路段級結果。
        """
        if not self.geocoder.osm_index.is_available():
            print("❌ OSM 索引尚未建立，請先執行 build_osm_index.py")
            return

        self.add_geocode_columns()

        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL")
        cur = con.cursor()

        # 取得所有「路段級」座標的記錄
        rows = cur.execute("""
            SELECT id, district, address FROM transactions
            WHERE geocode_level = 'road'
              AND address IS NOT NULL
              AND address LIKE '%號%'
              AND address NOT LIKE '%地號%'
        """).fetchall()

        total = len(rows)
        if total == 0:
            print("ℹ️  沒有路段級記錄需要升級")
            con.close()
            return

        if progress:
            print(f"\n🔄 升級路段級座標 → 精確門牌 ({total:,} 筆)")
            if dry_run:
                print("   [試跑模式，不實際寫入]")

        # 收集所有地址做批次查詢
        base_addrs = {}
        for row_id, district, address in rows:
            full_addr = self.normalizer.build_full_address(address, district)
            if not full_addr:
                continue
            base_addr = self.normalizer.extract_base_address(full_addr)
            if not base_addr:
                base_addr = full_addr
            base_addrs[row_id] = base_addr

        # 批次 OSM 查詢
        unique_addrs = list(set(base_addrs.values()))
        if progress:
            print(f"   不同地址: {len(unique_addrs):,}")

        batch_size = 5000
        all_osm_results = {}
        for i in range(0, len(unique_addrs), batch_size):
            batch = unique_addrs[i:i+batch_size]
            results = self.geocoder.osm_index.batch_geocode(batch)
            all_osm_results.update(results)
            if progress:
                print(f"   查詢進度: {min(i+batch_size, len(unique_addrs)):,}/{len(unique_addrs):,} | 命中: {len(all_osm_results):,}")

        # 組合更新
        updates = []
        for row_id, base_addr in base_addrs.items():
            if base_addr in all_osm_results:
                r = all_osm_results[base_addr]
                updates.append((r['lat'], r['lng'], 'exact', row_id))

        if progress:
            hit_rate = len(updates) / max(len(rows), 1) * 100
            print(f"   精確命中: {len(updates):,}/{total:,} ({hit_rate:.1f}%)")

        if not dry_run and updates:
            for i in range(0, len(updates), 10000):
                batch = updates[i:i+10000]
                cur.executemany(
                    "UPDATE transactions SET lat=?, lng=?, geocode_level=? WHERE id=?",
                    batch
                )
                con.commit()
            print(f"✅ 升級完成：{len(updates):,} 筆已更新為精確門牌座標")
        elif dry_run:
            print(f"[試跑] 預計升級：{len(updates):,}/{total:,} 筆")

        con.close()

    def export_csv(self, output_path: str, limit: int = None):
        """匯出 geocode 結果為 CSV"""
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()

        # 從快取匯出
        cache_db = self.geocoder.cache.db_path
        cache_con = sqlite3.connect(cache_db)

        query = """
            SELECT address_key, lat, lng, level, source, created_at
            FROM geocode_cache
            ORDER BY created_at DESC
        """
        if limit:
            query += f" LIMIT {limit}"

        rows = cache_con.execute(query).fetchall()
        cache_con.close()
        con.close()

        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['address', 'lat', 'lng', 'level', 'source', 'created_at'])
            writer.writerows(rows)

        print(f"✅ 匯出 {len(rows):,} 筆到 {output_path}")


def print_status(processor: LandDBProcessor):
    """印出狀態報告"""
    stats = processor.get_status()

    print("=" * 60)
    print("📊 land_a.db 地理編碼狀態")
    print("=" * 60)

    print(f"\n📋 資料庫:")
    print(f"   總交易筆數:       {stats['total_rows']:>12,}")
    print(f"   有效門牌地址數:    {stats['valid_addresses']:>12,}")

    if stats['has_geocode_columns']:
        pct = stats['geocoded_rows'] / max(stats['valid_addresses'], 1) * 100
        print(f"   已 geocode 筆數:  {stats['geocoded_rows']:>12,} ({pct:.1f}%)")
    else:
        print(f"   lat/lng 欄位:     ❌ 尚未建立")

    cache = stats['cache']
    print(f"\n💾 快取:")
    print(f"   快取總數:          {cache['total']:>12,}")
    if cache.get('by_level'):
        for level, count in cache['by_level'].items():
            print(f"     {level:15s}  {count:>10,}")
    if cache.get('by_source'):
        print(f"   來源分布:")
        for source, count in cache['by_source'].items():
            print(f"     {source:15s}  {count:>10,}")

    # OSM 索引狀態
    from geocoder import OSMIndexProvider
    osm = processor.geocoder.osm_index if hasattr(processor.geocoder, 'osm_index') else OSMIndexProvider()
    print(f"\n🏠 OSM 門牌索引（精確查詢）:")
    if osm.is_available():
        print(f"   狀態:              ✅ 可用 ({osm.node_count:,} 個節點)")
        print(f"   精度:              門牌級 (±10-50m)")
        print(f"   執行 build_osm_index.py --status 查看各縣市下載進度")
    else:
        print(f"   狀態:              ❌ 未建立（使用路段級精度）")
        print(f"   啟用方法：")
        print(f"     python3 build_osm_index.py  # 下載全台約 900 萬筆門牌資料")
        print(f"     預計耗時：15-25 分鐘，空間需求：約 1.5-2 GB")

    print(f"\n🗺️  前 10 大區域:")
    for district, count in list(stats['districts'].items())[:10]:
        print(f"   {district:12s}  {count:>10,} 不同地址")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='批次地理編碼 land_a.db 所有地址',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s --status                              # 查看進度
  %(prog)s --strategy road                       # 路段級批次處理（最快）
  %(prog)s --strategy road --limit 1000          # 先測試 1000 筆
  %(prog)s --strategy road --district 松山區      # 處理特定區域
  %(prog)s --write-back                          # 寫回 land_a.db
  %(prog)s --upgrade                             # 將路段級升級為門牌級（需 OSM 索引）
  %(prog)s --export result.csv                   # 匯出 CSV
  %(prog)s --import-cache ../../geocode_cache.json  # 匯入舊快取
  %(prog)s --strategy road --nominatim-url http://localhost:8080/search  # 本地 Nominatim
        """
    )

    parser.add_argument('--status', action='store_true', help='顯示目前進度')
    parser.add_argument('--strategy', choices=['smart', 'road', 'exact'],
                        default='smart', help='geocoding 策略 (預設: smart)')
    parser.add_argument('--district', '-d', help='只處理指定區域')
    parser.add_argument('--limit', '-n', type=int, help='限制處理筆數')
    parser.add_argument('--provider', choices=['nominatim', 'nlsc'],
                        default='nominatim', help='API provider')
    parser.add_argument('--nominatim-url', help='本地 Nominatim URL')
    parser.add_argument('--write-back', action='store_true',
                        help='將結果寫回 land_a.db')
    parser.add_argument('--upgrade', action='store_true',
                        help='將 road 級座標升級為門牌級（需先建立 OSM 索引）')
    parser.add_argument('--dry-run', action='store_true',
                        help='[--upgrade 配合] 試跑模式，不實際寫入')
    parser.add_argument('--export', metavar='CSV', help='匯出結果為 CSV')
    parser.add_argument('--import-cache', metavar='JSON', help='匯入 JSON 快取')
    parser.add_argument('--db', default=DEFAULT_DB, help='land_a.db 路徑')
    parser.add_argument('--verbose', '-v', action='store_true')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING)

    # 建立 geocoder
    gc = TaiwanGeocoder(
        provider=args.provider,
        nominatim_url=args.nominatim_url,
    )
    if gc.osm_index.is_available():
        print(f"🏠 OSM 門牌索引：{gc.osm_index.node_count:,} 個節點（精確門牌模式）")
    else:
        print(f"⚠️  OSM 索引未載入，使用路段級精度。建議先執行 build_osm_index.py")

    processor = LandDBProcessor(db_path=args.db, geocoder=gc)

    # ── 匯入快取 ──
    if args.import_cache:
        count = gc.cache.import_json_cache(args.import_cache)
        print(f"✅ 匯入 {count:,} 筆快取")
        return

    # ── 顯示狀態 ──
    if args.status:
        print_status(processor)
        return

    # ── 匯出 CSV ──
    if args.export:
        processor.export_csv(args.export, limit=args.limit)
        return

    # ── 寫回 land_a.db ──
    if args.write_back:
        processor.write_back()
        return

    # ── 升級 road → exact ──
    if args.upgrade:
        processor.upgrade_road_to_exact(dry_run=args.dry_run)
        return

    # ── 批次 geocode ──
    print("=" * 60)
    print("🌐 批次地理編碼")
    print("=" * 60)

    start_time = time.time()

    # 取得地址列表
    print(f"\n📖 讀取 land_a.db...")
    addresses = processor.get_unique_addresses(
        district=args.district,
        limit=args.limit
    )
    print(f"   取得 {len(addresses):,} 筆不同地址")

    if not addresses:
        print("⚠️  沒有找到需要處理的地址")
        return

    # 執行批次 geocode
    results = gc.batch_geocode(
        addresses,
        strategy=args.strategy,
        progress=True
    )

    elapsed = time.time() - start_time

    # ── 結果報告 ──
    print(f"\n{'='*60}")
    print(f"🎉 批次處理完成！")
    print(f"   耗時: {elapsed:.1f} 秒")
    print(f"   成功: {len(results):,} / {len(addresses):,}")
    print(f"   成功率: {len(results)/max(len(addresses),1)*100:.1f}%")

    # 統計精度分布
    levels = defaultdict(int)
    for r in results.values():
        levels[r.get('level', 'unknown')] += 1
    print(f"   精度分布: {dict(levels)}")

    print(f"\n💾 快取統計:")
    cache_stats = gc.stats()
    print(f"   快取總數: {cache_stats['total']:,}")

    print(f"\n💡 下一步:")
    print(f"   python3 batch_geocode.py --write-back   # 寫回 land_a.db")
    print(f"   python3 batch_geocode.py --export out.csv  # 匯出 CSV")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
