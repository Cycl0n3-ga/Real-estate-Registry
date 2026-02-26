#!/usr/bin/env python3
"""
address2community.py - 地址→社區/建案名稱 查詢工具 (SQLite + 591 API 版)

資料來源：
  1. land_data.db   - 內政部實價登錄交易資料庫（已匹配社區名稱 ~10 萬筆）
  2. 591 即時 API   - 本地查不到時自動呼叫 591 線上查詢

特色：
  - SQLite 直查：利用 land_data.db 的解析地址欄位（district、address）快速查詢
  - 591 即時 API：本地查不到時自動呼叫 591 線上查詢
  - 多層匹配：精確地址 → 門牌號 → 巷弄 → 路段 → 591 API

使用方式：
  1. 命令列：  python3 address2community.py "松山區八德路四段445號八樓"
  2. 互動：    python3 address2community.py
  3. 批次：    python3 address2community.py --batch input.txt
  4. 模組：    from address2community import lookup
              result = lookup("三民路29巷6號")
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from collections import defaultdict

# ========== 路徑設定 ==========
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
DB_PATH = LAND_DIR / "db" / "land_data.db"

# 共用模組
sys.path.insert(0, str(LAND_DIR))
from address_utils import (
    fullwidth_to_halfwidth, halfwidth_to_fullwidth,
    normalize_city_name,
    extract_city, extract_district_name, infer_city,
    strip_to_road_number, strip_city_district,
    extract_road, extract_road_alley, extract_road_number,
    extract_house_number, normalize_community_name,
    get_591_regionids,
    DISTRICT_CITY_MAP, CITIES, CITY_TO_591_REGION,
    parse_address,
)
from api591 import Api591Client

# ========== 相容性別名 ==========
# 舊版函式名 → address_utils 對應
extract_district = extract_district_name
DISTRICT_TO_CITY = DISTRICT_CITY_MAP


def get_county_city(addr: str) -> str:
    """從地址取得 land_data.db 格式的縣市名稱（台）"""
    city = infer_city(addr)
    return normalize_city_name(city) if city else ''


# ========== 591 API (使用統一客戶端) ==========
# Api591Client 已從 api591.py 匯入，提供:
#   - search_by_address(address) → 用地址搜尋社區
#   - search_by_name(name) → 用建案名稱搜尋
#   - search_community(keyword, regionid) → 基礎搜尋

# 相容性別名 (舊代碼可能用 Api591.search_by_address)
Api591 = Api591Client


# ========== 地址解析（用於索引查詢） ==========

def parse_address_fields(addr: str) -> dict:
    """
    解析地址為結構化欄位，對應 land_data.db 的 district/street/lane/number。
    回傳 {'district': '松山區', 'street': '八德路四段', 'lane': '112', 'number': '445'}
    委託 address_utils.parse_address() 執行。
    """
    p = parse_address(addr)
    return {
        'district': p.get('district', ''),
        'street': p.get('street', ''),
        'lane': p.get('lane', ''),
        'alley': p.get('alley', ''),
        'number': p.get('number', ''),
        'sub_number': p.get('sub_number', ''),
    }


# ========== 核心查詢引擎 ==========

class AddressCommunityLookup:
    """地址→社區名稱 查詢引擎 (land_data.db + 591 API)
    
    v2: 使用索引欄位 (district/street/lane/number) 查詢，
        比 LIKE 快 100-500 倍。
    """

    def __init__(self, db_path: str = None, enable_api: bool = True, verbose: bool = False):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.enable_api = enable_api
        self.verbose = verbose
        self.conn = None
        self._api591 = None  # 延遲初始化
        self._connect_db()

    def _connect_db(self):
        """連線 land_data.db"""
        if not self.db_path.exists():
            print(f"⚠️  資料庫不存在: {self.db_path}")
            return

        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA mmap_size=268435456")  # 256MB mmap

        # 確認記錄數
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM land_transaction WHERE community_name IS NOT NULL AND community_name != ''"
        )
        count = cursor.fetchone()[0]
        print(f"📂 已連線: {self.db_path.name}（{count:,} 筆有社區資料）")

    def close(self):
        """關閉資料庫連線"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    # ── 索引查詢層（取代 LIKE 全表掃描） ──

    def _query_indexed_exact(self, fields: dict, county_city: str = None) -> list:
        """Level 1: district+street+lane+number 精確匹配（使用複合索引，<5ms）"""
        if not self.conn or not fields.get('street') or not fields.get('number'):
            return []

        sql = """
            SELECT community_name, COUNT(*) as cnt, county_city, district, address
            FROM land_transaction
            WHERE street = ? AND number = ?
              AND community_name IS NOT NULL AND community_name != ''
        """
        params = [fields['street'], fields['number']]

        if fields.get('lane'):
            sql += " AND lane = ?"
            params.append(fields['lane'])

        if fields.get('district'):
            sql += " AND district = ?"
            params.append(fields['district'])
        elif county_city:
            sql += " AND county_city = ?"
            params.append(county_city)

        sql += " GROUP BY community_name ORDER BY cnt DESC LIMIT 5"
        rows = self.conn.execute(sql, params).fetchall()
        if rows:
            return [{"community": r["community_name"], "count": r["cnt"],
                     "county_city": r["county_city"], "district": r["district"],
                     "sample_address": r["address"]} for r in rows]

        # 若有 lane 但沒找到，嘗試放寬（不帶 lane）
        if fields.get('lane'):
            sql2 = """
                SELECT community_name, COUNT(*) as cnt, county_city, district, address
                FROM land_transaction
                WHERE street = ? AND number = ?
                  AND community_name IS NOT NULL AND community_name != ''
            """
            params2 = [fields['street'], fields['number']]
            if fields.get('district'):
                sql2 += " AND district = ?"
                params2.append(fields['district'])
            elif county_city:
                sql2 += " AND county_city = ?"
                params2.append(county_city)
            sql2 += " GROUP BY community_name ORDER BY cnt DESC LIMIT 5"
            rows = self.conn.execute(sql2, params2).fetchall()
            if rows:
                return [{"community": r["community_name"], "count": r["cnt"],
                         "county_city": r["county_city"], "district": r["district"],
                         "sample_address": r["address"]} for r in rows]
        return []

    def _query_indexed_nearby(self, fields: dict, county_city: str = None) -> list:
        """Level 2: street+lane 相鄰門牌號匹配（使用索引，<10ms）"""
        if not self.conn or not fields.get('street') or not fields.get('number'):
            return []

        try:
            target_num = int(fields['number'])
        except (ValueError, TypeError):
            return []

        # 查附近門牌 (±10)
        sql = """
            SELECT community_name, number, COUNT(*) as cnt, county_city, district
            FROM land_transaction
            WHERE street = ?
              AND CAST(number AS INTEGER) BETWEEN ? AND ?
              AND community_name IS NOT NULL AND community_name != ''
        """
        params = [fields['street'], target_num - 10, target_num + 10]

        if fields.get('lane'):
            sql += " AND lane = ?"
            params.append(fields['lane'])

        if fields.get('district'):
            sql += " AND district = ?"
            params.append(fields['district'])
        elif county_city:
            sql += " AND county_city = ?"
            params.append(county_city)

        sql += " GROUP BY community_name ORDER BY cnt DESC LIMIT 5"
        rows = self.conn.execute(sql, params).fetchall()
        if rows:
            return [{"community": r["community_name"], "count": r["cnt"],
                     "county_city": r["county_city"], "district": r["district"]} for r in rows]
        return []

    def _query_indexed_lane(self, fields: dict, county_city: str = None) -> list:
        """Level 3: street+lane 匹配（不限門牌號，使用索引，<10ms）"""
        if not self.conn or not fields.get('street') or not fields.get('lane'):
            return []

        sql = """
            SELECT community_name, COUNT(*) as cnt, county_city, district
            FROM land_transaction
            WHERE street = ? AND lane = ?
              AND community_name IS NOT NULL AND community_name != ''
        """
        params = [fields['street'], fields['lane']]

        if fields.get('district'):
            sql += " AND district = ?"
            params.append(fields['district'])
        elif county_city:
            sql += " AND county_city = ?"
            params.append(county_city)

        sql += " GROUP BY community_name ORDER BY cnt DESC LIMIT 5"
        rows = self.conn.execute(sql, params).fetchall()
        if rows:
            return [{"community": r["community_name"], "count": r["cnt"],
                     "county_city": r["county_city"], "district": r["district"]} for r in rows]
        return []

    def _query_indexed_road(self, fields: dict, county_city: str = None) -> list:
        """Level 4: street+district 匹配（僅路段，使用索引，<50ms）"""
        if not self.conn or not fields.get('street'):
            return []

        sql = """
            SELECT community_name, COUNT(*) as cnt, county_city, district
            FROM land_transaction
            WHERE street = ?
              AND community_name IS NOT NULL AND community_name != ''
        """
        params = [fields['street']]

        if fields.get('district'):
            sql += " AND district = ?"
            params.append(fields['district'])
        elif county_city:
            sql += " AND county_city = ?"
            params.append(county_city)

        sql += " GROUP BY community_name ORDER BY cnt DESC LIMIT 10"
        rows = self.conn.execute(sql, params).fetchall()
        if rows:
            return [{"community": r["community_name"], "count": r["cnt"],
                     "county_city": r["county_city"], "district": r["district"]} for r in rows]
        return []

    # ── 舊版 LIKE 查詢（fallback，僅在解析欄位不完整時使用） ──

    def _query_like_fallback(self, norm: str, county_city: str = None, district: str = None) -> list:
        """LIKE 備援查詢（僅當索引查詢無結果時使用）"""
        if not self.conn or not norm:
            return []

        fw_part = halfwidth_to_fullwidth(norm)
        patterns = []
        if district:
            patterns.append(f"%{district}{fw_part}%")
        patterns.append(f"%{fw_part}%")

        for pattern in patterns:
            sql = """
                SELECT community_name, COUNT(*) as cnt, county_city, district, address
                FROM land_transaction
                WHERE address LIKE ? AND community_name IS NOT NULL AND community_name != ''
            """
            params = [pattern]
            if district:
                sql += " AND district = ?"
                params.append(district)
            elif county_city:
                sql += " AND county_city = ?"
                params.append(county_city)
            sql += " GROUP BY community_name ORDER BY cnt DESC LIMIT 5"

            rows = self.conn.execute(sql, params).fetchall()
            if rows:
                return [{"community": r["community_name"], "count": r["cnt"],
                         "county_city": r["county_city"], "district": r["district"],
                         "sample_address": r["address"]} for r in rows]
        return []

    def query(self, address: str, top_n: int = 5) -> dict:
        """查詢地址對應的社區/建案名稱（使用索引查詢，毫秒級）"""
        norm = strip_to_road_number(address)
        input_district = extract_district(address)
        input_city = infer_city(address)
        county_city = get_county_city(address)
        fields = parse_address_fields(address)
        results = []

        if self.verbose:
            print(f"  🔍 查詢: {address}")
            print(f"     正規化: {norm}")
            print(f"     解析欄位: {fields}")
            if input_city:
                print(f"     城市: {input_city} ({county_city})")
            if input_district:
                print(f"     區域: {input_district}")

        # 若解析出 district，更新 fields
        if input_district and not fields.get('district'):
            fields['district'] = input_district

        if self.conn:
            # Level 1: 精確索引匹配 (district+street+lane+number)
            db_results = self._query_indexed_exact(fields, county_city)
            if db_results:
                for r in db_results:
                    results.append({
                        "community": r["community"],
                        "confidence": 98,
                        "match_level": "精確索引匹配",
                        "district": r.get("district") or input_district,
                        "source": "land_data.db",
                        "count": r["count"],
                    })
                if self.verbose:
                    print(f"     ✅ Level 1: {results[0]['community']} ({results[0]['count']}筆)")

            # Level 2: 相鄰門牌匹配
            if not results or results[0]["confidence"] < 80:
                db_results = self._query_indexed_nearby(fields, county_city)
                if db_results:
                    for r in db_results:
                        results.append({
                            "community": r["community"],
                            "confidence": 90,
                            "match_level": "相鄰門牌匹配",
                            "district": r.get("district") or input_district,
                            "source": "land_data.db",
                            "count": r["count"],
                        })
                    if self.verbose:
                        print(f"     ✅ Level 2: {db_results[0]['community']} ({db_results[0]['count']}筆)")

            # Level 3: 巷弄索引匹配 (street+lane)
            if not results or all(r["confidence"] < 70 for r in results):
                db_results = self._query_indexed_lane(fields, county_city)
                if db_results:
                    for r in db_results:
                        results.append({
                            "community": r["community"],
                            "confidence": 72,
                            "match_level": "巷弄索引匹配",
                            "district": r.get("district") or input_district,
                            "source": "land_data.db",
                            "count": r["count"],
                        })
                    if self.verbose:
                        print(f"     ✅ Level 3: {db_results[0]['community']} ({db_results[0]['count']}筆)")

            # Level 4: 路段索引匹配 (street+district)
            if not results or all(r["confidence"] < 50 for r in results):
                db_results = self._query_indexed_road(fields, county_city)
                if db_results:
                    for r in db_results:
                        results.append({
                            "community": r["community"],
                            "confidence": 40,
                            "match_level": "路段索引匹配",
                            "district": r.get("district") or input_district,
                            "source": "land_data.db",
                            "count": r["count"],
                        })
                    if self.verbose:
                        print(f"     ✅ Level 4: {db_results[0]['community']} ({db_results[0]['count']}筆)")

            # Level 4.5: LIKE 備援（若索引查不到，可能是解析欄位不完整）
            if not results and norm:
                db_results = self._query_like_fallback(norm, county_city, input_district)
                if db_results:
                    for r in db_results:
                        results.append({
                            "community": r["community"],
                            "confidence": 65,
                            "match_level": "LIKE 備援匹配",
                            "district": r.get("district") or input_district,
                            "source": "land_data.db",
                            "count": r["count"],
                        })
                    if self.verbose:
                        print(f"     ✅ Level 4.5: LIKE fallback {db_results[0]['community']} ({db_results[0]['count']}筆)")

        # Level 5: 591 API 線上查詢
        if self.enable_api and (not results or all(r["confidence"] < 70 for r in results)):
            api_results = self._query_591_api(address, norm)
            if api_results:
                results.extend(api_results)

        # 去重、排序
        seen = set()
        unique_results = []
        for r in sorted(results, key=lambda x: (-x["confidence"], -x.get("count", 0))):
            if r["community"] not in seen:
                seen.add(r["community"])
                unique_results.append(r)

        unique_results = unique_results[:top_n]
        best = unique_results[0]["community"] if unique_results else None

        return {
            "input": address,
            "normalized": norm,
            "results": unique_results,
            "best": best,
        }

    def _query_591_api(self, original_addr: str, norm: str) -> list:
        """呼叫 591 API 線上查詢"""
        if self.verbose:
            print(f"     🌐 查詢 591 API...")

        regionids = get_591_regionids(original_addr)
        if self._api591 is None:
            self._api591 = Api591Client()
        result = self._api591.search_by_address(original_addr, regionids)

        if result:
            name = result.get("name", "")
            if name:
                if self.verbose:
                    print(f"     ✅ 591 API: {name}")
                district = result.get("section", "")
                return [{
                    "community": name,
                    "confidence": 88,
                    "match_level": "591 即時查詢",
                    "district": district,
                    "source": "591_API",
                    "count": 0,
                }]

        # 路段搜尋取近鄰
        road = extract_road(norm)
        if road and regionids:
            results = self._api591.search_community(road, regionids[0])
            if results:
                candidates = []
                for item in results:
                    item_name = item.get("name")
                    if item_name:
                        candidates.append({
                            "community": item_name,
                            "confidence": 35,
                            "match_level": "591 路段附近",
                            "district": item.get("section", ""),
                            "source": "591_API",
                            "count": 0,
                        })
                return candidates[:3]

        return []

    def batch_query(self, addresses: list) -> list:
        return [self.query(addr) for addr in addresses]

    def stats(self) -> dict:
        """取得統計"""
        if not self.conn:
            return {"total_records": 0, "unique_communities": 0}
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM land_transaction WHERE community_name IS NOT NULL AND community_name != ''"
        )
        total = cursor.fetchone()[0]
        cursor = self.conn.execute(
            "SELECT COUNT(DISTINCT community_name) FROM land_transaction WHERE community_name IS NOT NULL AND community_name != ''"
        )
        unique = cursor.fetchone()[0]
        return {"total_records": total, "unique_communities": unique}


# ========== 便利函式 ==========

_global_lookup = None


def lookup(address: str, **kwargs) -> dict:
    """便利查詢函式"""
    global _global_lookup
    if _global_lookup is None:
        _global_lookup = AddressCommunityLookup(**kwargs)
    return _global_lookup.query(address)


def quick_lookup(address: str) -> str:
    """最簡查詢"""
    result = lookup(address)
    return result["best"] or "未找到"


# ========== CLI ==========

def print_result(result: dict, show_detail: bool = False):
    """格式化輸出"""
    addr = result["input"]
    best = result["best"]

    if best:
        top = result["results"][0]
        bar = "█" * (top["confidence"] // 10) + "░" * (10 - top["confidence"] // 10)
        print(f"\n📍 {addr}")
        print(f"   → 🏘️  {best}")
        print(f"   信心度: [{bar}] {top['confidence']}%")
        print(f"   匹配: {top['match_level']} (來源: {top['source']})")
        if top["district"]:
            print(f"   區域: {top['district']}")
        if top.get("count"):
            print(f"   交易筆數: {top['count']}")

        if show_detail and len(result["results"]) > 1:
            print(f"\n   其他候選：")
            for r in result["results"][1:]:
                extra = f", {r['count']}筆" if r.get("count") else ""
                print(f"   • {r['community']} ({r['confidence']}%, {r['match_level']}{extra})")
    else:
        print(f"\n📍 {addr}")
        print(f"   → ❓ 未找到")
        print(f"   正規化: {result['normalized']}")


def interactive_mode(lookup_engine: AddressCommunityLookup):
    """互動模式"""
    stats = lookup_engine.stats()
    print("=" * 60)
    print("🏠  地址→社區名稱 查詢工具 (land_data.db + 591 API)")
    print("=" * 60)
    print(f"📊 本地資料: {stats.get('total_records', 0):,} 筆 | "
          f"社區: {stats.get('unique_communities', 0):,}")
    print("-" * 60)
    print("輸入地址查詢，'q' 退出，'detail' 詳細模式")
    print("-" * 60)

    show_detail = False

    while True:
        try:
            addr = input("\n🔎 地址: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 再見！")
            break

        if not addr:
            continue
        if addr.lower() in ("q", "quit", "exit"):
            print("👋 再見！")
            break
        if addr.lower() == "detail":
            show_detail = not show_detail
            print(f"   詳細模式: {'開啟' if show_detail else '關閉'}")
            continue
        if addr.lower() == "stats":
            s = lookup_engine.stats()
            print(f"   記錄: {s['total_records']:,} | 社區: {s['unique_communities']:,}")
            continue

        t0 = time.time()
        result = lookup_engine.query(addr)
        elapsed = time.time() - t0
        print_result(result, show_detail)
        print(f"   ⏱️  {elapsed:.3f}s")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="地址→社區/建案名稱 查詢工具 (transactions.db + 591 API)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用範例：
  python3 address2community.py "松山區八德路四段445號八樓"
  python3 address2community.py "仁愛路三段53號E棟"
  python3 address2community.py --batch addresses.txt
  python3 address2community.py --no-api "三民路29巷5號"
        """,
    )
    parser.add_argument("address", nargs="*", help="查詢地址")
    parser.add_argument("--batch", "-b", help="批次檔案")
    parser.add_argument("--detail", "-d", action="store_true", help="詳細結果")
    parser.add_argument("--no-api", action="store_true", help="停用 591 API")
    parser.add_argument("--verbose", "-v", action="store_true", help="顯示詳細過程")
    parser.add_argument("--json", "-j", action="store_true", help="JSON 輸出")
    parser.add_argument("--db", help="指定 transactions.db 路徑")

    args = parser.parse_args()

    engine = AddressCommunityLookup(
        db_path=args.db,
        enable_api=not args.no_api,
        verbose=args.verbose,
    )

    if args.batch:
        with open(args.batch, "r", encoding="utf-8") as f:
            addresses = [line.strip() for line in f if line.strip()]
        print(f"📋 批次查詢 {len(addresses)} 個地址...\n")

        if args.json:
            results = engine.batch_query(addresses)
            output = [{
                "input": r["input"],
                "community": r["best"],
                "confidence": r["results"][0]["confidence"] if r["results"] else 0,
            } for r in results]
            print(json.dumps(output, ensure_ascii=False, indent=2))
        else:
            for addr in addresses:
                result = engine.query(addr)
                if result["best"]:
                    top = result["results"][0]
                    print(f"{addr} → {result['best']} ({top['confidence']}%)")
                else:
                    print(f"{addr} → ❓ 未找到")
        return

    if args.address:
        for addr in args.address:
            t0 = time.time()
            result = engine.query(addr)
            elapsed = time.time() - t0
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print_result(result, args.detail)
                if args.verbose:
                    print(f"   ⏱️  {elapsed:.3f}s")
        return

    interactive_mode(engine)


if __name__ == "__main__":
    main()
