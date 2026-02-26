#!/usr/bin/env python3
"""
良富居地產 v4.0 — 後端 API 伺服器
整合 address_match、com2address、address2com、OSM geocoding
使用 Flask + SQLite (land_data.db)

v4 改動:
- OSM 優先定位（離線快速），fallback DB 座標，否則放棄
- 絕不使用行政區座標、絕不使用偏移
- 搜尋欄同時做 com2address + address2com+address_match
- 地址正規化顯示
- 交易備忘錄（note）欄位恢復
- 位置模式切換：精確(OSM) / 建案(DB)
"""

import os
import sys
import re
import json
import time
import math
import threading
import sqlite3
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ── 路徑設定 ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent                # land/web
LAND_DIR = BASE_DIR.parent                      # land
ADDR_MATCH_DIR = LAND_DIR / "address_match"
COM2ADDR_DIR = LAND_DIR / "com2address"
ADDR2COM_DIR = LAND_DIR / "address2com"
GEODECODING_DIR = LAND_DIR / "geodecoding"

# 將模組路徑加入 sys.path
for p in [str(ADDR_MATCH_DIR), str(COM2ADDR_DIR), str(ADDR2COM_DIR),
          str(GEODECODING_DIR), str(LAND_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# 匯入模組
from address_match import search_address, parse_range, SORT_OPTIONS
from address_utils import fullwidth_to_halfwidth, normalize_address, parse_query
from community2address import Community2AddressLookup
from address2community import lookup as addr2com_lookup
from geocoder import TaiwanGeocoder

# 特殊交易關鍵字（用於 note 欄位判斷）
SPECIAL_TX_KEYWORDS = [
    '親友', '員工', '共有人', '特殊關係', '利害關係',
    '調協', '欻欄', '法拍', '濟助', '社會住宅',
    '总價顯著偏低', '價格顯著偏高',
    '政府機關', '建商與地主',
    '債權債務', '繼承',
    '急買急賣', '受債權人',
]

# ── Flask 設定 ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder="static")
CORS(app)

# ── 全域錯誤處理 ──────────────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(error):
    """404 錯誤 — 回傳 JSON，而非 HTML"""
    return jsonify({"success": False, "error": "找不到該路由"}), 404


@app.errorhandler(500)
def internal_error(error):
    """500 錯誤 — 回傳 JSON，而非 HTML"""
    import traceback
    traceback.print_exc()
    return jsonify({"success": False, "error": "伺服器內部錯誤"}), 500


@app.errorhandler(Exception)
def handle_exception(error):
    """捕捉所有未處理例外 — 回傳 JSON"""
    import traceback
    traceback.print_exc()
    return jsonify({"success": False, "error": f"錯誤: {str(error)}"}), 500

DB_PATH = str(LAND_DIR / "db" / "land_data.db")
PING_TO_SQM = 3.30579

# ── 全域資料 ──────────────────────────────────────────────────────────────────
com2addr_engine = None
com2addr_ready = False
geocoder_engine = None
geocoder_ready = False
_community_coords_cache = {}  # community_name → (lat, lng)


def _build_community_coords_cache():
    """建立建案平均座標快取（啟動時呼叫，約 2-3 秒）"""
    global _community_coords_cache
    try:
        t0 = time.time()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("""
            SELECT community_name, AVG(lat) AS avg_lat, AVG(lng) AS avg_lng
            FROM land_transaction
            WHERE community_name IS NOT NULL AND community_name != ''
              AND lat IS NOT NULL AND lat != 0
              AND lng IS NOT NULL AND lng != 0
            GROUP BY community_name
        """)
        _community_coords_cache = {row[0]: (row[1], row[2]) for row in cursor}
        conn.close()
        print(f"📍 建案座標快取: {len(_community_coords_cache)} 個建案 ({time.time()-t0:.2f}s)")
    except Exception as e:
        print(f"⚠️  建案座標快取建立失敗: {e}")

def init_com2addr():
    """背景初始化 com2address 查詢引擎"""
    global com2addr_engine, com2addr_ready
    try:
        print("🏘️  載入 com2address 查詢引擎...")
        com2addr_engine = Community2AddressLookup(verbose=False, use_591=False)
        com2addr_ready = True
        print("✅ com2address 就緒")
    except Exception as e:
        print(f"⚠️  com2address 載入失敗: {e}")
        import traceback; traceback.print_exc()
        com2addr_ready = True


def init_geocoder():
    """背景初始化地理編碼引擎"""
    global geocoder_engine, geocoder_ready
    try:
        print("🌍 載入 TaiwanGeocoder...")
        geocoder_engine = TaiwanGeocoder(
            cache_dir=str(LAND_DIR / "db"),
            provider="nominatim",
            concurrency=1
        )
        geocoder_ready = True
        print("✅ TaiwanGeocoder 就緒")
    except Exception as e:
        print(f"⚠️  TaiwanGeocoder 載入失敗: {e}")
        import traceback; traceback.print_exc()
        geocoder_ready = True


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def clean_nan(obj):
    """遞迴清理 NaN/Infinity"""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0
    return obj


def format_roc_date(roc_date):
    """民國日期 (1130101) → 西元 (2024/01/01)"""
    if not roc_date:
        return None
    ds = str(roc_date).strip()
    if len(ds) < 7:
        return None
    try:
        y = int(ds[:3]) + 1911
        return f"{y}/{ds[3:5]}/{ds[5:7]}"
    except Exception:
        return None


def get_osm_coords(address: str, district: str = "") -> tuple:
    """
    使用 OSM 離線索引取得精確座標（快速）

    Returns: (lat, lng) or (None, None)
    只接受 exact / road 層級，不接受 district
    """
    global geocoder_engine, geocoder_ready

    if not geocoder_ready or geocoder_engine is None:
        return None, None

    try:
        result = geocoder_engine.geocode(address, district=district)
        if result and 'lat' in result and 'lng' in result:
            level = result.get('level', 'unknown')
            if level in ('exact', 'road'):
                return result['lat'], result['lng']
    except Exception:
        pass

    return None, None


def batch_osm_geocode(rows: list) -> dict:
    """
    批次 OSM 地理編碼——先找出唯一地址，批次查詢一次，再映射回所有交易
    大幅提升精確位置模式的速度

    Returns: {address_raw: (lat, lng), ...}
    """
    if not geocoder_ready or geocoder_engine is None:
        return {}

    # 収集唯一地址
    unique_addrs = {}
    for r in rows:
        addr = str(r.get('address', '') or '')
        district = str(r.get('district', '') or '')
        if addr and addr not in unique_addrs:
            unique_addrs[addr] = district

    if not unique_addrs:
        return {}

    t0 = time.time()
    results = {}
    for addr, district in unique_addrs.items():
        lat, lng = get_osm_coords(addr, district)
        if lat and lng:
            results[addr] = (lat, lng)

    elapsed = time.time() - t0
    print(f"📍 OSM 批次定位: {len(unique_addrs)} 個唯一地址 → {len(results)} 個命中 ({elapsed:.2f}s)")
    return results


def is_special_transaction(note: str) -> bool:
    """判斷是否為特殊交易（根據備忘錄）"""
    if not note:
        return False
    for kw in SPECIAL_TX_KEYWORDS:
        if kw in note:
            return True
    return False


def format_tx_row(row: dict, location_mode: str = "osm", osm_cache: dict = None) -> dict:
    """
    將 address_search 回傳的 row 轉為前端友好格式

    location_mode:
      "osm"   - OSM 精確位置優先 → DB → 放棄
      "db"    - DB 位置優先（建案平均座標）→ 放棄
    osm_cache:
      批次 OSM 定位結果 {address_raw: (lat, lng)}，避免逐筆查詢
    """
    total_price = row.get("total_price", 0) or 0
    building_area = row.get("building_area_sqm", 0) or 0
    unit_price = row.get("unit_price", 0) or 0
    main_area = row.get("main_building_area", 0) or 0
    attached = row.get("attached_area", 0) or 0
    balcony = row.get("balcony_area", 0) or 0

    ping = round(building_area / PING_TO_SQM, 2) if building_area else 0
    unit_price_ping = round(unit_price * PING_TO_SQM, 2) if unit_price else 0

    # 公設比
    public_ratio = 0
    if building_area > 0 and main_area > 0:
        public_ratio = round(
            (building_area - main_area - attached - balcony) / building_area * 100, 1
        )
        if public_ratio < 0:
            public_ratio = 0

    date_raw = str(row.get("transaction_date", "") or "")
    floor_raw = str(row.get("floor_level", "") or "")
    total_floors_raw = str(row.get("total_floors", "") or "")
    district = str(row.get("district", "") or "")
    address_raw = str(row.get("address", "") or "")
    address_display = normalize_address(address_raw) if address_raw else ""
    community_name_raw = str(row.get("community_name", "") or "")
    note = str(row.get("note", "") or "")
    special = is_special_transaction(note)

    # 車位
    parking_type_raw = str(row.get("parking_type", "") or "")
    parking_price_raw = row.get("parking_price", 0) or 0
    has_parking = bool(parking_type_raw and parking_type_raw != "無")

    # ── 座標策略（絕不使用行政區、絕不偏移）──
    lat = None
    lng = None
    coord_source = "none"

    db_lat = row.get("lat")
    db_lng = row.get("lng")
    has_db = db_lat and db_lng and db_lat != 0 and db_lng != 0

    if location_mode == "osm":
        # 優先從批次快取取
        if osm_cache and address_raw in osm_cache:
            lat, lng = osm_cache[address_raw]
            coord_source = "osm"
        elif has_db:
            lat, lng = db_lat, db_lng
            coord_source = "db"
        # 否則放棄
    elif location_mode == "db":
        # DB 位置優先（建案平均座標）
        if has_db:
            lat, lng = db_lat, db_lng
            coord_source = "db"
        elif community_name_raw and community_name_raw in _community_coords_cache:
            lat, lng = _community_coords_cache[community_name_raw]
            coord_source = "community"
        # 否則放棄
    else:
        # 預設同 db（快速）
        if has_db:
            lat, lng = db_lat, db_lng
            coord_source = "db"

    return {
        "address": address_display,
        "address_raw": address_raw,
        "district": district,
        "date": format_roc_date(date_raw) or date_raw,
        "date_raw": date_raw,
        "price": total_price,
        "unit_price_sqm": round(unit_price, 2),
        "unit_price_ping": unit_price_ping,
        "area_sqm": round(building_area, 2),
        "area_ping": ping,
        "main_area_sqm": round(main_area, 2),
        "public_ratio": public_ratio,
        "floor": floor_raw,
        "total_floors": total_floors_raw,
        "rooms": row.get("rooms", 0) or 0,
        "halls": row.get("halls", 0) or 0,
        "baths": row.get("bathrooms", 0) or 0,
        "building_type": str(row.get("building_type", "") or ""),
        "main_use": str(row.get("main_use", "") or ""),
        "main_material": str(row.get("main_material", "") or ""),
        "completion_date": str(row.get("completion_date", "") or ""),
        "has_elevator": str(row.get("elevator", "") or ""),
        "has_management": str(row.get("has_management", "") or ""),
        "parking_type": str(row.get("parking_type", "") or ""),
        "parking_price": row.get("parking_price", 0) or 0,
        "parking_area_sqm": row.get("parking_area_sqm", 0) or 0,
        "note": note,
        "community_name": community_name_raw,
        "is_special": special,
        "has_parking": has_parking,
        "lat": lat,
        "lng": lng,
        "coord_source": coord_source,
    }


def compute_summary(transactions: list) -> dict:
    """計算統計摘要"""
    if not transactions:
        return {}
    prices = [t["price"] for t in transactions if t.get("price", 0) > 0]
    pings = [t["area_ping"] for t in transactions if t.get("area_ping", 0) > 0]
    unit_prices = [t["unit_price_ping"] for t in transactions if t.get("unit_price_ping", 0) > 0]
    ratios = [t["public_ratio"] for t in transactions if t.get("public_ratio", 0) > 0]

    return {
        "total": len(transactions),
        "avg_price": round(sum(prices) / len(prices)) if prices else 0,
        "min_price": min(prices) if prices else 0,
        "max_price": max(prices) if prices else 0,
        "avg_ping": round(sum(pings) / len(pings), 2) if pings else 0,
        "avg_unit_price_ping": round(sum(unit_prices) / len(unit_prices), 2) if unit_prices else 0,
        "min_unit_price_ping": round(min(unit_prices), 2) if unit_prices else 0,
        "max_unit_price_ping": round(max(unit_prices), 2) if unit_prices else 0,
        "avg_ratio": round(sum(ratios) / len(ratios), 1) if ratios else 0,
    }


def _build_filter_where(filters: dict, params: list) -> list:
    """建立篩選 WHERE 子句（給 area 和 community 直查共用）"""
    clauses = []
    if filters.get("building_types"):
        placeholders = ",".join(["?"] * len(filters["building_types"]))
        clauses.append(f"building_type IN ({placeholders})")
        params.extend(filters["building_types"])
    if filters.get("rooms"):
        placeholders = ",".join(["?"] * len(filters["rooms"]))
        clauses.append(f"rooms IN ({placeholders})")
        params.extend(filters["rooms"])
    if filters.get("public_ratio_min") is not None or filters.get("public_ratio_max") is not None:
        clauses.append("building_area > 0 AND main_area > 0")
        pr = "CAST((building_area - main_area - COALESCE(attached_area,0) - COALESCE(balcony_area,0)) * 100.0 / building_area AS REAL)"
        if filters.get("public_ratio_min") is not None:
            clauses.append(f"{pr} >= ?")
            params.append(float(filters["public_ratio_min"]))
        if filters.get("public_ratio_max") is not None:
            clauses.append(f"{pr} <= ?")
            params.append(float(filters["public_ratio_max"]))
    if filters.get("year_min") is not None:
        clauses.append("CAST(SUBSTR(transaction_date, 1, 3) AS INTEGER) >= ?")
        params.append(int(filters["year_min"]))
    if filters.get("year_max") is not None:
        clauses.append("CAST(SUBSTR(transaction_date, 1, 3) AS INTEGER) <= ?")
        params.append(int(filters["year_max"]))
    if filters.get("ping_min") is not None:
        clauses.append("building_area >= ?")
        params.append(float(filters["ping_min"]) * PING_TO_SQM)
    if filters.get("ping_max") is not None:
        clauses.append("building_area <= ?")
        params.append(float(filters["ping_max"]) * PING_TO_SQM)
    if filters.get("unit_price_min") is not None:
        clauses.append("unit_price >= ?")
        params.append(float(filters["unit_price_min"]) * 10000 / PING_TO_SQM)
    if filters.get("unit_price_max") is not None:
        clauses.append("unit_price <= ?")
        params.append(float(filters["unit_price_max"]) * 10000 / PING_TO_SQM)
    if filters.get("price_min") is not None:
        clauses.append("total_price >= ?")
        params.append(float(filters["price_min"]) * 10000)
    if filters.get("price_max") is not None:
        clauses.append("total_price <= ?")
        params.append(float(filters["price_max"]) * 10000)
    return clauses


SELECT_COLS = """
    id, district, address, transaction_date, total_price, unit_price,
    building_area AS building_area_sqm, main_area AS main_building_area,
    attached_area, balcony_area, rooms, halls, bathrooms,
    floor_level, total_floors, building_type, main_use, main_material,
    build_date AS completion_date, elevator, has_management,
    parking_type, parking_price, parking_area AS parking_area_sqm,
    note, lat, lng, community_name
"""


def _search_by_community_name(community_name: str, filters: dict, limit: int) -> list:
    """直接用 community_name 索引查詢 DB（回傳原始 row dict）"""
    params = [community_name]
    filter_clauses = _build_filter_where(filters, params)
    where_sql = "community_name = ?" + (" AND " + " AND ".join(filter_clauses) if filter_clauses else "")
    sql = f"SELECT {SELECT_COLS} FROM land_transaction WHERE {where_sql} ORDER BY transaction_date DESC LIMIT ?"
    params.append(limit)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def parse_filters_from_request() -> dict:
    """從 request.args 解析篩選參數"""
    filters = {}

    btype = request.args.get("building_type", "").strip()
    if btype:
        filters["building_types"] = [t.strip() for t in btype.split(",") if t.strip()]

    rooms = request.args.get("rooms", "").strip()
    if rooms:
        filters["rooms"] = [int(r) for r in rooms.split(",") if r.strip().isdigit()]

    for key, fmin, fmax in [
        ("public_ratio", "public_ratio_min", "public_ratio_max"),
        ("year", "year_min", "year_max"),
        ("ping", "ping_min", "ping_max"),
        ("unit_price", "unit_price_min", "unit_price_max"),
        ("price", "price_min", "price_max"),
    ]:
        val = request.args.get(key, "").strip()
        if val:
            lo, hi = parse_range(val)
            if lo is not None:
                filters[fmin] = lo
            if hi is not None:
                filters[fmax] = hi

    return filters


# ══════════════════════════════════════════════════════════════════════════════
# API 路由
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/search", methods=["GET"])
def api_search():
    """
    統一搜尋 API — 同時做社區搜尋 + 地址搜尋

    參數:
      keyword        - 搜尋關鍵字（必要）
      location_mode  - osm|db (預設 db)
      limit          - 回傳上限 (預設 500)
      + 篩選參數 (building_type, rooms, public_ratio, year, ping, unit_price, price)
    """
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "error": "缺少 keyword 參數"}), 400

    location_mode = request.args.get("location_mode", "db").strip()
    limit = min(int(request.args.get("limit", 500)), 2000)
    filters = parse_filters_from_request()

    community_name = None
    search_type = "address"

    # ════════════ 路徑 A: com2address — 把 keyword 當建案名搜尋 ════════════
    com_raw_rows = []
    if com2addr_ready and com2addr_engine:
        try:
            com_result = com2addr_engine.query(keyword, top_n=5)
            if com_result.get("found") and com_result.get("match_type") != "未找到":
                mt = com_result.get("match_type", "")
                tx_count = com_result.get("transaction_count", 0) or 0

                if "精確" in mt and tx_count >= 2:
                    community_name = com_result.get("matched_name", keyword)
                    search_type = "community"
                    print(f"🏘️  建案搜尋: {keyword} → {community_name} ({tx_count} 筆)")
                elif "精確" not in mt:
                    candidates = com_result.get("candidates", [])
                    best = max(candidates, key=lambda x: x.get("tx_count", 0), default=None)
                    if best and best.get("tx_count", 0) >= 2:
                        community_name = best["name"]
                        search_type = "community"
                        print(f"🏘️  建案模糊: {keyword} → {community_name} ({best['tx_count']} 筆)")

                if community_name:
                    com_raw_rows = _search_by_community_name(community_name, filters, limit)
                    print(f"   → com2address 直查: {len(com_raw_rows)} 筆")
        except Exception as e:
            print(f"⚠️  com2address 查詢錯誤: {e}")

    # ════════════ 路徑 B: address2com → 找到建案 → community_name 查 DB ════════════
    a2c_raw_rows = []
    if not community_name:
        try:
            a2c_result = addr2com_lookup(keyword)
            if a2c_result and isinstance(a2c_result, dict):
                best_name = a2c_result.get("best", "")
                if not best_name and a2c_result.get("results"):
                    for r in a2c_result["results"]:
                        if isinstance(r, dict) and r.get("community"):
                            best_name = r["community"]
                            break
                if best_name:
                    print(f"📍 地址→建案: {keyword} → {best_name}")
                    community_name = best_name
                    search_type = "address_to_community"
                    a2c_raw_rows = _search_by_community_name(best_name, filters, limit)
                    print(f"   → addr2com 直查: {len(a2c_raw_rows)} 筆")
        except Exception as e:
            print(f"⚠️  address2community 查詢錯誤: {e}")

    # ════════════ 路徑 C: 直接搜 address_match (fallback) ════════════
    addr_raw_rows = []
    try:
        result = search_address(
            keyword, db_path=DB_PATH,
            filters=filters, sort_by="date",
            limit=limit, show_sql=False
        )
        addr_raw_rows = result.get("results", [])
    except Exception as e:
        print(f"⚠️  address_search 錯誤: {e}")

    # ════════════ 合併 & 去重（以 id 為 key）════════════
    seen_ids = set()
    merged_raw = []

    for r in com_raw_rows:
        rid = r.get("id")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            merged_raw.append(r)

    for r in a2c_raw_rows:
        rid = r.get("id")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            merged_raw.append(r)

    for r in addr_raw_rows:
        rid = r.get("id")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            merged_raw.append(r)

    # ─── District 後過濾：keyword 含行政區時，排除不符的 ───
    parsed_kw = parse_query(keyword)
    kw_district = parsed_kw.get("district", "") if parsed_kw else ""
    if kw_district and merged_raw:
        before = len(merged_raw)
        merged_raw = [r for r in merged_raw
                      if kw_district in str(r.get("district", "") or "") or kw_district in str(r.get("address", "") or "")]
        print(f"📌 District 過濾: {before} → {len(merged_raw)} ({kw_district})")

    merged_raw = merged_raw[:limit]

    # 批次 OSM 地理編碼（一次處理所有不重複地址）
    osm_cache = batch_osm_geocode(merged_raw) if location_mode == "osm" else None

    # 格式化（含座標策略）
    exclude_special = request.args.get("exclude_special", "").lower() in ("1", "true", "yes")
    all_transactions = [format_tx_row(r, location_mode, osm_cache) for r in merged_raw]
    if exclude_special:
        all_transactions = [t for t in all_transactions if not t.get("is_special")]

    summary = compute_summary(all_transactions)
    community_summaries = _build_community_summaries(all_transactions)

    return jsonify(clean_nan({
        "success": True,
        "keyword": keyword,
        "search_type": search_type,
        "community_name": community_name,
        "location_mode": location_mode,
        "transactions": all_transactions,
        "community_summaries": community_summaries,
        "summary": summary,
        "total": len(all_transactions),
    }))


@app.route("/api/search_area", methods=["GET"])
def api_search_area():
    """地圖可視區域搜尋 API — 根據經緯度範圍搜尋"""
    try:
        south = float(request.args.get("south", 0))
        north = float(request.args.get("north", 0))
        west = float(request.args.get("west", 0))
        east = float(request.args.get("east", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "經緯度參數格式錯誤"}), 400

    if south == 0 and north == 0:
        return jsonify({"success": False, "error": "缺少經緯度範圍參數"}), 400

    location_mode = request.args.get("location_mode", "db").strip()
    limit = min(int(request.args.get("limit", 500)), 2000)
    filters = parse_filters_from_request()

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        where_clauses = [
            "lat BETWEEN ? AND ?",
            "lng BETWEEN ? AND ?",
            "lat IS NOT NULL",
            "lng IS NOT NULL",
        ]
        params = [south, north, west, east]
        where_clauses.extend(_build_filter_where(filters, params))
        where_sql = " AND ".join(where_clauses)
        sql = f"SELECT {SELECT_COLS} FROM land_transaction WHERE {where_sql} ORDER BY transaction_date DESC LIMIT ?"
        params.append(limit)

        cursor.execute(sql, params)
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()

        # batch OSM if needed
        osm_cache = batch_osm_geocode(rows) if location_mode == "osm" else None
        exclude_special = request.args.get("exclude_special", "").lower() in ("1", "true", "yes")
        all_transactions = [format_tx_row(r, location_mode, osm_cache) for r in rows]
        if exclude_special:
            all_transactions = [t for t in all_transactions if not t.get("is_special")]

        community_summaries = _build_community_summaries(all_transactions)
        summary = compute_summary(all_transactions)

        return jsonify(clean_nan({
            "success": True,
            "search_type": "area",
            "location_mode": location_mode,
            "transactions": all_transactions,
            "community_summaries": community_summaries,
            "summary": summary,
            "total": len(all_transactions),
        }))

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/address2community", methods=["GET"])
def api_address2community():
    """地址→社區名查詢"""
    address = request.args.get("address", "").strip()
    if not address:
        return jsonify({"success": False, "error": "缺少 address 參數"}), 400
    try:
        result = addr2com_lookup(address)
        if result:
            return jsonify({"success": True, "address": address, "result": result})
        return jsonify({"success": False, "address": address, "message": "未找到"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/community2address", methods=["GET"])
def api_community2address():
    """建案名→地址查詢"""
    community = request.args.get("community", "").strip()
    if not community:
        return jsonify({"success": False, "error": "缺少 community 參數"}), 400
    if not com2addr_ready or not com2addr_engine:
        return jsonify({"success": False, "error": "引擎尚未就緒"}), 503
    try:
        result = com2addr_engine.query(community, top_n=5)
        return jsonify({"success": True, "community": community, "result": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """系統統計"""
    stats = {}
    if com2addr_engine:
        stats.update(com2addr_engine.stats())
    stats["com2addr_ready"] = com2addr_ready
    stats["geocoder_ready"] = geocoder_ready
    stats["db_path"] = DB_PATH
    stats["db_exists"] = os.path.exists(DB_PATH)
    return jsonify({"success": True, **stats})


# ── 工具 ──

def _build_community_summaries(transactions: list) -> dict:
    """按建案名稱分組統計"""
    community_stats = {}
    for tx in transactions:
        cn = tx.get("community_name") or ""
        if cn:
            if cn not in community_stats:
                community_stats[cn] = {"count": 0, "prices": [], "unit_prices": [], "pings": [], "ratios": []}
            st = community_stats[cn]
            st["count"] += 1
            if tx.get("price", 0) > 0:
                st["prices"].append(tx["price"])
            if tx.get("unit_price_ping", 0) > 0:
                st["unit_prices"].append(tx["unit_price_ping"])
            if tx.get("area_ping", 0) > 0:
                st["pings"].append(tx["area_ping"])
            if tx.get("public_ratio", 0) > 0:
                st["ratios"].append(tx["public_ratio"])

    summaries = {}
    for cn, st in community_stats.items():
        summaries[cn] = {
            "count": st["count"],
            "avg_price": round(sum(st["prices"]) / len(st["prices"])) if st["prices"] else 0,
            "avg_unit_price_ping": round(sum(st["unit_prices"]) / len(st["unit_prices"]), 2) if st["unit_prices"] else 0,
            "avg_ping": round(sum(st["pings"]) / len(st["pings"]), 1) if st["pings"] else 0,
            "avg_ratio": round(sum(st["ratios"]) / len(st["ratios"]), 1) if st["ratios"] else 0,
        }
    return summaries


# ════════════════════════════════════════════════════════════════
# 啟動
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("🏢 良富居地產 v4.0 — API 伺服器")
    print("=" * 60)
    print(f"📁 資料庫: {DB_PATH}")
    print(f"🌐 http://localhost:5001")
    print("=" * 60)

    _build_community_coords_cache()

    t = threading.Thread(target=init_com2addr, daemon=True)
    t.start()

    t2 = threading.Thread(target=init_geocoder, daemon=True)
    t2.start()

    app.run(debug=False, host="0.0.0.0", port=5001)
