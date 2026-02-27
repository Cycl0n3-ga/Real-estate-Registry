#!/usr/bin/env python3
"""
良富居地產 v4.3 — 後端 API 伺服器
整合 address_match、com2address、address2com、OSM geocoding
使用 Flask + SQLite (land_data.db)

v4.3 改動:
- 雙圈 SVG marker（外環總價 + 內圈單價，使用者可自訂）
- 近兩年價格分析（排除特殊交易）顯示在圈內
- 手機版優化（響應式 + 自動收合 + 觸控友善）
- Flask Compress (gzip/brotli) 加速 API 回應
- lat/lng DB 索引加速區域搜尋
- 設定面板（localStorage 持久化）
"""

import os
import sys
import re
import time
import math
import threading
import sqlite3
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from flask_compress import Compress

# ── 路徑設定 ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent                # land/web
LAND_DIR = BASE_DIR.parent                      # land
ADDR_MATCH_DIR = LAND_DIR / "address_match"
COM2ADDR_DIR = LAND_DIR / "com2address"
ADDR2COM_DIR = LAND_DIR / "address2com"
GEODECODING_DIR = LAND_DIR / "geodecoding"

# 將模組路徑加入 sys.path
for p in [str(BASE_DIR), str(ADDR_MATCH_DIR), str(COM2ADDR_DIR),
          str(ADDR2COM_DIR), str(GEODECODING_DIR), str(LAND_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# ── 匯入模組 ─────────────────────────────────────────────────────────────────
from address_match import search_address, parse_range, SORT_OPTIONS
from address_utils import fullwidth_to_halfwidth, normalize_address, parse_query
from community2address import Community2AddressLookup
from address2community import lookup as addr2com_lookup
from geocoder import TaiwanGeocoder
from data_utils import (
    clean_nan, format_roc_date, strip_city, is_special_transaction,
    format_tx_row, compute_summary, build_community_summaries,
    batch_osm_geocode, PING_TO_SQM,
)
from search_area import (
    search_area as _search_area_db,
    search_by_community_name as _search_by_community_name_db,
    build_filter_where as _build_filter_where,
    SELECT_COLS,
)
from com_match import CommunityMatcher

# ── Flask 設定 ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder="static")
CORS(app)
Compress(app)
app.config['COMPRESS_MIMETYPES'] = ['application/json', 'text/html', 'text/css', 'application/javascript']

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

# ── 全域資料 ──────────────────────────────────────────────────────────────────
com2addr_engine = None
com2addr_ready = False
geocoder_engine = None
geocoder_ready = Falsecom_matcher = None         # CommunityMatcher 建案模糊搜尋引擎
com_matcher_ready = False_community_coords_cache = {}  # community_name → (lat, lng)
_search_cache = {}             # cache_key → (result_json, timestamp)
_CACHE_TTL = 180               # 3 分鐘快取


def _get_cached(cache_key):
    """取得快取結果（None 表示未命中）"""
    if cache_key in _search_cache:
        result, ts = _search_cache[cache_key]
        if time.time() - ts < _CACHE_TTL:
            return result
        del _search_cache[cache_key]
    return None


def _set_cache(cache_key, result):
    """設定快取"""
    _search_cache[cache_key] = (result, time.time())
    # 限制快取大小
    if len(_search_cache) > 200:
        now = time.time()
        expired = [k for k, (_, ts) in _search_cache.items() if now - ts > _CACHE_TTL]
        for k in expired:
            del _search_cache[k]


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


def init_com_matcher():
    """背景初始化建案模糊搜尋引擎"""
    global com_matcher, com_matcher_ready
    try:
        print("🔍 載入 CommunityMatcher...")
        com_matcher = CommunityMatcher(DB_PATH)
        com_matcher_ready = True
        print("✅ CommunityMatcher 就緒")
    except Exception as e:
        print(f"⚠️  CommunityMatcher 載入失敗: {e}")
        import traceback; traceback.print_exc()
        com_matcher_ready = True


# ── 工具函式（本地專用，未移至 data_utils）─────────────────────────────────────

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


# ── 篩選/查詢委派至 search_area 模組 ─────────────────────────────────────────
def _search_by_community_name(community_name: str, filters: dict, limit: int) -> list:
    """直接用 community_name 索引查詢 DB（委派至 search_area 模組）"""
    return _search_by_community_name_db(community_name, filters, limit, db_path=DB_PATH)


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
      community      - 直接指定建案名稱（優先使用，跳過模糊搜尋）
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
    exclude_special = request.args.get("exclude_special", "").lower() in ("1", "true", "yes")

    # 快取鍵（用 request query string 最簡單）
    cache_key = f"search:{request.query_string.decode('utf-8', errors='ignore')}"
    cached = _get_cached(cache_key)
    if cached:
        return jsonify(cached)

    community_name = None
    search_type = "address"

    # ════════════ 路徑 0: 明確指定建案名稱（前端選擇建案後直接傳入）════════════
    specified_community = request.args.get("community", "").strip()
    com_raw_rows = []
    if specified_community:
        community_name = specified_community
        search_type = "community"
        com_raw_rows = _search_by_community_name(community_name, filters, limit)
        print(f"🏘️  直接建案搜尋: {community_name} → {len(com_raw_rows)} 筆")

    # ════════════ 路徑 A: com2address — 把 keyword 當建案名搜尋 ════════════
    elif com2addr_ready and com2addr_engine:
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
    osm_cache = batch_osm_geocode(merged_raw, geocoder_engine) if location_mode == "osm" else None

    # 格式化（含座標策略）
    all_transactions = [format_tx_row(r, location_mode, osm_cache, normalize_address, _community_coords_cache) for r in merged_raw]
    if exclude_special:
        all_transactions = [t for t in all_transactions if not t.get("is_special")]

    summary = compute_summary(all_transactions)
    community_summaries = build_community_summaries(all_transactions)

    result_data = clean_nan({
        "success": True,
        "keyword": keyword,
        "search_type": search_type,
        "community_name": community_name,
        "location_mode": location_mode,
        "transactions": all_transactions,
        "community_summaries": community_summaries,
        "summary": summary,
        "total": len(all_transactions),
    })
    _set_cache(cache_key, result_data)
    return jsonify(result_data)


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
        rows = _search_area_db(south, north, west, east, filters, limit, db_path=DB_PATH)

        # batch OSM if needed
        osm_cache = batch_osm_geocode(rows, geocoder_engine) if location_mode == "osm" else None
        exclude_special = request.args.get("exclude_special", "").lower() in ("1", "true", "yes")
        all_transactions = [format_tx_row(r, location_mode, osm_cache, normalize_address, _community_coords_cache) for r in rows]
        if exclude_special:
            all_transactions = [t for t in all_transactions if not t.get("is_special")]

        community_summaries = build_community_summaries(all_transactions)
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
    stats["com_matcher_ready"] = com_matcher_ready
    stats["com_matcher_count"] = com_matcher.stats()["total_communities"] if com_matcher else 0
    stats["db_path"] = DB_PATH
    stats["db_exists"] = os.path.exists(DB_PATH)
    return jsonify({"success": True, **stats})


@app.route("/api/com_match", methods=["GET"])
def api_com_match():
    """
    建案名稱模糊搜尋 API

    參數:
      keyword  - 建案名稱關鍵字
      top_n    - 回傳筆數 (預設 15)
    """
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "error": "缺少 keyword 參數"}), 400
    if not com_matcher_ready or not com_matcher:
        return jsonify({"success": False, "error": "建案搜尋引擎尚未就緒"}), 503
    try:
        top_n = min(int(request.args.get("top_n", 15)), 50)
        results = com_matcher.search(keyword, top_n=top_n)
        return jsonify({"success": True, "keyword": keyword, "results": results})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ════════════════════════════════════════════════════════════════
# 啟動
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("🏢 良富居地產 v4.3 — API 伺服器")
    print("=" * 60)
    print(f"📁 資料庫: {DB_PATH}")
    print(f"🌐 http://localhost:5001")
    print("=" * 60)

    _build_community_coords_cache()

    t = threading.Thread(target=init_com2addr, daemon=True)
    t.start()

    t2 = threading.Thread(target=init_geocoder, daemon=True)
    t2.start()

    t3 = threading.Thread(target=init_com_matcher, daemon=True)
    t3.start()

    app.run(debug=False, host="0.0.0.0", port=5001)
