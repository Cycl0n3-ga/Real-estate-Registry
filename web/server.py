#!/usr/bin/env python3
"""
良富居地產 v3.0 — 後端 API 伺服器
整合 address_match、com2address、address2com 模組
使用 Flask + SQLite (land_data.db)
"""

import os
import sys
import re
import json
import hashlib
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
from address_match import (
    search_address, generate_address_variants, parse_range,
    SORT_OPTIONS,
)
from address_utils import fullwidth_to_halfwidth, halfwidth_to_fullwidth
from community2address import Community2AddressLookup
from address2community import lookup as addr2com_lookup
from geocoder import TaiwanGeocoder

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

# ── 行政區座標映射 ────────────────────────────────────────────────────────────
DISTRICT_COORDS = {
    '中壢區': (24.9696, 120.9843), '桃園區': (25.0330, 121.3167),
    '新竹市': (24.8026, 120.9693), '北屯區': (24.2169, 120.7901),
    '淡水區': (25.1654, 121.4529), '板橋區': (25.0121, 121.4627),
    '西屯區': (24.1884, 120.6350), '新莊區': (25.0568, 121.4315),
    '竹北市': (24.8363, 120.9863), '中和區': (25.0049, 121.4935),
    '北投區': (25.1370, 121.5130), '中山區': (25.0455, 121.5149),
    '大安區': (25.0330, 121.5254), '松山區': (25.0487, 121.5623),
    '南港區': (25.0543, 121.6090), '信義區': (25.0330, 121.5654),
    '內湖區': (25.0850, 121.5788), '士林區': (25.1122, 121.5254),
    '大同區': (25.0737, 121.5149), '文山區': (25.0035, 121.5674),
    '萬華區': (25.0329, 121.5004), '中正區': (25.0320, 121.5198),
    '三重區': (25.0617, 121.4879), '蘆洲區': (25.0855, 121.4738),
    '汐止區': (25.0626, 121.6610), '永和區': (25.0076, 121.5138),
    '三峽區': (24.9340, 121.3687), '土城區': (24.9723, 121.4437),
    '新店區': (24.9677, 121.5419), '林口區': (25.0786, 121.3919),
    '五股區': (25.0787, 121.4380), '泰山區': (25.0500, 121.4300),
    '樹林區': (24.9909, 121.4200), '鶯歌區': (24.9519, 121.3517),
    '八里區': (25.1400, 121.4000), '深坑區': (25.0020, 121.6155),
    '左營區': (22.6847, 120.2940), '前鎮區': (22.5955, 120.3268),
    '三民區': (22.6467, 120.3165), '鼓山區': (22.6555, 120.2710),
    '苓雅區': (22.6200, 120.3260), '楠梓區': (22.7308, 120.3262),
    '小港區': (22.5647, 120.3456), '鳳山區': (22.6268, 120.3595),
    '南屯區': (24.1003, 120.6684), '豐原區': (24.2444, 120.7181),
    '大里區': (24.0995, 120.6780), '太平區': (24.1456, 120.9383),
    '烏日區': (24.0630, 120.6717), '潭子區': (24.1995, 120.8610),
    '大雅區': (24.2575, 120.7870), '神岡區': (24.2456, 120.8080),
    '沙鹿區': (24.2330, 120.5699), '清水區': (24.2583, 120.5689),
    '梧棲區': (24.2495, 120.5439), '龍井區': (24.2507, 120.5690),
    '大肚區': (24.2250, 120.5519), '后里區': (24.3185, 120.7436),
    '霧峰區': (24.0580, 120.8225), '永康區': (22.9896, 120.2440),
    '安南區': (23.0468, 120.1853), '安平區': (22.9927, 120.1659),
    '東區_台南': (22.9798, 120.2252), '北區_台南': (23.0030, 120.2080),
    '南區_台南': (22.9600, 120.1980), '中西區': (22.9920, 120.2000),
    '善化區': (23.1310, 120.2978), '新化區': (23.0383, 120.3119),
    '仁德區': (22.9385, 120.2545), '歸仁區': (22.9049, 120.3027),
    '龍潭區': (24.8642, 121.2163), '楊梅區': (24.9077, 121.1449),
    '大溪區': (24.8832, 121.2863), '蘆竹區': (25.0439, 121.2917),
    '大園區': (25.0647, 121.2333), '龜山區': (25.0287, 121.3453),
    '八德區': (24.9456, 121.2900), '平鎮區': (24.9459, 121.2182),
    '竹東鎮': (24.7310, 121.0900), '新豐鄉': (24.8900, 120.9700),
    '湖口鄉': (24.9023, 121.0400), '竹南鎮': (24.6850, 120.8780),
    '頭份市': (24.6880, 120.9030), '基隆市': (25.1276, 121.7347),
    '屏東市': (22.6727, 120.4886), '宜蘭市': (24.7518, 121.7580),
    '羅東鎮': (24.6775, 121.7667), '花蓮市': (23.9768, 121.6044),
    '台東市': (22.7563, 121.1438), '斗六市': (23.7072, 120.5448),
    '彰化市': (24.0827, 120.5417), '員林市': (23.9590, 120.5740),
    '南投市': (23.9120, 120.6672), '草屯鎮': (23.9740, 120.6800),
    '新營區': (23.3032, 120.3031), '麻豆區': (23.1793, 120.2411),
    '鹽水區': (23.2832, 120.2788), '前金區': (22.6266, 120.2952),
    '新興區': (22.6296, 120.3090), '鹽埕區': (22.6230, 120.2836),
    '大寮區': (22.5965, 120.3987), '鳥松區': (22.6620, 120.3647),
    '仁武區': (22.7002, 120.3520), '岡山區': (22.7906, 120.2953),
    '路竹區': (22.8561, 120.2617), '橋頭區': (22.7575, 120.3058),
    '西區': (24.1400, 120.6600), '東區': (24.1400, 120.7000),
    '北區': (24.1650, 120.6800), '南區': (24.1200, 120.6600),
    '七堵區': (25.0930, 121.7180), '暖暖區': (25.0970, 121.7390),
    '仁愛區': (25.1200, 121.7360), '安樂區': (25.1340, 121.7220),
    '中正區_基隆': (25.1300, 121.7400), '信義區_基隆': (25.1170, 121.7660),
    '觀音區': (25.0340, 121.1640), '新屋區': (24.9740, 121.1040),
    '復興區': (24.7400, 121.3530),
}


def get_district_coords(district):
    """取得行政區座標"""
    if not district:
        return None, None
    d = str(district).strip()
    if d in DISTRICT_COORDS:
        return DISTRICT_COORDS[d]
    # 模糊匹配
    for k, v in DISTRICT_COORDS.items():
        if d in k or k in d:
            return v
    return None, None


# ── 全域資料 ──────────────────────────────────────────────────────────────────
com2addr_engine = None
com2addr_ready = False
geocoder_engine = None
geocoder_ready = False


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


def get_address_coords(address: str, district: str = "") -> tuple:
    """
    使用 OSM Nominatim 地理編碼取得準確座標
    
    Args:
        address: 完整地址
        district: 行政區（輔助用）
        
    Returns:
        (lat, lng, source_level) 元組，其中 source_level 為 'exact'|'road'|'district'|None
    """
    global geocoder_engine, geocoder_ready
    
    if not geocoder_ready or geocoder_engine is None:
        return None, None, None
    
    try:
        result = geocoder_engine.geocode(address, district=district)
        if result and 'lat' in result and 'lng' in result:
            level = result.get('level', 'unknown')  # 'exact', 'road', 'district' 等
            return result['lat'], result['lng'], level
    except Exception as e:
        # 靜默失敗，回退到行政區座標
        pass
    
    return None, None, None


def format_tx_row(row: dict) -> dict:
    """將 address_search 回傳的 row 轉為前端友好格式"""
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
    address = str(row.get("address", "") or "")

    # 座標：優先用 OSM Geocoding，其次用 DB 中的座標，最後用行政區
    lat = None
    lng = None
    coord_source = "unknown"
    
    # 優先嘗試 OSM Geocoding
    if geocoder_ready and geocoder_engine is not None and address:
        geocoded_lat, geocoded_lng, geocoded_source = get_address_coords(address, district)
        if geocoded_lat and geocoded_lng:
            lat = geocoded_lat
            lng = geocoded_lng
            coord_source = geocoded_source or "osm"
    
    # 回退：DB 中的座標
    if not lat or not lng:
        lat = row.get("lat")
        lng = row.get("lng")
        if lat and lng:
            coord_source = "db_cache"
    
    # 回退：行政區座標
    if not lat or not lng:
        lat, lng = get_district_coords(district)
        coord_source = "district"
    
    # 只在座標來自行政區時才加折疊偏移
    # OSM 精確座標不需要偏移，DB 快取也不需要
    if lat and lng and coord_source == "district":
        # 使用確定的折疊方式（基於地址 hash）而不是隨機
        h = abs(hash(address + date_raw))
        # 折疊偏移：确保同一地址每次都是同樣的偏移，但不同地址微小不同
        lat = lat + ((h % 1000) - 500) * 0.00005
        lng = lng + (((h >> 10) % 1000) - 500) * 0.00005

    return {
        "address": address,
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
        "note": str(row.get("note", "") or ""),
        "community_name": str(row.get("community_name", "") or ""),
        "lat": lat,
        "lng": lng,
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
    統一搜尋 API

    參數:
      keyword       - 搜尋關鍵字（必要）
      sort          - date|price|count|unit_price|ping|public_ratio (預設 date)
      limit         - 回傳上限 (預設 200)
      building_type - 建物型態 (逗號分隔)
      rooms         - 房數 (逗號分隔)
      public_ratio  - 公設比範圍 (如 0-35)
      year          - 年份範圍 (如 110-114)
      ping          - 坪數範圍 (如 20-40)
      unit_price    - 單坪價範圍 (如 60-120, 萬/坪)
      price         - 總價範圍 (如 1000-3000, 萬元)
    """
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"success": False, "error": "缺少 keyword 參數"}), 400

    sort_by = "date"  # 排序交給前端處理，後端固定用日期排序
    limit = min(int(request.args.get("limit", 500)), 2000)
    filters = parse_filters_from_request()

    search_type = "address"
    community_name = None
    search_addresses = []

    # ── Step 1: 嘗試用 com2address（是否為建案名稱？）──
    if com2addr_ready and com2addr_engine:
        try:
            com_result = com2addr_engine.query(keyword, top_n=3)
            if com_result.get("found") and com_result.get("match_type") != "未找到":
                mt = com_result.get("match_type", "")
                if "精確" in mt or (com_result.get("address_range", {}).get("total_addresses", 0) > 0):
                    search_type = "community"
                    community_name = com_result.get("matched_name", keyword)
                    raw_addrs = com_result.get("address_range", {}).get("raw_addresses", [])
                    if raw_addrs:
                        search_addresses = raw_addrs
                        print(f"🏘️  建案搜尋: {keyword} → {community_name} ({len(search_addresses)} 地址)")
        except Exception as e:
            print(f"⚠️  com2address 查詢錯誤: {e}")

    # ── Step 2: 嘗試 address2community（地址→建案）──
    if search_type == "address" and not search_addresses:
        try:
            a2c_result = addr2com_lookup(keyword)
            if a2c_result and isinstance(a2c_result, dict):
                best_name = a2c_result.get("best", "")
                if not best_name and a2c_result.get("results"):
                    for r in a2c_result["results"]:
                        if isinstance(r, dict) and r.get("community"):
                            best_name = r["community"]
                            break

                if best_name and com2addr_engine:
                    print(f"📍 地址→建案: {keyword} → {best_name}")
                    try:
                        com_result2 = com2addr_engine.query(best_name, top_n=3)
                        if com_result2.get("found"):
                            search_type = "address_to_community"
                            community_name = com_result2.get("matched_name", best_name)
                            raw_addrs2 = com_result2.get("address_range", {}).get("raw_addresses", [])
                            if raw_addrs2:
                                search_addresses = raw_addrs2
                                print(f"   → 建案地址: {len(search_addresses)} 個")
                    except Exception as e2:
                        print(f"   ⚠️  反查地址失敗: {e2}")
        except Exception as e:
            print(f"⚠️  address2community 查詢錯誤: {e}")

    # ── Step 3: 用 address_search 搜尋房價 ──
    all_transactions = []

    if search_addresses:
        seen_ids = set()
        for addr in search_addresses[:30]:
            try:
                result = search_address(
                    addr, db_path=DB_PATH,
                    filters=filters, sort_by=sort_by,
                    limit=100, show_sql=False
                )
                for row in result.get("results", []):
                    row_id = row.get("id")
                    if row_id and row_id not in seen_ids:
                        seen_ids.add(row_id)
                        all_transactions.append(format_tx_row(row))
            except Exception as e:
                print(f"  ⚠️  搜尋 {addr} 失敗: {e}")

    # fallback: 直接用關鍵字搜
    if not all_transactions:
        try:
            result = search_address(
                keyword, db_path=DB_PATH,
                filters=filters, sort_by=sort_by,
                limit=limit, show_sql=False
            )
            all_transactions = [format_tx_row(r) for r in result.get("results", [])]
            if not community_name:
                search_type = "address"
        except Exception as e:
            print(f"⚠️  address_search 錯誤: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    # 截斷到 limit（排序由前端負責）
    all_transactions = all_transactions[:limit]

    summary = compute_summary(all_transactions)

    # 按建案名稱分組統計
    community_stats = {}
    for tx in all_transactions:
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

    community_summaries = {}
    for cn, st in community_stats.items():
        community_summaries[cn] = {
            "count": st["count"],
            "avg_price": round(sum(st["prices"]) / len(st["prices"])) if st["prices"] else 0,
            "avg_unit_price_ping": round(sum(st["unit_prices"]) / len(st["unit_prices"]), 2) if st["unit_prices"] else 0,
            "avg_ping": round(sum(st["pings"]) / len(st["pings"]), 1) if st["pings"] else 0,
            "avg_ratio": round(sum(st["ratios"]) / len(st["ratios"]), 1) if st["ratios"] else 0,
        }

    return jsonify(clean_nan({
        "success": True,
        "keyword": keyword,
        "search_type": search_type,
        "community_name": community_name,
        "transactions": all_transactions,
        "community_summaries": community_summaries,
        "summary": summary,
        "total": len(all_transactions),
    }))


@app.route("/api/search_area", methods=["GET"])
def api_search_area():
    """
    地圖可視區域搜尋 API — 根據經緯度範圍搜尋成交紀錄

    參數:
      south, north, west, east  - 經緯度邊界（必要）
      limit                     - 回傳上限 (預設 500)
      building_type, rooms, public_ratio, year, ping, unit_price, price - 篩選
    """
    try:
        south = float(request.args.get("south", 0))
        north = float(request.args.get("north", 0))
        west = float(request.args.get("west", 0))
        east = float(request.args.get("east", 0))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "經緯度參數格式錯誤"}), 400

    if south == 0 and north == 0:
        return jsonify({"success": False, "error": "缺少經緯度範圍參數"}), 400

    limit = min(int(request.args.get("limit", 500)), 2000)
    filters = parse_filters_from_request()

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 建立基礎 SQL
        where_clauses = [
            "lat >= ? AND lat <= ?",
            "lng >= ? AND lng <= ?",
            "lat IS NOT NULL",
            "lng IS NOT NULL",
        ]
        params = [south, north, west, east]

        # 套用篩選條件
        if filters.get("building_types"):
            placeholders = ",".join(["?"] * len(filters["building_types"]))
            where_clauses.append(f"building_type IN ({placeholders})")
            params.extend(filters["building_types"])

        if filters.get("rooms"):
            placeholders = ",".join(["?"] * len(filters["rooms"]))
            where_clauses.append(f"rooms IN ({placeholders})")
            params.extend(filters["rooms"])

        if filters.get("public_ratio_min") is not None or filters.get("public_ratio_max") is not None:
            where_clauses.append("building_area > 0 AND main_area > 0")
            pr_expr = "CAST((building_area - main_area - COALESCE(attached_area,0) - COALESCE(balcony_area,0)) * 100.0 / building_area AS REAL)"
            if filters.get("public_ratio_min") is not None:
                where_clauses.append(f"{pr_expr} >= ?")
                params.append(float(filters["public_ratio_min"]))
            if filters.get("public_ratio_max") is not None:
                where_clauses.append(f"{pr_expr} <= ?")
                params.append(float(filters["public_ratio_max"]))

        if filters.get("year_min") is not None:
            where_clauses.append("CAST(SUBSTR(transaction_date, 1, 3) AS INTEGER) >= ?")
            params.append(int(filters["year_min"]))

        if filters.get("year_max") is not None:
            where_clauses.append("CAST(SUBSTR(transaction_date, 1, 3) AS INTEGER) <= ?")
            params.append(int(filters["year_max"]))

        if filters.get("ping_min") is not None:
            where_clauses.append("building_area >= ?")
            params.append(float(filters["ping_min"]) * PING_TO_SQM)

        if filters.get("ping_max") is not None:
            where_clauses.append("building_area <= ?")
            params.append(float(filters["ping_max"]) * PING_TO_SQM)

        if filters.get("unit_price_min") is not None:
            where_clauses.append("unit_price >= ?")
            params.append(float(filters["unit_price_min"]) * 10000 / PING_TO_SQM)

        if filters.get("unit_price_max") is not None:
            where_clauses.append("unit_price <= ?")
            params.append(float(filters["unit_price_max"]) * 10000 / PING_TO_SQM)

        if filters.get("price_min") is not None:
            where_clauses.append("total_price >= ?")
            params.append(float(filters["price_min"]) * 10000)

        if filters.get("price_max") is not None:
            where_clauses.append("total_price <= ?")
            params.append(float(filters["price_max"]) * 10000)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT id, district, address, transaction_date, total_price, unit_price,
                   building_area AS building_area_sqm, main_area AS main_building_area,
                   attached_area, balcony_area, rooms, halls, bathrooms,
                   floor_level, total_floors, building_type, main_use, main_material,
                   build_date AS completion_date, elevator, has_management,
                   parking_type, parking_price, parking_area AS parking_area_sqm,
                   note, lat, lng, community_name
            FROM land_transaction
            WHERE {where_sql}
            ORDER BY transaction_date DESC
            LIMIT ?
        """
        params.append(limit)

        cursor.execute(sql, params)
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()

        # 格式化
        all_transactions = []
        for row in rows:
            tx = format_tx_row(row)
            tx["community_name"] = row.get("community_name") or ""
            all_transactions.append(tx)

        # 按建案名稱分組統計
        community_stats = {}
        for tx in all_transactions:
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

        # 計算每個建案統計
        community_summaries = {}
        for cn, st in community_stats.items():
            community_summaries[cn] = {
                "count": st["count"],
                "avg_price": round(sum(st["prices"]) / len(st["prices"])) if st["prices"] else 0,
                "avg_unit_price_ping": round(sum(st["unit_prices"]) / len(st["unit_prices"]), 2) if st["unit_prices"] else 0,
                "avg_ping": round(sum(st["pings"]) / len(st["pings"]), 1) if st["pings"] else 0,
                "avg_ratio": round(sum(st["ratios"]) / len(st["ratios"]), 1) if st["ratios"] else 0,
            }

        summary = compute_summary(all_transactions)

        return jsonify(clean_nan({
            "success": True,
            "search_type": "area",
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
    stats["db_path"] = DB_PATH
    stats["db_exists"] = os.path.exists(DB_PATH)
    return jsonify({"success": True, **stats})


# ══════════════════════════════════════════════════════════════════════════════
# 啟動
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("🏢 良富居地產 v3.0 — API 伺服器")
    print("=" * 60)
    print(f"📁 資料庫: {DB_PATH}")
    print(f"📁 com2address: {COM2ADDR_DIR}")
    print(f"📁 address2com: {ADDR2COM_DIR}")
    print(f"🌐 http://localhost:5001")
    print("=" * 60)

    t = threading.Thread(target=init_com2addr, daemon=True)
    t.start()
    
    t2 = threading.Thread(target=init_geocoder, daemon=True)
    t2.start()

    app.run(debug=False, host="0.0.0.0", port=5001)
