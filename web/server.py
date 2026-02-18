#!/usr/bin/env python3
"""
良富居地產 v3.0 — 後端 API 伺服器
整合 address_search、com2address、address2com 模組
使用 Flask + SQLite (land_a.db)
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
LAND_REG_DIR = LAND_DIR / "land_reg"
ADDR_SEARCH_DIR = LAND_REG_DIR / "address_search"
COM2ADDR_DIR = LAND_DIR / "com2address"
ADDR2COM_DIR = LAND_DIR / "address2com"

# 將模組路徑加入 sys.path
for p in [str(ADDR_SEARCH_DIR), str(COM2ADDR_DIR), str(ADDR2COM_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# 匯入模組
from address_transfer import (
    search_address, generate_address_variants, parse_range,
    SORT_OPTIONS, fullwidth_to_halfwidth, halfwidth_to_fullwidth
)
from community2address import Community2AddressLookup
from address2community import lookup as addr2com_lookup

# ── Flask 設定 ────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder="static")
CORS(app)

DB_PATH = str(LAND_DIR / "db" / "land_a.db")
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

    # 座標：優先用 DB 中的座標，否則用行政區
    lat = row.get("lat")
    lng = row.get("lng")
    if not lat or not lng:
        lat, lng = get_district_coords(district)
    # 加入隨機偏移（用地址 hash）避免完全重疊
    if lat and lng:
        addr = str(row.get("address", ""))
        h = abs(hash(addr + date_raw))
        lat = lat + ((h % 1000) - 500) * 0.00005
        lng = lng + (((h >> 10) % 1000) - 500) * 0.00005

    return {
        "address": str(row.get("address", "") or ""),
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

    sort_by = request.args.get("sort", "date").strip()
    if sort_by not in SORT_OPTIONS and sort_by != "count":
        sort_by = "date"
    limit = min(int(request.args.get("limit", 200)), 1000)
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

    # 排序
    sort_map = {
        "date": lambda t: t.get("date_raw", ""),
        "price": lambda t: t.get("price", 0),
        "unit_price": lambda t: t.get("unit_price_ping", 0),
        "ping": lambda t: t.get("area_ping", 0),
        "public_ratio": lambda t: t.get("public_ratio", 999),
    }
    
    if sort_by == "count":
        # 計算每個地址的交易筆數，然後排序
        addr_count = {}
        for tx in all_transactions:
            addr = tx.get("address", "")
            addr_count[addr] = addr_count.get(addr, 0) + 1
        sort_fn = lambda t: -addr_count.get(t.get("address", ""), 0)
        all_transactions.sort(key=sort_fn)
    else:
        sort_fn = sort_map.get(sort_by, sort_map["date"])
        reverse = sort_by != "public_ratio"
        all_transactions.sort(key=sort_fn, reverse=reverse)
    all_transactions = all_transactions[:limit]

    summary = compute_summary(all_transactions)

    return jsonify(clean_nan({
        "success": True,
        "keyword": keyword,
        "search_type": search_type,
        "community_name": community_name,
        "transactions": all_transactions,
        "summary": summary,
        "total": len(all_transactions),
    }))


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
    print("🏢 良富居地產 v3.0 — 新版前端伺服器")
    print("=" * 60)
    print(f"📁 資料庫: {DB_PATH}")
    print(f"📁 com2address: {COM2ADDR_DIR}")
    print(f"📁 address2com: {ADDR2COM_DIR}")
    print(f"🌐 http://localhost:5001")
    print("=" * 60)

    t = threading.Thread(target=init_com2addr, daemon=True)
    t.start()

    app.run(debug=False, host="0.0.0.0", port=5001)
