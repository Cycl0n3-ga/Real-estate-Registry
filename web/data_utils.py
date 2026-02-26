#!/usr/bin/env python3
"""
data_utils.py — 資料格式化與統計工具
從 server.py 抽出的模組化元件
"""

import re
import math
import time
from typing import Optional

PING_TO_SQM = 3.30579

# 特殊交易關鍵字（用於 note 欄位判斷）
SPECIAL_TX_KEYWORDS = [
    '親友', '員工', '共有人', '特殊關係', '利害關係',
    '調協', '欻欄', '法拍', '濟助', '社會住宅',
    '总價顯著偏低', '價格顯著偏高',
    '政府機關', '建商與地主',
    '債權債務', '繼承',
    '急買急賣', '受債權人',
]

# 去除地址中的縣市前綴（顯示用）
_CITY_RE = re.compile(r'^(?:(?:台|臺)(?:北|中|南|東)市|(?:新北|桃園|高雄|基隆|新竹|嘉義)[市縣]|[^\s]{2,3}縣)')
# 修正重複行政區 e.g. "松山區松山區" → "松山區"
_DUP_DIST_RE = re.compile(r'([\u4e00-\u9fff]{2,3}[區鎮鄉市])\1')


def clean_nan(obj):
    """遞迴清理 NaN/Infinity"""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0
    return obj


def format_roc_date(roc_date) -> Optional[str]:
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


def strip_city(addr: str) -> str:
    """去除地址中的縣市前綴，保留行政區以下"""
    if not addr:
        return addr
    addr = _DUP_DIST_RE.sub(r'\1', addr)
    return _CITY_RE.sub('', addr)


def is_special_transaction(note: str) -> bool:
    """判斷是否為特殊交易（根據備忘錄）"""
    if not note:
        return False
    for kw in SPECIAL_TX_KEYWORDS:
        if kw in note:
            return True
    return False


def format_tx_row(row: dict, location_mode: str = "osm",
                  osm_cache: dict = None,
                  normalize_address_fn=None,
                  community_coords_cache: dict = None) -> dict:
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
    if normalize_address_fn and address_raw:
        address_display = strip_city(normalize_address_fn(address_raw))
    else:
        address_display = strip_city(address_raw) if address_raw else ""
    community_name_raw = str(row.get("community_name", "") or "")
    note = str(row.get("note", "") or "")
    special = is_special_transaction(note)

    # 車位
    parking_type_raw = str(row.get("parking_type", "") or "")
    has_parking = bool(parking_type_raw and parking_type_raw != "無")

    # ── 座標策略 ──
    lat = None
    lng = None
    coord_source = "none"

    db_lat = row.get("lat")
    db_lng = row.get("lng")
    has_db = db_lat and db_lng and db_lat != 0 and db_lng != 0

    if location_mode == "osm":
        if osm_cache and address_raw in osm_cache:
            lat, lng = osm_cache[address_raw]
            coord_source = "osm"
        elif has_db:
            lat, lng = db_lat, db_lng
            coord_source = "db"
    elif location_mode == "db":
        if has_db:
            lat, lng = db_lat, db_lng
            coord_source = "db"
        elif community_name_raw and community_coords_cache and community_name_raw in community_coords_cache:
            lat, lng = community_coords_cache[community_name_raw]
            coord_source = "community"
    else:
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


def build_community_summaries(transactions: list) -> dict:
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


def batch_osm_geocode(rows: list, geocoder_engine) -> dict:
    """
    批次 OSM 地理編碼 — 直接使用本地 OSM 索引，單連線批次查詢
    比逐筆 geocode() 快 ~100 倍

    Returns: {address_raw: (lat, lng), ...}
    """
    if geocoder_engine is None:
        return {}
    if not geocoder_engine.osm_index.is_available():
        return {}

    normalizer = geocoder_engine.normalizer

    # Step 1: 收集唯一地址並正規化
    norm_to_orig = {}   # normalized_base → original_address
    unique_addrs = set()
    seen_orig = set()
    for r in rows:
        addr = str(r.get('address', '') or '')
        district = str(r.get('district', '') or '')
        if not addr or addr in seen_orig:
            continue
        seen_orig.add(addr)
        try:
            full = normalizer.build_full_address(addr, district)
            if not full:
                continue
            base = normalizer.extract_base_address(full) or full
            if base not in norm_to_orig:
                norm_to_orig[base] = addr
                unique_addrs.add(base)
        except Exception:
            pass

    if not unique_addrs:
        return {}

    # Step 2: 單連線批次查詢 OSM 索引（毫秒級）
    t0 = time.time()
    osm_results = geocoder_engine.osm_index.batch_geocode(list(unique_addrs))

    # Step 3: 映射回原始地址
    results = {}
    for base, coord in osm_results.items():
        orig = norm_to_orig.get(base)
        if orig and coord:
            results[orig] = (coord['lat'], coord['lng'])

    elapsed = time.time() - t0
    print(f"📍 OSM 批次定位: {len(unique_addrs)} 唯一地址 → {len(results)} 命中 ({elapsed:.2f}s)")
    return results
