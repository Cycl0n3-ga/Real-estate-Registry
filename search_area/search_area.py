#!/usr/bin/env python3
"""
search_area.py — 區域搜尋與篩選模組

從 web/server.py 抽出的模組化元件，提供：
  - parse_filters: 從 dict args 解析篩選參數
  - build_filter_where: 建立篩選 WHERE 子句
  - search_by_community_name: 以建案名直查 DB
  - search_area: 依經緯度範圍搜尋交易
  - build_community_coords_cache: 建立建案平均座標快取
"""

import sqlite3
import threading
import time
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── 路徑設定 ──
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
DEFAULT_DB_PATH = str(LAND_DIR / "db" / "land_data.db")

# 共用模組
sys.path.insert(0, str(LAND_DIR))
from address_utils import parse_range

PING_TO_SQM = 3.30579

SELECT_COLS = """
    id, district, address, transaction_date, total_price, unit_price,
    building_area AS building_area_sqm, main_area AS main_building_area,
    attached_area, balcony_area, rooms, halls, bathrooms,
    floor_level, total_floors, building_type, main_use, main_material,
    build_date AS completion_date, elevator, has_management,
    parking_type, parking_price, parking_area AS parking_area_sqm,
    note, lat, lng, community_name
"""

# ── 連線快取（每執行緒獨立）──────────────────────────────────────────────────
_local = threading.local()


def _get_connection(db_path: str):
    """取得已優化的 SQLite 連線（per-thread 快取，避免跨執行緒存取）"""
    conns = getattr(_local, 'conns', None)
    if conns is None:
        _local.conns = {}
        conns = _local.conns
    if db_path in conns:
        return conns[db_path]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32768")      # 32MB cache
    conn.execute("PRAGMA mmap_size=268435456")     # 256MB mmap
    conn.execute("PRAGMA temp_store=MEMORY")
    conns[db_path] = conn
    return conn


def parse_filters(args: dict) -> dict:
    """
    從 dict (request.args 或任意 dict) 解析篩選參數

    Args:
        args: 參數 dict，key 為 building_type, rooms, public_ratio, year, ping,
              unit_price, price, exclude_special

    Returns:
        篩選條件 dict
    """
    filters = {}

    btype = args.get("building_type", "").strip() if isinstance(args.get("building_type"), str) else ""
    if btype:
        filters["building_types"] = [t.strip() for t in btype.split(",") if t.strip()]

    rooms = args.get("rooms", "").strip() if isinstance(args.get("rooms"), str) else ""
    if rooms:
        filters["rooms"] = [int(r) for r in rooms.split(",") if r.strip().isdigit()]

    for key, fmin, fmax in [
        ("public_ratio", "public_ratio_min", "public_ratio_max"),
        ("year", "year_min", "year_max"),
        ("ping", "ping_min", "ping_max"),
        ("unit_price", "unit_price_min", "unit_price_max"),
        ("price", "price_min", "price_max"),
    ]:
        val = args.get(key, "").strip() if isinstance(args.get(key), str) else ""
        if val:
            lo, hi = parse_range(val)
            if lo is not None:
                filters[fmin] = lo
            if hi is not None:
                filters[fmax] = hi

    exclude_sp = args.get("exclude_special", "")
    if isinstance(exclude_sp, str) and exclude_sp.lower() in ("1", "true", "yes"):
        filters["exclude_special"] = True

    return filters


def build_filter_where(filters: dict, params: list) -> list:
    """
    建立篩選 WHERE 子句（可被 area 搜尋、community 直查共用）

    Args:
        filters: 篩選條件 dict（parse_filters 回傳值）
        params: SQL 參數 list（會被就地 extend）

    Returns:
        WHERE 子句 list
    """
    clauses = []
    if filters.get("building_types"):
        like_parts = []
        for bt in filters["building_types"]:
            like_parts.append("building_type LIKE ?")
            params.append(f"%{bt}%")
        clauses.append("(" + " OR ".join(like_parts) + ")")
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


def search_by_community_name(
    community_name: str,
    filters: dict,
    limit: int = 500,
    db_path: str = None,
) -> list:
    """
    直接用 community_name 索引查詢 DB

    Args:
        community_name: 建案名稱
        filters: 篩選條件（parse_filters 回傳值）
        limit: 回傳上限
        db_path: 資料庫路徑

    Returns:
        list of dict
    """
    db = db_path or DEFAULT_DB_PATH
    params = [community_name]
    filter_clauses = build_filter_where(filters, params)
    where_sql = "community_name = ?" + (
        " AND " + " AND ".join(filter_clauses) if filter_clauses else ""
    )
    sql = f"SELECT {SELECT_COLS} FROM land_transaction WHERE {where_sql} ORDER BY transaction_date DESC LIMIT ?"
    params.append(limit)
    conn = _get_connection(db)
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return rows


def search_area(
    south: float,
    north: float,
    west: float,
    east: float,
    filters: dict = None,
    limit: int = 500,
    db_path: str = None,
) -> list:
    """
    依經緯度範圍搜尋交易

    Args:
        south, north, west, east: 經緯度邊界
        filters: 篩選條件 dict（parse_filters 回傳值）
        limit: 回傳上限
        db_path: 資料庫路徑

    Returns:
        list of dict
    """
    db = db_path or DEFAULT_DB_PATH
    filters = filters or {}

    where_clauses = [
        "lat BETWEEN ? AND ?",
        "lng BETWEEN ? AND ?",
        "lat IS NOT NULL",
        "lng IS NOT NULL",
    ]
    params = [south, north, west, east]
    where_clauses.extend(build_filter_where(filters, params))
    where_sql = " AND ".join(where_clauses)
    sql = f"SELECT {SELECT_COLS} FROM land_transaction WHERE {where_sql} ORDER BY transaction_date DESC LIMIT ?"
    params.append(limit)

    conn = _get_connection(db)
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return rows


def build_community_coords_cache(db_path: str = None) -> dict:
    """
    建立建案平均座標快取

    Args:
        db_path: 資料庫路徑

    Returns:
        dict: {community_name: (lat, lng)}
    """
    db = db_path or DEFAULT_DB_PATH
    try:
        t0 = time.time()
        conn = sqlite3.connect(db)
        cursor = conn.execute("""
            SELECT community_name, AVG(lat) AS avg_lat, AVG(lng) AS avg_lng
            FROM land_transaction
            WHERE community_name IS NOT NULL AND community_name != ''
              AND lat IS NOT NULL AND lat != 0
              AND lng IS NOT NULL AND lng != 0
            GROUP BY community_name
        """)
        cache = {row[0]: (row[1], row[2]) for row in cursor}
        conn.close()
        print(f"📍 建案座標快取: {len(cache)} 個建案 ({time.time()-t0:.2f}s)")
        return cache
    except Exception as e:
        print(f"⚠️  建案座標快取建立失敗: {e}")
        return {}
