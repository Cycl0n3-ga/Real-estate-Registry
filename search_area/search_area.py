#!/usr/bin/env python3
"""
search_area.py — 區域搜尋模組

從 web/server.py 抽出的模組化元件，提供：
  - build_filter_where: 建立篩選 WHERE 子句
  - search_by_community_name: 以建案名直查 DB
  - search_area: 依經緯度範圍搜尋交易
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── 路徑設定 ──
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
DEFAULT_DB_PATH = str(LAND_DIR / "db" / "land_data.db")

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


def build_filter_where(filters: dict, params: list) -> list:
    """
    建立篩選 WHERE 子句（可被 area 搜尋、community 直查共用）

    Args:
        filters: 篩選條件 dict
        params: SQL 參數 list（會被就地 extend）

    Returns:
        WHERE 子句 list
    """
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
        filters: 篩選條件
        limit: 回傳上限
        db_path: 資料庫路徑

    Returns:
        list of dict（原始 row）
    """
    db = db_path or DEFAULT_DB_PATH
    params = [community_name]
    filter_clauses = build_filter_where(filters, params)
    where_sql = "community_name = ?" + (
        " AND " + " AND ".join(filter_clauses) if filter_clauses else ""
    )
    sql = f"SELECT {SELECT_COLS} FROM land_transaction WHERE {where_sql} ORDER BY transaction_date DESC LIMIT ?"
    params.append(limit)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
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
        filters: 篩選條件 dict
        limit: 回傳上限
        db_path: 資料庫路徑

    Returns:
        list of dict（原始 row）
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

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows
