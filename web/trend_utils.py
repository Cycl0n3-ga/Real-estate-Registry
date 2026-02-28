#!/usr/bin/env python3
"""
trend_utils.py — 價格趨勢計算工具

提供按月/季/年彙總統計，供 /api/trend 端點使用。
"""
import sqlite3
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DEFAULT_DB = SCRIPT_DIR.parent / "db" / "land_data.db"


def get_trend_data(keyword: str, period: str = "monthly",
                   db_path: str = None, limit_months: int = 60) -> dict:
    """
    取得某建案/地址的價格趨勢資料

    Args:
        keyword: 建案名稱或地址關鍵字
        period: 彙總粒度 — "monthly" | "quarterly" | "yearly"
        db_path: 資料庫路徑
        limit_months: 最大回溯月數

    Returns:
        { "keyword", "period", "data": [{ "label", "avg_price", "avg_unit_price",
          "median_price", "count", "min_price", "max_price" }] }
    """
    db = db_path or str(DEFAULT_DB)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    # 查詢交易資料
    sql = """
        SELECT transaction_date, total_price, unit_price,
               community_name, building_area
        FROM land_transaction
        WHERE (community_name LIKE ? OR address LIKE ?)
          AND total_price > 0
          AND transaction_date IS NOT NULL
        ORDER BY transaction_date
    """
    pattern = f"%{keyword}%"
    rows = conn.execute(sql, (pattern, pattern)).fetchall()
    conn.close()

    if not rows:
        return {"keyword": keyword, "period": period, "data": []}

    # 按時段分桶
    buckets = {}
    for r in rows:
        dt = str(r["transaction_date"] or "").strip()
        if not dt or len(dt) < 5:
            continue
        label = _to_period_label(dt, period)
        if label not in buckets:
            buckets[label] = {"prices": [], "unit_prices": []}
        if r["total_price"] and r["total_price"] > 0:
            buckets[label]["prices"].append(r["total_price"])
        if r["unit_price"] and r["unit_price"] > 0:
            buckets[label]["unit_prices"].append(r["unit_price"])

    # 彙總
    data = []
    for label in sorted(buckets.keys()):
        b = buckets[label]
        ps = sorted(b["prices"]) if b["prices"] else []
        us = sorted(b["unit_prices"]) if b["unit_prices"] else []
        data.append({
            "label": label,
            "count": len(ps),
            "avg_price": round(sum(ps) / len(ps)) if ps else 0,
            "median_price": ps[len(ps) // 2] if ps else 0,
            "min_price": ps[0] if ps else 0,
            "max_price": ps[-1] if ps else 0,
            "avg_unit_price": round(sum(us) / len(us)) if us else 0,
        })

    return {"keyword": keyword, "period": period, "data": data}


def _to_period_label(date_str: str, period: str) -> str:
    """將民國年日期轉為時段標籤"""
    # date_str 格式: "1130101" (民國113年01月01日) 或 "113/01/01"
    clean = date_str.replace("/", "").replace("-", "").strip()
    if len(clean) < 5:
        return "未知"
    try:
        year = int(clean[:3]) if len(clean) >= 7 else int(clean[:2])
        month = int(clean[3:5]) if len(clean) >= 7 else int(clean[2:4])
    except (ValueError, IndexError):
        return "未知"

    if period == "yearly":
        return f"{year}"
    elif period == "quarterly":
        q = (month - 1) // 3 + 1
        return f"{year}Q{q}"
    else:  # monthly
        return f"{year}/{month:02d}"
