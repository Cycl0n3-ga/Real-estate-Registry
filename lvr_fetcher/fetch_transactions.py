"""
fetch_transactions.py
====================
下載全台（或指定縣市）的不動產買賣交易資料，存至 SQLite。

使用方式（全台下載）：
  python -m lvr_api.fetch_transactions --all \
      --starty 101 --startm 1 --endy 115 --endm 2

單一縣市：
  python -m lvr_api.fetch_transactions \
      --city C --starty 101 --startm 1 --endy 115 --endm 2

或從程式呼叫：
  from lvr_api.fetch_transactions import download_all
  download_all(starty=101, startm=1, endy=115, endm=2)
"""
import argparse
import json
import logging
import os
import sqlite3
import time
from typing import Generator

from .client import LvrApiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "..", "db", "transactions.db")

# ── 台灣縣市代碼 ──────────────────────────────────────────
CITIES = {
    "A": "臺北市",
    "B": "臺中市",
    "C": "基隆市",
    "D": "臺南市",
    "E": "高雄市",
    "F": "新北市",
    "G": "宜蘭縣",
    "H": "桃園市",
    "I": "嘉義市",
    "J": "新竹縣",
    "K": "苗栗縣",
    "M": "南投縣",
    "N": "彰化縣",
    "O": "新竹市",
    "P": "雲林縣",
    "Q": "嘉義縣",
    "T": "屏東縣",
    "U": "花蓮縣",
    "V": "臺東縣",
    "W": "金門縣",
    "X": "澎湖縣",
    "Z": "連江縣",
}

# 大城市（交易量大）用月查詢；其餘用季查詢
BIG_CITIES = {"A", "B", "D", "E", "F", "H"}


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def init_db(db_path: str):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city        TEXT,
            town        TEXT,
            address     TEXT,
            build_type  TEXT,
            community   TEXT,
            date_str    TEXT,
            floor       TEXT,
            area        TEXT,
            total_price TEXT,
            unit_price  TEXT,
            lat         REAL,
            lon         REAL,
            sq          TEXT UNIQUE,
            raw_json    TEXT
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_txn_city_town ON transactions(city, town)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(date_str)
    """)
    # 已完成的 (city, period) 組合，支援斷點續傳
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_progress (
            city    TEXT,
            period  TEXT,   -- 'YYYY-MM' (民國年-月)
            count   INTEGER,
            ts      TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (city, period)
        )
    """)
    conn.commit()
    return conn


def is_period_done(cur, city: str, period: str) -> bool:
    cur.execute(
        "SELECT 1 FROM fetch_progress WHERE city=? AND period=?",
        (city, period),
    )
    return cur.fetchone() is not None


def mark_period_done(cur, city: str, period: str, count: int):
    cur.execute(
        "INSERT OR REPLACE INTO fetch_progress (city, period, count) VALUES (?,?,?)",
        (city, period, count),
    )


def insert_records(cur, city: str, records: list) -> int:
    inserted = 0
    for r in records:
        try:
            cur.execute(
                """INSERT OR IGNORE INTO transactions
                   (city, town, address, build_type, community,
                    date_str, floor, area, total_price, unit_price,
                    lat, lon, sq, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    city,
                    r.get("town", ""),
                    r.get("a", ""),
                    r.get("b", ""),
                    r.get("bn", ""),
                    r.get("e", ""),
                    r.get("f", ""),
                    r.get("s", ""),
                    r.get("p", ""),
                    r.get("v", ""),
                    r.get("lat"),
                    r.get("lon"),
                    r.get("sq", ""),
                    json.dumps(r, ensure_ascii=False),
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        except sqlite3.Error as e:
            logger.debug(f"insert skip: {e}")
    return inserted


# ---------------------------------------------------------------------------
# 時間範圍產生器
# ---------------------------------------------------------------------------

def monthly_periods(starty, startm, endy, endm):
    """每月一段"""
    y, m = starty, startm
    while (y, m) <= (endy, endm):
        yield y, m, y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def quarter_periods(starty, startm, endy, endm):
    """每季一段（最長 3 個月）"""
    y, m = starty, startm
    while (y, m) <= (endy, endm):
        em = m + 2
        ey = y
        if em > 12:
            em -= 12
            ey += 1
        if (ey, em) > (endy, endm):
            ey, em = endy, endm
        yield y, m, ey, em
        nm = em + 1
        ny = ey
        if nm > 12:
            nm = 1
            ny += 1
        y, m = ny, nm


# ---------------------------------------------------------------------------
# 主要下載函數
# ---------------------------------------------------------------------------

def download_city(
    client,
    conn,
    city,
    starty=101,
    startm=1,
    endy=115,
    endm=2,
    delay=0.5,
    ptype="1,2,3,4,5",
):
    """
    下載單一城市的全部交易資料（town='', 取得整城市）。

    大城市（台北/新北/台中/台南/高雄/桃園）按月切分，
    其餘按季切分，避免單次查詢量過大。

    回傳新增筆數。
    """
    cur = conn.cursor()
    city_name = CITIES.get(city, city)

    if city in BIG_CITIES:
        periods = list(monthly_periods(starty, startm, endy, endm))
        mode = "月"
    else:
        periods = list(quarter_periods(starty, startm, endy, endm))
        mode = "季"

    total = len(periods)
    logger.info(f"── {city_name}({city}) 共 {total} 個{mode}段 ──")

    city_inserted = 0
    consecutive_fail = 0

    for i, (sy, sm, ey, em) in enumerate(periods, 1):
        period_key = f"{sy:03d}-{sm:02d}"
        if is_period_done(cur, city, period_key):
            continue

        label = f"{sy}/{sm:02d}~{ey}/{em:02d}" if (sy, sm) != (ey, em) else f"{sy}/{sm:02d}"
        logger.info(f"  [{i}/{total}] {city_name} {label} ...")

        try:
            records = client.query_price(city, "", sy, sm, ey, em, ptype=ptype)
        except Exception as e:
            logger.warning(f"    查詢失敗: {e}")
            consecutive_fail += 1
            if consecutive_fail >= 5:
                logger.error(f"    連續失敗 {consecutive_fail} 次，重新登入")
                client.login()
                consecutive_fail = 0
            time.sleep(delay * 3)
            continue

        if records is None:
            records = []

        inserted = insert_records(cur, city, records)
        mark_period_done(cur, city, period_key, len(records))
        conn.commit()
        city_inserted += inserted
        consecutive_fail = 0

        logger.info(f"    取得 {len(records)} 筆，新增 {inserted}（{city_name}累計 {city_inserted}）")
        time.sleep(delay)

    return city_inserted


def download_all(
    starty=101,
    startm=1,
    endy=115,
    endm=2,
    db_path=DEFAULT_DB,
    delay=0.5,
    ptype="1,2,3,4,5",
    cities=None,
):
    """
    下載全台（或指定城市列表）的不動產交易資料。

    Args:
        cities: 若為 None，下載全台 22 縣市；否則只下載指定城市代碼列表
    """
    conn = init_db(db_path)
    client = LvrApiClient()

    if not client.login():
        logger.error("無法登入，程式終止。")
        conn.close()
        return

    target_cities = cities if cities else sorted(CITIES.keys())
    grand_total = 0

    logger.info(f"目標: {len(target_cities)} 個縣市，{starty}/{startm:02d} ~ {endy}/{endm:02d}")
    logger.info(f"DB: {os.path.abspath(db_path)}")

    for ci, city_code in enumerate(target_cities, 1):
        city_name = CITIES.get(city_code, city_code)
        logger.info(f"\n{'='*60}")
        logger.info(f"[{ci}/{len(target_cities)}] 開始 {city_name}({city_code})")
        logger.info(f"{'='*60}")

        n = download_city(
            client, conn, city_code,
            starty=starty, startm=startm,
            endy=endy, endm=endm,
            delay=delay, ptype=ptype,
        )
        grand_total += n
        logger.info(f"  {city_name} 完成，新增 {n} 筆")

    # 最終統計
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions")
    total_in_db = cur.fetchone()[0]
    conn.close()

    logger.info(f"\n{'='*60}")
    logger.info(f"全部完成！本次新增 {grand_total} 筆，DB 共 {total_in_db} 筆")
    logger.info(f"DB 路徑: {os.path.abspath(db_path)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="下載 lvr.land.moi.gov.tw 不動產買賣交易資料（全台或指定縣市）"
    )
    parser.add_argument("--all", action="store_true", help="下載全台 22 縣市")
    parser.add_argument(
        "--city", default=None,
        help="城市代碼（可用逗號分隔多個，例如 A,B,C）。不指定 --all 時必須填",
    )
    parser.add_argument("--starty", type=int, default=101, help="起始年（民國）")
    parser.add_argument("--startm", type=int, default=1, help="起始月")
    parser.add_argument("--endy", type=int, default=115, help="結束年（民國）")
    parser.add_argument("--endm", type=int, default=2, help="結束月")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 路徑")
    parser.add_argument("--delay", type=float, default=0.5, help="請求間隔秒數")
    parser.add_argument(
        "--ptype", default="1,2,3,4,5",
        help="交易標的 1=房地 2=房地(車) 3=土地 4=建物 5=車位（預設全部）",
    )
    args = parser.parse_args()

    if not args.all and not args.city:
        parser.error("請指定 --all（全台）或 --city（指定城市）")

    city_list = None
    if args.city:
        city_list = [c.strip().upper() for c in args.city.split(",")]

    download_all(
        starty=args.starty,
        startm=args.startm,
        endy=args.endy,
        endm=args.endm,
        db_path=args.db,
        delay=args.delay,
        ptype=args.ptype,
        cities=city_list,
    )
