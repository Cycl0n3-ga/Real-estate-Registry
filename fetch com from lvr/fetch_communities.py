import os
import sqlite3
import logging
import time
import json
from .client import LvrApiClient, SWEEP_CHARS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'community_address.db')


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS community_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_code TEXT,
            town_code TEXT,
            address TEXT,
            community_name TEXT,
            short_name TEXT,
            raw_data TEXT,
            UNIQUE(town_code, community_name, address)
        )
    ''')
    # 紀錄已爬完的 (town_code, keyword) 組合，支援斷點續傳
    cur.execute('''
        CREATE TABLE IF NOT EXISTS crawl_progress (
            town_code TEXT,
            keyword   TEXT,
            PRIMARY KEY (town_code, keyword)
        )
    ''')
    conn.commit()
    conn.close()


def is_done(cur: sqlite3.Cursor, town_code: str, keyword: str) -> bool:
    cur.execute(
        "SELECT 1 FROM crawl_progress WHERE town_code=? AND keyword=?",
        (town_code, keyword)
    )
    return cur.fetchone() is not None


def mark_done(cur: sqlite3.Cursor, town_code: str, keyword: str):
    cur.execute(
        "INSERT OR IGNORE INTO crawl_progress (town_code, keyword) VALUES (?,?)",
        (town_code, keyword)
    )


def download_all_communities(
    delay: float = 0.5,
    chars: list | None = None,
):
    """
    爬取策略：
      對每個縣市 → 每個行政區 → 每個掃描字元，呼叫
      /SERVICE/QueryPrice/SaleBuild/{town_code}/{char}
      API 為「包含」搜尋，回傳建案名稱中含有該字的所有建案。
      支援斷點續傳（crawl_progress 資料表）。

    Args:
        delay: 每次 API 請求間的間隔秒數（預設 0.5）
        chars:  自訂掃描字元清單（None 則使用預設 SWEEP_CHARS）
    """
    if chars is None:
        chars = SWEEP_CHARS

    init_db()
    client = LvrApiClient()
    if not client.login():
        logger.error("無法取得 Session，程式終止。")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cities = client.get_cities()
    logger.info(f"取得 {len(cities)} 個縣市資料。（掃描字元數：{len(chars)}）")

    total_inserted = 0
    total_requests = 0

    for city in cities:
        city_code = city["code"]
        city_title = city["title"]
        towns = client.get_towns(city_code)

        for town in towns:
            town_code = town["code"]
            town_title = town["title"]
            town_inserted = 0

            for ch in chars:
                # 斷點續傳：已爬過的 (town, char) 直接跳過
                if is_done(cur, town_code, ch):
                    continue

                results = client.search_communities_raw(town_code, ch)
                total_requests += 1

                for item in results:
                    raw_data_str = json.dumps(item, ensure_ascii=False)
                    community_name = item.get("buildname", item.get("name", ""))
                    address = item.get(
                        "address", item.get("location", item.get("addr", ""))
                    )
                    short_name = item.get(
                        "sq_name", item.get("short_name", item.get("sq", ""))
                    )
                    if community_name or address:
                        try:
                            cur.execute(
                                '''INSERT OR IGNORE INTO community_mapping
                                   (city_code, town_code, address, community_name,
                                    short_name, raw_data)
                                   VALUES (?, ?, ?, ?, ?, ?)''',
                                (
                                    city_code, town_code, address,
                                    community_name, short_name, raw_data_str,
                                ),
                            )
                            if cur.rowcount > 0:
                                town_inserted += 1
                                total_inserted += 1
                        except Exception as e:
                            logger.warning(f"DB Error: {e}")

                mark_done(cur, town_code, ch)
                conn.commit()
                time.sleep(delay)

            if town_inserted > 0:
                logger.info(
                    f"  {city_title} {town_title} 新增 {town_inserted} 筆 "
                    f"（累計 {total_inserted} 筆，共 {total_requests} 次請求）"
                )

    conn.close()
    logger.info(
        f"\n完成！總計新增 {total_inserted} 筆資料，"
        f"共發送 {total_requests} 次 API 請求。\n"
        f"結果存於：{DB_PATH}"
    )


if __name__ == "__main__":
    download_all_communities()
