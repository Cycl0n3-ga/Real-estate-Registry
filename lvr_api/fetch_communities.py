import os
import sqlite3
import logging
import time
import json
from .client import LvrApiClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'community_address.db')

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # 建立一個存放地址與社區名稱的資料庫，並額外存放原始資料，以確保任何非預期的API欄位（例如社區簡稱）都被完整保留
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
    conn.commit()
    conn.close()

def download_all_communities():
    """
    修改後的爬蟲策略：
    針對每個行政區，直接使用關鍵字「區」搜尋，抓取該區所有的社區簡稱及地址資料，並存入 SQLite。
    """
    init_db()
    client = LvrApiClient()
    if not client.login():
        logger.error("無法連線並取得 Session，爬蟲終止。")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cities = client.get_cities()
    logger.info(f"成功取得 {len(cities)} 個縣市資料。")

    # KEYWORD = "區" - 依使用者需求改為城鄉名稱動態變化

    total_inserted = 0

    for city in cities:
        city_code = city["code"]
        city_title = city["title"]
        towns = client.get_towns(city_code)
        
        for town in towns:
            town_code = town["code"]
            town_title = town["title"]
            # 依照使用者指示，關鍵詞改為該鄉鎮區名稱 (例如: 松山區, 竹南鎮)
            KEYWORD = town_title
            
            # 設定交易期間：101年1月至115年1月，確保抓取最完整的資料
            # ptype=1,2 代表勾選「房地」與「房地+車位」
            search_params = {
                "starty": "101",
                "startm": "1",
                "endy": "115",
                "endm": "1",
                "ptype": "1,2"
            }
            
            logger.info(f"開始掃描: {city_title} {town_title} ({town_code})，關鍵字：'{KEYWORD}'")
            
            # 使用我們新修改的可以回傳完整 JSON 物件的 API 方法，並帶入日期參數
            results = client.search_communities_raw(town_code, KEYWORD, params=search_params)
            
            inserted = 0
            for item in results:
                raw_data_str = json.dumps(item, ensure_ascii=False)
                
                # 動態判斷常見欄位名稱，主要抓取：建案/社區名稱(buildname)、地址、社區簡稱(sq_name/short_name)
                community_name = item.get("buildname", item.get("name", ""))
                address = item.get("address", item.get("location", item.get("addr", "")))
                short_name = item.get("sq_name", item.get("short_name", item.get("sq", "")))
                
                # 如果 API 吐出的名稱包含了地址（例如: 遠雄之星(台中市清水區大秀段...)），我們可以在這裡或後續保留清理
                
                # 只有當我們有名字時才存入資料庫
                if community_name or address:
                    try:
                        cur.execute('''
                            INSERT OR IGNORE INTO community_mapping 
                            (city_code, town_code, address, community_name, short_name, raw_data)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (city_code, town_code, address, community_name, short_name, raw_data_str))
                        if cur.rowcount > 0:
                            inserted += 1
                    except Exception as e:
                        logger.warning(f"DB Error: {e}")
                        
            conn.commit()
            if inserted > 0:
                logger.info(f"  [+] {town_title} 新增了 {inserted} 筆社區資料")
                total_inserted += inserted
                
            time.sleep(0.5)

    conn.close()
    logger.info(f"全部爬取完成！總計新增 {total_inserted} 筆資料，結果已存於 {DB_PATH}")

if __name__ == "__main__":
    download_all_communities()
