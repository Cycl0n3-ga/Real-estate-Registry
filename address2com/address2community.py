#!/usr/bin/env python3
"""
address2community.py - 地址→社區/建案名稱 查詢工具 (SQLite + 591 API 版)

資料來源：
  1. transactions.db - 內政部實價登錄交易資料庫（約 180 萬筆有社區名稱）
  2. 591 即時 API   - 本地查不到時自動呼叫 591 線上查詢

特色：
  - SQLite 直查：無需預先建 CSV，直接查 transactions.db
  - 591 即時 API：本地查不到時自動呼叫 591 線上查詢
  - 多層匹配：精確地址 → 門牌號 → 巷弄 → 路段 → 591 API

使用方式：
  1. 命令列：  python3 address2community.py "松山區八德路四段445號八樓"
  2. 互動：    python3 address2community.py
  3. 批次：    python3 address2community.py --batch input.txt
  4. 模組：    from address2community import lookup
              result = lookup("三民路29巷5號")
"""

import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from collections import defaultdict

# ========== 路徑設定 ==========
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR.parent / "db" / "transactions.db"

# ========== 全形半形轉換 ==========
FULLWIDTH_DIGITS = "０１２３４５６７８９"
HALFWIDTH_DIGITS = "0123456789"
FW_TO_HW = str.maketrans(FULLWIDTH_DIGITS, HALFWIDTH_DIGITS)
HW_TO_FW = str.maketrans(HALFWIDTH_DIGITS, FULLWIDTH_DIGITS)


def fullwidth_to_halfwidth(s: str) -> str:
    return s.translate(FW_TO_HW)


def halfwidth_to_fullwidth(s: str) -> str:
    return s.translate(HW_TO_FW)


# ========== 城市代碼對照 ==========
CITY_CODE_TO_NAME = {
    "A": "臺北市", "B": "臺中市", "C": "基隆市", "D": "臺南市",
    "E": "高雄市", "F": "新北市", "G": "宜蘭縣", "H": "桃園市",
    "I": "嘉義市", "J": "新竹縣", "K": "苗栗縣", "M": "南投縣",
    "N": "彰化縣", "O": "新竹市", "P": "雲林縣", "Q": "嘉義縣",
    "T": "屏東縣", "U": "花蓮縣", "V": "臺東縣", "W": "金門縣",
    "X": "澎湖縣", "Z": "連江縣",
}
CITY_NAME_TO_CODE = {v: k for k, v in CITY_CODE_TO_NAME.items()}
# 加入台→臺的對照
CITY_NAME_TO_CODE.update({
    "台北市": "A", "台中市": "B", "台南市": "D", "台東縣": "V",
})


# ========== 縣市列表 ==========
CITIES = [
    "臺北市", "台北市", "新北市", "桃園市", "桃園縣",
    "臺中市", "台中市", "臺南市", "台南市", "高雄市",
    "基隆市", "新竹市", "新竹縣", "苗栗縣", "彰化縣",
    "南投縣", "雲林縣", "嘉義市", "嘉義縣", "屏東縣",
    "宜蘭縣", "花蓮縣", "臺東縣", "台東縣", "澎湖縣",
    "金門縣", "連江縣",
]

# 591 API 的 regionid 對照
CITY_TO_591_REGION = {
    "臺北市": 1,  "新北市": 3,  "基隆市": 2,
    "新竹市": 4,  "新竹縣": 5,  "桃園市": 6,
    "苗栗縣": 7,  "臺中市": 8,  "彰化縣": 10,
    "南投縣": 11, "嘉義市": 12, "嘉義縣": 13,
    "雲林縣": 14, "臺南市": 15, "高雄市": 17,
    "屏東縣": 19, "宜蘭縣": 21, "臺東縣": 22,
    "花蓮縣": 23, "澎湖縣": 24, "金門縣": 25,
}

# 區→縣市 對照
DISTRICT_TO_CITY = {
    # 台北市
    "松山區": "臺北市", "信義區": "臺北市", "大安區": "臺北市",
    "中山區": "臺北市", "中正區": "臺北市", "大同區": "臺北市",
    "萬華區": "臺北市", "文山區": "臺北市", "南港區": "臺北市",
    "內湖區": "臺北市", "士林區": "臺北市", "北投區": "臺北市",
    # 新北市
    "板橋區": "新北市", "新莊區": "新北市", "中和區": "新北市",
    "永和區": "新北市", "土城區": "新北市", "樹林區": "新北市",
    "三重區": "新北市", "蘆洲區": "新北市", "汐止區": "新北市",
    "三峽區": "新北市", "鶯歌區": "新北市", "淡水區": "新北市",
    "新店區": "新北市", "林口區": "新北市", "五股區": "新北市",
    "泰山區": "新北市", "瑞芳區": "新北市", "八里區": "新北市",
    "深坑區": "新北市", "三芝區": "新北市", "萬里區": "新北市",
    "金山區": "新北市", "貢寮區": "新北市", "石門區": "新北市",
    "坪林區": "新北市", "烏來區": "新北市", "雙溪區": "新北市",
    "石碇區": "新北市", "平溪區": "新北市",
    # 桃園市
    "桃園區": "桃園市", "中壢區": "桃園市", "平鎮區": "桃園市",
    "八德區": "桃園市", "楊梅區": "桃園市", "蘆竹區": "桃園市",
    "龜山區": "桃園市", "大溪區": "桃園市", "龍潭區": "桃園市",
    "大園區": "桃園市", "觀音區": "桃園市", "新屋區": "桃園市",
    "復興區": "桃園市",
    # 台中市
    "西屯區": "臺中市", "北屯區": "臺中市", "南屯區": "臺中市",
    "西區": "臺中市", "北區": "臺中市", "南區": "臺中市",
    "東區": "臺中市", "豐原區": "臺中市", "大里區": "臺中市",
    "太平區": "臺中市", "烏日區": "臺中市", "潭子區": "臺中市",
    "大雅區": "臺中市", "神岡區": "臺中市", "沙鹿區": "臺中市",
    "清水區": "臺中市", "梧棲區": "臺中市", "龍井區": "臺中市",
    "大肚區": "臺中市", "后里區": "臺中市", "霧峰區": "臺中市",
    # 台南市
    "安平區": "臺南市", "安南區": "臺南市", "永康區": "臺南市",
    "仁德區": "臺南市", "歸仁區": "臺南市", "新化區": "臺南市",
    "善化區": "臺南市",
    # 高雄市
    "苓雅區": "高雄市", "前鎮區": "高雄市", "三民區": "高雄市",
    "鼓山區": "高雄市", "左營區": "高雄市", "楠梓區": "高雄市",
    "鳳山區": "高雄市", "小港區": "高雄市", "前金區": "高雄市",
    "新興區": "高雄市", "鹽埕區": "高雄市", "旗津區": "高雄市",
}


# ========== 地址處理 ==========

def extract_city(addr: str) -> str:
    s = fullwidth_to_halfwidth(str(addr).strip())
    for city in CITIES:
        if s.startswith(city):
            return city.replace("台北市", "臺北市").replace("台中市", "臺中市").replace("台南市", "臺南市").replace("台東縣", "臺東縣")
    return ""


def extract_district(addr: str) -> str:
    s = fullwidth_to_halfwidth(str(addr).strip())
    for city in CITIES:
        if s.startswith(city):
            s = s[len(city):]
            break
    # 使用非貪婪匹配，避免 "西屯區市政..." 誤匹配為 "西屯區市"
    m = re.match(r"([\u4e00-\u9fff]{1,3}?[區鎮鄉市])", s)
    return m.group(1) if m else ""


def normalize_address(addr: str) -> str:
    """正規化地址：去除縣市/區/里鄰/樓層/棟號，僅保留路段+門牌"""
    s = str(addr).strip()
    if not s:
        return ""
    s = fullwidth_to_halfwidth(s)

    for city in CITIES:
        if s.startswith(city):
            s = s[len(city):]
            break
    for _ in range(2):
        s = re.sub(r"^[\u4e00-\u9fff]{1,3}[區鎮鄉市]", "", s)

    s = re.sub(r"[\u4e00-\u9fff]*里\d*鄰?", "", s)
    s = re.sub(r"\d+鄰", "", s)
    s = re.sub(r"[,\s]*(地下)?[\d]+樓.*$", "", s)
    s = re.sub(r"[,\s]*(地下)?(十|二十|三十)?[一二三四五六七八九十百]+樓.*$", "", s)
    s = re.sub(r"\s*\d+F$", "", s)
    s = re.sub(r"\s*[A-Za-z]\d*[-]\d+F$", "", s)
    s = re.sub(r"\s*[A-Za-z]\d*棟.*$", "", s)
    s = re.sub(r"\s+[A-Za-z]\d+[-][A-Za-z]?\d*F?$", "", s)
    s = re.sub(r"旁.*$", "", s)
    s = re.sub(r"之\d+$", "", s)
    s = re.sub(r"共\d+筆$", "", s)
    s = re.sub(r"\s+", "", s)
    return s.strip()


def extract_road_number(addr: str) -> str:
    m = re.search(r"(.*?\d+號)", addr)
    return m.group(1) if m else addr


def extract_road_alley(addr: str) -> str:
    m = re.search(r"(.*?\d+巷)", addr)
    return m.group(1) if m else ""


def extract_road(addr: str) -> str:
    m = re.search(
        r"([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]+段)?)", addr
    )
    return m.group(1) if m else ""


def infer_city(addr: str) -> str:
    """從地址推斷縣市"""
    city = extract_city(addr)
    if city:
        return city
    district = extract_district(addr)
    if district and district in DISTRICT_TO_CITY:
        return DISTRICT_TO_CITY[district]
    return ""


def get_city_code(addr: str) -> str:
    """從地址取得城市代碼 (A/B/C...)"""
    city = infer_city(addr)
    if city:
        return CITY_NAME_TO_CODE.get(city, "")
    return ""


def get_591_regionids(addr: str) -> list:
    """根據地址取得要嘗試的 591 regionid 列表"""
    city = infer_city(addr)
    if city and city in CITY_TO_591_REGION:
        return [CITY_TO_591_REGION[city]]
    return [1, 3, 6, 8, 15, 17, 5, 4, 10, 21, 19]


# ========== 591 API ==========

class Api591:
    """591 社區搜尋 API"""

    BASE_URL = "https://bff.591.com.tw"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://community.591.com.tw/",
    }

    @classmethod
    def search_community(cls, keyword: str, regionid: int, timeout: int = 8) -> list:
        """搜尋社區"""
        params = urllib.parse.urlencode({
            "keyword": keyword,
            "regionid": regionid,
        })
        url = f"{cls.BASE_URL}/v1/community/search/match?{params}"

        try:
            req = urllib.request.Request(url, headers=cls.HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read().decode("utf-8"))
                if data.get("status") == 1:
                    items = data.get("data", {}).get("items", [])
                    return [item for item in items if item.get("name")]
        except Exception:
            pass
        return []

    @classmethod
    def search_by_address(cls, address: str, regionids: list = None) -> dict:
        """用地址搜尋社區，回傳最佳匹配"""
        norm = normalize_address(address)

        keywords = []
        road_number = extract_road_number(norm)
        road = extract_road(norm)

        if road_number:
            keywords.append(road_number)
        if road_number and road_number.endswith("號"):
            keywords.append(road_number[:-1])
        if road:
            keywords.append(road)

        if not regionids:
            regionids = get_591_regionids(address)

        for rid in regionids:
            for keyword in keywords:
                results = cls.search_community(keyword, rid)
                if results:
                    best = cls._find_best_match(results, norm)
                    if best:
                        return best
            if road:
                results = cls.search_community(road, regionids[0] if regionids else rid)
                if results:
                    best = cls._find_best_match(results, norm)
                    if best:
                        return best

        return None

    @classmethod
    def _find_best_match(cls, results: list, norm_addr: str) -> dict:
        """從搜尋結果中找最佳匹配"""
        num_match = re.search(r"(\d+)號", norm_addr)
        target_num = int(num_match.group(1)) if num_match else None
        road = extract_road(norm_addr)
        target_alley = extract_road_alley(norm_addr)

        best = None
        best_score = -1

        for item in results:
            item_addr = item.get("address", "")
            if not item_addr or not item.get("name"):
                continue

            score = 0

            item_road = extract_road(item_addr)
            if road and item_road and road == item_road:
                score += 10

            item_alley = extract_road_alley(item_addr)
            if target_alley and item_alley and target_alley == item_alley:
                score += 10

            if target_num:
                item_num_match = re.search(r"(\d+)號", item_addr)
                if item_num_match:
                    item_num = int(item_num_match.group(1))
                    diff = abs(target_num - item_num)
                    if diff == 0:
                        score += 20
                    elif diff <= 2:
                        score += 15
                    elif diff <= 5:
                        score += 10
                    elif diff <= 20:
                        score += 5
                    elif diff <= 50:
                        score += 2

            if score > best_score:
                best_score = score
                best = item

        return best if best_score >= 15 else None


# ========== 核心查詢引擎 ==========

class AddressCommunityLookup:
    """地址→社區名稱 查詢引擎 (transactions.db + 591 API)"""

    def __init__(self, db_path: str = None, enable_api: bool = True, verbose: bool = False):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.enable_api = enable_api
        self.verbose = verbose
        self.conn = None
        self._connect_db()

    def _connect_db(self):
        """連線 transactions.db"""
        if not self.db_path.exists():
            print(f"⚠️  資料庫不存在: {self.db_path}")
            return

        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

        # 確認記錄數
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE community IS NOT NULL AND community != ''"
        )
        count = cursor.fetchone()[0]
        print(f"📂 已連線: {self.db_path.name}（{count:,} 筆有社區資料）")

    def close(self):
        """關閉資料庫連線"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    def _make_search_patterns(self, addr_part: str, district: str = None, fuzzy_number: bool = False) -> list:
        """
        產生搜尋用的 LIKE 模式列表。
        
        DB 地址格式: "松山區八德路四段０４４５號八樓#松山區八德路四段445號八樓"
        - # 前面是全形數字含前導零
        - # 後面是全形數字不含前導零
        - 中文字是一般的漢字
        
        fuzzy_number: 若 True，則在 "XX號" 前加入 "%" 以匹配 "XX之Y號" 等變體
        """
        patterns = []
        
        # 處理 "之X" 變體：將 "123號" 變成 "123%號"
        hw_part = addr_part
        fw_part = halfwidth_to_fullwidth(addr_part)
        
        if fuzzy_number and re.search(r'\d+號', hw_part):
            hw_fuzzy = re.sub(r'(\d+)號', r'\1%號', hw_part)
            fw_fuzzy = re.sub(r'([０-９]+)號', r'\1%號', fw_part)
        else:
            hw_fuzzy = None
            fw_fuzzy = None
        
        if district:
            patterns.append(f"%{district}{hw_part}%")
            patterns.append(f"%{district}{fw_part}%")
            if hw_fuzzy:
                patterns.append(f"%{district}{hw_fuzzy}%")
                patterns.append(f"%{district}{fw_fuzzy}%")
        
        patterns.append(f"%{hw_part}%")
        patterns.append(f"%{fw_part}%")
        if hw_fuzzy:
            patterns.append(f"%{hw_fuzzy}%")
            patterns.append(f"%{fw_fuzzy}%")
        
        # 去重但保持順序
        seen = set()
        unique = []
        for p in patterns:
            if p not in seen:
                seen.add(p)
                unique.append(p)
        return unique

    def _query_db_exact(self, norm: str, city_code: str = None, district: str = None) -> list:
        """Level 1: 精確地址匹配 - 在 DB 的 address 欄位中搜尋"""
        if not self.conn:
            return []

        # 先嘗試精確匹配，再嘗試模糊數字匹配（處理 "之X號" 變體）
        for fuzzy in (False, True):
            search_patterns = self._make_search_patterns(norm, district, fuzzy_number=fuzzy)
            
            for pattern in search_patterns:
                sql = """
                    SELECT community, COUNT(*) as cnt, city, town, address
                    FROM transactions
                    WHERE address LIKE ? AND community IS NOT NULL AND community != ''
                """
                params = [pattern]
                if city_code:
                    sql += " AND city = ?"
                    params.append(city_code)
                sql += " GROUP BY community ORDER BY cnt DESC LIMIT 5"

                cursor = self.conn.execute(sql, params)
                rows = cursor.fetchall()
                if rows:
                    return [{"community": r["community"], "count": r["cnt"],
                             "city_code": r["city"], "town": r["town"],
                             "sample_address": r["address"]} for r in rows]
        return []

    def _query_db_road_number(self, road_number: str, city_code: str = None, district: str = None) -> list:
        """Level 2: 路+門牌號匹配"""
        if not self.conn or not road_number:
            return []

        for fuzzy in (False, True):
            search_patterns = self._make_search_patterns(road_number, district, fuzzy_number=fuzzy)
            for pattern in search_patterns:
                sql = """
                    SELECT community, COUNT(*) as cnt, city, town
                    FROM transactions
                    WHERE address LIKE ? AND community IS NOT NULL AND community != ''
                """
                params = [pattern]
                if city_code:
                    sql += " AND city = ?"
                    params.append(city_code)
                sql += " GROUP BY community ORDER BY cnt DESC LIMIT 5"

                cursor = self.conn.execute(sql, params)
                rows = cursor.fetchall()
                if rows:
                    return [{"community": r["community"], "count": r["cnt"],
                             "city_code": r["city"], "town": r["town"]} for r in rows]
        return []

    def _query_db_alley(self, alley: str, city_code: str = None, district: str = None) -> list:
        """Level 3: 巷弄匹配"""
        if not self.conn or not alley:
            return []

        search_patterns = self._make_search_patterns(alley, district, fuzzy_number=False)
        for pattern in search_patterns:
            sql = """
                SELECT community, COUNT(*) as cnt, city, town
                FROM transactions
                WHERE address LIKE ? AND community IS NOT NULL AND community != ''
            """
            params = [pattern]
            if city_code:
                sql += " AND city = ?"
                params.append(city_code)
            sql += " GROUP BY community ORDER BY cnt DESC LIMIT 5"

            cursor = self.conn.execute(sql, params)
            rows = cursor.fetchall()
            if rows:
                return [{"community": r["community"], "count": r["cnt"],
                         "city_code": r["city"], "town": r["town"]} for r in rows]
        return []

    def _query_db_road(self, road: str, city_code: str = None, district: str = None) -> list:
        """Level 4: 路段匹配"""
        if not self.conn or not road:
            return []

        search_patterns = []
        if district:
            search_patterns.append(f"%{district}{road}%")
        search_patterns.append(f"%{road}%")

        for pattern in search_patterns:
            sql = """
                SELECT community, COUNT(*) as cnt, city, town
                FROM transactions
                WHERE address LIKE ? AND community IS NOT NULL AND community != ''
            """
            params = [pattern]
            if city_code:
                sql += " AND city = ?"
                params.append(city_code)
            sql += " GROUP BY community ORDER BY cnt DESC LIMIT 10"

            cursor = self.conn.execute(sql, params)
            rows = cursor.fetchall()
            if rows:
                return [{"community": r["community"], "count": r["cnt"],
                         "city_code": r["city"], "town": r["town"]} for r in rows]
        return []

    def _get_district_from_town(self, city_code: str, town: str) -> str:
        """從 city+town 代碼推斷區域名稱（從 DB 記錄中提取）"""
        if not self.conn:
            return ""
        try:
            cursor = self.conn.execute(
                "SELECT address FROM transactions WHERE city=? AND town=? LIMIT 1",
                (city_code, town)
            )
            row = cursor.fetchone()
            if row:
                addr = row["address"]
                if "#" in addr:
                    addr = addr.split("#", 1)[1]
                m = re.match(r"([\u4e00-\u9fff]{1,3}[區鎮鄉市])", addr)
                if m:
                    return m.group(1)
        except Exception:
            pass
        return ""

    def query(self, address: str, top_n: int = 5) -> dict:
        """查詢地址對應的社區/建案名稱"""
        norm = normalize_address(address)
        input_district = extract_district(address)
        input_city = infer_city(address)
        city_code = get_city_code(address)
        results = []

        if self.verbose:
            print(f"  🔍 查詢: {address}")
            print(f"     正規化: {norm}")
            if input_city:
                print(f"     城市: {input_city} ({city_code})")
            if input_district:
                print(f"     區域: {input_district}")

        if self.conn:
            # Level 1: 完整地址精確匹配
            db_results = self._query_db_exact(norm, city_code, input_district)
            if db_results:
                for r in db_results:
                    district = input_district or self._get_district_from_town(r["city_code"], r["town"])
                    results.append({
                        "community": r["community"],
                        "confidence": 98,
                        "match_level": "完整地址精確匹配",
                        "district": district,
                        "source": "transactions.db",
                        "count": r["count"],
                    })
                if self.verbose:
                    print(f"     ✅ Level 1: {results[0]['community']} ({results[0]['count']}筆)")

            # Level 2: 門牌號匹配
            if not results or results[0]["confidence"] < 80:
                to_num = extract_road_number(norm)
                db_results = self._query_db_road_number(to_num, city_code, input_district)
                if db_results:
                    for r in db_results:
                        district = input_district or self._get_district_from_town(r["city_code"], r["town"])
                        results.append({
                            "community": r["community"],
                            "confidence": 90,
                            "match_level": "門牌號匹配",
                            "district": district,
                            "source": "transactions.db",
                            "count": r["count"],
                        })
                    if self.verbose:
                        print(f"     ✅ Level 2: {db_results[0]['community']} ({db_results[0]['count']}筆)")

            # Level 3: 巷弄匹配
            if not results or all(r["confidence"] < 70 for r in results):
                to_alley = extract_road_alley(norm)
                if to_alley:
                    db_results = self._query_db_alley(to_alley, city_code, input_district)
                    if db_results:
                        for r in db_results:
                            district = input_district or self._get_district_from_town(r["city_code"], r["town"])
                            results.append({
                                "community": r["community"],
                                "confidence": 72,
                                "match_level": "巷弄匹配",
                                "district": district,
                                "source": "transactions.db",
                                "count": r["count"],
                            })
                        if self.verbose:
                            print(f"     ✅ Level 3: {db_results[0]['community']} ({db_results[0]['count']}筆)")

            # Level 4: 路段匹配
            if not results or all(r["confidence"] < 50 for r in results):
                road = extract_road(norm)
                if road:
                    db_results = self._query_db_road(road, city_code, input_district)
                    if db_results:
                        for r in db_results:
                            district = input_district or self._get_district_from_town(r["city_code"], r["town"])
                            results.append({
                                "community": r["community"],
                                "confidence": 40,
                                "match_level": "路段匹配",
                                "district": district,
                                "source": "transactions.db",
                                "count": r["count"],
                            })
                        if self.verbose:
                            print(f"     ✅ Level 4: {db_results[0]['community']} ({db_results[0]['count']}筆)")

        # Level 5: 591 API 線上查詢
        if self.enable_api and (not results or all(r["confidence"] < 70 for r in results)):
            api_results = self._query_591_api(address, norm)
            if api_results:
                results.extend(api_results)

        # 去重、排序
        seen = set()
        unique_results = []
        for r in sorted(results, key=lambda x: (-x["confidence"], -x.get("count", 0))):
            if r["community"] not in seen:
                seen.add(r["community"])
                unique_results.append(r)

        unique_results = unique_results[:top_n]
        best = unique_results[0]["community"] if unique_results else None

        return {
            "input": address,
            "normalized": norm,
            "results": unique_results,
            "best": best,
        }

    def _query_591_api(self, original_addr: str, norm: str) -> list:
        """呼叫 591 API 線上查詢"""
        if self.verbose:
            print(f"     🌐 查詢 591 API...")

        regionids = get_591_regionids(original_addr)
        result = Api591.search_by_address(original_addr, regionids)

        if result:
            name = result.get("name", "")
            if name:
                if self.verbose:
                    print(f"     ✅ 591 API: {name}")
                district = result.get("section", "")
                return [{
                    "community": name,
                    "confidence": 88,
                    "match_level": "591 即時查詢",
                    "district": district,
                    "source": "591_API",
                    "count": 0,
                }]

        # 路段搜尋取近鄰
        road = extract_road(norm)
        if road and regionids:
            results = Api591.search_community(road, regionids[0])
            if results:
                candidates = []
                for item in results:
                    item_name = item.get("name")
                    if item_name:
                        candidates.append({
                            "community": item_name,
                            "confidence": 35,
                            "match_level": "591 路段附近",
                            "district": item.get("section", ""),
                            "source": "591_API",
                            "count": 0,
                        })
                return candidates[:3]

        return []

    def batch_query(self, addresses: list) -> list:
        return [self.query(addr) for addr in addresses]

    def stats(self) -> dict:
        """取得統計"""
        if not self.conn:
            return {"total_records": 0, "unique_communities": 0}
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE community IS NOT NULL AND community != ''"
        )
        total = cursor.fetchone()[0]
        cursor = self.conn.execute(
            "SELECT COUNT(DISTINCT community) FROM transactions WHERE community IS NOT NULL AND community != ''"
        )
        unique = cursor.fetchone()[0]
        return {"total_records": total, "unique_communities": unique}


# ========== 便利函式 ==========

_global_lookup = None


def lookup(address: str, **kwargs) -> dict:
    """便利查詢函式"""
    global _global_lookup
    if _global_lookup is None:
        _global_lookup = AddressCommunityLookup(**kwargs)
    return _global_lookup.query(address)


def quick_lookup(address: str) -> str:
    """最簡查詢"""
    result = lookup(address)
    return result["best"] or "未找到"


# ========== CLI ==========

def print_result(result: dict, show_detail: bool = False):
    """格式化輸出"""
    addr = result["input"]
    best = result["best"]

    if best:
        top = result["results"][0]
        bar = "█" * (top["confidence"] // 10) + "░" * (10 - top["confidence"] // 10)
        print(f"\n📍 {addr}")
        print(f"   → 🏘️  {best}")
        print(f"   信心度: [{bar}] {top['confidence']}%")
        print(f"   匹配: {top['match_level']} (來源: {top['source']})")
        if top["district"]:
            print(f"   區域: {top['district']}")
        if top.get("count"):
            print(f"   交易筆數: {top['count']}")

        if show_detail and len(result["results"]) > 1:
            print(f"\n   其他候選：")
            for r in result["results"][1:]:
                extra = f", {r['count']}筆" if r.get("count") else ""
                print(f"   • {r['community']} ({r['confidence']}%, {r['match_level']}{extra})")
    else:
        print(f"\n📍 {addr}")
        print(f"   → ❓ 未找到")
        print(f"   正規化: {result['normalized']}")


def interactive_mode(lookup_engine: AddressCommunityLookup):
    """互動模式"""
    stats = lookup_engine.stats()
    print("=" * 60)
    print("🏘️  地址→社區名稱 查詢工具 (transactions.db + 591 API)")
    print("=" * 60)
    print(f"📊 本地資料: {stats.get('total_records', 0):,} 筆 | "
          f"社區: {stats.get('unique_communities', 0):,}")
    print("-" * 60)
    print("輸入地址查詢，'q' 退出，'detail' 詳細模式")
    print("-" * 60)

    show_detail = False

    while True:
        try:
            addr = input("\n🔎 地址: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 再見！")
            break

        if not addr:
            continue
        if addr.lower() in ("q", "quit", "exit"):
            print("👋 再見！")
            break
        if addr.lower() == "detail":
            show_detail = not show_detail
            print(f"   詳細模式: {'開啟' if show_detail else '關閉'}")
            continue
        if addr.lower() == "stats":
            s = lookup_engine.stats()
            print(f"   記錄: {s['total_records']:,} | 社區: {s['unique_communities']:,}")
            continue

        t0 = time.time()
        result = lookup_engine.query(addr)
        elapsed = time.time() - t0
        print_result(result, show_detail)
        print(f"   ⏱️  {elapsed:.3f}s")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="地址→社區/建案名稱 查詢工具 (transactions.db + 591 API)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用範例：
  python3 address2community.py "松山區八德路四段445號八樓"
  python3 address2community.py "仁愛路三段53號E棟"
  python3 address2community.py --batch addresses.txt
  python3 address2community.py --no-api "三民路29巷5號"
        """,
    )
    parser.add_argument("address", nargs="*", help="查詢地址")
    parser.add_argument("--batch", "-b", help="批次檔案")
    parser.add_argument("--detail", "-d", action="store_true", help="詳細結果")
    parser.add_argument("--no-api", action="store_true", help="停用 591 API")
    parser.add_argument("--verbose", "-v", action="store_true", help="顯示詳細過程")
    parser.add_argument("--json", "-j", action="store_true", help="JSON 輸出")
    parser.add_argument("--db", help="指定 transactions.db 路徑")

    args = parser.parse_args()

    engine = AddressCommunityLookup(
        db_path=args.db,
        enable_api=not args.no_api,
        verbose=args.verbose,
    )

    if args.batch:
        with open(args.batch, "r", encoding="utf-8") as f:
            addresses = [line.strip() for line in f if line.strip()]
        print(f"📋 批次查詢 {len(addresses)} 個地址...\n")

        if args.json:
            results = engine.batch_query(addresses)
            output = [{
                "input": r["input"],
                "community": r["best"],
                "confidence": r["results"][0]["confidence"] if r["results"] else 0,
            } for r in results]
            print(json.dumps(output, ensure_ascii=False, indent=2))
        else:
            for addr in addresses:
                result = engine.query(addr)
                if result["best"]:
                    top = result["results"][0]
                    print(f"{addr} → {result['best']} ({top['confidence']}%)")
                else:
                    print(f"{addr} → ❓ 未找到")
        return

    if args.address:
        for addr in args.address:
            t0 = time.time()
            result = engine.query(addr)
            elapsed = time.time() - t0
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print_result(result, args.detail)
                if args.verbose:
                    print(f"   ⏱️  {elapsed:.3f}s")
        return

    interactive_mode(engine)


if __name__ == "__main__":
    main()
