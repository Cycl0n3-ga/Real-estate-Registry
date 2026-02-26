#!/usr/bin/env python3
"""
community2address.py - 社區/建案名稱→地址範圍 查詢工具

功能與 address2community.py 完全相反：
  - 輸入建案名稱，輸出該建案對應的地址範圍
  - 例如: 健安新城F區 → 三民路29巷1、3、5、7號

資料來源：
  1. land_data.db (SQLite 資料庫，含 land_transaction 表)
  2. manual_mapping.csv (手動新增的對照)
  3. 591.com.tw API（線上備援）

使用方式：
  1. 命令列：  python3 community2address.py "健安新城F區"
  2. 互動：    python3 community2address.py
  3. JSON：    python3 community2address.py -j "都廳大院"
  4. 模組：    from community2address import lookup
              result = lookup("健安新城F區")
"""

import csv
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, List

# ========== 路徑設定 ==========
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
DB_PATH = LAND_DIR / "db" / "land_data.db"
MANUAL_CSV = LAND_DIR / "db" / "manual_mapping.csv"

# ========== 全形半形轉換 ==========
FULLWIDTH_DIGITS = "０１２３４５６７８９"
HALFWIDTH_DIGITS = "0123456789"
FULLWIDTH_UPPER = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
HALFWIDTH_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
FULLWIDTH_LOWER = "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
HALFWIDTH_LOWER = "abcdefghijklmnopqrstuvwxyz"
FW_TO_HW = str.maketrans(
    FULLWIDTH_DIGITS + FULLWIDTH_UPPER + FULLWIDTH_LOWER,
    HALFWIDTH_DIGITS + HALFWIDTH_UPPER + HALFWIDTH_LOWER,
)
HW_TO_FW = str.maketrans(HALFWIDTH_DIGITS, FULLWIDTH_DIGITS)


def fullwidth_to_halfwidth(s: str) -> str:
    return s.translate(FW_TO_HW)


# ========== 縣市列表 ==========
CITIES = [
    "臺北市", "台北市", "新北市", "桃園市", "桃園縣",
    "臺中市", "台中市", "臺南市", "台南市", "高雄市",
    "基隆市", "新竹市", "新竹縣", "苗栗縣", "彰化縣",
    "南投縣", "雲林縣", "嘉義市", "嘉義縣", "屏東縣",
    "宜蘭縣", "花蓮縣", "臺東縣", "台東縣", "澎湖縣",
    "金門縣", "連江縣",
]


def normalize_community_name(name: str) -> str:
    """正規化建案名稱：去空白、全形→半形數字/字母、英文字母統一為大寫"""
    if not name:
        return ""
    s = name.strip()
    s = fullwidth_to_halfwidth(s)
    # 英文字母統一大寫（建案名如 F區/f區 應視為相同）
    s = s.upper()
    # 去除多餘空白
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def strip_city_district(addr: str) -> str:
    """從地址中移除縣市和鄉鎮市區，只保留路段+門牌"""
    s = fullwidth_to_halfwidth(str(addr).strip())
    for city in CITIES:
        if s.startswith(city):
            s = s[len(city):]
            break
    # 去除鄉鎮市區
    s = re.sub(r'^[\u4e00-\u9fff]{1,3}[區鎮鄉市]', '', s)
    return s.strip()


def extract_number(addr: str) -> int:
    """從地址中提取門牌號碼"""
    s = fullwidth_to_halfwidth(str(addr))
    # 先嘗試巷弄號：如 "29巷5號" 取 5
    m = re.search(r'巷(\d+)號', s)
    if m:
        return int(m.group(1))
    # 再嘗試一般號碼：如 "三民路100號" 取 100
    m = re.search(r'(\d+)號', s)
    if m:
        return int(m.group(1))
    return -1


def extract_road_alley(addr: str) -> str:
    """提取路段+巷弄（不含門牌號）"""
    s = fullwidth_to_halfwidth(str(addr).strip())
    # 去除縣市和區
    for city in CITIES:
        if s.startswith(city):
            s = s[len(city):]
            break
    s = re.sub(r'^[\u4e00-\u9fff]{1,3}[區鎮鄉市]', '', s)
    # 去除里鄰
    s = re.sub(r'[\u4e00-\u9fff]*里\d*鄰?', '', s)
    s = re.sub(r'\d+鄰', '', s)
    # 提取到巷或路段（非貪婪，避免跨越多個路/街字元）
    m = re.search(r'([一-鿿]+?(?:路|街|大道)(?:[一二三四五六七八九十]+段)?(?:\d+巷(?:\d+弄)?)?)', s)
    return m.group(1) if m else ""


def format_address_range(addresses: list, raw_addresses: list = None) -> dict:
    """
    將地址列表格式化為地址範圍摘要
    
    回傳:
    {
        "summary": "三民路29巷1、3、5、7號",
        "road_groups": [
            {"road": "三民路29巷", "numbers": [1, 3, 5, 7], "formatted": "三民路29巷1、3、5、7號"}
        ],
        "total_addresses": 4,
        "raw_addresses": [...]
    }
    """
    if not addresses:
        return {
            "summary": "無地址資料",
            "road_groups": [],
            "total_addresses": 0,
            "raw_addresses": raw_addresses or [],
        }

    # 歸類: road_alley → list of (門牌號碼, 完整地址)
    INTERSECTION_KEYWORDS = ('口', '旁', '對面', '附近', '和', '與', '及', '交叉')
    road_map = defaultdict(list)
    ungrouped = []

    for addr in addresses:
        road = extract_road_alley(addr)
        num = extract_number(addr)
        if road and num > 0:
            road_map[road].append((num, addr))
        else:
            ungrouped.append(addr)

    # 無門牌號碼的地址：只有當路段尚未被有號碼的地址使用時，才建立空群組
    # 排除路口/交叉等非門牌地址（讓它們進入 truly_ungrouped）
    for addr in ungrouped:
        road = extract_road_alley(addr)
        is_intersection = any(kw in addr for kw in INTERSECTION_KEYWORDS)
        if road and road not in road_map and not is_intersection:
            road_map[road] = []

    road_groups = []
    for road, items in sorted(road_map.items()):
        numbers = sorted(set(num for num, _ in items)) if items else []
        if not numbers:
            road_groups.append({
                "road": road,
                "numbers": [],
                "formatted": f"{road}（無門牌號碼）",
                "count": 0,
            })
        elif len(numbers) <= 10:
            num_str = "、".join(str(n) for n in numbers) + "號"
            road_groups.append({
                "road": road,
                "numbers": numbers,
                "formatted": f"{road}{num_str}",
                "count": len(numbers),
            })
        else:
            num_str = f"{numbers[0]}～{numbers[-1]}號（共{len(numbers)}個門牌）"
            road_groups.append({
                "road": road,
                "numbers": numbers,
                "formatted": f"{road}{num_str}",
                "count": len(numbers),
            })

    # 排序：有門牌的排前面、交易最多的路段排前面
    road_groups.sort(key=lambda x: (-x["count"]))

    # 無法歸類的地址（或含交叉路口關鍵字的地址）
    INTERSECTION_KEYWORDS = ('路口', '交叉', '旁', '對面', '和', '與', '及')
    truly_ungrouped = []
    for a in ungrouped:
        road = extract_road_alley(a)
        if not road or any(kw in a for kw in INTERSECTION_KEYWORDS):
            truly_ungrouped.append(strip_city_district(a) or a)

    # 組合摘要
    summaries = [g["formatted"] for g in road_groups[:5]]
    # 交叉路口等非標準地址直接顯示（去重）
    seen_misc = set()
    for a in truly_ungrouped:
        if a not in seen_misc:
            summaries.append(a)
            seen_misc.add(a)
    if len(road_groups) > 5:
        summaries.append(f"...還有 {len(road_groups) - 5} 條路段")

    return {
        "summary": "；".join(summaries),
        "road_groups": road_groups,
        "total_addresses": len(addresses),
        "raw_addresses": raw_addresses or addresses,
    }


# ========== 591 API 客戶端 ==========

CITY_TO_591_REGION = {
    "臺北市": 1,  "台北市": 1,
    "基隆市": 2,  "新北市": 3,
    "新竹市": 4,  "新竹縣": 5,
    "桃園市": 6,  "桃園縣": 6,
    "苗栗縣": 7,  "臺中市": 8,
    "台中市": 8,  "彰化縣": 10,
    "南投縣": 11, "嘉義市": 12,
    "嘉義縣": 13, "雲林縣": 14,
    "臺南市": 15, "台南市": 15,
    "高雄市": 17, "屏東縣": 19,
    "宜蘭縣": 21, "臺東縣": 22,
    "台東縣": 22, "花蓮縣": 23,
    "澎湖縣": 24, "金門縣": 25,
}

DEFAULT_REGION_ORDER = [1, 3, 6, 8, 15, 17, 5, 4, 10, 21, 19]


class Api591Client:
    """591.com.tw 社區搜尋 API 客戶端（使用標準庫 urllib）"""

    BASE_URL = "https://bff.591.com.tw"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://community.591.com.tw/",
    }

    def __init__(self, cache_dir: str = None, timeout: int = 8):
        self.timeout = timeout
        self.cache_dir = Path(cache_dir or "/tmp/591_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def search_community(self, keyword: str, regionid: int) -> List[Dict]:
        """搜尋社區/建案名稱"""
        safe_key = keyword.replace("/", "_").replace("\\", "_")
        cache_key = f"{regionid}_{safe_key}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        params = urllib.parse.urlencode({"keyword": keyword, "regionid": regionid})
        url = f"{self.BASE_URL}/v1/community/search/match?{params}"

        try:
            req = urllib.request.Request(url, headers=self.HEADERS)
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                data = json.loads(r.read().decode("utf-8"))
                if data.get("status") == 1:
                    items = data.get("data", {}).get("items", [])
                    result = [item for item in items if item.get("name")]
                    self._save_cache(cache_key, result)
                    return result
        except Exception:
            pass

        self._save_cache(cache_key, [])
        return []

    def search_by_name(self, community_name: str,
                       regionids: List[int] = None) -> Optional[Dict]:
        """用建案名稱搜尋，回傳最佳匹配"""
        if not regionids:
            regionids = DEFAULT_REGION_ORDER

        for rid in regionids:
            items = self.search_community(community_name, rid)
            if not items:
                continue
            best = self._best_match(items, community_name)
            if best:
                return best
            time.sleep(0.1)

        return None

    @staticmethod
    def _best_match(items: List[Dict], query: str) -> Optional[Dict]:
        """從搜尋結果中選出最佳匹配"""
        if not items:
            return None

        best = None
        best_score = -1

        for item in items:
            name = item.get("name", "")
            if not name:
                continue
            score = 0
            if name == query:
                score = 100
            elif query in name:
                score = 80 + len(query) * 2
            elif name in query:
                score = 70
            else:
                common = sum(1 for c in query if c in name)
                if common:
                    score = int(common / max(len(query), 1) * 40)

            if score > best_score:
                best_score = score
                best = item

        return best if best_score >= 20 else None

    def _get_cache(self, key: str) -> Optional[List]:
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return None

    def _save_cache(self, key: str, data):
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass


# ========== 核心查詢引擎 ==========

class Community2AddressLookup:
    """社區/建案名稱→地址範圍 查詢引擎（SQLite + 591 API）"""

    def __init__(self, verbose: bool = False, use_591: bool = True):
        """
        初始化查詢引擎

        Args:
            verbose: 是否顯示詳細過程
            use_591: 是否啟用 591 API 補充（本地找不到時自動呼叫）
        """
        self.verbose = verbose
        self.use_591 = use_591

        # 建案名稱→地址列表 (來源: manual_mapping.csv, 小量資料直接載入)
        self._com_to_addr_manual = defaultdict(list)
        # 建案名稱→區域資訊 (輕量快取，按需載入)
        self._com_info = {}
        # 所有建案名稱（正規化）
        self._all_names = set()
        # 正規化名稱→原始名稱映射
        self._norm_to_original = {}
        # DB 持久連線（用於 on-demand 查詢）
        self._conn = None

        self._load_data()

        # 591 API client（延遲初始化，第一次查詢時才建立）
        self._api591 = None

    def _load_data(self):
        """載入建案名稱索引（輕量啟動，不載入地址）"""
        t0 = time.time()
        self._load_name_index()
        self._load_manual_csv()
        elapsed = time.time() - t0

        total_communities = len(self._all_names)
        print(f"  ✅ com2address: 已索引 {total_communities:,} 個建案 ({elapsed:.2f}s)")

    def _load_name_index(self):
        """從 land_data.db 僅載入建案名稱（不載入資訊，啟動極快 ~60ms）"""
        if not DB_PATH.exists():
            print(f"⚠️  資料庫不存在: {DB_PATH}")
            return

        self._conn = sqlite3.connect(str(DB_PATH))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA cache_size=-64000")
        self._conn.execute("PRAGMA mmap_size=268435456")

        # 只載入 DISTINCT 建案名稱（利用 community_name 索引，~60ms）
        cursor = self._conn.execute("""
            SELECT DISTINCT community_name
            FROM land_transaction
            WHERE community_name IS NOT NULL AND community_name != ''
        """)

        for (community,) in cursor:
            community = community.strip()
            if not community:
                continue

            norm_name = normalize_community_name(community)
            self._all_names.add(norm_name)
            self._norm_to_original.setdefault(norm_name, community)

        print(f"  ✅ DB 名稱索引: {len(self._all_names):,} 個建案")

    def _get_com_info(self, norm_name: str) -> dict:
        """按需查詢建案基本資訊（使用索引，<5ms）"""
        if norm_name in self._com_info:
            return self._com_info[norm_name]

        if not self._conn:
            return {'district': '', 'city': '', 'source': '', 'tx_count': 0}

        original_name = self._norm_to_original.get(norm_name, norm_name)
        row = self._conn.execute("""
            SELECT MIN(district) as district, MIN(county_city) as city, COUNT(*) as tx_count
            FROM land_transaction
            WHERE community_name = ?
        """, (original_name,)).fetchone()

        if row:
            info = {
                'district': (row[0] or '').strip(),
                'city': (row[1] or '').strip(),
                'source': 'land_data.db',
                'tx_count': row[2] or 0,
            }
        else:
            info = {'district': '', 'city': '', 'source': '', 'tx_count': 0}

        self._com_info[norm_name] = info
        return info

    def _load_manual_csv(self):
        """從 manual_mapping.csv 載入"""
        if not MANUAL_CSV.exists():
            return

        with open(MANUAL_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                community = row.get('社區名稱', '').strip()
                addr = row.get('地址', '').strip()
                district = row.get('鄉鎮市區', '').strip()

                if not community or not addr:
                    continue

                norm_name = normalize_community_name(community)
                self._com_to_addr_manual[norm_name].append(addr)
                self._all_names.add(norm_name)
                self._norm_to_original.setdefault(norm_name, community)
                if norm_name not in self._com_info:
                    self._com_info[norm_name] = {
                        'district': district,
                        'city': '',
                        'source': '手動',
                        'tx_count': 0,
                    }

    def _fuzzy_match(self, keyword: str, top_n: int = 10) -> list:
        """模糊匹配建案名稱（optimized: 先做快速篩選再精確評分）"""
        norm_kw = normalize_community_name(keyword)
        if not norm_kw:
            return []

        matches = []
        kw_set = set(norm_kw)  # 用於快速字元交集篩選

        for name in self._all_names:
            score = 0
            # 完全匹配
            if name == norm_kw:
                score = 100
            # 包含匹配（關鍵字是建案名的子字串）
            elif norm_kw in name:
                ratio = len(norm_kw) / max(len(name), 1)
                score = int(70 + ratio * 15)  # 70~85
            # 包含匹配（建案名是關鍵字的子字串）
            elif name in norm_kw:
                ratio = len(name) / max(len(norm_kw), 1)
                score = int(60 + ratio * 10)  # 60~70
            # 快速字元交集篩選（避免昂貴的 Counter 計算）
            elif len(kw_set & set(name)) / max(len(norm_kw), 1) >= 0.55:
                from collections import Counter
                kw_cnt = Counter(norm_kw)
                name_cnt = Counter(name)
                common = sum(min(kw_cnt[c], name_cnt[c]) for c in kw_cnt)
                ratio = common / max(len(norm_kw), 1)
                if ratio >= 0.55:
                    score = int(ratio * 55)

            if score > 0:
                info = self._get_com_info(name)
                matches.append({
                    'name': self._norm_to_original.get(name, name),
                    'norm_name': name,
                    'score': score,
                    'district': info.get('district', ''),
                    'tx_count': info.get('tx_count', 0),
                })

        # 排序: 分數 → 交易數
        matches.sort(key=lambda x: (-x['score'], -x['tx_count']))
        return matches[:top_n]

    def _query_db_addresses(self, community_name: str) -> list:
        """從 DB 按需查詢建案對應的地址（使用 community_name 索引，<5ms）"""
        if not self._conn:
            return []
        rows = self._conn.execute("""
            SELECT DISTINCT address
            FROM land_transaction
            WHERE community_name = ? AND address IS NOT NULL AND address != ''
        """, (community_name,)).fetchall()
        return [r[0].strip() for r in rows if r[0]]

    def query(self, community_name: str, top_n: int = 5, use_591: bool = None) -> dict:
        """
        查詢建案名稱對應的地址範圍

        查詢順序：本地精確 → 本地模糊 → 591 API 備援（本地完全找不到時）
        使用索引查詢，毫秒級回應。
        """
        enable_591 = use_591 if use_591 is not None else self.use_591
        norm_name = normalize_community_name(community_name)

        if self.verbose:
            print(f"  🔍 查詢建案: {community_name}")
            print(f"     正規化: {norm_name}")

        addresses = []
        raw_addresses = []
        match_type = None
        matched_name = norm_name
        district = ''
        city = ''

        # === 第 1 層：精確匹配（索引查詢） ===
        if norm_name in self._all_names:
            match_type = "精確匹配"
            original_name = self._norm_to_original.get(norm_name, community_name)
            # 從 DB on-demand 查詢地址（利用 community_name 索引）
            db_addrs = self._query_db_addresses(original_name)
            manual_addrs = self._com_to_addr_manual.get(norm_name, [])
            raw_addresses = list(set(db_addrs + manual_addrs))
            addresses = raw_addresses
            if self.verbose:
                print(f"     ✅ Level 1: 精確匹配, {len(addresses)} 個地址")

        # === 第 2 層：模糊匹配 ===
        if match_type is None:
            fuzzy_results = self._fuzzy_match(norm_name, top_n=5)
            if fuzzy_results and fuzzy_results[0]['score'] >= 50:
                best = fuzzy_results[0]
                matched_name = best['norm_name']
                match_type = f"模糊匹配 ({best['score']}%)"
                original_name = self._norm_to_original.get(matched_name, best['name'])
                db_addrs = self._query_db_addresses(original_name)
                manual_addrs = self._com_to_addr_manual.get(matched_name, [])
                raw_addresses = list(set(db_addrs + manual_addrs))
                addresses = raw_addresses
                if self.verbose:
                    print(f"     ✅ Level 2: 模糊匹配 {best['name']} ({best['score']}%)")

        # === 第 3 層：591 API 備援（本地完全找不到時） ===
        if match_type is None and enable_591:
            if self.verbose:
                print(f"     🌐 Level 3: 呼叫 591 API...")
            api_result = self._query_591_fallback(community_name)
            if api_result:
                match_type = "591 API"
                matched_name = normalize_community_name(api_result.get('name', community_name))
                addresses = api_result.get('addresses', [])
                raw_addresses = addresses
                district = api_result.get('district', '')
                if self.verbose:
                    print(f"     ✅ 591 找到: {api_result.get('name')} | {addresses}")
                # 儲存到記憶體和 manual_mapping.csv（下次直接本地命中）
                self._persist_591_result(community_name, api_result)

        # === 格式化 ===
        # 用 DB 擴展地址（從代表地址找出同社區所有門牌號）
        unique_addrs = list(set(addresses))
        info = self._get_com_info(matched_name)
        if not district:
            district = info.get('district', '')
        if not city:
            city = info.get('city', '')
        expanded = self._expand_addresses_from_db(unique_addrs, district)
        if expanded:
            unique_addrs = expanded
            if self.verbose:
                print(f"     📍 DB 擴展: {len(addresses)} → {len(unique_addrs)} 個地址")
        address_range = format_address_range(unique_addrs, raw_addresses or unique_addrs)

        candidates = self._fuzzy_match(norm_name, top_n=top_n)

        return {
            'input': community_name,
            'matched_name': self._norm_to_original.get(matched_name, matched_name),
            'match_type': match_type or "未找到",
            'district': district,
            'city': city,
            'transaction_count': info.get('tx_count', 0),
            'address_range': address_range,
            'candidates': candidates,
            'found': match_type is not None,
        }

    def _expand_addresses_from_db(self, addresses: list, district: str = '') -> list:
        """
        從代表地址擴展出同社區的所有門牌號。
        使用持久連線和索引查詢。
        """
        if not addresses or not self._conn:
            return []

        expanded = set()
        conn = self._conn
        for addr in addresses:
            s = fullwidth_to_halfwidth(str(addr).strip())

            # 去除縣市
            for c in CITIES:
                if s.startswith(c):
                    s = s[len(c):]
                    break
            # 去除鄉鎮市區
            s = re.sub(r'^[\u4e00-\u9fff]{1,3}[區鎮鄉市]', '', s)

            # 解析 street (路/街/大道) 和 lane (巷)
            m = re.search(r'([一-鿿]+?(?:路|街|大道)(?:[一二三四五六七八九十]+段)?)', s)
            if not m:
                expanded.add(addr)
                continue
            street = m.group(1)
            lane_m = re.search(r'(\d+)巷', s)
            lane = lane_m.group(1) if lane_m else ''

            # 門牌號
            num_m = re.search(r'(\d+)號', s)
            if not num_m:
                expanded.add(addr)
                continue
            ref_number = num_m.group(1)

            # 找 district（若未提供，從原始地址解析）
            addr_district = district
            if not addr_district:
                raw = fullwidth_to_halfwidth(str(addr).strip())
                for c in CITIES:
                    if raw.startswith(c):
                        raw = raw[len(c):]
                        dm = re.match(r'([\u4e00-\u9fff]{1,3}[區鎮鄉市])', raw)
                        if dm:
                            addr_district = dm.group(1)
                        break

            if not addr_district:
                expanded.add(addr)
                continue

            # 從 DB 取得代表地址的建物特徵（使用索引）
            rows = conn.execute("""
                SELECT total_floors, build_date FROM land_transaction
                WHERE street=? AND lane=? AND number=? AND district=?
                LIMIT 1
            """, (street, lane, ref_number, addr_district)).fetchall()

            if not rows:
                expanded.add(addr)
                continue

            total_floors, build_date = rows[0]

            # 找同社區所有門牌號（使用索引）
            all_numbers = conn.execute("""
                SELECT DISTINCT CAST(number AS INTEGER) as num
                FROM land_transaction
                WHERE street=? AND lane=? AND district=?
                  AND total_floors=? AND build_date=?
                  AND number IS NOT NULL AND number != ''
                ORDER BY num
            """, (street, lane, addr_district,
                  total_floors, build_date)).fetchall()

            if all_numbers:
                road = street + (f"{lane}巷" if lane else "")
                for (num,) in all_numbers:
                    expanded.add(f"{road}{num}號")
            else:
                expanded.add(addr)

        return list(expanded) if expanded else []

    def _query_591_fallback(self, community_name: str) -> dict:
        """
        591 API 備援查詢（本地完全找不到時呼叫）
        回傳 {name, addresses, district} 或 None
        """
        try:
            if self._api591 is None:
                self._api591 = Api591Client()

            item = self._api591.search_by_name(community_name)
            if item:
                address = item.get('address', '')
                return {
                    'name': item.get('name', community_name),
                    'addresses': [address] if address else [],
                    'district': item.get('section', ''),
                }
        except Exception as e:
            if self.verbose:
                print(f"  ⚠️  591 API 錯誤: {e}")
        return None

    def _persist_591_result(self, original_query: str, api_result: dict):
        """將 591 查詢結果存入記憶體和 manual_mapping.csv（避免重複 API 呼叫）"""
        name = api_result.get('name', original_query)
        addresses = api_result.get('addresses', [])
        district = api_result.get('district', '')
        if not addresses:
            return

        norm = normalize_community_name(name)

        # 更新記憶體
        for addr in addresses:
            if addr not in self._com_to_addr_manual[norm]:
                self._com_to_addr_manual[norm].append(addr)
        self._all_names.add(norm)
        self._norm_to_original.setdefault(norm, name)
        if norm not in self._com_info:
            self._com_info[norm] = {
                'district': district, 'city': '', 'source': '591_API', 'tx_count': 0
            }

        # 寫入 manual_mapping.csv
        file_exists = MANUAL_CSV.exists()
        try:
            with open(MANUAL_CSV, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['地址', '社區名稱', '鄉鎮市區', '備註'])
                for addr in addresses:
                    writer.writerow([addr, name, district, '591_API'])
            if self.verbose:
                print(f"  💾 已存入 manual_mapping.csv: {name} → {addresses}")
        except Exception as e:
            if self.verbose:
                print(f"  ⚠️  儲存失敗: {e}")

    def close(self):
        """關閉資料庫連線"""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    def search(self, keyword: str, limit: int = 20) -> list:
        """搜尋建案名稱（用於自動完成）"""
        return self._fuzzy_match(keyword, top_n=limit)

    def stats(self) -> dict:
        """統計資訊"""
        return {
            'total_communities': len(self._all_names),
            'db_communities': len([n for n in self._all_names if n not in self._com_to_addr_manual]),
            'manual_communities': len(self._com_to_addr_manual),
        }


# ========== 便利函式 ==========

_global_lookup = None


def lookup(community_name: str, **kwargs) -> dict:
    """便利查詢函式"""
    global _global_lookup
    if _global_lookup is None:
        _global_lookup = Community2AddressLookup(**kwargs)
    return _global_lookup.query(community_name)


def quick_lookup(community_name: str) -> str:
    """最簡查詢，回傳地址摘要"""
    result = lookup(community_name)
    if result['found']:
        return result['address_range']['summary']
    return "未找到"


# ========== CLI ==========

def print_result(result: dict, show_detail: bool = False):
    """格式化輸出"""
    name = result['input']
    found = result['found']

    if found:
        matched = result['matched_name']
        match_type = result['match_type']
        addr_range = result['address_range']
        district = result['district']
        tx_count = result['transaction_count']

        print(f"\n🏘️  {name}")
        if matched != name:
            print(f"   → 匹配: {matched} ({match_type})")
        else:
            print(f"   → {match_type}")

        if district:
            print(f"   📍 區域: {district}")
        if tx_count:
            print(f"   📊 交易筆數: {tx_count:,}")

        print(f"   📬 地址數: {addr_range['total_addresses']}")
        print()

        # 輸出路段分組
        for g in addr_range['road_groups'][:8]:
            print(f"   🏠 {g['formatted']}")

        if len(addr_range['road_groups']) > 8:
            remaining = len(addr_range['road_groups']) - 8
            print(f"   ... 還有 {remaining} 條路段")

        if show_detail:
            print(f"\n   === 所有原始地址 ===")
            for i, addr in enumerate(sorted(set(addr_range['raw_addresses']))[:30], 1):
                print(f"   {i:3d}. {addr}")
            if len(addr_range['raw_addresses']) > 30:
                print(f"   ... 共 {len(addr_range['raw_addresses'])} 筆")
    else:
        print(f"\n🏘️  {name}")
        print(f"   → ❓ 未找到")

        # 顯示候選
        if result['candidates']:
            print(f"\n   💡 你可能在找：")
            for c in result['candidates'][:5]:
                print(f"   • {c['name']} ({c['district']}, {c['tx_count']}筆)")


def interactive_mode(engine: Community2AddressLookup):
    """互動模式"""
    stats = engine.stats()
    print("=" * 60)
    print("🏘️  建案名稱→地址範圍 查詢工具 (com2address)")
    print("=" * 60)
    print(f"📊 建案數: {stats['total_communities']:,}")
    print("-" * 60)
    print("輸入建案名稱查詢，'q' 退出，'detail' 詳細模式")
    print("-" * 60)

    show_detail = False

    while True:
        try:
            name = input("\n🔎 建案名稱: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 再見！")
            break

        if not name:
            continue
        if name.lower() in ("q", "quit", "exit"):
            print("👋 再見！")
            break
        if name.lower() == "detail":
            show_detail = not show_detail
            print(f"   詳細模式: {'開啟' if show_detail else '關閉'}")
            continue
        if name.lower() == "stats":
            s = engine.stats()
            print(f"   建案數: {s['total_communities']:,}")
            continue

        t0 = time.time()
        result = engine.query(name)
        elapsed = time.time() - t0
        print_result(result, show_detail)
        print(f"   ⏱️  {elapsed:.3f}s")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="社區/建案名稱→地址範圍 查詢工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用範例：
  python3 community2address.py "健安新城A區"
  python3 community2address.py "健安新城F區"
  python3 community2address.py "都廳大院"
  python3 community2address.py --detail "仁愛帝寶"
  python3 community2address.py -j "信義星池"
  python3 community2address.py --search "健安"
  python3 community2address.py --no-591 "建案名稱"   (離線模式，停用 591 API)
        """,
    )
    parser.add_argument("name", nargs="*", help="建案名稱")
    parser.add_argument("--detail", "-d", action="store_true", help="顯示詳細地址")
    parser.add_argument("--verbose", "-v", action="store_true", help="顯示詳細過程")
    parser.add_argument("--json", "-j", action="store_true", help="JSON 輸出")
    parser.add_argument("--search", "-s", help="搜尋建案名稱")
    parser.add_argument("--no-591", action="store_true", help="停用 591 API（離線模式）")
    # 向下相容舊的 --with-591 旗標（已廢棄，591 預設為啟用）
    parser.add_argument("--with-591", action="store_true", help=argparse.SUPPRESS)

    args = parser.parse_args()
    use_591 = not args.no_591

    engine = Community2AddressLookup(verbose=args.verbose, use_591=use_591)

    if args.search:
        results = engine.search(args.search, limit=20)
        if args.json:
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            print(f"\n🔍 搜尋「{args.search}」找到 {len(results)} 個建案：")
            for r in results:
                print(f"  • {r['name']} ({r['district']}, {r['tx_count']}筆, 相似度:{r['score']}%)")
        return

    if args.name:
        for name in args.name:
            t0 = time.time()
            result = engine.query(name)
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
