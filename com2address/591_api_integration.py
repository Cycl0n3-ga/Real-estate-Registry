#!/usr/bin/env python3
"""
591_api_integration.py - 591.com.tw API 集成模組

功能：
  - 使用 591 社區搜尋 API 補充本地 CSV 缺失的建案資料
  - 不需要 requests，只用 Python 標準庫 urllib

API 端點（與 address2community.py 一致）：
  https://bff.591.com.tw/v1/community/search/match?keyword=XXX&regionid=YYY
"""

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Dict, List

# ============ 591 regionid 對照 ============
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

# 主要都市預設搜尋順序
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
        """
        搜尋社區/建案名稱

        Args:
            keyword:  搜尋關鍵字（建案名稱）
            regionid: 591 城市代碼

        Returns:
            items 列表，每項含 name/address/section 等欄位
        """
        # 先查快取
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
        """
        用建案名稱搜尋，回傳最佳匹配（地址、社區名稱）

        Args:
            community_name: 建案/社區名稱
            regionids:      要嘗試的 591 regionid 列表

        Returns:
            最佳匹配字典 {name, address, section} 或 None
        """
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


class HybridLookup:
    """混合查詢（本地資料 + 591 API）"""

    def __init__(self, local_data: Dict, use_591: bool = True):
        """
        Args:
            local_data: {建案名稱: {addresses, district, transaction_count}}
            use_591:    是否啟用 591 API 補充
        """
        self.local_data = local_data
        self.use_591 = use_591
        self.api = Api591Client() if use_591 else None

    def lookup(self, community_name: str, use_591: bool = None) -> Dict:
        """
        混合查詢

        Args:
            community_name: 建案/社區名稱
            use_591:        是否使用 591 API（None 表示使用初始化設置）

        Returns:
            {success, data: {addresses, district, ...}, source}
        """
        enable_api = use_591 if use_591 is not None else self.use_591

        # 第 1 層：本地資料
        if community_name in self.local_data:
            local = self.local_data[community_name]
            return {
                "success": True,
                "source": "本地資料",
                "data": local,
            }

        # 第 2 層：591 API
        if enable_api and self.api:
            item = self.api.search_by_name(community_name)
            if item:
                address = item.get("address", "")
                addresses = [address] if address else []
                return {
                    "success": True,
                    "source": "591 API",
                    "data": {
                        "addresses": addresses,
                        "district": item.get("section", ""),
                        "transaction_count": 0,
                    },
                }

        return {
            "success": False,
            "source": "無",
            "error": f"未找到「{community_name}」",
        }


# ============ 測試 ============

if __name__ == "__main__":
    print("=" * 60)
    print("591 API 集成模組 - 測試（不需要 requests）")
    print("=" * 60)

    client = Api591Client()
    test_cases = ["健安新城F區", "仁愛帝寶", "都廳大院"]

    print("\n📌 測試搜尋（需要網路連接）\n")
    for name in test_cases:
        print(f"搜尋: {name}")
        result = client.search_by_name(name)
        if result:
            print(f"  ✅ {result.get('name')} | {result.get('address', '')}")
        else:
            print(f"  ⚠️  未找到或 API 受限")

    print("\n" + "=" * 60)
