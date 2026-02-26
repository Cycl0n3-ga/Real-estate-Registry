#!/usr/bin/env python3
"""
api591.py — 591 社區搜尋 API 統一客戶端
==========================================
合併自 address2community.Api591 與 community2address.Api591Client，
提供統一介面避免重複實作。

功能:
  - search_community(keyword, regionid) — 基礎搜尋
  - search_by_address(address) — 用地址搜尋社區
  - search_by_name(community_name) — 用建案名稱搜尋
  - 磁碟快取（可選）
"""

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Dict, List

from address_utils import (
    fullwidth_to_halfwidth,
    strip_to_road_number, extract_road, extract_road_alley,
    extract_road_number, get_591_regionids,
    DEFAULT_591_REGION_ORDER,
)


class Api591Client:
    """591 社區搜尋 API 統一客戶端"""

    BASE_URL = "https://bff.591.com.tw"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://community.591.com.tw/",
    }

    def __init__(self, cache_dir: str = None, timeout: int = 8):
        self.timeout = timeout
        self._cache_dir = None
        if cache_dir is not None:
            self._cache_dir = Path(cache_dir)
            self._cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 基礎搜尋
    # ------------------------------------------------------------------

    def search_community(self, keyword: str, regionid: int) -> List[Dict]:
        """搜尋社區/建案名稱，回傳 items 列表"""
        # 檢查快取
        if self._cache_dir:
            cached = self._get_cache(regionid, keyword)
            if cached is not None:
                return cached

        params = urllib.parse.urlencode({
            "keyword": keyword,
            "regionid": regionid,
        })
        url = f"{self.BASE_URL}/v1/community/search/match?{params}"

        result = []
        try:
            req = urllib.request.Request(url, headers=self.HEADERS)
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                data = json.loads(r.read().decode("utf-8"))
                if data.get("status") == 1:
                    items = data.get("data", {}).get("items", [])
                    result = [item for item in items if item.get("name")]
        except Exception:
            pass

        if self._cache_dir:
            self._save_cache(regionid, keyword, result)
        return result

    # ------------------------------------------------------------------
    # 用地址搜尋社區
    # ------------------------------------------------------------------

    def search_by_address(self, address: str,
                          regionids: list = None) -> Optional[Dict]:
        """用地址搜尋社區，回傳最佳匹配的 item dict"""
        norm = strip_to_road_number(fullwidth_to_halfwidth(address))

        keywords = []
        road_number = extract_road_number(norm)
        road = extract_road(norm)

        if road_number:
            keywords.append(road_number)
            if road_number.endswith('號'):
                keywords.append(road_number[:-1])
        if road:
            keywords.append(road)

        if not regionids:
            regionids = get_591_regionids(address)

        for rid in regionids:
            for keyword in keywords:
                results = self.search_community(keyword, rid)
                if results:
                    best = self._best_match_by_address(results, norm)
                    if best:
                        return best
            if road:
                results = self.search_community(
                    road, regionids[0] if regionids else rid)
                if results:
                    best = self._best_match_by_address(results, norm)
                    if best:
                        return best

        return None

    # ------------------------------------------------------------------
    # 用建案名稱搜尋
    # ------------------------------------------------------------------

    def search_by_name(self, community_name: str,
                       regionids: list = None) -> Optional[Dict]:
        """用建案名稱搜尋，回傳最佳匹配的 item dict"""
        if not regionids:
            regionids = list(DEFAULT_591_REGION_ORDER)

        for rid in regionids:
            items = self.search_community(community_name, rid)
            if not items:
                continue
            best = self._best_match_by_name(items, community_name)
            if best:
                return best
            time.sleep(0.1)

        return None

    # ------------------------------------------------------------------
    # 匹配策略
    # ------------------------------------------------------------------

    @staticmethod
    def _best_match_by_address(results: list, norm_addr: str) -> Optional[Dict]:
        """從搜尋結果中找與地址最匹配的項目"""
        num_match = re.search(r'(\d+)號', norm_addr)
        target_num = int(num_match.group(1)) if num_match else None
        road = extract_road(norm_addr)
        target_alley = extract_road_alley(norm_addr)

        best = None
        best_score = -1

        for item in results:
            item_addr = item.get('address', '')
            if not item_addr or not item.get('name'):
                continue

            score = 0

            item_road = extract_road(item_addr)
            if road and item_road and road == item_road:
                score += 10

            item_alley = extract_road_alley(item_addr)
            if target_alley and item_alley and target_alley == item_alley:
                score += 10

            if target_num:
                item_num_match = re.search(r'(\d+)號', item_addr)
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

    @staticmethod
    def _best_match_by_name(items: list, query: str) -> Optional[Dict]:
        """從搜尋結果中選出與名稱最匹配的項目"""
        if not items:
            return None

        best = None
        best_score = -1

        for item in items:
            name = item.get('name', '')
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

    # ------------------------------------------------------------------
    # 快取
    # ------------------------------------------------------------------

    def _get_cache(self, regionid: int, keyword: str) -> Optional[List]:
        if not self._cache_dir:
            return None
        safe_key = keyword.replace('/', '_').replace('\\', '_')
        cache_file = self._cache_dir / f"{regionid}_{safe_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return None

    def _save_cache(self, regionid: int, keyword: str, data):
        if not self._cache_dir:
            return
        safe_key = keyword.replace('/', '_').replace('\\', '_')
        cache_file = self._cache_dir / f"{regionid}_{safe_key}.json"
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass
