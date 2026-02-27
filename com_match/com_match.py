#!/usr/bin/env python3
"""
com_match.py — 建案名稱模糊搜尋引擎

功能：
  - 輸入建案關鍵字（部分名稱、拼字錯誤等），回傳所有可能匹配的建案
  - 使用 SQLite LIKE + 子序列匹配 + 編輯距離，多層搜尋
  - 回傳結果包含交易筆數、均價等摘要

使用方式：
  from com_match import CommunityMatcher, fuzzy_search
  matcher = CommunityMatcher(db_path)
  results = matcher.search("遠雄幸福")
  # 或快速呼叫
  results = fuzzy_search("遠雄", db_path)
"""

import re
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict

# ── 路徑設定 ──
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
DEFAULT_DB_PATH = str(LAND_DIR / "db" / "land_data.db")

# ── 全形半形轉換 ──
_FW_DIGITS = "０１２３４５６７８９"
_HW_DIGITS = "0123456789"
_FW_UPPER = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
_HW_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_FW_LOWER = "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
_HW_LOWER = "abcdefghijklmnopqrstuvwxyz"
_FW_TO_HW = str.maketrans(
    _FW_DIGITS + _FW_UPPER + _FW_LOWER,
    _HW_DIGITS + _HW_UPPER + _HW_LOWER,
)


def _normalize(name: str) -> str:
    """正規化建案名稱：全形→半形、大寫、去空白"""
    if not name:
        return ""
    s = name.strip().translate(_FW_TO_HW).upper()
    s = re.sub(r'\s+', '', s)
    return s


def _edit_distance(a: str, b: str, max_dist: int = 5) -> int:
    """計算編輯距離（Levenshtein），超過 max_dist 提前中止"""
    la, lb = len(a), len(b)
    if abs(la - lb) > max_dist:
        return max_dist + 1
    if la == 0:
        return lb
    if lb == 0:
        return la

    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        curr = [i] + [0] * lb
        min_val = i
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
            if curr[j] < min_val:
                min_val = curr[j]
        if min_val > max_dist:
            return max_dist + 1
        prev = curr
    return prev[lb]


def _is_subsequence(query: str, target: str) -> bool:
    """檢查 query 是否為 target 的子序列"""
    qi = 0
    for ch in target:
        if qi < len(query) and ch == query[qi]:
            qi += 1
    return qi == len(query)


def _common_chars_ratio(a: str, b: str) -> float:
    """計算共同字元比率（相對於較短字串）"""
    if not a or not b:
        return 0.0
    sa = set(a)
    sb = set(b)
    common = len(sa & sb)
    return common / max(len(sa), len(sb))


class CommunityMatcher:
    """建案名稱模糊搜尋引擎"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self._cache = None  # {normalized_name: (original_name, tx_count, avg_price, avg_unit_price, district)}
        self._load_cache()

    def _load_cache(self):
        """載入所有建案名稱到記憶體（約 37K 筆，很快）"""
        t0 = time.time()
        self._cache = {}
        try:
            conn = sqlite3.connect(self.db_path)
            rows = conn.execute("""
                SELECT community_name,
                       COUNT(*) as tx_count,
                       ROUND(AVG(total_price)) as avg_price,
                       ROUND(AVG(unit_price), 2) as avg_unit,
                       district
                FROM land_transaction
                WHERE community_name IS NOT NULL AND community_name != ''
                GROUP BY community_name
                ORDER BY tx_count DESC
            """).fetchall()
            conn.close()

            for name, cnt, avg_p, avg_u, dist in rows:
                norm = _normalize(name)
                if norm:
                    self._cache[norm] = {
                        "name": name,
                        "tx_count": cnt or 0,
                        "avg_price": avg_p or 0,
                        "avg_unit_price": avg_u or 0,
                        "district": dist or "",
                    }
            elapsed = time.time() - t0
            print(f"🔍 CommunityMatcher: {len(self._cache)} 個建案載入 ({elapsed:.2f}s)")
        except Exception as e:
            print(f"⚠️ CommunityMatcher 載入失敗: {e}")
            self._cache = {}

    def search(self, keyword: str, top_n: int = 20) -> List[Dict]:
        """
        模糊搜尋建案名稱

        回傳 list of dict:
          - name: 建案原名
          - match_type: 精確/包含/子序列/模糊
          - score: 匹配分數 (越高越好)
          - tx_count: 交易筆數
          - avg_price: 平均總價
          - avg_unit_price: 平均單價
          - district: 行政區
        """
        if not keyword or not keyword.strip():
            return []

        norm_kw = _normalize(keyword)
        if not norm_kw:
            return []

        results = []

        for norm_name, info in self._cache.items():
            score = 0
            match_type = ""

            # 1. 精確匹配
            if norm_kw == norm_name:
                score = 1000 + info["tx_count"]
                match_type = "精確"

            # 2. 包含匹配（query 包含在 name 中，或反向）
            elif norm_kw in norm_name:
                # query 是 name 的子字串
                ratio = len(norm_kw) / len(norm_name)
                score = 500 + ratio * 200 + min(info["tx_count"], 200) * 0.5
                match_type = "包含"
            elif norm_name in norm_kw:
                # name 是 query 的子字串
                ratio = len(norm_name) / len(norm_kw)
                score = 400 + ratio * 200 + min(info["tx_count"], 200) * 0.5
                match_type = "包含"

            # 3. 子序列匹配
            elif len(norm_kw) >= 2 and _is_subsequence(norm_kw, norm_name):
                ratio = len(norm_kw) / len(norm_name)
                score = 200 + ratio * 200 + min(info["tx_count"], 50)
                match_type = "子序列"

            # 4. 編輯距離模糊匹配
            else:
                max_allowed = max(1, len(norm_kw) // 3)
                dist = _edit_distance(norm_kw, norm_name, max_allowed)
                if dist <= max_allowed:
                    score = 100 - dist * 20 + min(info["tx_count"], 30)
                    match_type = "模糊"
                else:
                    # 5. 共同字元比率 (最後手段，門檻較高)
                    cr = _common_chars_ratio(norm_kw, norm_name)
                    if cr >= 0.6 and len(norm_kw) >= 2:
                        score = 50 + cr * 80 + min(info["tx_count"], 20)
                        match_type = "相似"

            if score > 0:
                results.append({
                    "name": info["name"],
                    "match_type": match_type,
                    "score": round(score, 1),
                    "tx_count": info["tx_count"],
                    "avg_price": info["avg_price"],
                    "avg_unit_price": info["avg_unit_price"],
                    "district": info["district"],
                })

        # 排序：分數降序
        results.sort(key=lambda x: -x["score"])
        return results[:top_n]

    def stats(self) -> dict:
        """回傳統計資訊"""
        return {
            "total_communities": len(self._cache),
            "db_path": self.db_path,
        }


def fuzzy_search(keyword: str, db_path: str = None, top_n: int = 20) -> List[Dict]:
    """快速模糊搜尋（每次建立新連線，適合單次呼叫）"""
    matcher = CommunityMatcher(db_path)
    return matcher.search(keyword, top_n)


# ── CLI ──
if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        # 互動模式
        matcher = CommunityMatcher()
        print(f"\n建案模糊搜尋引擎 ({matcher.stats()['total_communities']} 個建案)")
        print("輸入建案名稱關鍵字，輸入 q 離開\n")
        while True:
            kw = input("搜尋> ").strip()
            if kw.lower() in ('q', 'quit', 'exit'):
                break
            if not kw:
                continue
            t0 = time.time()
            results = matcher.search(kw)
            elapsed = time.time() - t0
            print(f"\n找到 {len(results)} 個結果 ({elapsed*1000:.1f}ms):")
            for i, r in enumerate(results, 1):
                price_wan = r["avg_price"] / 10000 if r["avg_price"] else 0
                unit_wan = r["avg_unit_price"] * 3.30579 / 10000 if r["avg_unit_price"] else 0
                print(f"  {i:2d}. [{r['match_type']}] {r['name']}"
                      f"  ({r['tx_count']}筆, 均{price_wan:.0f}萬, "
                      f"均{unit_wan:.1f}萬/坪) "
                      f"[{r['district']}] score={r['score']}")
            print()
    else:
        keyword = " ".join(sys.argv[1:])
        matcher = CommunityMatcher()
        results = matcher.search(keyword)
        print(json.dumps(results, ensure_ascii=False, indent=2))
