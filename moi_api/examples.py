#!/usr/bin/env python3
"""
社區名稱查詢範例
================
使用 lvr.land.moi.gov.tw 資料，示範如何用社區/建案名稱查詢歷史成交。

資料來源說明：
  - Type B（預售屋）：CSV 本身有「建案名稱」欄位，可直接過濾
  - Type A（成屋）：CSV 無社區名稱，只能用門牌地址查詢

執行：
  python3 examples.py
"""
import csv
import io
import time
import urllib.request

from moi_client import MoiClient, CITY_CODES

PLVR = "https://plvr.land.moi.gov.tw"


def download_season_csv(city_code: str, trade_type: str, season: str) -> list[dict]:
    """
    下載指定縣市 + 類型 + 季度的 CSV，回傳 dict list（已跳過英文標頭行）

    season 格式：114S4（民國年 + S1~S4）
    """
    filename = f"{city_code.lower()}_lvr_land_{trade_type.lower()}.csv"
    url = f"{PLVR}/DownloadSeason?season={season}&fileName={filename}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=60)
    data = resp.read().decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    return rows[1:] if len(rows) > 1 else rows   # 跳過英文標頭行


def download_current_csv(city_code: str, trade_type: str) -> list[dict]:
    """下載本期（最新一期）CSV"""
    filename = f"{city_code.lower()}_lvr_land_{trade_type.lower()}.csv"
    url = f"{PLVR}/Download?fileName={filename}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=60)
    data = resp.read().decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    return rows[1:] if len(rows) > 1 else rows


# ─────────────────────────────────────────────
# 範例 1：找出某建案名稱（預售屋）
# ─────────────────────────────────────────────
def example1_find_building(keyword: str = "信義", city_code: str = "A"):
    """
    用 SaleBuild API 自動完成，找出符合關鍵字的建案名稱清單。

    適用：預售屋（Type B）
    """
    print(f"\n{'=' * 55}")
    print(f"範例 1：搜尋建案名稱（關鍵字：{keyword}，城市：{CITY_CODES.get(city_code, city_code)}）")
    print("=" * 55)

    client = MoiClient()
    client.login()
    buildings = client.search_building(city_code, keyword)

    print(f"找到 {len(buildings)} 個建案：")
    for b in buildings:
        print(f"  - {b}")
    return buildings


# ─────────────────────────────────────────────
# 範例 2：查詢特定建案的所有成交（單一季度）
# ─────────────────────────────────────────────
def example2_query_one_season(building_name: str, city_code: str = "A",
                               season: str = "114S4"):
    """
    下載指定季度的預售屋 CSV，過濾出特定建案的成交紀錄。
    """
    print(f"\n{'=' * 55}")
    print(f"範例 2：{building_name}  {season} 成交明細")
    print("=" * 55)

    rows = download_season_csv(city_code, "B", season)
    found = [r for r in rows if r.get("建案名稱", "") == building_name]

    print(f"共 {len(found)} 筆：")
    print(f"{'樓層':8s} {'面積(㎡)':>10s} {'總價(萬)':>10s} {'單價(萬/坪)':>12s}")
    print("-" * 45)
    for r in found:
        floor = r.get("移轉層次", "")
        area = float(r.get("建物移轉總面積平方公尺") or 0)
        total_price = int(r.get("總價元") or 0) // 10000
        unit_price_sqm = float(r.get("單價元平方公尺") or 0)
        unit_price_ping = unit_price_sqm * 3.305 / 10000  # 元/㎡ → 萬/坪
        print(f"{floor:8s} {area:>10.2f} {total_price:>10,d} {unit_price_ping:>11.1f}")

    return found


# ─────────────────────────────────────────────
# 範例 3：跨季度追蹤建案均價趨勢
# ─────────────────────────────────────────────
def example3_price_trend(building_name: str, city_code: str = "A",
                          seasons: list[str] | None = None):
    """
    跨多個季度下載資料，計算建案均價趨勢。
    """
    if seasons is None:
        seasons = ["114S4", "114S3", "114S2", "114S1",
                   "113S4", "113S3", "113S2", "113S1"]

    print(f"\n{'=' * 55}")
    print(f"範例 3：{building_name} 歷史均價趨勢")
    print("=" * 55)
    print(f"{'季度':10s} {'筆數':>5s} {'均總價(萬)':>12s} {'均單價(萬/坪)':>14s}")
    print("-" * 48)

    for season in seasons:
        try:
            rows = download_season_csv(city_code, "B", season)
            found = [r for r in rows if r.get("建案名稱", "") == building_name]
            if found:
                prices = [int(r["總價元"]) for r in found if r.get("總價元")]
                unit_prices = [float(r["單價元平方公尺"]) for r in found
                               if r.get("單價元平方公尺")]
                avg_p = sum(prices) // len(prices) // 10000 if prices else 0
                avg_u = (sum(unit_prices) / len(unit_prices) * 3.305 / 10000
                         if unit_prices else 0)
                print(f"{season:10s} {len(found):>5d} {avg_p:>10,d}  {avg_u:>12.1f}")
            else:
                print(f"{season:10s} {'---':>5s}")
            time.sleep(0.2)  # 避免過於頻繁請求
        except Exception as e:
            print(f"{season:10s} 錯誤：{e}")


# ─────────────────────────────────────────────
# 範例 4：搜尋全台某關鍵字建案
# ─────────────────────────────────────────────
def example4_search_all_cities(keyword: str = "遠雄", season: str = "114S4"):
    """
    在全國各縣市的預售屋 CSV 中，搜尋建案名稱含關鍵字的成交。
    """
    print(f"\n{'=' * 55}")
    print(f"範例 4：全國搜尋「{keyword}」建案 ({season})")
    print("=" * 55)

    total_found = 0
    for city in CITY_CODES:
        try:
            rows = download_season_csv(city, "B", season)
            found = [r for r in rows if keyword in r.get("建案名稱", "")]
            if found:
                # 按建案分組
                by_building: dict[str, list] = {}
                for r in found:
                    bn = r.get("建案名稱", "")
                    by_building.setdefault(bn, []).append(r)

                print(f"\n  {CITY_CODES[city]}：")
                for bn, rs in by_building.items():
                    prices = [int(r["總價元"]) for r in rs if r.get("總價元")]
                    avg_p = sum(prices) // len(prices) // 10000 if prices else 0
                    print(f"    {bn:25s} {len(rs):3d}筆  均價{avg_p:,}萬")
                total_found += len(found)
            time.sleep(0.15)
        except Exception:
            pass  # 某些縣市可能無預售屋資料

    print(f"\n全國共 {total_found} 筆")


# ─────────────────────────────────────────────
# 範例 5：用「建案名稱 + SaleBuild」做模糊查詢
# ─────────────────────────────────────────────
def example5_fuzzy_with_api(keyword: str, city_code: str = "A",
                              seasons: list[str] | None = None):
    """
    Step 1：用 SaleBuild API 找出所有相關建案名稱（模糊比對）
    Step 2：下載 CSV 查出這些建案的成交紀錄
    """
    if seasons is None:
        seasons = ["114S4", "114S3"]

    print(f"\n{'=' * 55}")
    print(f"範例 5：模糊搜尋「{keyword}」→ 確切建案 → 成交資料")
    print("=" * 55)

    # Step 1
    client = MoiClient()
    client.login()
    buildings = client.search_building(city_code, keyword)
    print(f"Step 1 — 找到建案：{buildings}")

    if not buildings:
        print("  無結果")
        return

    # Step 2
    all_rows = []
    for season in seasons:
        try:
            rows = download_season_csv(city_code, "B", season)
            for r in rows:
                if r.get("建案名稱", "") in buildings:
                    r["_season"] = season
                    all_rows.append(r)
            time.sleep(0.2)
        except Exception as e:
            print(f"  {season} 下載失敗：{e}")

    # 彙整
    print(f"\nStep 2 — 從 {len(seasons)} 個季度找到 {len(all_rows)} 筆成交：")
    print(f"{'季度':8s} {'建案名稱':22s} {'樓層':6s} {'總價(萬)':>10s}")
    print("-" * 52)
    for r in all_rows[:10]:
        print(f"{r['_season']:8s} {r.get('建案名稱',''):22s} "
              f"{r.get('移轉層次',''):6s} "
              f"{int(r.get('總價元','0') or 0) // 10000:>10,d}")
    if len(all_rows) > 10:
        print(f"  ... 共 {len(all_rows)} 筆")

    return all_rows


# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # 1. 找建案名稱
    buildings = example1_find_building("元利", "A")

    # 2. 查詢單季明細
    if buildings:
        example2_query_one_season(buildings[0], "A", "114S4")

    # 3. 趨勢追蹤
    example3_price_trend("元利四季莊園", "A",
                          ["114S4", "114S3", "114S2", "114S1", "113S4"])

    # 4. 全國搜尋（資料量大，較慢）
    # example4_search_all_cities("遠雄", "114S4")

    # 5. 模糊搜尋 → 確切建案 → 成交
    example5_fuzzy_with_api("信義", "A", ["114S4", "114S3"])
