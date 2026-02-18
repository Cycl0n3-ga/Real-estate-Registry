#!/usr/bin/env python3
"""
build_db.py - 從 ALL_lvr_land_b.csv 建立地址→社區名稱 對照表

輸出檔案：
  - address_community_mapping.csv: 完整對照 (正規化地址、路段、社區、交易筆數等)
  - manual_mapping.csv: 手動新增 (若不存在)

資料來源：
  1. B表 (ALL_lvr_land_b.csv): 486K 件建案交易
  2. manual_mapping.csv: 用戶手動新增的對照
"""

import csv
import re
from pathlib import Path
from collections import defaultdict

# ========== 路徑設定 ==========
SCRIPT_DIR = Path(__file__).parent
LAND_DIR = SCRIPT_DIR.parent
B_TABLE = LAND_DIR / "db" / "ALL_lvr_land_b.csv"
OUTPUT_CSV = SCRIPT_DIR.parent / "db" / "address_community_mapping.csv"
MANUAL_CSV = SCRIPT_DIR.parent / "db" / "manual_mapping.csv"

# ========== 全形→半形 ==========
FULLWIDTH_DIGITS = "０１２３４５６７８９"
HALFWIDTH_DIGITS = "0123456789"
FW_TO_HW = str.maketrans(FULLWIDTH_DIGITS, HALFWIDTH_DIGITS)


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

def extract_city_district(addr: str) -> tuple:
    """從原始地址提取 (縣市, 區)"""
    s = str(addr).strip()
    
    cities = [
        "臺北市", "台北市", "新北市", "桃園市", "桃園縣",
        "臺中市", "台中市", "臺南市", "台南市", "高雄市",
        "基隆市", "新竹市", "新竹縣", "苗栗縣", "彰化縣",
        "南投縣", "雲林縣", "嘉義市", "嘉義縣", "屏東縣",
        "宜蘭縣", "花蓮縣", "臺東縣", "台東縣", "澎湖縣",
        "金門縣", "連江縣",
    ]
    
    city = ""
    for c in cities:
        if s.startswith(c):
            city = c
            s = s[len(c):]
            break
    
    # 提取區
    m = re.match(r"([\u4e00-\u9fff]{1,3}[區鎮鄉市])", s)
    district = m.group(1) if m else ""
    
    # 正規化縣市 (台 → 臺)
    city = city.replace("台北市", "臺北市").replace("台中市", "臺中市").replace("台南市", "臺南市").replace("台東縣", "臺東縣")
    
    return city, district


def normalize_address(addr: str) -> str:
    """
    正規化地址：去除縣市/區/里鄰/樓層/棟號，僅保留路段+門牌
    """
    s = str(addr).strip()
    if not s:
        return ""
    s = fullwidth_to_halfwidth(s)

    # 去除縣市
    for city in CITIES:
        if s.startswith(city):
            s = s[len(city):]
            break

    # 去除鄉鎮市區
    for _ in range(2):
        s = re.sub(r"^[\u4e00-\u9fff]{1,3}[區鎮鄉市]", "", s)

    # 去除里鄰
    s = re.sub(r"[\u4e00-\u9fff]*里\d*鄰?", "", s)
    s = re.sub(r"\d+鄰", "", s)

    # 去除樓層
    s = re.sub(r"[,\s]*(地下)?[\d]+樓.*$", "", s)
    s = re.sub(
        r"[,\s]*(地下)?(十|二十|三十)?[一二三四五六七八九十百]+樓.*$", "", s
    )
    s = re.sub(r"\s*\d+F$", "", s)

    # 去除棟號
    s = re.sub(r"\s*[A-Za-z]\d*[-]\d+F$", "", s)
    s = re.sub(r"\s*[A-Za-z]\d*棟.*$", "", s)
    s = re.sub(r"\s+[A-Za-z]\d+[-][A-Za-z]?\d*F?$", "", s)

    # 去除「旁」「之X」「共N筆」
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


# ========== 主程式 ==========

def build_from_b_table():
    """從 B 表匯入資料"""
    if not B_TABLE.exists():
        print(f"❌ B 表不存在: {B_TABLE}")
        return {}

    print(f"📖 讀取 B 表: {B_TABLE.name}")
    
    mapping = defaultdict(lambda: {
        "communities": defaultdict(int),
        "normalized": "",
        "to_number": "",
        "to_alley": "",
        "road": "",
        "city": "",
        "district": "",
        "source": "B表",
    })

    count = 0
    with open(B_TABLE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            if count % 100000 == 0:
                print(f"  ⏳ 已處理 {count:,} 筆...")

            address = row.get("土地位置建物門牌", "").strip()
            community = row.get("建案名稱", "").strip()
            total_price = row.get("總價元", "").strip()

            # 過濾: 需要有社區名和正常成交價
            if not address or not community:
                continue
            try:
                price = float(total_price) if total_price else 0
                if price <= 0:
                    continue
            except ValueError:
                continue

            # 正規化地址
            norm = normalize_address(address)
            if not norm:
                continue

            city, district = extract_city_district(address)
            to_number = extract_road_number(norm)
            to_alley = extract_road_alley(norm)
            road = extract_road(norm)

            key = norm
            mapping[key]["communities"][community] += 1
            mapping[key]["normalized"] = norm
            mapping[key]["to_number"] = to_number
            mapping[key]["to_alley"] = to_alley
            mapping[key]["road"] = road
            mapping[key]["city"] = city
            mapping[key]["district"] = district

    print(f"  ✅ 讀取完成: {count:,} 筆交易")
    print(f"  📊 產生 {len(mapping):,} 個唯一地址")
    
    return mapping


def load_manual_mapping():
    """讀取手動對照"""
    manual = {}
    if not MANUAL_CSV.exists():
        create_manual_template()
        return manual

    print(f"📖 讀取手動對照: {MANUAL_CSV.name}")
    with open(MANUAL_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = row.get("地址", "").strip()
            community = row.get("社區名稱", "").strip()
            district = row.get("鄉鎮市區", "").strip()

            if addr and community:
                norm = normalize_address(addr)
                if norm:
                    manual[norm] = {
                        "community": community,
                        "city": extract_city_district(addr)[0],
                        "district": district,
                        "source": "手動",
                    }
    
    print(f"  ✅ 讀取完成: {len(manual)} 筆手動對照")
    return manual


def create_manual_template():
    """建立手動對照表範本"""
    if MANUAL_CSV.exists():
        return
    print(f"📝 建立手動對照表範本 ({MANUAL_CSV.name})...")
    with open(MANUAL_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["地址", "社區名稱", "鄉鎮市區", "備註"])
        writer.writerow(["三民路29巷5號", "健安新城F區", "松山區", ""])
        writer.writerow(["三民路29巷3號", "健安新城F區", "松山區", ""])
        writer.writerow(["三民路29巷1號", "健安新城F區", "松山區", ""])
        writer.writerow(["三民路29巷7號", "健安新城F區", "松山區", ""])
        writer.writerow(["仁愛路三段53號", "仁愛帝寶", "大安區", ""])
        writer.writerow(["延壽街330號", "平安新城甲區", "松山區", ""])
        writer.writerow(["延壽街332號", "平安新城甲區", "松山區", ""])
        writer.writerow(["延壽街334號", "平安新城甲區", "松山區", ""])
        writer.writerow(["日興一街6號", "仁發喜悅", "竹北市", ""])


def merge_and_export(b_mapping: dict, manual: dict):
    """合併並輸出到 CSV"""
    print(f"\n💾 寫入 CSV: {OUTPUT_CSV.name}")

    # 準備 CSV 列
    rows = []

    # 1. 從 B 表匯入（優先順序：交易筆數多的社區排前）
    for norm, data in sorted(b_mapping.items()):
        # 社區名稱排序（交易多的優先）
        communities = sorted(
            data["communities"].items(),
            key=lambda x: -x[1]
        )
        
        for community, count in communities:
            # 如果手動有對照，優先用手動的（覆蓋）
            if norm in manual:
                m = manual[norm]
                rows.append({
                    "正規化地址": norm,
                    "到號地址": data["to_number"],
                    "到巷地址": data["to_alley"],
                    "路段": data["road"],
                    "社區名稱": m["community"],
                    "縣市": m["city"],
                    "鄉鎮市區": m["district"],
                    "交易筆數": 0,
                    "資料來源": "手動",
                    "所有建案名": "",
                })
                break  # 手動已覆蓋，跳過其他建案
            else:
                all_names = ",".join([c[0] for c in communities])
                rows.append({
                    "正規化地址": norm,
                    "到號地址": data["to_number"],
                    "到巷地址": data["to_alley"],
                    "路段": data["road"],
                    "社區名稱": community,
                    "縣市": data["city"],
                    "鄉鎮市區": data["district"],
                    "交易筆數": count,
                    "資料來源": "B表",
                    "所有建案名": all_names,
                })

    # 2. 手動對照（若未被 B 表涵蓋）
    for norm, m in manual.items():
        if not any(r["正規化地址"] == norm for r in rows):
            rows.append({
                "正規化地址": norm,
                "到號地址": extract_road_number(norm),
                "到巷地址": extract_road_alley(norm),
                "路段": extract_road(norm),
                "社區名稱": m["community"],
                "縣市": m["city"],
                "鄉鎮市區": m["district"],
                "交易筆數": 0,
                "資料來源": "手動",
                "所有建案名": "",
            })

    # 寫入 CSV
    fieldnames = [
        "正規化地址", "到號地址", "到巷地址", "路段",
        "社區名稱", "縣市", "鄉鎮市區", "交易筆數",
        "資料來源", "所有建案名"
    ]
    
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✅ 寫入完成: {len(rows):,} 筆記錄")
    return len(rows)


def main():
    print("=" * 60)
    print("🔨 建立地址→社區名稱 對照表 (CSV 版)")
    print("=" * 60)

    # 1. 讀取 B 表
    b_mapping = build_from_b_table()
    
    # 2. 讀取手動對照
    manual = load_manual_mapping()
    
    # 3. 合併並輸出
    total = merge_and_export(b_mapping, manual)

    # 4. 統計
    file_size_mb = OUTPUT_CSV.stat().st_size / (1024 * 1024)
    print(f"\n📊 統計:")
    print(f"  • 從 B 表: {len(b_mapping):,} 筆")
    print(f"  • 手動對照: {len(manual):,} 筆")
    print(f"  • 輸出總計: {total:,} 筆")
    print(f"  • CSV 檔案: {file_size_mb:.1f} MB")
    print("=" * 60)
    print("✅ 完成！可用 address2community.py 進行查詢")
    print("=" * 60)


if __name__ == "__main__":
    main()
