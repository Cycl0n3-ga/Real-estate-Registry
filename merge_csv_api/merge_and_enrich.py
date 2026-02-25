#!/usr/bin/env python3
"""
merge_and_enrich.py
===================
整合三個原本獨立的腳本，一次完成「transactions.db → land_data.db」的全部處理：

  Step 1 — merge     : 將 transactions.db (LVR API) 獨有的交易記錄插入 land_data.db
  Step 2 — enrich    : 用 transactions.db 的欄位補充 land_data.db 既有記錄的缺失值
  Step 3 — backfill  : 從 transactions.db 回填 community_name

原始來源：
  db/merge_transactions.py
  db/enrich_from_transactions.py
  db/backfill_community.py

用法：
    python3 -m merge_csv_api.merge_and_enrich                # 執行全部三步
    python3 -m merge_csv_api.merge_and_enrich --step merge    # 只執行 Step 1
    python3 -m merge_csv_api.merge_and_enrich --step enrich   # 只執行 Step 2
    python3 -m merge_csv_api.merge_and_enrich --step backfill # 只執行 Step 3
    python3 -m merge_csv_api.merge_and_enrich --dry-run       # backfill 僅統計不寫入
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time

# ═══════════════════════════════════════════════════════════════════════════════
# 路徑設定
# ═══════════════════════════════════════════════════════════════════════════════
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_LAND  = os.path.join(SCRIPT_DIR, '..', 'db', 'land_data.db')
DB_TRANS = os.path.join(SCRIPT_DIR, '..', 'db', 'transactions.db')


# ═══════════════════════════════════════════════════════════════════════════════
# 共用常數 / 工具函式
# ═══════════════════════════════════════════════════════════════════════════════

# ── 中文樓層 → 數字 ──────────────────────────────────────────────────────────
CHINESE_FLOOR = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '十': 10, '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
    '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
    '二十一': 21, '二十二': 22, '二十三': 23, '二十四': 24, '二十五': 25,
    '二十六': 26, '二十七': 27, '二十八': 28, '二十九': 29, '三十': 30,
    '地下一': -1, '地下二': -2, '地下三': -3,
}

# ── 城市前綴（去城市用）────────────────────────────────────────────────────────
CITY_PREFIXES = [
    '台北市', '新北市', '桃園市', '台中市', '台南市', '高雄市',
    '基隆市', '新竹市', '嘉義市', '新竹縣', '苗栗縣', '彰化縣',
    '南投縣', '雲林縣', '嘉義縣', '屏東縣', '宜蘭縣', '花蓮縣',
    '台東縣', '澎湖縣', '金門縣', '連江縣', '桃園縣', '台北縣',
    '台中縣', '台南縣', '高雄縣',
]

# ── 行政區 → 縣市 (merge 用) ──────────────────────────────────────────────────
DISTRICT_CITY_MAP = {
    '中正區': '台北市', '大同區': '台北市', '中山區': '台北市', '松山區': '台北市',
    '大安區': '台北市', '萬華區': '台北市', '信義區': '台北市', '士林區': '台北市',
    '北投區': '台北市', '內湖區': '台北市', '南港區': '台北市', '文山區': '台北市',
    '板橋區': '新北市', '三重區': '新北市', '中和區': '新北市', '永和區': '新北市',
    '新莊區': '新北市', '新店區': '新北市', '樹林區': '新北市', '鶯歌區': '新北市',
    '三峽區': '新北市', '淡水區': '新北市', '汐止區': '新北市', '瑞芳區': '新北市',
    '土城區': '新北市', '蘆洲區': '新北市', '五股區': '新北市', '泰山區': '新北市',
    '林口區': '新北市', '深坑區': '新北市', '石碇區': '新北市', '坪林區': '新北市',
    '三芝區': '新北市', '石門區': '新北市', '八里區': '新北市', '平溪區': '新北市',
    '雙溪區': '新北市', '貢寮區': '新北市', '金山區': '新北市', '萬里區': '新北市',
    '烏來區': '新北市',
    '桃園區': '桃園市', '中壢區': '桃園市', '平鎮區': '桃園市', '八德區': '桃園市',
    '楊梅區': '桃園市', '蘆竹區': '桃園市', '大溪區': '桃園市', '龍潭區': '桃園市',
    '龜山區': '桃園市', '大園區': '桃園市', '觀音區': '桃園市', '新屋區': '桃園市',
    '復興區': '桃園市',
    '豐原區': '台中市', '大里區': '台中市', '太平區': '台中市', '清水區': '台中市',
    '沙鹿區': '台中市', '梧棲區': '台中市', '后里區': '台中市', '神岡區': '台中市',
    '潭子區': '台中市', '大雅區': '台中市', '新社區': '台中市', '石岡區': '台中市',
    '外埔區': '台中市', '大甲區': '台中市', '大肚區': '台中市', '龍井區': '台中市',
    '霧峰區': '台中市', '烏日區': '台中市', '和平區': '台中市', '西屯區': '台中市',
    '南屯區': '台中市', '北屯區': '台中市', '北區': '台中市', '南區': '台中市',
    '東區': '台中市', '西區': '台中市', '中區': '台中市',
    '新營區': '台南市', '鹽水區': '台南市', '白河區': '台南市', '柳營區': '台南市',
    '後壁區': '台南市', '麻豆區': '台南市', '下營區': '台南市', '六甲區': '台南市',
    '官田區': '台南市', '大內區': '台南市', '佳里區': '台南市', '學甲區': '台南市',
    '西港區': '台南市', '七股區': '台南市', '將軍區': '台南市', '北門區': '台南市',
    '新化區': '台南市', '善化區': '台南市', '新市區': '台南市', '安定區': '台南市',
    '山上區': '台南市', '玉井區': '台南市', '楠西區': '台南市', '南化區': '台南市',
    '左鎮區': '台南市', '仁德區': '台南市', '歸仁區': '台南市', '關廟區': '台南市',
    '龍崎區': '台南市', '永康區': '台南市', '安南區': '台南市', '安平區': '台南市',
    '東山區': '台南市',
    '鳳山區': '高雄市', '林園區': '高雄市', '大寮區': '高雄市', '大樹區': '高雄市',
    '大社區': '高雄市', '仁武區': '高雄市', '鳥松區': '高雄市', '岡山區': '高雄市',
    '橋頭區': '高雄市', '燕巢區': '高雄市', '田寮區': '高雄市', '阿蓮區': '高雄市',
    '路竹區': '高雄市', '湖內區': '高雄市', '茄萣區': '高雄市', '永安區': '高雄市',
    '彌陀區': '高雄市', '梓官區': '高雄市', '旗山區': '高雄市', '美濃區': '高雄市',
    '六龜區': '高雄市', '甲仙區': '高雄市', '杉林區': '高雄市', '內門區': '高雄市',
    '茂林區': '高雄市', '桃源區': '高雄市', '那瑪夏區': '高雄市', '楠梓區': '高雄市',
    '左營區': '高雄市', '鼓山區': '高雄市', '三民區': '高雄市', '苓雅區': '高雄市',
    '前鎮區': '高雄市', '旗津區': '高雄市', '小港區': '高雄市', '前金區': '高雄市',
    '鹽埕區': '高雄市', '新興區': '高雄市',
    '仁愛區': '基隆市', '安樂區': '基隆市', '暖暖區': '基隆市', '七堵區': '基隆市',
    '香山區': '新竹市',
    '竹北市': '新竹縣', '竹東鎮': '新竹縣', '新埔鎮': '新竹縣', '關西鎮': '新竹縣',
    '湖口鄉': '新竹縣', '新豐鄉': '新竹縣', '芎林鄉': '新竹縣', '橫山鄉': '新竹縣',
    '北埔鄉': '新竹縣', '寶山鄉': '新竹縣', '峨眉鄉': '新竹縣', '尖石鄉': '新竹縣',
    '五峰鄉': '新竹縣',
    '苗栗市': '苗栗縣', '頭份市': '苗栗縣', '竹南鎮': '苗栗縣', '後龍鎮': '苗栗縣',
    '通霄鎮': '苗栗縣', '苑裡鎮': '苗栗縣', '卓蘭鎮': '苗栗縣', '大湖鄉': '苗栗縣',
    '公館鄉': '苗栗縣', '銅鑼鄉': '苗栗縣', '南庄鄉': '苗栗縣', '頭屋鄉': '苗栗縣',
    '三義鄉': '苗栗縣', '西湖鄉': '苗栗縣', '造橋鄉': '苗栗縣', '三灣鄉': '苗栗縣',
    '獅潭鄉': '苗栗縣', '泰安鄉': '苗栗縣',
    '彰化市': '彰化縣', '員林市': '彰化縣', '鹿港鎮': '彰化縣', '和美鎮': '彰化縣',
    '溪湖鎮': '彰化縣', '北斗鎮': '彰化縣', '田中鎮': '彰化縣', '二林鎮': '彰化縣',
    '線西鄉': '彰化縣', '伸港鄉': '彰化縣', '福興鄉': '彰化縣', '秀水鄉': '彰化縣',
    '花壇鄉': '彰化縣', '芬園鄉': '彰化縣', '大村鄉': '彰化縣', '埔鹽鄉': '彰化縣',
    '埔心鄉': '彰化縣', '永靖鄉': '彰化縣', '社頭鄉': '彰化縣', '二水鄉': '彰化縣',
    '田尾鄉': '彰化縣', '埤頭鄉': '彰化縣', '芳苑鄉': '彰化縣', '大城鄉': '彰化縣',
    '竹塘鄉': '彰化縣', '溪州鄉': '彰化縣',
    '南投市': '南投縣', '埔里鎮': '南投縣', '草屯鎮': '南投縣', '竹山鎮': '南投縣',
    '集集鎮': '南投縣', '名間鄉': '南投縣', '鹿谷鄉': '南投縣', '中寮鄉': '南投縣',
    '魚池鄉': '南投縣', '國姓鄉': '南投縣', '水里鄉': '南投縣', '信義鄉': '南投縣',
    '仁愛鄉': '南投縣',
    '斗六市': '雲林縣', '斗南鎮': '雲林縣', '虎尾鎮': '雲林縣', '西螺鎮': '雲林縣',
    '土庫鎮': '雲林縣', '北港鎮': '雲林縣', '古坑鄉': '雲林縣', '大埤鄉': '雲林縣',
    '莿桐鄉': '雲林縣', '林內鄉': '雲林縣', '二崙鄉': '雲林縣', '崙背鄉': '雲林縣',
    '麥寮鄉': '雲林縣', '東勢鄉': '雲林縣', '褒忠鄉': '雲林縣', '台西鄉': '雲林縣',
    '元長鄉': '雲林縣', '四湖鄉': '雲林縣', '口湖鄉': '雲林縣', '水林鄉': '雲林縣',
    '太保市': '嘉義縣', '朴子市': '嘉義縣', '布袋鎮': '嘉義縣', '大林鎮': '嘉義縣',
    '民雄鄉': '嘉義縣', '溪口鄉': '嘉義縣', '新港鄉': '嘉義縣', '六腳鄉': '嘉義縣',
    '東石鄉': '嘉義縣', '義竹鄉': '嘉義縣', '鹿草鄉': '嘉義縣', '水上鄉': '嘉義縣',
    '中埔鄉': '嘉義縣', '竹崎鄉': '嘉義縣', '梅山鄉': '嘉義縣', '番路鄉': '嘉義縣',
    '大埔鄉': '嘉義縣', '阿里山鄉': '嘉義縣',
    '屏東市': '屏東縣', '潮州鎮': '屏東縣', '東港鎮': '屏東縣', '恆春鎮': '屏東縣',
    '萬丹鄉': '屏東縣', '長治鄉': '屏東縣', '麟洛鄉': '屏東縣', '九如鄉': '屏東縣',
    '里港鄉': '屏東縣', '鹽埔鄉': '屏東縣', '高樹鄉': '屏東縣', '萬巒鄉': '屏東縣',
    '內埔鄉': '屏東縣', '竹田鄉': '屏東縣', '新埤鄉': '屏東縣', '枋寮鄉': '屏東縣',
    '新園鄉': '屏東縣', '崁頂鄉': '屏東縣', '林邊鄉': '屏東縣', '南州鄉': '屏東縣',
    '佳冬鄉': '屏東縣', '琉球鄉': '屏東縣', '車城鄉': '屏東縣', '滿州鄉': '屏東縣',
    '枋山鄉': '屏東縣', '霧台鄉': '屏東縣', '瑪家鄉': '屏東縣', '泰武鄉': '屏東縣',
    '來義鄉': '屏東縣', '春日鄉': '屏東縣', '獅子鄉': '屏東縣', '牡丹鄉': '屏東縣',
    '三地門鄉': '屏東縣',
    '宜蘭市': '宜蘭縣', '羅東鎮': '宜蘭縣', '蘇澳鎮': '宜蘭縣', '頭城鎮': '宜蘭縣',
    '礁溪鄉': '宜蘭縣', '壯圍鄉': '宜蘭縣', '員山鄉': '宜蘭縣', '冬山鄉': '宜蘭縣',
    '五結鄉': '宜蘭縣', '三星鄉': '宜蘭縣', '大同鄉': '宜蘭縣', '南澳鄉': '宜蘭縣',
    '花蓮市': '花蓮縣', '鳳林鎮': '花蓮縣', '玉里鎮': '花蓮縣', '新城鄉': '花蓮縣',
    '吉安鄉': '花蓮縣', '壽豐鄉': '花蓮縣', '光復鄉': '花蓮縣', '豐濱鄉': '花蓮縣',
    '瑞穗鄉': '花蓮縣', '富里鄉': '花蓮縣', '秀林鄉': '花蓮縣', '萬榮鄉': '花蓮縣',
    '卓溪鄉': '花蓮縣',
    '台東市': '台東縣', '成功鎮': '台東縣', '關山鎮': '台東縣', '卑南鄉': '台東縣',
    '大武鄉': '台東縣', '太麻里鄉': '台東縣', '東河鄉': '台東縣', '長濱鄉': '台東縣',
    '鹿野鄉': '台東縣', '池上鄉': '台東縣', '綠島鄉': '台東縣', '延平鄉': '台東縣',
    '海端鄉': '台東縣', '達仁鄉': '台東縣', '金峰鄉': '台東縣', '蘭嶼鄉': '台東縣',
    '馬公市': '澎湖縣', '湖西鄉': '澎湖縣', '白沙鄉': '澎湖縣', '西嶼鄉': '澎湖縣',
    '望安鄉': '澎湖縣', '七美鄉': '澎湖縣',
    '金城鎮': '金門縣', '金湖鎮': '金門縣', '金沙鎮': '金門縣', '金寧鄉': '金門縣',
    '烈嶼鄉': '金門縣', '烏坵鄉': '金門縣',
    '南竿鄉': '連江縣', '北竿鄉': '連江縣', '莒光鄉': '連江縣', '東引鄉': '連江縣',
    '新竹市': '新竹市', '嘉義市': '嘉義市',
}

# ── 城市代碼 → 縣市名 (backfill 用) ───────────────────────────────────────────
CITY_CODE_MAP = {
    'A': '台北市', 'B': '台中市', 'C': '基隆市', 'D': '台南市',
    'E': '高雄市', 'F': '新北市', 'G': '宜蘭縣', 'H': '桃園市',
    'I': '嘉義市', 'J': '新竹縣', 'K': '苗栗縣', 'M': '南投縣',
    'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣', 'Q': '嘉義縣',
    'T': '屏東縣', 'U': '花蓮縣', 'V': '台東縣', 'W': '金門縣',
    'X': '澎湖縣', 'Z': '連江縣',
}


# ─── 共用地址工具函式 ─────────────────────────────────────────────────────────

def norm_addr(addr):
    """全形→半形、臺→台、中文樓層→數字"""
    result = []
    for ch in (addr or ''):
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(ch)
    addr = ''.join(result).replace('臺', '台').replace(' ', '')
    addr = re.sub(
        r'(地下[一二三]|二十[一二三四五六七八九]|三十|二十|十[一二三四五六七八九]|[一二三四五六七八九十])(樓|層)',
        lambda m: str(CHINESE_FLOOR.get(m.group(1), m.group(1))) + m.group(2),
        addr
    )
    return addr


def strip_city(addr):
    """移除地址開頭的縣市名"""
    for prefix in CITY_PREFIXES:
        if addr.startswith(prefix):
            return addr[len(prefix):]
    return addr


def strip_floor(addr):
    """去除尾端樓層資訊，取得建物基礎地址"""
    addr = re.sub(r'(-\d+|地下\d+|\d+)樓[之\d]*$', '', addr)
    addr = addr.rstrip('之号號 ')
    return addr


def strip_floor_backfill(addr):
    """去掉地址末尾的樓層資訊，保留門牌號（backfill 專用，較保守）。
    例: '中正區汀州路一段76號二十樓' → '中正區汀州路一段76號'
    """
    m = re.search(r'號', addr)
    if m:
        pos = m.end()
        rest = addr[pos:]
        if re.match(r'^[一二三四五六七八九十百零\d]+樓', rest) or \
           re.match(r'^(地下)?[一二三四五六七八九十百零\d]+[層F]', rest):
            return addr[:pos]
        if not rest.strip():
            return addr[:pos]
    return addr


def extract_district(addr):
    """從地址取出行政區"""
    for length in (4, 3, 2):
        candidate = addr[:length]
        if candidate in DISTRICT_CITY_MAP:
            return candidate
    return ''


def extract_district_backfill(addr):
    """從地址中提取行政區（backfill 用，先去縣市前綴）"""
    for prefix_len in (3, 2):
        pref = addr[:prefix_len]
        if pref.endswith(('市', '縣')):
            addr = addr[prefix_len:]
            break
    m = re.match(r'^(.{1,4}?[區鎮鄉市])', addr)
    if m:
        return m.group(1)
    return ''


def clean_trans_addr(addr_raw):
    """取 transactions.db 地址 '#' 後半部的乾淨地址"""
    if addr_raw and '#' in addr_raw:
        return addr_raw.split('#', 1)[1]
    return addr_raw or ''


def parse_price(val):
    if not val:
        return None
    try:
        return int(str(val).replace(',', '').replace(' ', ''))
    except Exception:
        return None


def parse_area(val):
    if not val:
        return None
    try:
        return float(str(val).replace(',', ''))
    except Exception:
        return None


def normalize_date(date_str):
    """101/01/05 → 1010105"""
    if not date_str:
        return ''
    return date_str.replace('/', '')


def parse_floor_info(floor_str):
    """'九層/十五層' → (floor_level, total_floors)"""
    if not floor_str:
        return '', ''
    parts = floor_str.split('/')
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return floor_str.strip(), ''


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1: MERGE — 插入 transactions.db 獨有記錄
# ═══════════════════════════════════════════════════════════════════════════════

INSERT_SQL = """
INSERT INTO land_transaction (
    raw_district, transaction_type, address,
    land_area, urban_zone, non_urban_zone, non_urban_use,
    transaction_date, transaction_count, floor_level, total_floors,
    building_type, main_use, main_material, build_date,
    building_area, rooms, halls, bathrooms, partitioned,
    has_management, total_price, unit_price,
    parking_type, parking_area, parking_price,
    note, serial_no, main_area, attached_area,
    balcony_area, elevator, transfer_no,
    county_city, district, village, street, lane, alley,
    number, floor, sub_number,
    community_name, lat, lng
) VALUES (
    ?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,?,
    ?,?,?,  ?,?,?,  ?,?,?,?,  ?,?,?,
    ?,?,?,?,?,?,  ?,?,?,  ?,?,?
)
"""


def _build_land_data_keys(cl):
    """建立 land_data.db 的 (date, norm_addr) 和 (date, price) key set"""
    print('  讀取 land_data.db 鍵值...', flush=True)
    cl.execute('SELECT transaction_date, address, total_price FROM land_transaction WHERE address LIKE "%號%"')
    addr_date_keys = set()
    date_price_keys = set()
    for r in cl.fetchall():
        date = r[0].replace('/', '')[:7]
        addr = strip_city(norm_addr(r[1]))
        addr_date_keys.add((date, addr))
        price = parse_price(r[2])
        if price:
            date_price_keys.add((date, price))
    print(f'  land_data (date+addr) keys: {len(addr_date_keys):,}', flush=True)
    return addr_date_keys, date_price_keys


def _map_record_for_insert(row, rj):
    """將 transactions.db 一列 + raw_json → land_transaction 欄位 tuple"""
    tid, city, town, addr_raw, build_type, community, date_str, floor_col, \
        area_col, tp_raw, up_raw, lat, lon, sq, _ = row

    j = {}
    try:
        if rj:
            j = json.loads(rj)
    except Exception:
        pass

    addr_clean = clean_trans_addr(addr_raw)
    addr_norm = norm_addr(addr_clean)

    district = extract_district(addr_norm)
    county_city = DISTRICT_CITY_MAP.get(district, '')

    transaction_date = date_str.replace('/', '') if date_str else ''

    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    transaction_type = j.get('t', '') or ''

    rooms = halls = bathrooms = None
    try:
        rooms = int(j.get('j', '')) if j.get('j', '') != '' else None
    except Exception:
        pass
    try:
        halls = int(j.get('k', '')) if j.get('k', '') != '' else None
    except Exception:
        pass
    try:
        bathrooms = int(j.get('l', '')) if j.get('l', '') != '' else None
    except Exception:
        pass

    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''

    total_price = parse_price(tp_raw) or parse_price(j.get('tp'))
    unit_price = parse_area(up_raw) or parse_area(j.get('cp'))
    building_area = parse_area(area_col) or parse_area(j.get('s'))

    serial_no = f'591_{sq}' if sq else None

    return (
        town or '',           # raw_district
        transaction_type,     # transaction_type
        addr_clean,           # address
        None,                 # land_area
        '', '', '',           # urban_zone, non_urban_zone, non_urban_use
        transaction_date,     # transaction_date
        '',                   # transaction_count
        floor_level,          # floor_level
        total_floors,         # total_floors
        build_type or '',     # building_type
        main_use,             # main_use
        '',                   # main_material
        '',                   # build_date
        building_area,        # building_area
        rooms,                # rooms
        halls,                # halls
        bathrooms,            # bathrooms
        '',                   # partitioned
        has_management,       # has_management
        total_price,          # total_price
        unit_price,           # unit_price
        '',                   # parking_type
        None,                 # parking_area
        None,                 # parking_price
        '',                   # note
        serial_no,            # serial_no
        None,                 # main_area
        None,                 # attached_area
        None,                 # balcony_area
        '',                   # elevator
        '',                   # transfer_no
        county_city,          # county_city
        district,             # district
        '',                   # village
        '',                   # street
        '',                   # lane
        '',                   # alley
        '',                   # number
        '',                   # floor
        '',                   # sub_number
        community or '',      # community_name
        lat,                  # lat
        lon,                  # lng
    )


def _rebuild_fts(conn):
    """重建 FTS5 address_fts 索引"""
    print('\n  重建 FTS5 索引...', flush=True)
    conn.execute('DROP TABLE IF EXISTS address_fts')
    conn.commit()
    conn.execute("""
        CREATE VIRTUAL TABLE address_fts USING fts5(
            address,
            content='land_transaction',
            content_rowid='id',
            tokenize='unicode61'
        )
    """)
    conn.commit()
    print('  填充 FTS...', flush=True)
    conn.execute("""
        INSERT INTO address_fts(rowid, address)
        SELECT id, address FROM land_transaction WHERE address != ''
    """)
    conn.commit()
    print('  FTS 重建完成', flush=True)


def step_merge():
    """Step 1: 將 transactions.db 獨有的交易記錄插入 land_data.db"""
    print('=' * 60)
    print('Step 1: MERGE — 插入 transactions.db 獨有記錄')
    print('=' * 60)
    t0 = time.time()

    conn_l = sqlite3.connect(DB_LAND)
    conn_l.text_factory = lambda b: b.decode('utf-8', errors='replace')
    conn_l.execute('PRAGMA journal_mode=WAL')
    conn_l.execute('PRAGMA synchronous=NORMAL')
    conn_l.execute('PRAGMA cache_size=-65536')

    conn_t = sqlite3.connect(DB_TRANS)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')

    cl = conn_l.cursor()
    ct = conn_t.cursor()

    addr_date_keys, date_price_keys = _build_land_data_keys(cl)

    print('  掃描 transactions.db 並插入獨有資料...', flush=True)
    ct.execute(
        'SELECT id, city, town, address, build_type, community, date_str, '
        'floor, area, total_price, unit_price, lat, lon, sq, raw_json '
        'FROM transactions'
    )

    BATCH = 10_000
    batch = []
    total_scanned = inserted = skipped_addr = skipped_price = 0

    for row in ct:
        total_scanned += 1
        date_str = row[6] or ''
        addr_raw = row[3] or ''
        tp_raw = row[9]
        rj = row[14]

        date = date_str.replace('/', '')
        addr = norm_addr(clean_trans_addr(addr_raw))

        if (date, addr) in addr_date_keys:
            skipped_addr += 1
            continue

        price = parse_price(tp_raw)
        if price and (date, price) in date_price_keys:
            skipped_price += 1
            continue

        try:
            rec = _map_record_for_insert(row, rj)
        except Exception as e:
            print(f'\n  [WARN] 跳過 id={row[0]}: {e}', flush=True)
            continue

        batch.append(rec)
        inserted += 1

        if len(batch) >= BATCH:
            cl.executemany(INSERT_SQL, batch)
            conn_l.commit()
            elapsed = time.time() - t0
            print(f'\r  已掃描 {total_scanned:,} | 插入 {inserted:,} | '
                  f'跳過(地址重複) {skipped_addr:,} | 跳過(總價重複) {skipped_price:,}  '
                  f'({elapsed:.0f}s)', end='', flush=True)
            batch.clear()

    if batch:
        cl.executemany(INSERT_SQL, batch)
        conn_l.commit()

    elapsed = time.time() - t0
    print(f'\n\n  ✅ 插入完成')
    print(f'     掃描總計: {total_scanned:,}')
    print(f'     已插入:   {inserted:,}')
    print(f'     跳過(date+addr重複): {skipped_addr:,}')
    print(f'     跳過(date+price重複): {skipped_price:,}')
    print(f'     耗時: {elapsed:.1f}s')

    _rebuild_fts(conn_l)

    print('  ANALYZE...', flush=True)
    conn_l.execute('ANALYZE')
    conn_l.commit()

    cl.execute('SELECT COUNT(*) FROM land_transaction')
    total = cl.fetchone()[0]
    db_size = os.path.getsize(DB_LAND) / 1024 / 1024
    print(f'\n  📊 merge 後統計')
    print(f'     總筆數: {total:,}')
    print(f'     DB 大小: {db_size:.1f} MB')

    conn_l.close()
    conn_t.close()
    return time.time() - t0


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2: ENRICH — 補充 land_data.db 缺失欄位
# ═══════════════════════════════════════════════════════════════════════════════

ENRICH_FIELDS = [
    'lat', 'lng', 'community', 'building_type', 'main_use',
    'has_management', 'rooms', 'halls', 'bathrooms', 'building_area',
    'unit_price', 'transaction_type', 'floor_level', 'total_floors', 'note',
]

# land_data 欄位名 → edata key → 空值判斷
FIELD_MAP = [
    ('lat',              'lat',              lambda v: v is None or v == 0),
    ('lng',              'lng',              lambda v: v is None or v == 0),
    ('community_name',   'community',        lambda v: not v),
    ('building_type',    'building_type',    lambda v: not v),
    ('main_use',         'main_use',         lambda v: not v),
    ('has_management',   'has_management',   lambda v: not v),
    ('rooms',            'rooms',            lambda v: v is None),
    ('halls',            'halls',            lambda v: v is None),
    ('bathrooms',        'bathrooms',        lambda v: v is None),
    ('building_area',    'building_area',    lambda v: v is None or v == 0),
    ('unit_price',       'unit_price',       lambda v: v is None or v == 0),
    ('transaction_type', 'transaction_type', lambda v: not v),
    ('floor_level',      'floor_level',      lambda v: not v),
    ('total_floors',     'total_floors',     lambda v: not v),
    ('note',             'note',             lambda v: not v),
]


def _make_edata(lat=None, lng=None, community='', building_type='',
                main_use='', has_management='', rooms=None, halls=None,
                bathrooms=None, building_area=None, unit_price=None,
                transaction_type='', floor_level='', total_floors='', note=''):
    return {
        'lat': lat, 'lng': lng, 'community': community,
        'building_type': building_type, 'main_use': main_use,
        'has_management': has_management, 'rooms': rooms, 'halls': halls,
        'bathrooms': bathrooms, 'building_area': building_area,
        'unit_price': unit_price, 'transaction_type': transaction_type,
        'floor_level': floor_level, 'total_floors': total_floors,
        'note': note,
    }


def _richness(d):
    score = 0
    if d.get('lat') and d['lat'] != 0: score += 3
    if d.get('community'): score += 3
    if d.get('rooms') is not None: score += 1
    if d.get('halls') is not None: score += 1
    if d.get('bathrooms') is not None: score += 1
    if d.get('building_area'): score += 1
    if d.get('building_type'): score += 1
    if d.get('main_use'): score += 1
    if d.get('has_management'): score += 1
    if d.get('transaction_type'): score += 1
    return score


def _merge_into(target, source):
    """從 source 補充 target 缺失的欄位"""
    for f in ENRICH_FIELDS:
        tv = target.get(f)
        if tv is None or tv == '' or tv == 0:
            sv = source.get(f)
            if sv is not None and sv != '' and sv != 0:
                target[f] = sv


def _parse_transaction_row(row):
    """從 transactions.db 一列解析出 edata dict + keys"""
    addr_raw, lat, lon, community, build_type, date_str, floor_col, area, tp, up, rj_text = row

    lat = lat if (lat and lat != 0) else None
    lng = lon if (lon and lon != 0) else None
    community = (community or '').strip()

    j = {}
    if rj_text:
        try:
            j = json.loads(rj_text)
        except Exception:
            pass

    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    rooms = halls = bathrooms = None
    try:
        rooms = int(j['j']) if j.get('j', '') != '' else None
    except Exception:
        pass
    try:
        halls = int(j['k']) if j.get('k', '') != '' else None
    except Exception:
        pass
    try:
        bathrooms = int(j['l']) if j.get('l', '') != '' else None
    except Exception:
        pass

    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''
    transaction_type = j.get('t', '') or ''
    note = j.get('note', '') or ''
    building_type = build_type or j.get('b', '') or ''

    building_area = None
    try:
        v = area or j.get('s', '')
        if v:
            building_area = float(str(v).replace(',', ''))
    except Exception:
        pass

    unit_price = None
    try:
        v = j.get('cp', '')
        if v:
            unit_price = float(str(v).replace(',', ''))
    except Exception:
        pass

    total_price = parse_price(j.get('tp'))

    clean = clean_trans_addr(addr_raw)
    norm = strip_city(norm_addr(clean))
    base = strip_floor(norm)
    date_norm = normalize_date(date_str)

    edata = _make_edata(
        lat=lat, lng=lng, community=community, building_type=building_type,
        main_use=main_use, has_management=has_management,
        rooms=rooms, halls=halls, bathrooms=bathrooms,
        building_area=building_area, unit_price=unit_price,
        transaction_type=transaction_type, floor_level=floor_level,
        total_floors=total_floors, note=note,
    )
    return edata, norm, base, date_norm, total_price


def _build_trans_maps():
    """從 transactions.db 建立三種映射表"""
    print('  讀取 transactions.db 建立映射表...', flush=True)
    conn = sqlite3.connect(DB_TRANS)
    conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    cur = conn.cursor()
    cur.execute("""
        SELECT address, lat, lon, community, build_type, date_str, floor, area,
               total_price, unit_price, raw_json
        FROM transactions
        WHERE address IS NOT NULL AND address != '' AND address != '#'
    """)

    full_map = {}       # norm 完整地址 → edata
    date_price_map = {} # (date, total_price) → edata
    base_map = {}       # norm 基礎地址（去樓層）→ edata

    count = 0
    for row in cur:
        try:
            edata, norm, base, date_norm, total_price = _parse_transaction_row(row)
        except Exception:
            continue

        if not norm:
            continue

        # 1. 全址映射
        if norm not in full_map:
            full_map[norm] = edata
        elif _richness(edata) > _richness(full_map[norm]):
            _merge_into(edata, full_map[norm])
            full_map[norm] = edata
        else:
            _merge_into(full_map[norm], edata)

        # 2. 日期+總價映射
        if date_norm and total_price and total_price > 0:
            key = (date_norm, total_price)
            if key not in date_price_map:
                date_price_map[key] = edata
            elif _richness(edata) > _richness(date_price_map[key]):
                _merge_into(edata, date_price_map[key])
                date_price_map[key] = edata
            else:
                _merge_into(date_price_map[key], edata)

        # 3. 基礎地址映射（去樓層）
        if base and base != norm:
            if base not in base_map:
                base_map[base] = edata
            elif _richness(edata) > _richness(base_map[base]):
                _merge_into(edata, base_map[base])
                base_map[base] = edata
            else:
                _merge_into(base_map[base], edata)

        count += 1
        if count % 500_000 == 0:
            print(f"    已讀取 {count:,} 筆...", flush=True)

    conn.close()
    print(f"    完成: 共 {count:,} 筆")
    print(f"    full_map:       {len(full_map):,} 個地址")
    print(f"    date_price_map: {len(date_price_map):,} 個日期+總價")
    print(f"    base_map:       {len(base_map):,} 個基礎地址")
    return full_map, date_price_map, base_map


def _flush_enrich_batch(conn, batch):
    """批次更新"""
    cur = conn.cursor()
    for updates, row_id in batch:
        set_clauses = []
        values = []
        for col, val in updates.items():
            set_clauses.append(f"{col} = ?")
            values.append(val)
        values.append(row_id)
        sql = f"UPDATE land_transaction SET {', '.join(set_clauses)} WHERE id = ?"
        cur.execute(sql, values)
    conn.commit()


def _enrich_land_data(full_map, date_price_map, base_map):
    """更新 land_data.db 缺失欄位"""
    print('\n  更新 land_data.db 缺失欄位...', flush=True)

    read_conn = sqlite3.connect(f'file:{DB_LAND}?mode=ro', uri=True)
    read_conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    read_conn.execute('PRAGMA cache_size=-131072')

    write_conn = sqlite3.connect(DB_LAND)
    write_conn.text_factory = lambda b: b.decode('utf-8', errors='replace')
    write_conn.execute('PRAGMA journal_mode=WAL')
    write_conn.execute('PRAGMA synchronous=NORMAL')
    write_conn.execute('PRAGMA cache_size=-65536')
    write_conn.execute('PRAGMA wal_autocheckpoint=0')

    cols = ', '.join(['rowid', 'id', 'address', 'transaction_date', 'total_price'] +
                     [f[0] for f in FIELD_MAP])

    cur2 = read_conn.cursor()
    cur2.execute("""
        SELECT COUNT(*) FROM land_transaction
        WHERE (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
          AND address LIKE '%號%'
    """)
    total = cur2.fetchone()[0]
    print(f"    候選記錄: {total:,}", flush=True)

    updated_full = updated_dp = updated_base = 0
    not_found = already_full = 0
    batch = []
    BATCH_SIZE = 10_000
    CHUNK_SIZE = 50_000
    t0 = time.time()
    global_i = 0

    cur_max = read_conn.cursor()
    cur_max.execute("""
        SELECT MAX(rowid) FROM land_transaction
        WHERE (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
          AND address LIKE '%號%'
    """)
    max_rowid = cur_max.fetchone()[0] or 0

    last_rowid = 0
    while last_rowid <= max_rowid:
        try:
            cur = read_conn.cursor()
            cur.execute(f"""
                SELECT {cols}
                FROM land_transaction
                WHERE rowid > {last_rowid}
                  AND (serial_no NOT LIKE '591_%' OR serial_no IS NULL)
                  AND address LIKE '%號%'
                ORDER BY rowid
                LIMIT {CHUNK_SIZE}
            """)
            chunk = cur.fetchall()
        except sqlite3.DatabaseError as e:
            print(f"\n    [WARN] rowid>{last_rowid:,} 讀取失敗 ({e})，跳過 {CHUNK_SIZE} 筆",
                  flush=True)
            last_rowid += CHUNK_SIZE
            continue

        if not chunk:
            break

        last_rowid = chunk[-1][0]

        for row in chunk:
            global_i += 1
            row_id       = row[1]
            address      = row[2]
            trans_date   = row[3]
            land_total_price = row[4]

            current_values = {}
            for j_idx, (db_col, _, is_empty) in enumerate(FIELD_MAP):
                current_values[db_col] = row[5 + j_idx]

            needs_enrich = any(
                is_empty(current_values[db_col])
                for db_col, _, is_empty in FIELD_MAP
            )
            if not needs_enrich:
                already_full += 1
                continue

            norm = strip_city(norm_addr(address))
            base = strip_floor(norm)
            date_norm = normalize_date(trans_date)

            match = None
            match_type = None

            if norm in full_map:
                match = full_map[norm].copy()
                match_type = 'full'

            if date_norm and land_total_price and land_total_price > 0:
                dp_key = (date_norm, land_total_price)
                dp_match = date_price_map.get(dp_key)
                if dp_match:
                    if match is None:
                        match = dp_match.copy()
                        match_type = 'date_price'
                    else:
                        _merge_into(match, dp_match)

            if base and base != norm and base in base_map:
                base_match = base_map[base]
                if match is None:
                    match = base_match.copy()
                    match_type = 'base'
                else:
                    _merge_into(match, base_match)

            if match is None:
                not_found += 1
                continue

            updates = {}
            for db_col, edata_key, is_empty in FIELD_MAP:
                if is_empty(current_values[db_col]):
                    new_val = match.get(edata_key)
                    if new_val is not None and new_val != '' and new_val != 0:
                        updates[db_col] = new_val

            if not updates:
                not_found += 1
                continue

            batch.append((updates, row_id))
            if match_type == 'full':
                updated_full += 1
            elif match_type == 'date_price':
                updated_dp += 1
            else:
                updated_base += 1

            if len(batch) >= BATCH_SIZE:
                _flush_enrich_batch(write_conn, batch)
                batch.clear()
                total_updated = updated_full + updated_dp + updated_base
                if total_updated % 100_000 == 0:
                    write_conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
                elapsed = time.time() - t0
                print(f"\r    進度: {global_i:,}/{total:,} | 更新: {total_updated:,} "
                      f"(全址:{updated_full:,} 日期價格:{updated_dp:,} 基礎:{updated_base:,}) "
                      f"({elapsed:.0f}s)",
                      end='', flush=True)

    if batch:
        _flush_enrich_batch(write_conn, batch)

    write_conn.execute('PRAGMA wal_checkpoint(FULL)')
    write_conn.commit()
    write_conn.close()
    read_conn.close()

    elapsed = time.time() - t0
    total_updated = updated_full + updated_dp + updated_base
    print(f"\n\n  ✅ enrich 完成")
    print(f"     候選記錄:     {total:,}")
    print(f"     已有完整資料: {already_full:,}")
    print(f"     成功更新:     {total_updated:,}")
    print(f"       全址匹配:     {updated_full:,}")
    print(f"       日期+總價:    {updated_dp:,}")
    print(f"       基礎地址:     {updated_base:,}")
    print(f"     未找匹配:     {not_found:,}")
    print(f"     耗時: {elapsed:.1f}s")


def step_enrich():
    """Step 2: 用 transactions.db 補充 land_data.db 缺失欄位"""
    print('=' * 60)
    print('Step 2: ENRICH — 補充缺失欄位')
    print('=' * 60)
    t0 = time.time()

    full_map, date_price_map, base_map = _build_trans_maps()
    _enrich_land_data(full_map, date_price_map, base_map)

    # 驗證
    print("\n  驗證結果...", flush=True)
    conn = sqlite3.connect(DB_LAND)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM land_transaction")
    n_total = cur.fetchone()[0]

    stats = []
    for db_col, _, is_empty in FIELD_MAP:
        if db_col in ('lat', 'lng', 'building_area', 'unit_price'):
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL AND {db_col} != 0")
        elif db_col in ('rooms', 'halls', 'bathrooms'):
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL")
        else:
            cur.execute(f"SELECT COUNT(*) FROM land_transaction WHERE {db_col} IS NOT NULL AND {db_col} != ''")
        cnt = cur.fetchone()[0]
        pct = cnt / n_total * 100
        stats.append((db_col, cnt, pct))

    conn.close()

    print(f"\n  📊 land_data.db 最終統計 (總筆數: {n_total:,})")
    print(f"  {'欄位':<20} {'有值筆數':>12} {'覆蓋率':>8}")
    print(f"  {'─'*20} {'─'*12} {'─'*8}")
    for col, cnt, pct in stats:
        print(f"  {col:<20} {cnt:>12,} {pct:>7.1f}%")

    return time.time() - t0


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3: BACKFILL — 回填 community_name
# ═══════════════════════════════════════════════════════════════════════════════

def _build_community_map():
    """從 transactions.db 建立 (county_city, district, 門牌無樓) → community 映射"""
    print('  讀取 transactions.db 社區對應...', flush=True)
    conn = sqlite3.connect(DB_TRANS)
    rows = conn.execute(
        "SELECT city, address, community FROM transactions "
        "WHERE community != '' AND community IS NOT NULL AND address != ''"
    ).fetchall()
    conn.close()
    print(f'    有 community 的記錄: {len(rows):,}', flush=True)

    mapping = {}

    for city_code, addr_raw, community in rows:
        addr = clean_trans_addr(addr_raw)
        addr = norm_addr(addr)
        district = extract_district_backfill(addr)
        if not district:
            continue

        # 去掉 district 前綴
        if addr.startswith(district):
            road_number = addr[len(district):]
        else:
            for prefix_len in (3, 2):
                if addr[prefix_len:].startswith(district):
                    road_number = addr[prefix_len + len(district):]
                    break
            else:
                road_number = addr

        road_number = strip_floor_backfill(road_number)
        if not road_number or '號' not in road_number:
            continue

        county_city = CITY_CODE_MAP.get(city_code, '')
        key = (county_city, district, road_number)

        if key not in mapping:
            mapping[key] = {}
        mapping[key][community] = mapping[key].get(community, 0) + 1

    result = {}
    for key, comm_counts in mapping.items():
        best = max(comm_counts, key=comm_counts.get)
        result[key] = best

    print(f'    不重複 (county_city, district, road+number) 組合: {len(result):,}', flush=True)
    return result


def step_backfill(dry_run=False):
    """Step 3: 從 transactions.db 回填 community_name"""
    print('=' * 60)
    print(f'Step 3: BACKFILL — 回填 community_name {"(DRY RUN)" if dry_run else ""}')
    print('=' * 60)
    t0 = time.time()

    comm_map = _build_community_map()

    print(f'\n  連接 land_data.db ...', flush=True)
    land = sqlite3.connect(DB_LAND)
    land.execute('PRAGMA journal_mode=WAL')
    land.execute('PRAGMA synchronous=NORMAL')
    land.execute('PRAGMA cache_size=-200000')

    try:
        land.execute('CREATE INDEX IF NOT EXISTS idx_lt_district ON land_transaction(district)')
    except Exception:
        pass

    updated_total = 0
    batch_size = 500
    batch_count = 0
    total_keys = len(comm_map)

    print(f'  開始回填 community_name ({total_keys:,} 個門牌) ...', flush=True)

    land.execute('BEGIN')

    for i, ((county_city, district, road_number), community) in enumerate(comm_map.items()):
        pattern = f'%{district}{road_number}%'

        if dry_run:
            count = land.execute(
                "SELECT COUNT(*) FROM land_transaction "
                "WHERE district = ? AND address LIKE ? "
                "AND (community_name IS NULL OR community_name = '')",
                (district, pattern)
            ).fetchone()[0]
            updated_total += count
        else:
            cursor = land.execute(
                "UPDATE land_transaction SET community_name = ? "
                "WHERE district = ? AND address LIKE ? "
                "AND (community_name IS NULL OR community_name = '')",
                (community, district, pattern)
            )
            updated_total += cursor.rowcount

        batch_count += 1
        if batch_count >= batch_size:
            if not dry_run:
                land.execute('COMMIT')
                land.execute('BEGIN')
            batch_count = 0
            elapsed = time.time() - t0
            pct = (i + 1) / total_keys * 100
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total_keys - i - 1) / rate if rate > 0 else 0
            print(f'    [{pct:5.1f}%] {i+1:,}/{total_keys:,} 已更新 {updated_total:,} 筆 '
                  f'({elapsed:.0f}s, ETA {eta:.0f}s)', flush=True)

    if not dry_run:
        land.execute('COMMIT')

    elapsed = time.time() - t0
    print(f'\n  {"[DRY RUN] 預計" if dry_run else "✅ 已"}更新 {updated_total:,} 筆 community_name')
    print(f'  耗時 {elapsed:.1f} 秒')

    # 統計結果
    stats = land.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN community_name IS NOT NULL AND community_name != '' THEN 1 ELSE 0 END) as has_comm "
        "FROM land_transaction"
    ).fetchone()
    print(f'\n  📊 backfill 後統計:')
    print(f'     總筆數: {stats[0]:,}')
    print(f'     有 community_name: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)')

    land.close()
    return time.time() - t0


# ═══════════════════════════════════════════════════════════════════════════════
# 主程式入口
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='整合 merge / enrich / backfill 三步驟，處理 transactions.db → land_data.db'
    )
    parser.add_argument('--step', choices=['merge', 'enrich', 'backfill'],
                        help='只執行指定步驟（預設全部執行）')
    parser.add_argument('--dry-run', action='store_true',
                        help='backfill 步驟僅統計不寫入')
    args = parser.parse_args()

    t0 = time.time()
    steps = [args.step] if args.step else ['merge', 'enrich', 'backfill']

    print(f'\n{"=" * 60}')
    print(f'  merge_and_enrich — 執行步驟: {", ".join(steps)}')
    print(f'  land_data.db:    {DB_LAND}')
    print(f'  transactions.db: {DB_TRANS}')
    print(f'{"=" * 60}\n')

    elapsed_steps = {}

    if 'merge' in steps:
        elapsed_steps['merge'] = step_merge()
        print()

    if 'enrich' in steps:
        elapsed_steps['enrich'] = step_enrich()
        print()

    if 'backfill' in steps:
        elapsed_steps['backfill'] = step_backfill(dry_run=args.dry_run)
        print()

    total_elapsed = time.time() - t0
    print(f'\n{"=" * 60}')
    print(f'  全部完成！')
    for step, elapsed in elapsed_steps.items():
        print(f'    {step:>10}: {elapsed:.1f}s')
    print(f'    {"總耗時":>8}: {total_elapsed:.1f}s')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    main()
