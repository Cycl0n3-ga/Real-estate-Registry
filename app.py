#!/usr/bin/env python3
"""
良富居地產 - 專業房地產地圖系統 v2.0
整合建案地圖、價格查詢、銷控面板、地址轉社區
使用 Leaflet.js + OpenStreetMap（完全免費，不需要 API Key）
資料來源：Building_Projects_B.csv（11,169 建案）、ALL_lvr_land_a.csv（交易紀錄）
         address_community_mapping.csv（地址↔社區對照）
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
import os
import re
import json
import math
import time
import hashlib
import csv
from collections import defaultdict
from urllib.parse import quote_plus
from urllib.request import urlopen, Request

app = Flask(__name__)
CORS(app)

# ============================================================
# 路徑設定
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, 'ALL_lvr_land_a.csv')
BUILDING_B_PATH = os.path.join(BASE_DIR, 'Building_Projects_B.csv')
ADDR2COM_PATH = os.path.join(BASE_DIR, 'address2com', 'address_community_mapping.csv')
GEOCODE_CACHE_PATH = os.path.join(BASE_DIR, 'geocode_cache.json')

PING_TO_SQM = 3.30579

# ============================================================
# 全域資料
# ============================================================
BUILDING_PROJECTS = {}       # pid -> project dict (from Building_Projects_B.csv)
BUILDING_PROJECTS_READY = False
ADDR2COM_DATA = {}           # 地址→社區名 多層索引
ADDR2COM_READY = False
_address_coordinates_db = {} # 地址→座標
_geocode_cache = {}

# ============================================================
# 台灣主要鄉鎮市區座標映射
# ============================================================
DISTRICT_COORDINATES = {
    '中壢區': (24.9696, 120.9843), '桃園區': (25.0330, 121.3167),
    '新竹市': (24.8026, 120.9693), '北屯區': (24.2169, 120.7901),
    '淡水區': (25.1654, 121.4529), '板橋區': (25.0121, 121.4627),
    '西屯區': (24.1884, 120.6350), '新莊區': (25.0568, 121.4315),
    '竹北市': (24.8363, 120.9863), '中和區': (25.0049, 121.4935),
    '台中市': (24.1477, 120.6736), '新竹縣': (24.9474, 121.0119),
    '北投區': (25.1370, 121.5130), '苗栗市': (24.5595, 120.8196),
    '台南市': (22.9973, 120.2171), '高雄市': (22.6172, 120.3014),
    '中山區': (25.0455, 121.5149), '大安區': (25.0330, 121.5254),
    '松山區': (25.0487, 121.5623), '南港區': (25.0543, 121.6090),
    '信義區': (25.0330, 121.5654), '內湖區': (25.0850, 121.5788),
    '士林區': (25.1122, 121.5254), '大同區': (25.0737, 121.5149),
    '文山區': (25.0035, 121.5674), '南屯區': (24.1003, 120.6684),
    '烏日區': (24.0630, 120.6717), '龍井區': (24.2507, 120.5690),
    '霧峰區': (24.0580, 120.8225), '東勢區': (24.2569, 120.7920),
    '太平區': (24.1456, 120.9383), '石岡區': (24.2169, 120.7901),
    '后里區': (24.3185, 120.7436), '潭子區': (24.1995, 120.8610),
    '大雅區': (24.2575, 120.7870), '神岡區': (24.2456, 120.8080),
    '清水區': (24.2583, 120.5689), '梧棲區': (24.2495, 120.5439),
    '大肚區': (24.2250, 120.5519), '沙鹿區': (24.2330, 120.5699),
    '鹿港鎮': (24.0585, 120.4325), '花壇鄉': (24.0937, 120.5146),
    '芬園鄉': (24.0880, 120.5738), '彰化縣': (24.0827, 120.4167),
    '竹山鎮': (23.7599, 120.6861), '南投縣': (23.9120, 120.6672),
    '埔里鎮': (23.9610, 120.9660), '魚池鄉': (23.8827, 120.9071),
    '基隆市': (25.1276, 121.7347), '宜蘭縣': (24.7599, 121.7497),
    '花蓮縣': (24.0046, 121.5743), '台東縣': (22.7696, 121.1446),
    '澎湖縣': (23.5731, 119.5922), '金門縣': (24.4353, 118.3157),
    '連江縣': (26.1583, 119.9583), '屏東縣': (22.5442, 120.4886),
    '雲林縣': (23.7071, 120.4334), '嘉義市': (23.4788, 120.4432),
    '嘉義縣': (23.4534, 120.6081), '白河區': (22.9153, 120.3789),
    '將軍區': (23.1648, 120.2226), '七股區': (23.1527, 120.1363),
    '學甲區': (23.2315, 120.2693), '北門區': (23.2728, 120.1704),
    '新營區': (23.3032, 120.3031), '永康區': (22.9896, 120.2440),
    '仁德區': (22.9385, 120.2545), '左鎮區': (22.8146, 120.3696),
    '歸仁區': (22.9049, 120.3027), '關廟區': (22.8921, 120.3196),
    '東山區': (23.0000, 120.4500), '下營區': (23.1329, 120.3107),
    '六甲區': (23.2074, 120.4006), '官田區': (23.1933, 120.4319),
    '大內區': (23.1167, 120.4667), '山上區': (23.1424, 120.4619),
    '麻豆區': (23.1793, 120.2411), '佳里區': (23.1602, 120.1808),
    '西港區': (23.1417, 120.1865), '後壁區': (23.3452, 120.4089),
    '柳營區': (23.2839, 120.3730), '鹽水區': (23.2832, 120.2788),
    '玉井區': (23.0777, 120.5452), '南化區': (22.9005, 120.4833),
    '楠西區': (23.0238, 120.5567), '新北市': (25.0170, 121.4627),
    '三重區': (25.0617, 121.4879), '蘆洲區': (25.0855, 121.4738),
    '汐止區': (25.0626, 121.6610), '永和區': (25.0076, 121.5138),
    '三峽區': (24.9340, 121.3687), '土城區': (24.9723, 121.4437),
    '鶯歌區': (24.9519, 121.3517), '泰山區': (25.0500, 121.4300),
    '林口區': (25.0786, 121.3919), '五股區': (25.0787, 121.4380),
    '八里區': (25.1400, 121.4000), '樹林區': (24.9909, 121.4200),
    '深坑區': (25.0020, 121.6155), '石碇區': (24.9915, 121.5910),
    '平溪區': (25.0262, 121.7387), '雙溪區': (24.9940, 121.8260),
    '貢寮區': (25.0223, 121.9063), '瑞芳區': (25.1092, 121.8100),
    '萬里區': (25.1792, 121.6891), '金山區': (25.2220, 121.6370),
    '左營區': (22.6847, 120.2940), '前鎮區': (22.5955, 120.3268),
    '三民區': (22.6467, 120.3165), '鼓山區': (22.6555, 120.2710),
    '苓雅區': (22.6200, 120.3260), '楠梓區': (22.7308, 120.3262),
    '小港區': (22.5647, 120.3456), '鳳山區': (22.6268, 120.3595),
    '大寮區': (22.5965, 120.3987), '鳥松區': (22.6620, 120.3647),
    '仁武區': (22.7002, 120.3520), '岡山區': (22.7906, 120.2953),
    '路竹區': (22.8561, 120.2617), '橋頭區': (22.7575, 120.3058),
    '梓官區': (22.7581, 120.2637), '旗山區': (22.8861, 120.4839),
    '美濃區': (22.8982, 120.5421), '大樹區': (22.7240, 120.4300),
    '林園區': (22.5100, 120.3927), '前金區': (22.6266, 120.2952),
    '新興區': (22.6296, 120.3090), '鹽埕區': (22.6230, 120.2836),
    '旗津區': (22.5898, 120.2653), '龍潭區': (24.8642, 121.2163),
    '楊梅區': (24.9077, 121.1449), '大溪區': (24.8832, 121.2863),
    '蘆竹區': (25.0439, 121.2917), '大園區': (25.0647, 121.2333),
    '龜山區': (25.0287, 121.3453), '八德區': (24.9456, 121.2900),
    '平鎮區': (24.9459, 121.2182), '觀音區': (25.0349, 121.1417),
    '新屋區': (24.9736, 121.1067), '復興區': (24.8200, 121.3500),
    '竹東鎮': (24.7310, 121.0900), '新豐鄉': (24.8900, 120.9700),
    '湖口鄉': (24.9023, 121.0400), '關西鎮': (24.7890, 121.1770),
    '新埔鎮': (24.8270, 121.0733), '寶山鄉': (24.7600, 120.9800),
    '芎林鄉': (24.7770, 121.0700), '峨眉鄉': (24.6880, 120.9930),
    '北埔鄉': (24.6996, 121.0530), '橫山鄉': (24.7200, 121.1130),
    '尖石鄉': (24.7050, 121.2000), '五峰鄉': (24.6000, 121.1000),
    '安南區': (23.0468, 120.1853), '安平區': (22.9927, 120.1659),
    '東區': (22.9798, 120.2252), '北區': (23.0030, 120.2080),
    '南區': (22.9600, 120.1980), '中西區': (22.9920, 120.2000),
    '善化區': (23.1310, 120.2978), '新化區': (23.0383, 120.3119),
    '安定區': (23.0880, 120.2267), '彰化市': (24.0827, 120.5417),
    '員林市': (23.9590, 120.5740), '和美鎮': (24.1125, 120.4990),
    '北斗鎮': (23.8692, 120.5200), '溪湖鎮': (23.9630, 120.4810),
    '田中鎮': (23.8570, 120.5810), '二林鎮': (23.8990, 120.3730),
    '線西鄉': (24.1317, 120.4680), '伸港鄉': (24.1560, 120.4840),
    '福興鄉': (24.0470, 120.4410), '秀水鄉': (24.0350, 120.5010),
    '埔心鄉': (23.9520, 120.5430), '永靖鄉': (23.9240, 120.5490),
    '社頭鄉': (23.8960, 120.5870), '大村鄉': (23.9970, 120.5570),
    '南投市': (23.9120, 120.6672), '草屯鎮': (23.9740, 120.6800),
    '名間鄉': (23.8380, 120.6580), '集集鎮': (23.8290, 120.6870),
    '水里鄉': (23.8120, 120.8530), '鹿谷鄉': (23.7510, 120.7530),
    '信義鄉': (23.7000, 120.8800), '國姓鄉': (24.0410, 120.8580),
    '中寮鄉': (23.8790, 120.7670), '仁愛鄉': (24.0240, 121.1330),
    '新店區': (24.9677, 121.5419), '萬華區': (25.0329, 121.5004),
    '豐原區': (24.2444, 120.7181), '大里區': (24.0995, 120.6780),
    '頭份市': (24.6880, 120.9030), '竹南鎮': (24.6850, 120.8780),
    '屏東市': (22.6727, 120.4886), '宜蘭市': (24.7518, 121.7580),
    '羅東鎮': (24.6775, 121.7667), '花蓮市': (23.9768, 121.6044),
    '台東市': (22.7563, 121.1438), '斗六市': (23.7072, 120.5448),
    '虎尾鎮': (23.7082, 120.4318), '朴子市': (23.4647, 120.2480),
}


# ============================================================
# 工具函數
# ============================================================

def clean_nan_values(obj):
    """遞歸清理字典/列表中的 NaN / Infinity 值"""
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return obj
    return obj


def get_district_coordinates(district):
    """取得鄉鎮市區的座標"""
    if not district:
        return (24.0, 121.0)
    if district in DISTRICT_COORDINATES:
        return DISTRICT_COORDINATES[district]
    for key in DISTRICT_COORDINATES:
        if district in key or key in district:
            return DISTRICT_COORDINATES[key]
    return (24.0, 121.0)


_CN_NUM = {
    '零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
    '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
    '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
    '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
    '二十一': 21, '二十二': 22, '二十三': 23, '二十四': 24, '二十五': 25,
    '二十六': 26, '二十七': 27, '二十八': 28, '二十九': 29, '三十': 30,
    '三十一': 31, '三十二': 32, '三十三': 33, '三十四': 34, '三十五': 35,
    '四十': 40, '五十': 50,
}

def _cn_to_int(s):
    """將中文數字字串轉換為整數，失敗則回傳 None"""
    s = s.strip()
    if s in _CN_NUM:
        return _CN_NUM[s]
    # 例如「三十六」= 36
    m = re.match(r'^([二三四五六七八九]?)十([一二三四五六七八九]?)$', s)
    if m:
        tens = _CN_NUM.get(m.group(1) or '一', 1)
        units = _CN_NUM.get(m.group(2), 0) if m.group(2) else 0
        return tens * 10 + units
    return None

def fullwidth_to_halfwidth(s):
    """全形數字/字母轉半形"""
    if not s:
        return s
    result = []
    for c in s:
        o = ord(c)
        if 0xFF01 <= o <= 0xFF5E:
            result.append(chr(o - 0xFEE0))
        else:
            result.append(c)
    return ''.join(result)

def convert_floor_str(s):
    """將樓層字串中的中文數字轉換為阿拉伯數字，並將全形數字轉半形。
    例如: '十四層' -> '14層', '七層，夾層' -> '7層，夾層', '全' -> '全'
    """
    if not s:
        return s
    s = fullwidth_to_halfwidth(str(s))
    # 逐段替換中文數字
    def replace_cn(m):
        num = _cn_to_int(m.group(1))
        return str(num) if num is not None else m.group(0)
    # 匹配中文數字（最長優先：先試兩字符再試一字符）
    s = re.sub(r'([二三四五六七八九]?十[一二三四五六七八九]?|[一二三四五六七八九十零])', replace_cn, s)
    return s


def normalize_search_text(text):
    """正規化搜尋文字 - 生成多個版本用於搜尋
    中文數字 '六' 可能需要轉成 '６'（全形），因為 CSV 中就是用全形數字
    """
    if not text:
        return ('', '')
    full_to_half = {
        '０': '0', '１': '1', '２': '2', '３': '3', '４': '4',
        '５': '5', '６': '6', '７': '7', '８': '8', '９': '9',
        'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E',
        'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
        'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O',
        'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
        'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y', 'Ｚ': 'Z',
    }
    half_to_full = {v: k for k, v in full_to_half.items()}
    
    # 先轉全形阿拉伯數字
    half_width = ''.join(full_to_half.get(c, c) for c in text)
    
    # 中文數字直接轉全形數字（因為 CSV 中用的是全形 ０-９）
    # 六 -> ６, 七 -> ７ 等等
    cn_digits = {'零': '０', '一': '１', '二': '２', '三': '３', '四': '４', 
                 '五': '５', '六': '６', '七': '７', '八': '８', '九': '９', '十': '１０'}
    cn_to_fullwidth_num = ''.join(cn_digits.get(c, c) for c in text)
    
    # 回傳：(半形版本, 中文數字→全形版本)
    return (half_width, cn_to_fullwidth_num)


def is_real_building(address):
    """判斷是否為真實建案（非純地號）"""
    if not address or len(address) < 5:
        return False
    if '地號' in address and '號' not in address.replace('地號', ''):
        return False
    return True


def extract_building_project_name(address):
    """從地址中提取或生成建案名稱"""
    patterns = [
        r'([\u4e00-\u9fff]+(?:大樓|華廈|大廈|花園|社區|廣場|公寓|別墅|新村|山莊|首府|天廈|之星|御品|豪庭|名邸|雅築))',
        r'([\u4e00-\u9fff]+[一二三四五六七八九十百]期)',
    ]
    for pattern in patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(1)
    match = re.search(r'([\u4e00-\u9fff]+(?:路|街|大道)[\u4e00-\u9fff]*\d+號)', address)
    if match:
        return match.group(1)
    if len(address) > 15:
        return address[:15] + '...'
    return address


def format_roc_date(roc_date):
    """將民國日期(1130101)轉為西元格式(2024/01/01)"""
    if not roc_date:
        return None
    ds = str(roc_date).strip()
    if len(ds) < 7:
        return None
    try:
        y = int(ds[:3]) + 1911
        return f"{y}/{ds[3:5]}/{ds[5:7]}"
    except Exception:
        return None


def get_coordinates_for_address(address, district):
    """取得地址座標 - 優先用高精度庫"""
    if address and address in _address_coordinates_db:
        return _address_coordinates_db[address]
    return get_district_coordinates(district)


def make_project_id(name, address, district):
    """從建案名+地址+區域產生穩定 ID"""
    key = f"{name}|{address}|{district}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def make_address_id(address):
    """從地址產生穩定 ID"""
    return hashlib.md5(address.encode()).hexdigest()[:12]


# ============================================================
# 初始化: 載入 Building_Projects_B.csv
# ============================================================

def init_building_projects():
    """從 Building_Projects_B.csv 載入 11,169 個建案"""
    global BUILDING_PROJECTS, BUILDING_PROJECTS_READY
    print("🏗️  載入建案資料 (Building_Projects_B.csv)...")
    t0 = time.time()

    if not os.path.exists(BUILDING_B_PATH):
        print(f"⚠️  找不到 {BUILDING_B_PATH}，改用 CSV 聚合模式")
        init_building_projects_from_csv()
        return

    try:
        projects = {}
        with open(BUILDING_B_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get('建案名稱') or '').strip()
                district = (row.get('鄉鎮市區') or '').strip()
                address = (row.get('代表地址') or '').strip()
                if not name or not district:
                    continue

                pid = make_project_id(name, address, district)
                tx_count = int(row.get('交易筆數', 0) or 0)
                avg_price = float(row.get('平均成交價元', 0) or 0)
                avg_area_sqm = float(row.get('平均面積平方公尺', 0) or 0)
                max_floor = row.get('最高樓層', '')
                building_type = (row.get('建物型態') or '住宅').split(';')[0].strip()
                year_range = row.get('交易年份範圍', '')
                addr_count = int(row.get('地址數量', 1) or 1)

                avg_ping = avg_area_sqm / PING_TO_SQM if avg_area_sqm else 0
                avg_unit_price_ping = (avg_price / avg_ping) if avg_ping > 0 else 0

                lat, lng = get_coordinates_for_address(address, district)

                projects[pid] = {
                    'id': pid,
                    'name': name,
                    'address': address,
                    'district': district,
                    'type': building_type,
                    'transaction_count': tx_count,
                    'avg_price': avg_price,
                    'avg_unit_price': round(avg_unit_price_ping, 2),
                    'avg_ping': round(avg_ping, 2),
                    'avg_area_sqm': round(avg_area_sqm, 2),
                    'max_floor': max_floor,
                    'year_range': year_range,
                    'addr_count': addr_count,
                    'source': 'B',
                    'lat': lat,
                    'lng': lng,
                }

        BUILDING_PROJECTS = projects
        BUILDING_PROJECTS_READY = True
        elapsed = time.time() - t0
        print(f"✅ 建案載入完成: {len(projects)} 個建案, 耗時 {elapsed:.1f}s")
    except Exception as e:
        print(f"❌ 建案載入失敗: {e}")
        import traceback; traceback.print_exc()
        BUILDING_PROJECTS_READY = True


def init_building_projects_from_csv():
    """後備方案：從 ALL_lvr_land_a.csv 聚合建案"""
    global BUILDING_PROJECTS, BUILDING_PROJECTS_READY
    try:
        con = duckdb.connect()
        query = f"""
        SELECT
            土地位置建物門牌,
            鄉鎮市區,
            建物型態,
            COUNT(*) as cnt,
            AVG(TRY_CAST(總價元 AS DOUBLE)) as avg_price,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * {PING_TO_SQM}) as avg_unit_price_ping,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) / {PING_TO_SQM}) as avg_ping,
            MAX(交易年月日) as latest_date,
            MAX(總樓層數) as total_floors
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL
            AND 土地位置建物門牌 != ''
            AND 土地位置建物門牌 != '土地位置建物門牌'
            AND TRY_CAST(總價元 AS DOUBLE) > 0
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        HAVING COUNT(*) >= 2
        ORDER BY COUNT(*) DESC
        LIMIT 10000
        """
        result = con.execute(query).fetchdf()
        con.close()

        projects = {}
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            if not is_real_building(address):
                continue
            district = row['鄉鎮市區']
            pid = make_address_id(address)
            name = extract_building_project_name(address)
            lat, lng = get_coordinates_for_address(address, district)

            projects[pid] = {
                'id': pid,
                'name': name,
                'address': address,
                'district': district,
                'type': row['建物型態'] or '住宅',
                'transaction_count': int(row['cnt']),
                'avg_price': float(row['avg_price']) if row['avg_price'] else 0,
                'avg_unit_price': round(float(row['avg_unit_price_ping']), 2) if row['avg_unit_price_ping'] else 0,
                'avg_ping': round(float(row['avg_ping']), 2) if row['avg_ping'] else 0,
                'latest_date': row['latest_date'],
                'max_floor': row['total_floors'],
                'source': 'CSV',
                'lat': lat,
                'lng': lng,
            }

        BUILDING_PROJECTS = projects
        BUILDING_PROJECTS_READY = True
        print(f"✅ CSV 聚合建案完成: {len(projects)} 個建案")
    except Exception as e:
        print(f"❌ CSV 聚合建案失敗: {e}")
        BUILDING_PROJECTS_READY = True


# ============================================================
# 初始化: 載入 address_community_mapping.csv
# ============================================================

def init_addr2com():
    """載入地址↔社區名對照表，建立多層索引"""
    global ADDR2COM_DATA, ADDR2COM_READY
    print("🏘️  載入地址→社區對照表...")
    t0 = time.time()

    if not os.path.exists(ADDR2COM_PATH):
        print(f"⚠️  找不到 {ADDR2COM_PATH}")
        ADDR2COM_READY = True
        return

    try:
        index = {
            'normalized': {},
            'to_number': {},
            'to_alley': {},
            'road': {},
        }
        with open(ADDR2COM_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                normalized = (row.get('正規化地址') or '').strip()
                to_number = (row.get('到號地址') or '').strip()
                to_alley = (row.get('到巷地址') or '').strip()
                road = (row.get('路段') or '').strip()
                community = (row.get('社區名稱') or '').strip()
                district = (row.get('鄉鎮市區') or '').strip()
                tx_count = int(row.get('交易筆數', 0) or 0)
                source = (row.get('資料來源') or '').strip()
                all_names = (row.get('所有建案名') or '').strip()

                if not community:
                    continue

                entry = {
                    'community': community,
                    'district': district,
                    'tx_count': tx_count,
                    'source': source,
                    'all_names': all_names,
                }

                if normalized:
                    index['normalized'][normalized] = entry
                if to_number:
                    if to_number not in index['to_number'] or tx_count > index['to_number'][to_number].get('tx_count', 0):
                        index['to_number'][to_number] = entry
                if to_alley:
                    if to_alley not in index['to_alley'] or tx_count > index['to_alley'][to_alley].get('tx_count', 0):
                        index['to_alley'][to_alley] = entry
                if road:
                    if road not in index['road']:
                        index['road'][road] = []
                    index['road'][road].append(entry)

        ADDR2COM_DATA = index
        ADDR2COM_READY = True
        elapsed = time.time() - t0
        total = len(index['normalized']) + len(index['to_number']) + len(index['to_alley']) + len(index['road'])
        print(f"✅ 地址→社區對照表載入完成: {total} 筆索引, 耗時 {elapsed:.1f}s")
    except Exception as e:
        print(f"❌ 地址→社區對照表載入失敗: {e}")
        import traceback; traceback.print_exc()
        ADDR2COM_READY = True


def lookup_community(address, district=None):
    """查詢地址對應的社區名稱，回傳最佳匹配"""
    if not ADDR2COM_READY or not ADDR2COM_DATA:
        return None

    addr = address.strip() if address else ''
    if not addr:
        return None

    # Level 1: 完整正規化地址 (98%)
    if addr in ADDR2COM_DATA['normalized']:
        entry = ADDR2COM_DATA['normalized'][addr]
        if not district or not entry.get('district') or entry['district'] == district:
            return {**entry, 'confidence': 98, 'match_level': 'normalized'}

    # 從地址提取不同層級 key
    addr_to_number = re.sub(r'\d+號.*$', '', addr)
    addr_no_number = re.sub(r'\d+號$', '', addr)

    # Level 2: 到號地址 (90%)
    for key in [addr_to_number, addr_no_number]:
        if key and key in ADDR2COM_DATA['to_number']:
            entry = ADDR2COM_DATA['to_number'][key]
            if not district or not entry.get('district') or entry['district'] == district:
                return {**entry, 'confidence': 90, 'match_level': 'to_number'}

    # Level 3: 到巷地址 (72%)
    addr_to_alley = re.sub(r'\d+巷.*$', '', addr)
    addr_to_alley2 = re.sub(r'\d+弄.*$', '', addr)
    for key in [addr_to_alley, addr_to_alley2]:
        if key and key in ADDR2COM_DATA['to_alley']:
            entry = ADDR2COM_DATA['to_alley'][key]
            if not district or not entry.get('district') or entry['district'] == district:
                return {**entry, 'confidence': 72, 'match_level': 'to_alley'}

    # Level 4: 路段 (40%)
    road_match = re.search(r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[\u4e00-\u9fff]*段)?)', addr)
    if road_match:
        road = road_match.group(1)
        if road in ADDR2COM_DATA['road']:
            entries = ADDR2COM_DATA['road'][road]
            if district:
                filtered = [e for e in entries if e.get('district') == district]
                if filtered:
                    best = max(filtered, key=lambda x: x.get('tx_count', 0))
                    return {**best, 'confidence': 40, 'match_level': 'road'}
            if entries:
                best = max(entries, key=lambda x: x.get('tx_count', 0))
                return {**best, 'confidence': 40, 'match_level': 'road'}

    return None


# ============================================================
# 初始化: 建構地址座標庫
# ============================================================

def build_address_coordinates_db():
    """從 CSV 建構高精度地址座標庫（向量化版本）"""
    global _address_coordinates_db
    try:
        print("🗺️  建構地址座標庫...")
        t0 = time.time()
        con = duckdb.connect()
        query = f"""
        SELECT DISTINCT
            土地位置建物門牌 as addr,
            鄉鎮市區 as district
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL AND 土地位置建物門牌 != ''
        """
        result = con.execute(query).fetchdf()
        con.close()

        # 批量處理
        db = {}
        for addr, district in zip(result['addr'].values, result['district'].values):
            district_lat, district_lng = get_district_coordinates(str(district))
            addr_str = str(addr)
            addr_seed = int(hashlib.md5(addr_str.encode()).hexdigest()[:8], 16)
            lat_offset = ((addr_seed % 1000) - 500) * 0.0001
            lng_offset = (((addr_seed // 1000) % 1000) - 500) * 0.0001
            db[addr_str] = (district_lat + lat_offset, district_lng + lng_offset)

        # 載入 geocode cache
        if os.path.exists(GEOCODE_CACHE_PATH):
            try:
                with open(GEOCODE_CACHE_PATH, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    for addr, coords in cache.items():
                        if coords and isinstance(coords, list) and len(coords) == 2:
                            db[addr] = tuple(coords)
            except Exception:
                pass

        _address_coordinates_db = db
        elapsed = time.time() - t0
        print(f"✅ 地址座標庫建構完成: {len(db)} 筆地址, 耗時 {elapsed:.1f}s")
    except Exception as e:
        print(f"⚠️  地址座標庫建構失敗: {e}")
        import traceback; traceback.print_exc()


# ============================================================
# Geocoding (Nominatim fallback)
# ============================================================
_geocode_last_call = 0

def nominatim_geocode(address):
    """使用 Nominatim（OpenStreetMap 免費 geocoding）"""
    global _geocode_last_call, _geocode_cache

    if address in _geocode_cache:
        return _geocode_cache[address]

    now = time.time()
    elapsed = now - _geocode_last_call
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)

    try:
        search_addr = address + ', 台灣'
        url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(search_addr)}&format=json&limit=1&countrycodes=tw"
        req = Request(url, headers={'User-Agent': 'LiangFuEstate/2.0'})
        _geocode_last_call = time.time()

        with urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                _geocode_cache[address] = (lat, lng)
                return (lat, lng)
    except Exception as e:
        print(f"Nominatim error for '{address}': {e}")

    _geocode_cache[address] = None
    return None


# ============================================================
# 交易紀錄格式化
# ============================================================

def make_tx_record(row):
    """將原始 CSV row 轉換為前端友好的交易紀錄"""
    price = 0
    try:
        price = float(row.get('總價元', 0) or 0)
    except (ValueError, TypeError):
        pass

    area_sqm = 0
    try:
        area_sqm = float(row.get('建物移轉總面積平方公尺', 0) or 0)
    except (ValueError, TypeError):
        pass

    unit_price_sqm = 0
    try:
        unit_price_sqm = float(row.get('單價元平方公尺', 0) or 0)
    except (ValueError, TypeError):
        pass

    area_ping = area_sqm / PING_TO_SQM if area_sqm else 0
    unit_price_ping = unit_price_sqm * PING_TO_SQM if unit_price_sqm else 0

    main_area = 0
    try:
        main_area = float(row.get('主建物面積', 0) or 0)
    except (ValueError, TypeError):
        pass

    ratio = 0
    if area_sqm > 0 and main_area > 0:
        ratio = round(((area_sqm - main_area) / area_sqm) * 100, 1)

    rooms = str(row.get('建物現況格局-房', '0') or '0')
    halls = str(row.get('建物現況格局-廳', '0') or '0')
    baths = str(row.get('建物現況格局-衛', '0') or '0')

    date_raw = str(row.get('交易年月日', '') or '')
    date_formatted = format_roc_date(date_raw)

    return {
        'price': price,
        'date': date_formatted or date_raw,
        'date_raw': date_raw,
        'floor': convert_floor_str(str(row.get('移轉層次', '') or '')),
        'total_floor': convert_floor_str(str(row.get('總樓層數', '') or '')),
        'address': fullwidth_to_halfwidth(str(row.get('土地位置建物門牌', '') or '')),
        'district': str(row.get('鄉鎮市區', '') or ''),
        'area_sqm': round(area_sqm, 2),
        'area_ping': round(area_ping, 2),
        'unit_price_sqm': round(unit_price_sqm, 2),
        'unit_price_ping': round(unit_price_ping, 2),
        'rooms': rooms,
        'halls': halls,
        'baths': baths,
        'building_type': str(row.get('建物型態', '') or ''),
        'main_use': str(row.get('主要用途', '') or ''),
        'main_material': str(row.get('主要建材', '') or ''),
        'complete_date': str(row.get('建築完成年月', '') or ''),
        'has_elevator': str(row.get('電梯', '') or ''),
        'has_management': str(row.get('有無管理組織', '') or ''),
        'parking_type': str(row.get('車位類別', '') or ''),
        'parking_price': float(row.get('車位總價元', 0) or 0),
        'ratio': ratio,
        'note': str(row.get('備註', '') or ''),
        # 向後相容
        '總價元': price,
        '交易年月日': date_raw,
        '移轉層次': str(row.get('移轉層次', '') or ''),
        '總樓層數': str(row.get('總樓層數', '') or ''),
        '建物移轉總面積平方公尺': area_sqm,
        '單價元平方公尺': unit_price_sqm,
        '建物現況格局-房': rooms,
        '建物現況格局-廳': halls,
        '建物現況格局-衛': baths,
        '土地位置建物門牌': str(row.get('土地位置建物門牌', '') or ''),
    }


def compute_summary(transactions):
    """計算交易紀錄的摘要統計"""
    if not transactions:
        return {}
    prices = [t['price'] for t in transactions if t.get('price', 0) > 0]
    areas = [t['area_ping'] for t in transactions if t.get('area_ping', 0) > 0]
    unit_prices = [t['unit_price_ping'] for t in transactions if t.get('unit_price_ping', 0) > 0]

    return {
        'total_transactions': len(transactions),
        'avg_price': round(sum(prices) / len(prices), 0) if prices else 0,
        'min_price': min(prices) if prices else 0,
        'max_price': max(prices) if prices else 0,
        'avg_area_ping': round(sum(areas) / len(areas), 2) if areas else 0,
        'avg_unit_price_ping': round(sum(unit_prices) / len(unit_prices), 2) if unit_prices else 0,
        'latest_date': max((t.get('date_raw', '') for t in transactions), default=''),
        'oldest_date': min((t.get('date_raw', '') for t in transactions if t.get('date_raw')), default=''),
    }


# ============================================================
# 搜尋建案 (在記憶體中)
# ============================================================

def search_building_projects(keyword='', district='', limit=200):
    """在 BUILDING_PROJECTS 中搜尋"""
    if not BUILDING_PROJECTS_READY:
        return []

    results = []
    half_kw, full_kw = normalize_search_text(keyword) if keyword else ('', '')

    for pid, proj in BUILDING_PROJECTS.items():
        if keyword:
            name = proj.get('name', '')
            addr = proj.get('address', '')
            dist = proj.get('district', '')
            searchable = f"{name} {addr} {dist}"
            # 台/臺同義轉換
            kw_variants = [keyword, half_kw, full_kw]
            if '台' in keyword:
                kw_variants.append(keyword.replace('台', '臺'))
            elif '臺' in keyword:
                kw_variants.append(keyword.replace('臺', '台'))
            match = any(kw and kw in searchable for kw in kw_variants)
            if not match:
                continue

        if district and district not in proj.get('district', ''):
            continue

        results.append(proj)

    results.sort(key=lambda x: x.get('transaction_count', 0), reverse=True)
    return results[:limit]


def search_addresses_from_csv(keyword, limit=500):
    """從 CSV 直接搜尋地址，回傳「虛擬建案」格式（按地址聚合）。
    支援中文數字、全形、半形等多種輸入格式
    例如：「日興一街六號七樓」搜尋時會轉成「日興一街６號」，忽略樓層信息
    """
    try:
        con = duckdb.connect()
        
        # 轉換策略：只轉號後的數字，用於部分匹配
        def convert_to_search_keywords(text):
            """生成多個搜尋變體"""
            # 將「六號」轉成「６號」，但不轉「七樓」
            cn_to_full = {
                '零': '０', '一': '１', '二': '２', '三': '３', '四': '４',
                '五': '５', '六': '６', '七': '７', '八': '８', '九': '９', '十': '１０'
            }
            keywords = [text]  # 原始搜尋
            
            # 轉換「號」後的數字
            import re
            converted = re.sub(r'([零一二三四五六七八九十])號',
                             lambda m: cn_to_full.get(m.group(1), m.group(1)) + '號',
                             text)
            if converted != text:
                keywords.append(converted)
            
            # 另外，嘗試去掉「樓」或「層」後的部分（只搜地址號碼部分）
            # 例如：「日興一街六號七樓」 -> 搜 「日興一街６號」
            truncated = re.sub(r'[樓層弄巷].*$', '', converted)
            if truncated and truncated != text:
                keywords.append(truncated)
            
            return keywords
        
        search_keywords = convert_to_search_keywords(keyword)

        query = f"""
        SELECT
            土地位置建物門牌 AS address,
            鄉鎮市區        AS district,
            COUNT(*)        AS tx_count,
            AVG(TRY_CAST(總價元 AS DOUBLE))          AS avg_price,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) AS avg_area_sqm,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE))  AS avg_unit_sqm,
            MAX(建物型態)   AS building_type,
            MAX(總樓層數)   AS max_floor,
            MAX(交易年月日) AS latest_date
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL
          AND 土地位置建物門牌 != ''
          AND (土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ?)
          AND TRY_CAST(總價元 AS DOUBLE) > 0
        GROUP BY 土地位置建物門牌, 鄉鎮市區
        ORDER BY 
            -- 優先顯示「含號碼」的地址（精確匹配度高）
            (LENGTH(土地位置建物門牌) > LENGTH(?)) DESC,
            -- 其次按交易筆數排序
            tx_count DESC,
            -- 最後按最新交易日期排序
            latest_date DESC
        LIMIT {limit}
        """
        # 用前兩個關鍵字搜尋：原始 + 轉換版本
        params = [
            f'%{search_keywords[0]}%', 
            f'%{search_keywords[1]}%' if len(search_keywords) > 1 else f'%{search_keywords[0]}%',
            search_keywords[0]  # ORDER BY 參數：用於長度比較
        ]
        df = con.execute(query, params).fetchdf()
        con.close()

        results = []
        for _, row in df.iterrows():
            addr = str(row['address'])
            district = str(row['district'])
            avg_price = float(row['avg_price'] or 0)
            avg_area_sqm = float(row['avg_area_sqm'] or 0)
            avg_area_ping = avg_area_sqm / PING_TO_SQM if avg_area_sqm else 0
            avg_unit_sqm = float(row['avg_unit_sqm'] or 0)
            avg_unit_ping = avg_unit_sqm * PING_TO_SQM if avg_unit_sqm else 0
            tx_count = int(row['tx_count'])

            pid = 'addr_' + hashlib.md5(addr.encode()).hexdigest()[:12]
            lat, lng = get_coordinates_for_address(addr, district)

            results.append({
                'id': pid,
                'name': addr,           # 以地址作為名稱
                'address': addr,
                'district': district,
                'type': str(row['building_type'] or ''),
                'transaction_count': tx_count,
                'avg_price': round(avg_price, 0),
                'avg_unit_price': round(avg_unit_ping, 2),
                'avg_ping': round(avg_area_ping, 2),
                'avg_area_sqm': round(avg_area_sqm, 2),
                'max_floor': str(row['max_floor'] or ''),
                'year_range': '',
                'source': 'address',        # 標記來源為地址搜尋
                'is_address_result': True,
                'lat': lat,
                'lng': lng,
            })
        return results
    except Exception as e:
        print(f"search_addresses_from_csv error: {e}")
        return []


# ============================================================
# 查詢交易紀錄 (from CSV via DuckDB)
# ============================================================

def query_transactions_by_address(address, limit=100):
    """根據地址精確查詢交易紀錄"""
    try:
        con = duckdb.connect()
        query = f"""
        SELECT *
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 = ?
        ORDER BY 交易年月日 DESC
        LIMIT {limit}
        """
        result = con.execute(query, [address]).fetchdf()
        con.close()
        return [make_tx_record(row.to_dict()) for _, row in result.iterrows()]
    except Exception as e:
        print(f"query_transactions_by_address error: {e}")
        return []


def query_transactions_by_keyword(keyword, district=None, limit=200):
    """根據關鍵字模糊查詢交易紀錄"""
    try:
        con = duckdb.connect()
        half_kw, full_kw = normalize_search_text(keyword)

        query = f"""
        SELECT *
        FROM read_csv_auto('{CSV_PATH}')
        WHERE (土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ?)
        """
        params = [f'%{keyword}%', f'%{half_kw}%', f'%{full_kw}%']

        if district:
            query += " AND 鄉鎮市區 = ?"
            params.append(district)

        query += f"""
        ORDER BY 交易年月日 DESC
        LIMIT {limit}
        """
        result = con.execute(query, params).fetchdf()
        con.close()
        return [make_tx_record(row.to_dict()) for _, row in result.iterrows()]
    except Exception as e:
        print(f"query_transactions_by_keyword error: {e}")
        return []


# ============================================================
# Flask 路由
# ============================================================

@app.route('/')
def index():
    """主頁面"""
    html_path = os.path.join(BASE_DIR, 'liangfu_map.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        return f.read()


@app.route('/api/projects', methods=['GET'])
def api_projects():
    """取得建案列表

    參數:
    - keyword / search: 搜尋關鍵字（建案名、地址、區域）
    - district: 指定鄉鎮市區
    - limit: 回傳上限（預設 200）
    - sort_by: 排序欄位
    - sort_order: asc / desc

    進階篩選:
    - min_price / max_price, min_unit_price / max_unit_price
    - min_ping / max_ping, min_year / max_year
    - min_ratio / max_ratio, building_type, room_count
    """
    keyword = request.args.get('keyword', '').strip() or request.args.get('search', '').strip()
    district = request.args.get('district', '').strip()
    limit = int(request.args.get('limit', 500))  # 增加預設 limit 以包含更多地址搜尋結果
    sort_by = request.args.get('sort_by', 'transaction_count').strip()
    sort_order = request.args.get('sort_order', 'desc').strip().lower()

    # 進階篩選參數
    min_price = request.args.get('min_price', '').strip()
    max_price = request.args.get('max_price', '').strip()
    min_unit_price = request.args.get('min_unit_price', '').strip()
    max_unit_price = request.args.get('max_unit_price', '').strip()
    min_ping = request.args.get('min_ping', '').strip()
    max_ping = request.args.get('max_ping', '').strip()
    min_year = request.args.get('min_year', '').strip()
    max_year = request.args.get('max_year', '').strip()
    min_ratio = request.args.get('min_ratio', '').strip()
    max_ratio = request.args.get('max_ratio', '').strip()
    building_type = request.args.get('building_type', '').strip()
    room_count = request.args.get('room_count', '').strip()

    has_advanced = any([
        min_price, max_price, min_unit_price, max_unit_price,
        min_ping, max_ping, min_year, max_year,
        min_ratio, max_ratio, building_type, room_count
    ])

    try:
        # ── 快速路徑：從 BUILDING_PROJECTS 記憶體搜尋 ──
        if not has_advanced:
            results = search_building_projects(keyword, district, limit)

            # address2community 擴展搜尋
            if keyword and len(results) < 3:
                community_result = lookup_community(keyword)
                if community_result:
                    community_name = community_result.get('community', '')
                    if community_name:
                        extra = search_building_projects(community_name, district, 50)
                        existing_ids = {r['id'] for r in results}
                        for e in extra:
                            if e['id'] not in existing_ids:
                                results.append(e)

            # ── 地址 fallback：有 keyword 時一律合併 CSV 地址搜尋 ──
            if keyword:
                addr_results = search_addresses_from_csv(keyword, limit=500)  # 返回全部地址結果，避免遺漏
                existing_ids = {r['id'] for r in results}
                for ar in addr_results:
                    if ar['id'] not in existing_ids:
                        results.append(ar)

            # 更新座標
            for proj in results:
                if not proj.get('lat') or not proj.get('lng') or (proj['lat'] == 24.0 and proj['lng'] == 121.0):
                    lat, lng = get_coordinates_for_address(proj.get('address', ''), proj.get('district', ''))
                    proj['lat'] = lat
                    proj['lng'] = lng

            # 排序（優先顯示精確匹配的結果）
            sort_key_map = {
                'transaction_count': 'transaction_count',
                'price': 'avg_price',
                'unit_price': 'avg_unit_price',
                'area': 'avg_ping',
                'date': 'year_range',
            }
            sk = sort_key_map.get(sort_by, 'transaction_count')
            
            # 自定義排序：優先顯示「來自地址搜尋的結果」，然後是建案表
            # 這樣在搜尋「日興一街」時，會優先顯示該路的地址結果
            def sort_key(x):
                is_address = x.get('is_address_result', False)
                sort_value = x.get(sk, 0) or 0
                # 返回 tuple：(是否地址結果降序, 排序值降序)
                return (is_address, sort_value) if sort_order != 'asc' else (not is_address, -sort_value)
            
            results.sort(key=lambda x: (int(x.get('is_address_result', False)), x.get(sk, 0) or 0), 
                        reverse=(sort_order != 'asc'))

            return jsonify({
                'success': True,
                'count': len(results),
                'projects': clean_nan_values(results[:limit])
            })

        # ── 進階篩選：從 CSV 即時查詢 ──
        return _search_projects_advanced(
            keyword=keyword, district=district, limit=limit,
            sort_by=sort_by, sort_order=sort_order,
            min_price=min_price, max_price=max_price,
            min_unit_price=min_unit_price, max_unit_price=max_unit_price,
            min_ping=min_ping, max_ping=max_ping,
            min_year=min_year, max_year=max_year,
            min_ratio=min_ratio, max_ratio=max_ratio,
            building_type=building_type, room_count=room_count,
        )

    except Exception as e:
        print(f"Error in api_projects: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


def _search_projects_advanced(**kwargs):
    """進階篩選 — 從 CSV 即時聚合查詢"""
    keyword = kwargs.get('keyword', '')
    limit = kwargs.get('limit', 200)
    sort_by = kwargs.get('sort_by', 'transaction_count')
    sort_order = kwargs.get('sort_order', 'desc')

    half_kw, full_kw = normalize_search_text(keyword) if keyword else ('', '')

    try:
        con = duckdb.connect()

        base_query = f"""
        SELECT
            土地位置建物門牌,
            鄉鎮市區,
            COUNT(*) as 交易筆數,
            AVG(TRY_CAST(總價元 AS DOUBLE)) as 平均總價,
            MIN(TRY_CAST(總價元 AS DOUBLE)) as 最低價,
            MAX(TRY_CAST(總價元 AS DOUBLE)) as 最高價,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * {PING_TO_SQM}) as 平均單價每坪,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) / {PING_TO_SQM}) as 平均坪數,
            AVG(
                CASE
                    WHEN TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) > 0
                         AND TRY_CAST(主建物面積 AS DOUBLE) IS NOT NULL
                    THEN ((TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) - TRY_CAST(主建物面積 AS DOUBLE))
                          / TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) * 100
                    ELSE NULL
                END
            ) as 平均公設比,
            MAX(交易年月日) as 最新交易日期,
            MIN(交易年月日) as 最舊交易日期,
            MAX(SUBSTR(交易年月日, 1, 3)) as 最新年份,
            MIN(SUBSTR(交易年月日, 1, 3)) as 最舊年份,
            建物型態,
            MAX("建物現況格局-房") as 主要房數
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL
            AND 土地位置建物門牌 != ''
            AND 土地位置建物門牌 != '土地位置建物門牌'
            AND TRY_CAST(總價元 AS DOUBLE) IS NOT NULL
            AND TRY_CAST(總價元 AS DOUBLE) > 0
        """

        conditions = []
        params = []

        if keyword:
            conditions.append("(土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 鄉鎮市區 LIKE ?)")
            params.extend([f'%{keyword}%', f'%{half_kw}%', f'%{full_kw}%', f'%{keyword}%'])

        if kwargs.get('building_type'):
            conditions.append("建物型態 LIKE ?")
            params.append(f'%{kwargs["building_type"]}%')

        if kwargs.get('room_count'):
            conditions.append('"建物現況格局-房" = ?')
            params.append(kwargs['room_count'])

        adv_min_year = kwargs.get('min_year', '')
        adv_max_year = kwargs.get('max_year', '')
        if adv_min_year:
            conditions.append("SUBSTR(交易年月日, 1, 3) >= ?")
            params.append(str(adv_min_year).zfill(3))
        if adv_max_year:
            conditions.append("SUBSTR(交易年月日, 1, 3) <= ?")
            params.append(str(adv_max_year).zfill(3))

        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        base_query += """
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        HAVING COUNT(*) >= 1
        """

        having = []
        adv_min_price = kwargs.get('min_price', '')
        adv_max_price = kwargs.get('max_price', '')
        if adv_min_price:
            having.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) >= {float(adv_min_price)}")
        if adv_max_price:
            having.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) <= {float(adv_max_price)}")

        adv_min_up = kwargs.get('min_unit_price', '')
        adv_max_up = kwargs.get('max_unit_price', '')
        if adv_min_up:
            having.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * {PING_TO_SQM}) >= {float(adv_min_up)}")
        if adv_max_up:
            having.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * {PING_TO_SQM}) <= {float(adv_max_up)}")

        adv_min_ping = kwargs.get('min_ping', '')
        adv_max_ping = kwargs.get('max_ping', '')
        if adv_min_ping:
            having.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) >= {float(adv_min_ping) * PING_TO_SQM}")
        if adv_max_ping:
            having.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) <= {float(adv_max_ping) * PING_TO_SQM}")

        adv_min_ratio = kwargs.get('min_ratio', '')
        adv_max_ratio = kwargs.get('max_ratio', '')
        if adv_min_ratio:
            having.append(f"""AVG(CASE WHEN TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) > 0 AND TRY_CAST(主建物面積 AS DOUBLE) IS NOT NULL
                THEN ((TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) - TRY_CAST(主建物面積 AS DOUBLE)) / TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) * 100 ELSE NULL END) >= {float(adv_min_ratio)}""")
        if adv_max_ratio:
            having.append(f"""AVG(CASE WHEN TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) > 0 AND TRY_CAST(主建物面積 AS DOUBLE) IS NOT NULL
                THEN ((TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) - TRY_CAST(主建物面積 AS DOUBLE)) / TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) * 100 ELSE NULL END) <= {float(adv_max_ratio)}""")

        if having:
            base_query += " AND " + " AND ".join(having)

        sort_map = {
            'date': '最新交易日期', 'price': '平均總價',
            'unit_price': '平均單價每坪', 'area': '平均坪數',
            'ratio': '平均公設比', 'transaction_count': '交易筆數'
        }
        sort_col = sort_map.get(sort_by, '交易筆數')
        sort_dir = 'ASC' if sort_order == 'asc' else 'DESC'

        base_query += f" ORDER BY {sort_col} {sort_dir} LIMIT {limit}"

        result = con.execute(base_query, params).fetchdf()
        con.close()

        projects = []
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            if not is_real_building(address):
                continue
            district_val = row['鄉鎮市區']
            lat, lng = get_coordinates_for_address(address, district_val)

            latest_year_roc = row.get('最新年份', '')
            oldest_year_roc = row.get('最舊年份', '')
            latest_year = int(latest_year_roc) + 1911 if latest_year_roc and str(latest_year_roc).strip().isdigit() else None
            oldest_year = int(oldest_year_roc) + 1911 if oldest_year_roc and str(oldest_year_roc).strip().isdigit() else None

            projects.append({
                'id': make_address_id(address),
                'name': extract_building_project_name(address),
                'address': address,
                'district': district_val,
                'type': row.get('建物型態', '') or '住宅',
                'room_count': int(row['主要房數']) if row.get('主要房數') and str(row['主要房數']).strip().isdigit() else None,
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': round(float(row['平均單價每坪']), 2) if row['平均單價每坪'] else 0,
                'avg_ping': round(float(row['平均坪數']), 2) if row['平均坪數'] else 0,
                'avg_ratio': round(float(row['平均公設比']), 2) if row['平均公設比'] else None,
                'latest_year': latest_year,
                'oldest_year': oldest_year,
                'latest_date': format_roc_date(row.get('最新交易日期', '')),
                'oldest_date': format_roc_date(row.get('最舊交易日期', '')),
                'year_range': f"{oldest_year}-{latest_year}" if oldest_year and latest_year else None,
                'source': 'CSV_advanced',
                'lat': lat,
                'lng': lng,
            })

        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': clean_nan_values(projects)
        })

    except Exception as e:
        print(f"Error in _search_projects_advanced: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/project/<project_id>', methods=['GET'])
def api_project_detail(project_id):
    """取得建案詳細交易紀錄

    參數:
    - address: 地址（用於精確查詢）
    - name: 建案名稱（備用搜尋）
    """
    address = request.args.get('address', '').strip()
    name = request.args.get('name', '').strip()

    try:
        transactions = []
        project_info = None

        # addr_ 開頭：地址搜尋的虛擬建案 → 直接精確查該地址
        if project_id.startswith('addr_'):
            # address 參數就是實際地址（等同 name）
            target_addr = address or name
            if target_addr:
                transactions = query_transactions_by_address(target_addr, limit=500)
                if not transactions:
                    transactions = query_transactions_by_keyword(target_addr, limit=200)
                project_info = {
                    'id': project_id,
                    'name': target_addr,
                    'address': target_addr,
                    'source': 'address',
                }

        # 方案1: 從 BUILDING_PROJECTS 找到建案（addr_ 的已在上面處理）
        if not project_id.startswith('addr_'):
            if not project_info:
                project_info = BUILDING_PROJECTS.get(project_id)
            if project_info and project_info.get('source') != 'address':
                proj_addr = project_info.get('address', '')
                proj_name = project_info.get('name', '')
                proj_district = project_info.get('district', '')

                # 1a: 用代表地址精確查詢
                if proj_addr:
                    transactions = query_transactions_by_address(proj_addr, limit=500)

                # 1b: 用代表地址模糊查詢
                if not transactions and proj_addr:
                    transactions = query_transactions_by_keyword(
                        proj_addr, district=proj_district, limit=500
                    )

                # 1c: 從代表地址提取路段關鍵字搜尋
                if not transactions and proj_addr:
                    # 先去掉「XX市XX區」前綴，再提取第一個路段名
                    cleaned_addr = re.sub(
                        r'^[\u4e00-\u9fff]{2,3}[市縣][\u4e00-\u9fff]{1,3}[區鎮鄉市]', '', proj_addr
                    )
                    road_match = re.match(
                        r'([\u4e00-\u9fff]{2,6}(?:路|街|大道)(?:[\u4e00-\u9fff]{1,2}段)?)',
                        cleaned_addr
                    )
                    if road_match:
                        road_keyword = road_match.group(1)
                        print(f"  1c: 提取路段 '{road_keyword}' from '{proj_addr}'")
                        transactions = query_transactions_by_keyword(
                            road_keyword, district=proj_district, limit=500
                        )
                        print(f"  1c: 找到 {len(transactions)} 筆")

                # 1c2: 提取地段名（如「智興段」）
                if not transactions and proj_addr:
                    cleaned_for_section = re.sub(
                        r'^[\u4e00-\u9fff]{2,3}[市縣][\u4e00-\u9fff]{1,3}[區鎮鄉市]', '', proj_addr
                    )
                    section_match = re.search(r'([\u4e00-\u9fff]{2,6}段)', cleaned_for_section)
                    if section_match:
                        section_keyword = section_match.group(1)
                        # 排除路段名（如「五段」「三段」）
                        if not re.match(r'^[一二三四五六七八九十]+段$', section_keyword):
                            print(f"  1c2: 提取地段 '{section_keyword}' from '{proj_addr}'")
                            transactions = query_transactions_by_keyword(
                                section_keyword, district=proj_district, limit=500
                            )
                            print(f"  1c2: 找到 {len(transactions)} 筆")

                # 1d: 用建案名搜尋
                if not transactions and proj_name:
                    transactions = query_transactions_by_keyword(
                        proj_name, district=proj_district, limit=500
                    )

        # 方案2: 用傳入的 address 精確查
        if not transactions and address:
            transactions = query_transactions_by_address(address, limit=500)

        # 方案3: 用 address 模糊查
        if not transactions and address:
            transactions = query_transactions_by_keyword(address, limit=200)

        # 方案4: 用 name 模糊查
        if not transactions and name:
            transactions = query_transactions_by_keyword(name, limit=200)

        # 方案5: address2community 反查
        if not transactions and address:
            community = lookup_community(address)
            if community and community.get('community'):
                transactions = query_transactions_by_keyword(
                    community['community'], limit=200
                )

        summary = compute_summary(transactions)

        # 如果有 Building_Projects_B 的建案資料，用它的統計數據覆蓋 summary
        # 因為 B 表統計是精確的，而交易搜尋可能包含整條路的數據
        if project_info and project_info.get('source') == 'B':
            b_summary = {
                'avg_price': project_info.get('avg_price', 0),
                'avg_area_ping': project_info.get('avg_ping', 0),
                'avg_unit_price_ping': project_info.get('avg_unit_price', 0),
                'total_transactions': project_info.get('transaction_count', 0),
                'building_type': project_info.get('type', ''),
                'max_floor': project_info.get('max_floor', ''),
                'year_range': project_info.get('year_range', ''),
            }
            # 保留從交易紀錄計算出的時間範圍（如果有）
            if summary.get('latest_date'):
                b_summary['latest_date'] = summary['latest_date']
                b_summary['oldest_date'] = summary['oldest_date']
            if summary.get('min_price'):
                b_summary['min_price'] = summary['min_price']
                b_summary['max_price'] = summary['max_price']
            summary = b_summary

        # 標記交易紀錄的搜尋方式
        search_note = ''
        if project_info and project_info.get('source') == 'B' and transactions:
            # 如果地址像是路口描述或地段描述，標記為模糊搜尋
            addr = project_info.get('address', '')
            if re.search(r'[和與]|路口|之旁|對面|附近|段\d+地', addr):
                search_note = '交易紀錄為該路段/地段近期交易，可能包含鄰近建案'

        result = {
            'success': True,
            'project': project_info or {
                'id': project_id,
                'address': address,
                'name': name or extract_building_project_name(address or ''),
            },
            'transactions': clean_nan_values(transactions),
            'summary': summary,
            'count': len(transactions),
            'note': search_note,
        }
        return jsonify(clean_nan_values(result))

    except Exception as e:
        print(f"Error in api_project_detail: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/search', methods=['GET'])
def api_search():
    """搜尋建案（/api/projects 的別名）"""
    return api_projects()


@app.route('/api/address2community', methods=['GET'])
def api_address2community():
    """地址→社區名稱查詢

    參數:
    - address: 要查詢的地址
    - district: 鄉鎮市區（可選）
    """
    address = request.args.get('address', '').strip()
    district = request.args.get('district', '').strip()

    if not address:
        return jsonify({'success': False, 'error': '缺少 address 參數'}), 400

    result = lookup_community(address, district or None)
    if result:
        return jsonify({
            'success': True,
            'address': address,
            'best': result.get('community', ''),
            'district': result.get('district', ''),
            'confidence': result.get('confidence', 0),
            'match_level': result.get('match_level', ''),
            'tx_count': result.get('tx_count', 0),
            'source': result.get('source', ''),
            'all_names': result.get('all_names', ''),
        })
    else:
        return jsonify({
            'success': False,
            'address': address,
            'message': '找不到對應社區',
        })


@app.route('/api/community2address', methods=['GET'])
def api_community2address():
    """社區名稱→地址查詢"""
    community = request.args.get('community', '').strip()
    district = request.args.get('district', '').strip()

    if not community:
        return jsonify({'success': False, 'error': '缺少 community 參數'}), 400

    # 在 BUILDING_PROJECTS 中搜尋
    matches = []
    for pid, proj in BUILDING_PROJECTS.items():
        if community in proj.get('name', ''):
            if district and district not in proj.get('district', ''):
                continue
            matches.append(proj)

    # 在 address2community 表中反查
    addr_matches = []
    if ADDR2COM_READY and ADDR2COM_DATA:
        for level_name in ['normalized', 'to_number', 'to_alley']:
            level = ADDR2COM_DATA.get(level_name, {})
            for addr_key, entry in level.items():
                if entry.get('community') == community:
                    if district and entry.get('district') != district:
                        continue
                    addr_matches.append({
                        'address': addr_key,
                        'district': entry.get('district', ''),
                        'tx_count': entry.get('tx_count', 0),
                    })

    return jsonify({
        'success': True,
        'community': community,
        'building_projects': clean_nan_values(matches[:20]),
        'addresses': addr_matches[:50],
    })


@app.route('/api/building_projects', methods=['GET'])
def api_building_projects():
    """取得建案名稱表（向後相容）"""
    keyword = request.args.get('keyword', '').strip()
    district = request.args.get('district', '').strip()
    limit = int(request.args.get('limit', 100))

    if not BUILDING_PROJECTS_READY:
        return jsonify({'success': False, 'error': '建案資料尚未載入完成'}), 503

    results = search_building_projects(keyword, district, limit)

    return jsonify({
        'success': True,
        'count': len(results),
        'projects': clean_nan_values(results)
    })


@app.route('/api/stats', methods=['GET'])
def api_stats():
    """取得系統統計資訊"""
    return jsonify({
        'success': True,
        'building_projects_count': len(BUILDING_PROJECTS),
        'building_projects_ready': BUILDING_PROJECTS_READY,
        'addr2com_ready': ADDR2COM_READY,
        'addr2com_normalized': len(ADDR2COM_DATA.get('normalized', {})) if ADDR2COM_DATA else 0,
        'addr2com_to_number': len(ADDR2COM_DATA.get('to_number', {})) if ADDR2COM_DATA else 0,
        'addr2com_to_alley': len(ADDR2COM_DATA.get('to_alley', {})) if ADDR2COM_DATA else 0,
        'addr2com_road': len(ADDR2COM_DATA.get('road', {})) if ADDR2COM_DATA else 0,
        'address_coordinates_count': len(_address_coordinates_db),
    })


@app.route('/api/geocode', methods=['GET'])
def api_geocode():
    """Nominatim 免費 geocoding"""
    address = request.args.get('address', '').strip()
    if not address:
        return jsonify({'success': False, 'error': '缺少 address 參數'}), 400

    if address in _address_coordinates_db:
        lat, lng = _address_coordinates_db[address]
        return jsonify({'success': True, 'lat': lat, 'lng': lng, 'source': 'local_db'})

    result = nominatim_geocode(address)
    if result:
        return jsonify({'success': True, 'lat': result[0], 'lng': result[1], 'source': 'nominatim'})
    else:
        return jsonify({'success': False, 'error': f'無法找到地址: {address}'}), 404


@app.route('/api/districts', methods=['GET'])
def api_districts():
    """取得所有可用的鄉鎮市區列表"""
    districts = set()
    for proj in BUILDING_PROJECTS.values():
        d = proj.get('district', '')
        if d:
            districts.add(d)

    return jsonify({
        'success': True,
        'districts': sorted(list(districts)),
        'count': len(districts),
    })


# ============================================================
# 啟動
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("🏢 良富居地產 - 專業房地產地圖系統 v2.0")
    print("=" * 60)
    print(f"📁 交易紀錄: {CSV_PATH}")
    print(f"📁 建案資料: {BUILDING_B_PATH}")
    print(f"📁 社區對照: {ADDR2COM_PATH}")
    print(f"🗺️  地圖引擎: Leaflet.js + OpenStreetMap（免費）")
    print(f"🌍 Geocoding: 本地座標庫 + Nominatim（免費）")
    print(f"🖥️  服務器啟動於: http://localhost:5000")
    print("=" * 60)

    # 背景初始化
    import threading
    def init_all():
        # 先載入建案和 addr2com（快），再建構座標庫（慢）
        init_building_projects()
        init_addr2com()
        build_address_coordinates_db()
        # 座標庫建好後更新建案座標
        for pid, proj in BUILDING_PROJECTS.items():
            if not proj.get('lat') or (proj['lat'] == 24.0 and proj['lng'] == 121.0):
                lat, lng = get_coordinates_for_address(proj.get('address', ''), proj.get('district', ''))
                proj['lat'] = lat
                proj['lng'] = lng

    t = threading.Thread(target=init_all, daemon=True)
    t.start()

    app.run(debug=True, host='0.0.0.0', port=5000)
