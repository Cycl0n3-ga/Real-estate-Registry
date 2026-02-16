#!/usr/bin/env python3
"""
良富居地產 - 專業房地產地圖系統
整合建案地圖、價格查詢、銷控面板
使用 Leaflet.js + OpenStreetMap（完全免費，不需要 API Key）
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
import os
import re
import random
import json
import math
import time
import hashlib
from collections import defaultdict
from urllib.parse import quote_plus
from urllib.request import urlopen, Request

app = Flask(__name__)
CORS(app)

# CSV 文件路徑
CSV_PATH = '/home/cyclone/land/ALL_lvr_land_a.csv'

# ============================================================
# 建案名稱表（從地址聚合而來）
# ============================================================
BUILDING_PROJECTS = {}
BUILDING_PROJECTS_READY = False

# 台灣主要鄉鎮市區的座標映射 (緯度, 經度)
DISTRICT_COORDINATES = {
    '中壢區': (24.9696, 120.9843),
    '桃園區': (25.0330, 121.3167),
    '新竹市': (24.8026, 120.9693),
    '北屯區': (24.2169, 120.7901),
    '淡水區': (25.1654, 121.4529),
    '板橋區': (25.0121, 121.4627),
    '西屯區': (24.1884, 120.6350),
    '新莊區': (25.0568, 121.4315),
    '竹北市': (24.8363, 120.9863),
    '中和區': (25.0049, 121.4935),
    '台中市': (24.1477, 120.6736),
    '新竹縣': (24.9474, 121.0119),
    '北投區': (25.1370, 121.5130),
    '苗栗市': (24.5595, 120.8196),
    '台南市': (22.9973, 120.2171),
    '高雄市': (22.6172, 120.3014),
    '中山區': (25.0455, 121.5149),
    '大安區': (25.0330, 121.5254),
    '松山區': (25.0487, 121.5623),
    '南港區': (25.0543, 121.6090),
    '信義區': (25.0330, 121.5654),
    '內湖區': (25.0850, 121.5788),
    '士林區': (25.1122, 121.5254),
    '大同區': (25.0737, 121.5149),
    '文山區': (25.0035, 121.5674),
    '南屯區': (24.1003, 120.6684),
    '烏日區': (24.0630, 120.6717),
    '龍井區': (24.2507, 120.5690),
    '霧峰區': (24.0580, 120.8225),
    '東勢區': (24.2569, 120.7920),
    '太平區': (24.1456, 120.9383),
    '石岡區': (24.2169, 120.7901),
    '后里區': (24.3185, 120.7436),
    '潭子區': (24.1995, 120.8610),
    '大雅區': (24.2575, 120.7870),
    '神岡區': (24.2456, 120.8080),
    '清水區': (24.2583, 120.5689),
    '梧棲區': (24.2495, 120.5439),
    '大肚區': (24.2250, 120.5519),
    '沙鹿區': (24.2330, 120.5699),
    '鹿港鎮': (24.0585, 120.4325),
    '花壇鄉': (24.0937, 120.5146),
    '芬園鄉': (24.0880, 120.5738),
    '彰化縣': (24.0827, 120.4167),
    '竹山鎮': (23.7599, 120.6861),
    '南投縣': (23.9120, 120.6672),
    '埔里鎮': (23.9610, 120.9660),
    '魚池鄉': (23.8827, 120.9071),
    '基隆市': (25.1276, 121.7347),
    '宜蘭縣': (24.7599, 121.7497),
    '花蓮縣': (24.0046, 121.5743),
    '台東縣': (22.7696, 121.1446),
    '澎湖縣': (23.5731, 119.5922),
    '金門縣': (24.4353, 118.3157),
    '連江縣': (26.1583, 119.9583),
    '屏東縣': (22.5442, 120.4886),
    '雲林縣': (23.7071, 120.4334),
    '嘉義市': (23.4788, 120.4432),
    '嘉義縣': (23.4534, 120.6081),
    '白河區': (22.9153, 120.3789),
    '將軍區': (23.1648, 120.2226),
    '七股區': (23.1527, 120.1363),
    '學甲區': (23.2315, 120.2693),
    '北門區': (23.2728, 120.1704),
    '新營區': (23.3032, 120.3031),
    '永康區': (22.9896, 120.2440),
    '仁德區': (22.9385, 120.2545),
    '左鎮區': (22.8146, 120.3696),
    '歸仁區': (22.9049, 120.3027),
    '關廟區': (22.8921, 120.3196),
    '東山區': (23.0000, 120.4500),
    '下營區': (23.1329, 120.3107),
    '六甲區': (23.2074, 120.4006),
    '官田區': (23.1933, 120.4319),
    '大內區': (23.1167, 120.4667),
    '山上區': (23.1424, 120.4619),
    '麻豆區': (23.1793, 120.2411),
    '佳里區': (23.1602, 120.1808),
    '西港區': (23.1417, 120.1865),
    '後壁區': (23.3452, 120.4089),
    '柳營區': (23.2839, 120.3730),
    '鹽水區': (23.2832, 120.2788),
    '玉井區': (23.0777, 120.5452),
    '南化區': (22.9005, 120.4833),
    '楠西區': (23.0238, 120.5567),
    '新北市': (25.0170, 121.4627),
    '三重區': (25.0617, 121.4879),
    '蘆洲區': (25.0855, 121.4738),
    '汐止區': (25.0626, 121.6610),
    '永和區': (25.0076, 121.5138),
    '三峽區': (24.9340, 121.3687),
    '土城區': (24.9723, 121.4437),
    '鶯歌區': (24.9519, 121.3517),
    '泰山區': (25.0500, 121.4300),
    '林口區': (25.0786, 121.3919),
    '五股區': (25.0787, 121.4380),
    '八里區': (25.1400, 121.4000),
    '樹林區': (24.9909, 121.4200),
    '深坑區': (25.0020, 121.6155),
    '石碇區': (24.9915, 121.5910),
    '平溪區': (25.0262, 121.7387),
    '雙溪區': (24.9940, 121.8260),
    '貢寮區': (25.0223, 121.9063),
    '瑞芳區': (25.1092, 121.8100),
    '萬里區': (25.1792, 121.6891),
    '金山區': (25.2220, 121.6370),
    '左營區': (22.6847, 120.2940),
    '前鎮區': (22.5955, 120.3268),
    '三民區': (22.6467, 120.3165),
    '鼓山區': (22.6555, 120.2710),
    '苓雅區': (22.6200, 120.3260),
    '楠梓區': (22.7308, 120.3262),
    '小港區': (22.5647, 120.3456),
    '鳳山區': (22.6268, 120.3595),
    '大寮區': (22.5965, 120.3987),
    '鳥松區': (22.6620, 120.3647),
    '仁武區': (22.7002, 120.3520),
    '岡山區': (22.7906, 120.2953),
    '路竹區': (22.8561, 120.2617),
    '橋頭區': (22.7575, 120.3058),
    '梓官區': (22.7581, 120.2637),
    '旗山區': (22.8861, 120.4839),
    '美濃區': (22.8982, 120.5421),
    '大樹區': (22.7240, 120.4300),
    '林園區': (22.5100, 120.3927),
    '前金區': (22.6266, 120.2952),
    '新興區': (22.6296, 120.3090),
    '鹽埕區': (22.6230, 120.2836),
    '旗津區': (22.5898, 120.2653),
    '龍潭區': (24.8642, 121.2163),
    '楊梅區': (24.9077, 121.1449),
    '大溪區': (24.8832, 121.2863),
    '蘆竹區': (25.0439, 121.2917),
    '大園區': (25.0647, 121.2333),
    '龜山區': (25.0287, 121.3453),
    '八德區': (24.9456, 121.2900),
    '平鎮區': (24.9459, 121.2182),
    '觀音區': (25.0349, 121.1417),
    '新屋區': (24.9736, 121.1067),
    '復興區': (24.8200, 121.3500),
    '竹東鎮': (24.7310, 121.0900),
    '新豐鄉': (24.8900, 120.9700),
    '湖口鄉': (24.9023, 121.0400),
    '關西鎮': (24.7890, 121.1770),
    '新埔鎮': (24.8270, 121.0733),
    '寶山鄉': (24.7600, 120.9800),
    '芎林鄉': (24.7770, 121.0700),
    '峨眉鄉': (24.6880, 120.9930),
    '北埔鄉': (24.6996, 121.0530),
    '橫山鄉': (24.7200, 121.1130),
    '尖石鄉': (24.7050, 121.2000),
    '五峰鄉': (24.6000, 121.1000),
    '安南區': (23.0468, 120.1853),
    '安平區': (22.9927, 120.1659),
    '東區': (22.9798, 120.2252),
    '北區': (23.0030, 120.2080),
    '南區': (22.9600, 120.1980),
    '中西區': (22.9920, 120.2000),
    '善化區': (23.1310, 120.2978),
    '新化區': (23.0383, 120.3119),
    '安定區': (23.0880, 120.2267),
    '彰化市': (24.0827, 120.5417),
    '員林市': (23.9590, 120.5740),
    '和美鎮': (24.1125, 120.4990),
    '北斗鎮': (23.8692, 120.5200),
    '溪湖鎮': (23.9630, 120.4810),
    '田中鎮': (23.8570, 120.5810),
    '二林鎮': (23.8990, 120.3730),
    '線西鄉': (24.1317, 120.4680),
    '伸港鄉': (24.1560, 120.4840),
    '福興鄉': (24.0470, 120.4410),
    '秀水鄉': (24.0350, 120.5010),
    '埔心鄉': (23.9520, 120.5430),
    '永靖鄉': (23.9240, 120.5490),
    '社頭鄉': (23.8960, 120.5870),
    '大村鄉': (23.9970, 120.5570),
    '南投市': (23.9120, 120.6672),
    '草屯鎮': (23.9740, 120.6800),
    '名間鄉': (23.8380, 120.6580),
    '集集鎮': (23.8290, 120.6870),
    '水里鄉': (23.8120, 120.8530),
    '鹿谷鄉': (23.7510, 120.7530),
    '信義鄉': (23.7000, 120.8800),
    '國姓鄉': (24.0410, 120.8580),
    '中寮鄉': (23.8790, 120.7670),
    '仁愛鄉': (24.0240, 121.1330),
}

# Geocoding 快取
_geocode_cache = {}
_geocode_last_call = 0


def get_connection():
    """建立 DuckDB 連接"""
    return duckdb.connect()


def clean_nan_values(obj):
    """遞歸清理字典/列表中的 NaN 值"""
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj):
            return 0
        return obj
    return obj


def get_district_coordinates(district):
    """取得鄉鎮市區的座標"""
    if district in DISTRICT_COORDINATES:
        return DISTRICT_COORDINATES[district]
    district_clean = district.replace('市', '').replace('縣', '').replace('區', '')
    for key in DISTRICT_COORDINATES:
        if district_clean in key or key in district:
            return DISTRICT_COORDINATES[key]
    return (24.0, 121.0)


def nominatim_geocode(address):
    """使用 Nominatim（OpenStreetMap 免費 geocoding）"""
    global _geocode_last_call

    if address in _geocode_cache:
        return _geocode_cache[address]

    now = time.time()
    elapsed = now - _geocode_last_call
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)

    try:
        search_addr = address + ', 台灣'
        url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(search_addr)}&format=json&limit=1&countrycodes=tw"
        req = Request(url, headers={'User-Agent': 'LiangFuEstate/1.0'})
        _geocode_last_call = time.time()

        with urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                _geocode_cache[address] = (lat, lng)
                return (lat, lng)
    except Exception as e:
        print(f"Nominatim geocode error for '{address}': {e}")

    _geocode_cache[address] = None
    return None


def normalize_search_text(text):
    """正規化搜尋文字 - 生成半形和全形兩個版本"""
    if not text:
        return (text, text)
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
    half_width = ''.join(full_to_half.get(c, c) for c in text)
    full_width = ''.join(half_to_full.get(c, c) if c not in full_to_half else c for c in text)
    return (half_width, full_width)


def is_real_building(address):
    """判斷是否為真實建案（非純地號）"""
    if not address or len(address) < 5:
        return False
    if '地號' in address and '號' not in address:
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


def init_building_projects():
    """初始化建案名稱表 — 從 CSV 聚合同地址資料"""
    global BUILDING_PROJECTS, BUILDING_PROJECTS_READY
    print("🏗️  初始化建案名稱表...")
    t0 = time.time()

    try:
        con = get_connection()
        query = f"""
        SELECT
            土地位置建物門牌,
            鄉鎮市區,
            建物型態,
            COUNT(*) as cnt,
            AVG(TRY_CAST(總價元 AS DOUBLE)) as avg_price,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) as avg_unit_price_ping,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) / 3.30579) as avg_ping,
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
        LIMIT 5000
        """
        result = con.execute(query).fetchdf()
        con.close()

        projects = {}
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            if not is_real_building(address):
                continue

            pid = hashlib.md5(address.encode()).hexdigest()[:12]
            name = extract_building_project_name(address)
            district = row['鄉鎮市區']

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
                'total_floors': row['total_floors'],
            }

        BUILDING_PROJECTS = projects
        BUILDING_PROJECTS_READY = True
        elapsed = time.time() - t0
        print(f"✅ 建案名稱表初始化完成: {len(projects)} 個建案, 耗時 {elapsed:.1f}s")

    except Exception as e:
        print(f"❌ 建案名稱表初始化失敗: {e}")
        BUILDING_PROJECTS_READY = True


# ============================================================
# Flask 路由
# ============================================================

@app.route('/')
def index():
    """主頁面 — 不再需要 API key"""
    with open('liangfu_map.html', 'r', encoding='utf-8') as f:
        return f.read()


@app.route('/api/projects', methods=['GET'])
def get_projects():
    """獲取建案列表（聚合數據）"""
    try:
        con = get_connection()

        query = f"""
        SELECT
            土地位置建物門牌,
            鄉鎮市區,
            COUNT(*) as 交易筆數,
            AVG(TRY_CAST(總價元 AS DOUBLE)) as 平均總價,
            MIN(TRY_CAST(總價元 AS DOUBLE)) as 最低價,
            MAX(TRY_CAST(總價元 AS DOUBLE)) as 最高價,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) as 平均單價每坪,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) / 3.30579) as 平均坪數,
            MAX(交易年月日) as 最新交易日期,
            建物型態
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL
            AND 土地位置建物門牌 != ''
            AND 土地位置建物門牌 != '土地位置建物門牌'
            AND TRY_CAST(總價元 AS DOUBLE) IS NOT NULL
            AND TRY_CAST(總價元 AS DOUBLE) > 0
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        HAVING COUNT(*) >= 1
        ORDER BY 交易筆數 DESC
        LIMIT 200
        """

        result = con.execute(query).fetchdf()

        projects = []
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            if not is_real_building(address):
                continue

            building_name = extract_building_project_name(address)
            district = row['鄉鎮市區']
            lat, lng = get_district_coordinates(district)
            lat += random.uniform(-0.01, 0.01)
            lng += random.uniform(-0.01, 0.01)

            project = {
                'id': hashlib.md5(address.encode()).hexdigest()[:12],
                'name': building_name,
                'address': address,
                'district': district,
                'type': row['建物型態'] or '住宅',
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': round(float(row['平均單價每坪']), 2) if row['平均單價每坪'] else 0,
                'avg_ping': round(float(row['平均坪數']), 2) if row['平均坪數'] else 0,
                'latest_date': row['最新交易日期'],
                'lat': lat,
                'lng': lng
            }
            projects.append(project)

        con.close()
        projects = clean_nan_values(projects)

        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })

    except Exception as e:
        print(f"Error in get_projects: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/project/<project_id>', methods=['GET'])
def get_project_detail(project_id):
    """獲取建案詳細資訊"""
    address = request.args.get('address', '')
    if not address:
        return jsonify({'error': '缺少地址參數'}), 400

    try:
        con = get_connection()
        query = f"""
        SELECT *
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 = ?
        ORDER BY 交易年月日 DESC
        """
        result = con.execute(query, [address]).fetchdf()
        transactions = result.to_dict('records')
        sales_control = generate_sales_control(transactions)
        con.close()

        result_data = {
            'success': True,
            'project': {
                'id': project_id,
                'address': address,
                'transactions': transactions,
                'sales_control': sales_control
            }
        }
        return jsonify(clean_nan_values(result_data))

    except Exception as e:
        print(f"Error in get_project_detail: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def generate_sales_control(transactions):
    """生成銷控數據"""
    floors = defaultdict(list)
    for trans in transactions:
        floor = trans.get('移轉層次', '未知')
        total_floors = trans.get('總樓層數', '未知')
        price = trans.get('總價元', 0)
        area = trans.get('建物移轉總面積平方公尺', 0)

        unit = {
            'floor': floor,
            'unit_number': f"{floor}樓",
            'area': float(area) if area else 0,
            'price': float(price) if price else 0,
            'status': '已售',
            'date': trans.get('交易年月日', '')
        }
        floors[str(total_floors)].append(unit)

    return {
        'total_floors': len(floors),
        'total_units': len(transactions),
        'sold_units': len(transactions),
        'available_units': 0,
        'reserved_units': 0,
        'floors': dict(floors)
    }


@app.route('/api/search', methods=['GET'])
def search_projects():
    """搜尋建案（支持模糊搜尋和全形/半形轉換）"""
    keyword = request.args.get('keyword', '').strip()
    min_price = request.args.get('min_price', '').strip()
    max_price = request.args.get('max_price', '').strip()
    min_unit_price = request.args.get('min_unit_price', '').strip()
    max_unit_price = request.args.get('max_unit_price', '').strip()
    min_year = request.args.get('min_year', '').strip()
    max_year = request.args.get('max_year', '').strip()
    min_ping = request.args.get('min_ping', '').strip()
    max_ping = request.args.get('max_ping', '').strip()
    min_ratio = request.args.get('min_ratio', '').strip()
    max_ratio = request.args.get('max_ratio', '').strip()
    building_type = request.args.get('building_type', '').strip()
    room_count = request.args.get('room_count', '').strip()
    sort_by = request.args.get('sort_by', 'transaction_count').strip()
    sort_order = request.args.get('sort_order', 'desc').strip().lower()

    PING_TO_SQM = 3.30579
    min_area = str(float(min_ping) * PING_TO_SQM) if min_ping else ''
    max_area = str(float(max_ping) * PING_TO_SQM) if max_ping else ''

    try:
        con = get_connection()
        half_keyword, full_keyword = normalize_search_text(keyword) if keyword else ('', '')

        base_query = f"""
        SELECT
            土地位置建物門牌,
            鄉鎮市區,
            COUNT(*) as 交易筆數,
            AVG(TRY_CAST(總價元 AS DOUBLE)) as 平均總價,
            MIN(TRY_CAST(總價元 AS DOUBLE)) as 最低價,
            MAX(TRY_CAST(總價元 AS DOUBLE)) as 最高價,
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) as 平均單價每坪,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) / 3.30579) as 平均坪數,
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

        search_conditions = []
        params = []

        if keyword:
            search_conditions.append("(土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 鄉鎮市區 LIKE ?)")
            params.extend([f'%{keyword}%', f'%{half_keyword}%', f'%{full_keyword}%', f'%{keyword}%'])

        if building_type:
            search_conditions.append("建物型態 LIKE ?")
            params.append(f'%{building_type}%')

        if room_count:
            search_conditions.append('"建物現況格局-房" = ?')
            params.append(room_count)

        if search_conditions:
            base_query += " AND " + " AND ".join(search_conditions)

        if min_year:
            base_query += " AND SUBSTR(交易年月日, 1, 3) >= ?"
            params.append(str(min_year).zfill(3))
        if max_year:
            base_query += " AND SUBSTR(交易年月日, 1, 3) <= ?"
            params.append(str(max_year).zfill(3))

        base_query += """
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        HAVING COUNT(*) >= 1
        """

        having_conditions = []

        if min_price:
            having_conditions.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) >= {float(min_price)}")
        if max_price:
            having_conditions.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) <= {float(max_price)}")
        if min_unit_price:
            having_conditions.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) >= {float(min_unit_price)}")
        if max_unit_price:
            having_conditions.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) <= {float(max_unit_price)}")
        if min_area:
            having_conditions.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) >= {float(min_area)}")
        if max_area:
            having_conditions.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) <= {float(max_area)}")

        if min_ratio:
            having_conditions.append(f"""AVG(
                CASE
                    WHEN TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) > 0
                         AND TRY_CAST(主建物面積 AS DOUBLE) IS NOT NULL
                    THEN ((TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) - TRY_CAST(主建物面積 AS DOUBLE))
                          / TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) * 100
                    ELSE NULL
                END
            ) >= {float(min_ratio)}""")
        if max_ratio:
            having_conditions.append(f"""AVG(
                CASE
                    WHEN TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) > 0
                         AND TRY_CAST(主建物面積 AS DOUBLE) IS NOT NULL
                    THEN ((TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE) - TRY_CAST(主建物面積 AS DOUBLE))
                          / TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) * 100
                    ELSE NULL
                END
            ) <= {float(max_ratio)}""")

        if having_conditions:
            base_query += " AND " + " AND ".join(having_conditions)

        sort_mapping = {
            'date': '最新交易日期',
            'price': '平均總價',
            'unit_price': '平均單價每坪',
            'area': '平均坪數',
            'ratio': '平均公設比',
            'transaction_count': '交易筆數'
        }
        sort_column = sort_mapping.get(sort_by, '交易筆數')
        sort_direction = 'ASC' if sort_order == 'asc' else 'DESC'

        base_query += f"""
        ORDER BY {sort_column} {sort_direction}
        LIMIT 200
        """

        if params:
            result = con.execute(base_query, params).fetchdf()
        else:
            result = con.execute(base_query).fetchdf()

        projects = []
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            if not is_real_building(address):
                continue

            district = row['鄉鎮市區']
            lat, lng = get_district_coordinates(district)
            lat += random.uniform(-0.01, 0.01)
            lng += random.uniform(-0.01, 0.01)

            latest_year_roc = row['最新年份'] if row['最新年份'] else ''
            oldest_year_roc = row['最舊年份'] if row['最舊年份'] else ''
            latest_year = int(latest_year_roc) + 1911 if latest_year_roc and str(latest_year_roc).strip().isdigit() else None
            oldest_year = int(oldest_year_roc) + 1911 if oldest_year_roc and str(oldest_year_roc).strip().isdigit() else None

            latest_date = row['最新交易日期'] if row['最新交易日期'] else ''
            oldest_date = row['最舊交易日期'] if row['最舊交易日期'] else ''

            def format_roc_date(roc_date):
                if not roc_date or len(str(roc_date)) < 7:
                    return None
                try:
                    ds = str(roc_date)
                    y = int(ds[:3]) + 1911
                    return f"{y}/{ds[3:5]}/{ds[5:7]}"
                except:
                    return None

            projects.append({
                'id': hashlib.md5(address.encode()).hexdigest()[:12],
                'name': extract_building_project_name(address),
                'address': address,
                'district': district,
                'type': row['建物型態'] or '住宅',
                'room_count': int(row['主要房數']) if row['主要房數'] and str(row['主要房數']).strip().isdigit() else None,
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': round(float(row['平均單價每坪']), 2) if row['平均單價每坪'] else 0,
                'avg_ping': round(float(row['平均坪數']), 2) if row['平均坪數'] else 0,
                'avg_ratio': round(float(row['平均公設比']), 2) if row['平均公設比'] else None,
                'latest_year': latest_year,
                'oldest_year': oldest_year,
                'latest_date': format_roc_date(latest_date),
                'oldest_date': format_roc_date(oldest_date),
                'year_range': f"{oldest_year}-{latest_year}" if oldest_year and latest_year else None,
                'lat': lat,
                'lng': lng
            })

        con.close()
        projects = clean_nan_values(projects)

        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })

    except Exception as e:
        print(f"Error in search_projects: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/building_projects', methods=['GET'])
def get_building_projects():
    """獲取建案名稱表 — 聚合後的建案清單

    參數:
    - keyword: 搜尋關鍵字
    - district: 鄉鎮市區
    - min_count: 最少交易筆數（預設 2）
    - limit: 回傳筆數上限（預設 100）
    """
    keyword = request.args.get('keyword', '').strip()
    district = request.args.get('district', '').strip()
    min_count = int(request.args.get('min_count', '2'))
    limit = int(request.args.get('limit', '100'))

    try:
        if not BUILDING_PROJECTS_READY:
            return jsonify({'success': False, 'error': '建案名稱表尚未初始化完成，請稍候'}), 503

        results = []
        for pid, proj in BUILDING_PROJECTS.items():
            if keyword:
                half_kw, full_kw = normalize_search_text(keyword)
                name_match = (keyword in proj['name'] or half_kw in proj['name'] or full_kw in proj['name'])
                addr_match = (keyword in proj['address'] or half_kw in proj['address'] or full_kw in proj['address'])
                if not name_match and not addr_match:
                    continue
            if district and district not in proj['district']:
                continue
            if proj['transaction_count'] < min_count:
                continue

            lat, lng = get_district_coordinates(proj['district'])
            results.append({
                **proj,
                'lat': lat + random.uniform(-0.005, 0.005),
                'lng': lng + random.uniform(-0.005, 0.005),
            })

        results.sort(key=lambda x: x['transaction_count'], reverse=True)
        results = results[:limit]

        return jsonify({
            'success': True,
            'count': len(results),
            'projects': clean_nan_values(results)
        })

    except Exception as e:
        print(f"Error in get_building_projects: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/geocode', methods=['GET'])
def geocode_address():
    """使用 Nominatim 免費 geocoding 服務"""
    address = request.args.get('address', '').strip()
    if not address:
        return jsonify({'success': False, 'error': '缺少 address 參數'}), 400

    result = nominatim_geocode(address)
    if result:
        return jsonify({
            'success': True,
            'lat': result[0],
            'lng': result[1],
            'source': 'nominatim'
        })
    else:
        return jsonify({
            'success': False,
            'error': f'無法找到地址: {address}'
        }), 404


if __name__ == '__main__':
    print("=" * 60)
    print("🏢 良富居地產專業房地產地圖系統")
    print("=" * 60)
    print(f"📁 CSV 文件: {CSV_PATH}")
    print(f"🗺️  地圖引擎: Leaflet.js + OpenStreetMap（免費）")
    print(f"🌍 Geocoding: Nominatim（免費）")
    print(f"�� 服務器啟動於: http://localhost:5000")
    print("=" * 60)

    # 在背景初始化建案名稱表
    import threading
    t = threading.Thread(target=init_building_projects, daemon=True)
    t.start()

    app.run(debug=True, host='0.0.0.0', port=5000)
