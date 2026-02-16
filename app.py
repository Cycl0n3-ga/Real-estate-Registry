#!/usr/bin/env python3
"""
良富居地產 - 專業房地產地圖系統
整合建案地圖、價格查詢、銷控面板
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
import os
from dotenv import load_dotenv
from collections import defaultdict
import re
import random
import json
import math

# 加載 .env 文件
load_dotenv()

app = Flask(__name__)
CORS(app)

# CSV 文件路徑
CSV_PATH = '/home/cyclone/land/ALL_lvr_land_a.csv'

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
    '北投區': (25.1370, 121.5130),
    '大同區': (25.0737, 121.5149),
    '文山區': (25.0035, 121.5674),
    '南屯區': (24.1003, 120.6684),
    '東屯區': (24.2102, 120.8052),
    '西屯區': (24.1884, 120.6350),
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
    '白河鎮': (22.9153, 120.3789),
    '將軍鄉': (23.1648, 120.2226),
    '七股鄉': (23.1527, 120.1363),
    '學甲鎮': (23.2315, 120.2693),
    '北門鎮': (23.2728, 120.1704),
    '新營市': (23.3032, 120.3031),
    '永康市': (22.9896, 120.2440),
    '仁德區': (22.9385, 120.2545),
    '左鎮區': (22.8146, 120.3696),
    '南關線': (22.8633, 120.2433),
    '歸仁區': (22.9049, 120.3027),
    '關廟區': (22.8921, 120.3196),
    '龍崗區': (22.8824, 120.3274),
    '東山區': (23.0000, 120.4500),
    '下營鎮': (23.1329, 120.3107),
    '六甲鎮': (23.2074, 120.4006),
    '官田鎮': (23.1933, 120.4319),
    '大內鄉': (23.1167, 120.4667),
    '山上鄉': (23.1424, 120.4619),
    '麻豆鎮': (23.1793, 120.2411),
    '佳里鎮': (23.1602, 120.1808),
    '西港鎮': (23.1417, 120.1865),
    '後壁鄉': (23.3452, 120.4089),
    '柳營鄉': (23.2839, 120.3730),
    '鹽水鎮': (23.2832, 120.2788),
    '玉井鄉': (23.0777, 120.5452),
    '南化鄉': (22.9005, 120.4833),
    '楠西鄉': (23.0238, 120.5567),
    '中埤鎮': (23.4932, 120.2588),
}

def get_connection():
    """建立 DuckDB 連接"""
    con = duckdb.connect()
    return con

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
    """取得鄉鎮市區的座標，如果沒有則返回台灣中心"""
    # 移除市/縣/區
    district_clean = district.replace('市', '').replace('縣', '').replace('區', '')
    
    # 先試試完整的鄉鎮市區名
    if district in DISTRICT_COORDINATES:
        return DISTRICT_COORDINATES[district]
    
    # 試試去除尾部的區/鎮/市
    for key in DISTRICT_COORDINATES:
        if district_clean in key or key in district:
            return DISTRICT_COORDINATES[key]
    
    # 預設值：台灣中心
    return (24.0, 121.0)

def normalize_search_text(text):
    """正規化搜尋文字 - 生成半形和全形兩個版本
    返回 (半形版本, 全形版本) 的tuple
    """
    if not text:
        return (text, text)
    
    # 全形轉半形數字對照表
    full_to_half = {
        '０': '0', '１': '1', '２': '2', '３': '3', '４': '4',
        '５': '5', '６': '6', '７': '7', '８': '8', '９': '9',
        'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E',
        'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
        'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O',
        'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
        'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y', 'Ｚ': 'Z',
    }
    
    # 半形轉全形對照表
    half_to_full = {v: k for k, v in full_to_half.items()}
    
    # 生成半形版本
    half_width = []
    for char in text:
        half_width.append(full_to_half.get(char, char))
    
    # 生成全形版本（將數字轉換為全形）
    full_width = []
    for char in text:
        # 如果已經是全形，保持不變
        if char in full_to_half:
            full_width.append(char)
        # 如果是半形數字或字母，轉換為全形
        elif char in half_to_full:
            full_width.append(half_to_full[char])
        # 其他字符保持不變
        else:
            full_width.append(char)
    
    return (''.join(half_width), ''.join(full_width))

def is_real_building(address):
    """判斷是否為真實建案（非純地號）
    簡化邏輯：只排除明顯的純地號，其他都保留
    """
    if not address or len(address) < 5:
        return False
    
    # 只排除明顯的純地號格式："XX段XX地號" 且沒有門牌號碼
    if '地號' in address and '號' not in address:
        return False
    
    # 其他都保留（包含路、街、號等的地址）
    return True

def extract_building_name(address):
    """從地址中提取建案名稱"""
    patterns = [
        r'(.+?(?:大樓|華廈|大廈|花園|社區|廣場|公寓|別墅|透天|新村))',
        r'(.+?[一二三四五六七八九十百]期)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(1)
    
    if len(address) > 10:
        return address[:10] + '...'
    return address

@app.route('/')
def index():
    """主頁面"""
    with open('liangfu_map.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
    html_content = html_content.replace('YOUR_GOOGLE_MAPS_API_KEY', api_key)
    return html_content

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
            AVG(TRY_CAST(單價元平方公尺 AS DOUBLE)) as 平均單價,
            AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) as 平均面積,
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
            
            # 過濾掉純地號，只保留真實建案
            if not is_real_building(address):
                continue
            
            building_name = extract_building_name(address)
            district = row['鄉鎮市區']
            lat, lng = get_district_coordinates(district)
            
            # 添加隨機偏移量，使相同區域的標記不會完全重疊
            lat += random.uniform(-0.01, 0.01)
            lng += random.uniform(-0.01, 0.01)
            
            project = {
                'id': abs(hash(address)) % 1000000,
                'name': building_name,
                'address': address,
                'district': district,
                'type': row['建物型態'] or '住宅',
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': float(row['平均單價']) if row['平均單價'] else 0,
                'avg_area': float(row['平均面積']) if row['平均面積'] else 0,
                'latest_date': row['最新交易日期'],
                'lat': lat,
                'lng': lng
            }
            projects.append(project)
        
        con.close()
        
        # 清理 NaN 值
        projects = clean_nan_values(projects)
        
        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })
        
    except Exception as e:
        print(f"Error in get_projects: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/project/<int:project_id>', methods=['GET'])
def get_project_detail(project_id):
    """獲取建案詳細資訊"""
    address = request.args.get('address', '')
    
    if not address:
        return jsonify({'error': '缺少地址參數'}), 400
    
    try:
        con = get_connection()
        
        # 使用參數化查詢避免 SQL 注入
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
        
        # 清理 NaN 值
        result_data = {
            'success': True,
            'project': {
                'id': project_id,
                'address': address,
                'transactions': transactions,
                'sales_control': sales_control
            }
        }
        result_data = clean_nan_values(result_data)
        
        return jsonify(result_data)
        
    except Exception as e:
        print(f"Error in get_project_detail: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def generate_sales_control(transactions):
    """生成銷控數據（基於交易記錄）"""
    floors = defaultdict(list)
    
    for trans in transactions:
        floor = trans.get('移轉層次', '未知')
        total_floors = trans.get('總樓層數', '未知')
        price = trans.get('總價元', 0)
        area = trans.get('建物移轉總面積平方公尺', 0)
        layout = f"{trans.get('房', '-')}房{trans.get('廳', '-')}廳{trans.get('衛', '-')}衛"
        
        unit = {
            'floor': floor,
            'unit_number': f"{floor}樓",
            'layout': layout,
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
    """搜尋建案（支持模糊搜尋和全形/半形轉換）
    
    參數說明：
    - keyword: 關鍵字（地址或區域）
    - min_price, max_price: 總價範圍（元）
    - min_unit_price, max_unit_price: 單價範圍（元/坪）
    - min_year, max_year: 年份範圍（民國年）
    - min_ping, max_ping: 坪數範圍（坪）
    - min_ratio, max_ratio: 公設比範圍（%）
    - building_type: 建物型態（住宅大樓、華廈、公寓等）
    - room_count: 房數（2、3、4等）
    - sort_by: 排序欄位（date, price, unit_price, area, ratio, transaction_count）
    - sort_order: 排序方向（asc, desc）
    """
    keyword = request.args.get('keyword', '').strip()
    min_price = request.args.get('min_price', '').strip()
    max_price = request.args.get('max_price', '').strip()
    min_unit_price = request.args.get('min_unit_price', '').strip()  # 元/坪
    max_unit_price = request.args.get('max_unit_price', '').strip()
    min_year = request.args.get('min_year', '').strip()  # 民國年
    max_year = request.args.get('max_year', '').strip()
    min_ping = request.args.get('min_ping', '').strip()  # 坪數
    max_ping = request.args.get('max_ping', '').strip()
    min_ratio = request.args.get('min_ratio', '').strip()  # 公設比 (0-100)
    max_ratio = request.args.get('max_ratio', '').strip()
    building_type = request.args.get('building_type', '').strip()  # 建物型態
    room_count = request.args.get('room_count', '').strip()  # 房數
    sort_by = request.args.get('sort_by', 'transaction_count').strip()  # date, price, unit_price, area, ratio, transaction_count
    sort_order = request.args.get('sort_order', 'desc').strip().lower()  # asc 或 desc
    
    # 坪數轉換為平方公尺（1坪 = 3.30579平方公尺）
    PING_TO_SQM = 3.30579
    min_area = str(float(min_ping) * PING_TO_SQM) if min_ping else ''
    max_area = str(float(max_ping) * PING_TO_SQM) if max_ping else ''
    
    try:
        con = get_connection()
        
        # 正規化搜尋關鍵字（生成半形和全形兩個版本）
        half_keyword, full_keyword = normalize_search_text(keyword) if keyword else ('', '')
        
        # 基礎查詢 - 計算公設比和單價（元/坪）
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
        
        # 添加模糊搜尋（支持多種匹配）
        search_conditions = []
        params = []
        
        if keyword:
            # 同時搜尋原始、半形、全形三個版本
            search_conditions.append("(土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 土地位置建物門牌 LIKE ? OR 鄉鎮市區 LIKE ?)")
            params.extend([f'%{keyword}%', f'%{half_keyword}%', f'%{full_keyword}%', f'%{keyword}%'])
        
        # 建物型態篩選
        if building_type:
            search_conditions.append("建物型態 LIKE ?")
            params.append(f'%{building_type}%')
        
        # 房數篩選
        if room_count:
            search_conditions.append("\"建物現況格局-房\" = ?")
            params.append(room_count)
        
        if search_conditions:
            base_query += " AND " + " AND ".join(search_conditions)
        
        # 年份篩選 (民國年，取前3位)
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
        
        # 在 HAVING 子句中進行聚合後的篩選
        having_conditions = []
        
        # 價格篩選
        if min_price:
            having_conditions.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) >= {float(min_price)}")
        if max_price:
            having_conditions.append(f"AVG(TRY_CAST(總價元 AS DOUBLE)) <= {float(max_price)}")
        
        # 單價篩選（元/坪）
        if min_unit_price:
            having_conditions.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) >= {float(min_unit_price)}")
        if max_unit_price:
            having_conditions.append(f"AVG(TRY_CAST(單價元平方公尺 AS DOUBLE) * 3.30579) <= {float(max_unit_price)}")
        
        # 坪數篩選
        if min_area:
            having_conditions.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) >= {float(min_area)}")
        if max_area:
            having_conditions.append(f"AVG(TRY_CAST(建物移轉總面積平方公尺 AS DOUBLE)) <= {float(max_area)}")
        
        # 公設比篩選
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
        
        # 排序邏輯
        sort_mapping = {
            'date': '最新交易日期',          # 成交日期
            'price': '平均總價',            # 成交金額
            'unit_price': '平均單價每坪',   # 單價（元/坪）
            'area': '平均坪數',             # 坪數
            'ratio': '平均公設比',          # 公設比
            'transaction_count': '交易筆數'  # 交易筆數（預設）
        }
        
        sort_column = sort_mapping.get(sort_by, '交易筆數')
        sort_direction = 'ASC' if sort_order == 'asc' else 'DESC'
        
        base_query += f"""
        ORDER BY {sort_column} {sort_direction}
        LIMIT 200
        """
        
        # 執行查詢
        if params:
            result = con.execute(base_query, params).fetchdf()
        else:
            result = con.execute(base_query).fetchdf()
        
        projects = []
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            
            # 過濾掉純地號，只保留真實建案
            if not is_real_building(address):
                continue
            
            district = row['鄉鎮市區']
            lat, lng = get_district_coordinates(district)
            
            # 添加隨機偏移量
            lat += random.uniform(-0.01, 0.01)
            lng += random.uniform(-0.01, 0.01)
            
            # 轉換民國年為西元年 (加1911)
            latest_year_roc = row['最新年份'] if row['最新年份'] else ''
            oldest_year_roc = row['最舊年份'] if row['最舊年份'] else ''
            latest_year = int(latest_year_roc) + 1911 if latest_year_roc and latest_year_roc.isdigit() else None
            oldest_year = int(oldest_year_roc) + 1911 if oldest_year_roc and oldest_year_roc.isdigit() else None
            
            # 格式化交易日期（民國年月日 -> 西元年/月/日）
            latest_date = row['最新交易日期'] if row['最新交易日期'] else ''
            oldest_date = row['最舊交易日期'] if row['最舊交易日期'] else ''
            
            def format_roc_date(roc_date):
                if not roc_date or len(str(roc_date)) < 7:
                    return None
                try:
                    date_str = str(roc_date)
                    year = int(date_str[:3]) + 1911
                    month = date_str[3:5]
                    day = date_str[5:7]
                    return f"{year}/{month}/{day}"
                except:
                    return None
            
            projects.append({
                'id': abs(hash(address)) % 1000000,
                'name': extract_building_name(address),
                'address': address,
                'district': district,
                'type': row['建物型態'] or '住宅',
                'room_count': int(row['主要房數']) if row['主要房數'] and str(row['主要房數']).isdigit() else None,
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': round(float(row['平均單價每坪']), 2) if row['平均單價每坪'] else 0,  # 元/坪
                'avg_ping': round(float(row['平均坪數']), 2) if row['平均坪數'] else 0,  # 坪數
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
        
        # 清理 NaN 值
        projects = clean_nan_values(projects)
        
        # 排序處理
        if sort_by in ['年份', '坪數', '公設比']:
            result = result.sort_values(by=sort_by, ascending=(sort_order == 'asc'))
        
        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })
        
    except Exception as e:
        print(f"Error in search_projects: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🏢 良富居地產專業房地產地圖系統")
    print("=" * 60)
    print(f"CSV 文件: {CSV_PATH}")
    print(f"服務器啟動於: http://localhost:5000")
    print("功能：建案地圖、價格查詢、銷控面板、單位切換")
    print("按 Ctrl+C 停止服務器")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
