#!/usr/bin/env python3
"""
專業房地產地圖查詢系統 - Flask 後端
類似 leju.com.tw 的建案地圖和銷控面板系統
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import duckdb
import os
from dotenv import load_dotenv
from collections import defaultdict
import re

# 加載 .env 文件
load_dotenv()

app = Flask(__name__)
CORS(app)

# CSV 文件路徑
CSV_PATH = '/home/cyclone/land/ALL_lvr_land_a.csv'

def get_connection():
    """建立 DuckDB 連接"""
    con = duckdb.connect()
    return con

def extract_building_name(address):
    """從地址中提取建案名稱"""
    # 常見建案模式：XX大樓、XX華廈、XX社區等
    patterns = [
        r'(.+?(?:大樓|華廈|大廈|花園|社區|廣場|公寓|別墅|透天|新村))',
        r'(.+?[一二三四五六七八九十百]期)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(1)
    
    # 如果沒有匹配，取前幾個字作為建案名
    if len(address) > 10:
        return address[:10] + '...'
    return address

@app.route('/')
def index():
    """主頁面"""
    with open('leju_style_map.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    # 注入 API Key
    api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
    html_content = html_content.replace('YOUR_GOOGLE_MAPS_API_KEY', api_key)
    return html_content

@app.route('/api/projects', methods=['GET'])
def get_projects():
    """獲取建案列表（聚合數據）"""
    try:
        con = get_connection()
        
        # 查詢所有交易記錄並按地址聚合
        query = f"""
        SELECT 
            土地位置建物門牌,
            鄉鎮市區,
            COUNT(*) as 交易筆數,
            AVG(CAST(總價元 AS DOUBLE)) as 平均總價,
            MIN(CAST(總價元 AS DOUBLE)) as 最低價,
            MAX(CAST(總價元 AS DOUBLE)) as 最高價,
            AVG(CAST(單價元平方公尺 AS DOUBLE)) as 平均單價,
            AVG(CAST(建物移轉總面積平方公尺 AS DOUBLE)) as 平均面積,
            MAX(交易年月日) as 最新交易日期,
            建物型態
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 IS NOT NULL 
            AND 土地位置建物門牌 != ''
            AND 總價元 IS NOT NULL
            AND 總價元 != ''
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        HAVING COUNT(*) >= 1
        ORDER BY 交易筆數 DESC
        LIMIT 200
        """
        
        result = con.execute(query).fetchdf()
        
        # 轉換為建案格式
        projects = []
        for _, row in result.iterrows():
            address = row['土地位置建物門牌']
            building_name = extract_building_name(address)
            
            project = {
                'id': hash(address) % 1000000,
                'name': building_name,
                'address': address,
                'district': row['鄉鎮市區'],
                'type': row['建物型態'] or '住宅',
                'transaction_count': int(row['交易筆數']),
                'avg_price': float(row['平均總價']) if row['平均總價'] else 0,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': float(row['平均單價']) if row['平均單價'] else 0,
                'avg_area': float(row['平均面積']) if row['平均面積'] else 0,
                'latest_date': row['最新交易日期']
            }
            projects.append(project)
        
        con.close()
        
        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })
        
    except Exception as e:
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
        
        # 查詢該地址的所有交易記錄
        query = f"""
        SELECT *
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 土地位置建物門牌 = '{address}'
        ORDER BY 交易年月日 DESC
        """
        
        result = con.execute(query).fetchdf()
        transactions = result.to_dict('records')
        
        # 生成銷控數據（模擬）
        sales_control = generate_sales_control(transactions)
        
        con.close()
        
        return jsonify({
            'success': True,
            'project': {
                'id': project_id,
                'address': address,
                'transactions': transactions,
                'sales_control': sales_control
            }
        })
        
    except Exception as e:
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
            'status': '已售',  # 因為是交易記錄
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
    """搜尋建案"""
    keyword = request.args.get('keyword', '')
    district = request.args.get('district', '')
    min_price = request.args.get('min_price', '')
    max_price = request.args.get('max_price', '')
    
    try:
        con = get_connection()
        
        conditions = []
        if keyword:
            conditions.append(f"土地位置建物門牌 LIKE '%{keyword}%'")
        if district:
            conditions.append(f"鄉鎮市區 = '{district}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            土地位置建物門牌,
            鄉鎮市區,
            COUNT(*) as 交易筆數,
            AVG(CAST(總價元 AS DOUBLE)) as 平均總價,
            MIN(CAST(總價元 AS DOUBLE)) as 最低價,
            MAX(CAST(總價元 AS DOUBLE)) as 最高價,
            AVG(CAST(單價元平方公尺 AS DOUBLE)) as 平均單價,
            建物型態
        FROM read_csv_auto('{CSV_PATH}')
        WHERE {where_clause}
            AND 土地位置建物門牌 IS NOT NULL 
            AND 總價元 IS NOT NULL
        GROUP BY 土地位置建物門牌, 鄉鎮市區, 建物型態
        ORDER BY 交易筆數 DESC
        LIMIT 100
        """
        
        result = con.execute(query).fetchdf()
        
        projects = []
        for _, row in result.iterrows():
            avg_price = float(row['平均總價']) if row['平均總價'] else 0
            
            # 價格過濾
            if min_price and avg_price < float(min_price):
                continue
            if max_price and avg_price > float(max_price):
                continue
            
            address = row['土地位置建物門牌']
            projects.append({
                'id': hash(address) % 1000000,
                'name': extract_building_name(address),
                'address': address,
                'district': row['鄉鎮市區'],
                'type': row['建物型態'] or '住宅',
                'transaction_count': int(row['交易筆數']),
                'avg_price': avg_price,
                'min_price': float(row['最低價']) if row['最低價'] else 0,
                'max_price': float(row['最高價']) if row['最高價'] else 0,
                'avg_unit_price': float(row['平均單價']) if row['平均單價'] else 0
            })
        
        con.close()
        
        return jsonify({
            'success': True,
            'count': len(projects),
            'projects': projects
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/districts', methods=['GET'])
def get_districts():
    """獲取所有行政區列表"""
    try:
        con = get_connection()
        
        query = f"""
        SELECT DISTINCT 鄉鎮市區, COUNT(*) as count
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 鄉鎮市區 IS NOT NULL AND 鄉鎮市區 != ''
        GROUP BY 鄉鎮市區
        ORDER BY count DESC
        """
        
        result = con.execute(query).fetchdf()
        districts = result.to_dict('records')
        
        con.close()
        
        return jsonify({
            'success': True,
            'districts': districts
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🏢 專業房地產地圖系統 - Leju Style")
    print("=" * 60)
    print(f"CSV 文件: {CSV_PATH}")
    print(f"服務器啟動於: http://localhost:5000")
    print("功能：建案地圖、銷控面板、價格分析")
    print("按 Ctrl+C 停止服務器")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
