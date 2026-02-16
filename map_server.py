#!/usr/bin/env python3
"""
房地產地圖查詢服務器
使用 Flask + DuckDB 提供快速的房地產數據查詢API
"""

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import duckdb
import os
from dotenv import load_dotenv

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

@app.route('/')
def index():
    """主頁面"""
    with open('real_estate_map_flask.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    # 注入 API Key
    api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
    html_content = html_content.replace('YOUR_GOOGLE_MAPS_API_KEY', api_key)
    return html_content

@app.route('/api/config', methods=['GET'])
def get_config():
    """獲取前端配置（包括 API Key）"""
    return jsonify({
        'google_maps_api_key': os.getenv('GOOGLE_MAPS_API_KEY', '')
    })

@app.route('/api/search', methods=['GET'])
def search_properties():
    """搜尋房地產資料"""
    location = request.args.get('location', '')
    
    if not location:
        return jsonify({'error': '請提供搜尋地址'}), 400
    
    try:
        con = get_connection()
        
        # 創建臨時表並查詢
        query = f"""
        CREATE TEMP TABLE all_data AS 
        SELECT * FROM read_csv_auto('{CSV_PATH}');
        
        SELECT 
            鄉鎮市區,
            交易標的,
            土地位置建物門牌,
            土地移轉總面積平方公尺,
            交易年月日,
            交易筆棟數,
            移轉層次,
            總樓層數,
            建物型態,
            主要用途,
            建築完成年月,
            建物移轉總面積平方公尺,
            "建物現況格局-房" as 房,
            "建物現況格局-廳" as 廳,
            "建物現況格局-衛" as 衛,
            總價元,
            單價元平方公尺,
            車位類別,
            車位總價元,
            編號
        FROM all_data 
        WHERE 土地位置建物門牌 LIKE '%{location}%'
        ORDER BY 交易年月日 DESC
        """
        
        result = con.execute(query).fetchdf()
        
        # 轉換為JSON格式
        data = result.to_dict('records')
        
        # 計算統計資訊
        stats = {}
        if len(data) > 0:
            prices = [float(d['總價元']) for d in data if d['總價元'] and str(d['總價元']).replace('.','').isdigit()]
            unit_prices = [float(d['單價元平方公尺']) for d in data if d['單價元平方公尺'] and str(d['單價元平方公尺']).replace('.','').isdigit()]
            
            if prices:
                stats = {
                    'total_count': len(data),
                    'avg_price': sum(prices) / len(prices),
                    'max_price': max(prices),
                    'min_price': min(prices),
                    'avg_unit_price': sum(unit_prices) / len(unit_prices) if unit_prices else 0
                }
        
        con.close()
        
        return jsonify({
            'success': True,
            'count': len(data),
            'data': data,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_all_stats():
    """獲取全部資料統計"""
    try:
        con = get_connection()
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT 鄉鎮市區) as districts,
            MIN(CAST(總價元 AS DOUBLE)) as min_price,
            MAX(CAST(總價元 AS DOUBLE)) as max_price,
            AVG(CAST(總價元 AS DOUBLE)) as avg_price
        FROM read_csv_auto('{CSV_PATH}')
        WHERE 總價元 IS NOT NULL AND 總價元 != ''
        """
        
        result = con.execute(query).fetchdf()
        stats = result.to_dict('records')[0]
        
        con.close()
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🏠 房地產地圖查詢服務器")
    print("=" * 60)
    print(f"CSV 文件: {CSV_PATH}")
    print(f"服務器啟動於: http://localhost:5000")
    print("按 Ctrl+C 停止服務器")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
