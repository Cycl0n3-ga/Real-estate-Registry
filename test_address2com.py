#!/usr/bin/env python3
"""
address2community API 測試腳本
用法：python3 test_address2com.py "地址"
"""

import sys
import urllib.request
import urllib.parse
import json


def test_address(address):
    """測試單一地址查詢"""
    url = f'http://localhost:5000/api/address2community?address={urllib.parse.quote(address)}'
    
    try:
        with urllib.request.urlopen(url) as r:
            data = json.loads(r.read())
        
        if data.get('success'):
            print(f"\n📍 輸入地址: {data['input']}")
            print(f"🔄 正規化: {data['normalized']}")
            print(f"🏘️  最佳結果: {data['best'] or '未找到'}")
            
            if data.get('results'):
                print(f"\n📊 所有候選結果：")
                for i, r in enumerate(data['results'], 1):
                    bar = "█" * (r['confidence'] // 10) + "░" * (10 - r['confidence'] // 10)
                    print(f"  {i}. {r['community']}")
                    print(f"     信心度: [{bar}] {r['confidence']}%")
                    print(f"     匹配: {r['match_level']}")
                    if r.get('district'):
                        print(f"     區域: {r['district']}")
                    if r.get('count'):
                        print(f"     交易: {r['count']} 筆")
        else:
            print(f"❌ 錯誤: {data.get('error', '未知錯誤')}")
    
    except Exception as e:
        print(f"❌ 連線錯誤: {e}")


def test_batch():
    """批次測試多個地址"""
    test_addresses = [
        "台北市松山區敦化北路123號",
        "新北市板橋區民族路25號",
        "台中市西屯區文華路100號",
        "高雄市前鎮區三多路15號",
        "桃園市中壢區中山路68號",
    ]
    
    print("=" * 60)
    print("🧪 批次測試 address2community API")
    print("=" * 60)
    
    for addr in test_addresses:
        test_address(addr)
        print()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        # 命令列模式
        address = ' '.join(sys.argv[1:])
        test_address(address)
    else:
        # 批次測試模式
        test_batch()
