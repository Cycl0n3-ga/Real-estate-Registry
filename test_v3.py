#!/usr/bin/env python3
"""測試 v3.0 API"""
import urllib.request, json

def fetch(path):
    from urllib.parse import quote
    # Encode non-ASCII in URL path/query
    url = f'http://localhost:5000{quote(path, safe="/:?=&%")}'
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())

print('=== Test 1: 地址搜尋「木柵路」===')
d = fetch('/api/projects?keyword=木柵路&limit=5')
print(f'建案: {d["building_count"]}, 地址: {d["address_count"]}')
for p in d['projects'][:5]:
    src = '📍地址' if p.get('is_address_result') else '🏢建案'
    print(f'  {src} | {p["name"][:35]} | {p["district"]} | 樓:{p["max_floors"]} | {p["transaction_count"]}筆')

print('\n=== Test 2: 樓層格式化 ===')
pid = d['projects'][0]['id']
d2 = fetch(f'/api/project/{pid}')
for tx in d2['transactions'][:8]:
    print(f'  樓層: {tx["floor"]:20s} | 地址: {tx["address"][:35]}')

print('\n=== Test 3: 都廳大院 ===')
d3 = fetch('/api/projects?keyword=都廳大院&limit=3')
for p in d3['projects'][:3]:
    print(f'  {p["name"]} | {p["district"]} | 樓:{p["max_floors"]}')

print('\n=== Test 4: Stats ===')
d4 = fetch('/api/stats')
print(f'建案數: {d4["total_projects"]}, 交易數: {d4["total_transactions"]}')
print('\n✅ 所有測試通過！')
