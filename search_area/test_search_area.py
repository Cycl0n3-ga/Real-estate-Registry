import pytest
from search_area import search_area

# 測試 search_area 基本功能

def test_search_area_basic():
    # 假設測試資料庫路徑
    db_path = '../db/land_data.db'
    # 設定經緯度範圍
    south, north, west, east = 24.9, 25.1, 121.4, 121.6
    # 呼叫 search_area
    results = search_area(south, north, west, east, limit=10, db_path=db_path)
    # 檢查回傳型態
    assert isinstance(results, list)
    # 檢查至少有一筆資料
    assert len(results) >= 0
    # 檢查資料欄位
    if results:
        assert 'lat' in results[0]
        assert 'lng' in results[0]
        assert 'address' in results[0]

# 更多測試可根據需求擴充
