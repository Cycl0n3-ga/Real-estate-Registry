# search_area.py 使用說明

## 功能簡介
search_area.py 提供區域搜尋功能，可依經緯度範圍查詢土地交易資料。

### 主要函式
- `search_area(south, north, west, east, filters=None, limit=500, db_path=None)`
  - 依經緯度邊界搜尋交易資料。
  - 回傳符合條件的交易資料列表（dict）。
  - 參數說明：
    - `south, north, west, east`: 經緯度邊界（float）
    - `filters`: 篩選條件（dict，可選）
    - `limit`: 回傳筆數上限（int，可選，預設500）
    - `db_path`: 資料庫路徑（str，可選，預設 land/db/land_data.db）

- `search_by_community_name(community_name, filters, limit=500, db_path=None)`
  - 以建案名稱查詢交易資料。

- `build_filter_where(filters, params)`
  - 建立 SQL WHERE 篩選條件。

## 測試
請參考 test_search_area.py，使用 pytest 執行單元測試：

```bash
pytest land/search_area/test_search_area.py
```

## 注意事項
- 須有正確的 SQLite 資料庫檔案（預設路徑：land/db/land_data.db）。
- 經緯度範圍需合理，否則查無資料。

## 範例
```python
from search_area import search_area
results = search_area(24.9, 25.1, 121.4, 121.6, limit=10)
for row in results:
    print(row)
```
