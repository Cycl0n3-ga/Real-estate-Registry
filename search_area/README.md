# search_area.py 使用說明

## 功能簡介
search_area.py 提供區域搜尋功能，可依經緯度範圍查詢土地交易資料，並支援多種篩選條件。

## 安裝與準備
1. 確保已安裝 Python 3.7 以上版本。
2. 安裝依賴套件（如 pytest 用於測試）：
   ```bash
   pip install pytest
   ```
3. 準備 SQLite 資料庫檔案，預設路徑為 `land/db/land_data.db`。

## 主要函式說明
### search_area
依經緯度範圍搜尋交易資料。
```python
search_area(south, north, west, east, filters=None, limit=500, db_path=None)
```
- **參數**：
  - `south, north, west, east`：經緯度邊界（float）
  - `filters`：篩選條件 dict（可選，支援建物型態、房數、公設比等）
  - `limit`：回傳筆數上限（int，可選，預設500）
  - `db_path`：資料庫路徑（str，可選，預設 land/db/land_data.db）
- **回傳**：list of dict（每筆交易資料）

#### 範例
```python
from search_area import search_area
results = search_area(24.9, 25.1, 121.4, 121.6, filters={"building_types": ["住宅大樓"]}, limit=10)
for row in results:
    print(row["address"], row["total_price"])
```

### search_by_community_name
以建案名稱查詢交易資料。
```python
search_by_community_name(community_name, filters, limit=500, db_path=None)
```
- **參數**：
  - `community_name`：建案名稱（str）
  - 其他同 search_area
- **回傳**：list of dict

#### 範例
```python
from search_area import search_by_community_name
results = search_by_community_name("遠雄", {}, limit=5)
for row in results:
    print(row["address"], row["total_price"])
```

### build_filter_where
建立 SQL WHERE 篩選條件（進階用法，通常由主函式自動呼叫）。

## 篩選條件說明
filters dict 支援以下鍵值：
- `building_types`：建物型態（list of str）
- `rooms`：房數（list of int）
- `public_ratio_min`/`public_ratio_max`：公設比範圍（float）

## 注意事項
- 經緯度範圍需合理，否則查無資料。
- 資料庫檔案需存在且結構正確。

## 單元測試
請參考 test_search_area.py，使用 pytest 執行：
```bash
pytest land/search_area/test_search_area.py
```

## 聯絡/貢獻
如有問題或建議，歡迎於專案頁面提出 issue。
