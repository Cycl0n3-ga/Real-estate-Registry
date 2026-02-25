# convert.py — 台灣實價登錄 CSV → SQLite 轉換工具

## 功能概述

將內政部實價登錄開放資料（CSV 格式）批次匯入 SQLite 資料庫，並同步完成：

- **地址正規化**：全形→半形、中文數字→阿拉伯數字、地址變體字統一
- **地址結構化解析**：拆解為縣市、鄉鎮市區、里、街路名、巷、弄、號、樓、附號等欄位
- **效能優化設定**：WAL 模式、`page_size=4096`、批次寫入、VACUUM/ANALYZE
- **搜尋索引建立**：精簡 B-Tree 索引 + FTS5 全文檢索（地址查詢 < 1 秒）

---

## 使用方式

```bash
# 使用預設路徑 (../db/ALL_lvr_land_a.csv → ../db/land_data.db)
python3 convert.py

# 指定輸入/輸出路徑
python3 convert.py --input /path/to/input.csv --output /path/to/output.db

# 縮寫參數
python3 convert.py -i input.csv -o output.db
```

### 預設路徑

| 項目 | 路徑 |
|------|------|
| 輸入 CSV | `../db/ALL_lvr_land_a.csv` |
| 輸出 SQLite | `../db/land_data.db` |

---

## 輸入 CSV 格式

符合內政部實價登錄 `lvr_land_a`（不動產買賣）CSV 格式，**前兩列為標頭**（第一列中文欄名、第二列英文欄名），第三列起為資料。

欄位順序（共 33 欄）：

| 欄位索引 | 說明 | 資料型態 |
|----------|------|----------|
| 0 | 鄉鎮市區 | TEXT |
| 1 | 交易標的 | TEXT |
| 2 | 土地/建物區段位置 | TEXT |
| 3 | 土地移轉總面積 | REAL |
| 4 | 都市土地使用分區 | TEXT |
| 5 | 非都市土地使用分區 | TEXT |
| 6 | 非都市土地使用編定 | TEXT |
| 7 | 交易年月日 | TEXT |
| 8 | 交易筆棟數 | TEXT |
| 9 | 移轉層次 | TEXT |
| 10 | 總樓層數 | TEXT |
| 11 | 建物型態 | TEXT |
| 12 | 主要用途 | TEXT |
| 13 | 主要建材 | TEXT |
| 14 | 建築完成年月 | TEXT |
| 15 | 建物移轉總面積 | REAL |
| 16 | 車位移轉總面積（坪） | INTEGER |
| 17 | 建物現況格局-廳 | INTEGER |
| 18 | 建物現況格局-衛 | INTEGER |
| 19 | 建物現況格局-隔間 | TEXT |
| 20 | 有無管理組織 | TEXT |
| 21 | 總價元 | INTEGER |
| 22 | 單價元/坪 | REAL |
| 23 | 車位類別 | TEXT |
| 24 | 車位移轉總面積 | REAL |
| 25 | 車位總價元 | INTEGER |
| 26 | 備註 | TEXT |
| 27 | 編號 | TEXT |
| 28 | 主建物面積 | REAL |
| 29 | 附屬建物面積 | REAL |
| 30 | 陽台面積 | REAL |
| 31 | 電梯 | TEXT |
| 32 | 移轉編號 | TEXT |

---

## SQLite 資料庫結構

### 主表：`land_transaction`

```sql
id              INTEGER PRIMARY KEY AUTOINCREMENT
-- 原始欄位
raw_district    TEXT        -- 原始鄉鎮市區
transaction_type TEXT
address         TEXT        -- 原始地址
land_area       REAL
...
total_price     INTEGER
unit_price      REAL
...
-- 解析後地址欄位
county_city     TEXT        -- 縣/直轄市
district        TEXT        -- 鄉鎮市區
village         TEXT        -- 里
street          TEXT        -- 街路名（含段）
lane            TEXT        -- 巷
alley           TEXT        -- 弄
number          TEXT        -- 號
floor           TEXT        -- 樓
sub_number      TEXT        -- 附號（之X）
-- 預留欄位
community_name  TEXT
lat             REAL
lng             REAL
```

### 索引

| 索引名稱 | 欄位 |
|----------|------|
| `idx_county_city` | `county_city` |
| `idx_district` | `district` |
| `idx_street` | `street` |
| `idx_lane` | `lane` |
| `idx_number` | `number` |
| `idx_floor` | `floor` |
| `idx_date` | `transaction_date` |
| `idx_price` | `total_price` |
| `idx_serial` | `serial_no` |
| `idx_addr_combo` | `county_city, district, street, lane, number`（複合） |

### FTS5 全文檢索表：`address_fts`

對 `address` 欄位以 `unicode61` tokenizer 建立全文索引，支援快速模糊地址搜尋。

```sql
SELECT lt.*
FROM address_fts fts
JOIN land_transaction lt ON lt.id = fts.rowid
WHERE address_fts MATCH '中山路';
```

---

## 地址正規化邏輯

### `fullwidth_to_halfwidth(text)`
將 Unicode 全形字元（`Ａ`→`A`、`１`→`1`、`　`→` `）轉為半形。

### `chinese_numeral_to_int(text)`
支援傳統繁體中文數字（含大寫），例如：
- `三` → `3`
- `十` → `10`
- `二十三` → `23`

### `normalize_address_numbers(text)`
整合以上功能，針對地址做完整正規化：
- 全形→半形
- `臺`→`台`、字型變體修正
- 中文數字+單位（`三樓`、`五號`、`二巷`）→阿拉伯數字
- 阿拉伯數字段（`3段`）→中文段（`三段`）

---

## 地址解析邏輯

`parse_address(raw_address, district_col)` 依序以正則表達式拆解以下層級：

```
[縣市] → [鄉鎮市區] → [里] → [鄰] → [街路名] → [巷] → [弄] → [號] → [樓]
```

- 縣市名稱自動修正舊縣市名（如 `台北縣`→`新北市`）
- 若地址未含縣市，透過 `DISTRICT_CITY_MAP`（涵蓋全國鄉鎮市區）推算所屬縣市
- 土地地號（含「地號」字樣）自動略過，不解析

---

## 效能優化

| 設定 | 說明 |
|------|------|
| `PRAGMA page_size=4096` | 最佳化大量讀取效能 |
| `PRAGMA journal_mode=WAL` | 寫入期間仍可讀取 |
| `PRAGMA synchronous=NORMAL` | 提升寫入速度 |
| `PRAGMA cache_size=-200000` | 約 200MB 記憶體快取 |
| 批次寫入 (`batch_size=10000`) | 減少交易次數 |
| 最終 VACUUM | 壓縮碎片，縮小檔案大小 |
| ANALYZE | 更新查詢最佳化統計資訊 |

---

## 依賴套件

僅使用 Python 標準函式庫，無需額外安裝：

- `csv`、`sqlite3`、`re`、`os`、`sys`、`argparse`、`time`

Python 版本需求：**≥ 3.6**

---

## 輸出範例

```
📂 輸入: db/ALL_lvr_land_a.csv
💾 輸出: db/land_data.db
  ⏳ 已處理 150,000 筆 (42,000 筆/秒)
  ✅ 資料載入完成: 152,348 筆
  📇 建立索引...
  🔍 建立 FTS5 全文檢索...
  📊 更新統計資訊...
  🗜  壓縮資料庫 (VACUUM)...

🎉 完成!
  總筆數:        152,348
  地址解析成功:  148,901 (97.7%)
  耗時:          12.3 秒
  資料庫大小:    45.2 MB
```
