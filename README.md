# 🏠 良富居地產 — 不動產實價登錄查詢系統 v3.0

全台灣不動產交易實價登錄資料查詢系統，整合地圖視覺化、建案名稱反查與智慧地址匹配。

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/flask-3.0+-green.svg)
![SQLite](https://img.shields.io/badge/sqlite-3-blue.svg)
![Leaflet](https://img.shields.io/badge/leaflet-1.9-green.svg)

## ✨ 功能特色

- 🔍 **智慧搜尋** — 地址、建案名稱、區域，支援全形/半形數字自動轉換
- 🗺️ **互動地圖** — Leaflet 地圖，標記顯示單價/坪，叢集點擊顯示列表
- 📊 **多維篩選** — 總價、單價、年份、坪數、公設比、建物型態
- 🏘️ **建案分組** — 交易自動依建案分組，顯示均價、單價、公設比統計
- 🔄 **雙向查詢** — 地址↔建案名稱透過 address2com / com2address 模組雙向查詢
- 📍 **區域搜尋** — 拖動地圖後點「搜此區域」即可搜尋可視範圍內的成交紀錄
- 📱 **響應式設計** — 桌機與手機皆適用

## 🚀 快速開始

### 環境需求

- Python 3.8+
- SQLite3

### 安裝

```bash
git clone https://github.com/Cycl0n3-ga/Real-estate-Registry.git
cd Real-estate-Registry/land

pip install flask flask-cors requests
```

### 資料準備

1. 將內政部實價登錄 CSV（`ALL_lvr_land_a.csv`）放入 `db/` 目錄
2. 執行資料轉換，建立 SQLite 資料庫：

```bash
cd land_reg
python3 csv_to_sqlite.py
```

### 啟動伺服器

```bash
cd web
python3 server.py
```

瀏覽器開啟 **http://localhost:5001**

---

## 📂 專案結構

```
land/
├── web/                          # 🌐 Web 應用
│   ├── server.py                 #    Flask 後端 API 伺服器 (port 5001)
│   └── static/
│       └── index.html            #    前端搜尋+地圖頁面
├── address_utils.py              # 🔧 共用地址工具（正規化、解析、中文數字轉換）
├── address_match/                # 🔍 地址搜尋引擎
│   └── address_match.py          #    搜尋策略：結構化→FTS5→LIKE
├── address2com/                  # 🏠 地址 → 建案名稱
│   └── address2community.py      #    查詢模組
├── com2address/                  # 🔄 建案名稱 → 地址
│   └── community2address.py      #    查詢模組（輕量啟動）
├── geodecoding/                  # 🌍 地理編碼
│   ├── geocoder.py               #    OSM Nominatim 地址→座標
│   └── build_osm_index.py        #    本地門牌索引建立
├── address_convert/              # 🔤 地址格式轉換
├── address2com/                  # 🏘️ 地址→社區
├── com2address/                  # 🏘️ 社區→地址
├── db/                           # 💾 資料庫
│   └── land_data.db              #    SQLite 主資料庫
├── land_reg/                     # 📦 資料處理工具
│   ├── csv_to_sqlite.py          #    CSV → SQLite 轉換（含索引建立）
│   └── ...
└── API_使用說明.md               # 📖 API 詳細說明
```

---

## 🗺️ 前端功能說明

### 地圖標記
- 每個成交紀錄以圓形標記顯示於地圖上
- 標記顏色與數字代表**單價（萬/坪）**：
  - 🟢 綠色 ≤ 50萬/坪
  - 🟡 黃色 50~100萬/坪
  - 🔴 紅色 > 100萬/坪
- 叢集（多點重疊）顯示筆數與平均單價
- **點擊叢集** → 在左側側欄顯示該位置全部交易列表

### 交易列表
- 支援按 日期 / 總價 / 單價 / 坪數 / 公設比 / 建案 排序
- 每筆交易顯示建案名稱標籤（綠色標籤）
- 左側色條快速判斷單價等級
- 建案群組可點擊折疊/展開，群組標頭顯示均單價、均坪數、公設比

### 搜尋策略
1. 輸入**建案名稱** → `com2address` 反查地址 → `address_match` 搜尋
2. 輸入**地址** → `address_match` 直接搜尋，同時嘗試 `address2com` 找出所屬建案
3. 輸入**區域關鍵字** → `address_match` 模糊搜尋

---

## 📊 API 端點

### `GET /api/search`
搜尋交易資料，支援關鍵字與多維篩選。

| 參數 | 說明 | 範例 |
|------|------|------|
| `keyword` | 關鍵字（地址/建案名稱） | `keyword=信義路三段` |
| `limit` | 回傳筆數上限（最大 2000） | `limit=200` |
| `building_type` | 建物型態（逗號分隔） | `building_type=住宅大樓` |
| `rooms` | 房數（逗號分隔） | `rooms=2,3` |
| `public_ratio` | 公設比範圍（%） | `public_ratio=0-35` |
| `year` | 民國年範圍 | `year=110-114` |
| `ping` | 坪數範圍 | `ping=20-45` |
| `unit_price` | 單價範圍（萬/坪） | `unit_price=50-100` |
| `price` | 總價範圍（萬） | `price=1000-3000` |

### `GET /api/search_area`
依地圖範圍（經緯度）搜尋成交紀錄，支援相同篩選參數。

| 參數 | 說明 |
|------|------|
| `south/north/west/east` | 地圖邊界經緯度（必要） |

### `GET /api/address2community`
地址反查建案名稱。

### `GET /api/community2address`
建案名稱查詢對應地址範圍。

### `GET /api/stats`
系統狀態與資料庫統計。

---

## 🛠️ 核心模組說明

### `address_utils.py`
共用地址工具，被 address_match、address_convert 等模組使用：
- 全形/半形轉換
- 中文數字 ↔ 阿拉伯數字
- 台灣地址正規化 (`normalize_address`)
- 結構化地址解析 (`parse_query`)：拆解縣市/區/路/巷/號/樓

### `address_match`
高效能地址搜尋，3 層策略（依序嘗試）：
1. **結構化索引查詢** — 解析後欄位精準比對（< 50ms）
2. **FTS5 全文搜尋** — 文字比對
3. **LIKE 後備** — 數字格式變體模糊比對

### `address2com` / `com2address`
建案名稱雙向查詢，使用 SQLite 索引快速查詢（啟動 < 0.1s）。

---

## ⚡ 效能優化

### 查詢效能

| 操作 | 時間 |
|------|------|
| address2community 查詢 | 0.1–6ms |
| community2address 啟動 | ~0.06s |
| address_match 結構化搜尋 | < 50ms |
| 區域搜尋（座標範圍） | < 100ms |

### SQLite 索引
資料庫建立 18 個索引 + FTS5 全文索引，支援各種查詢場景。

---

## 📄 授權

MIT License


全台灣不動產交易實價登錄資料查詢系統，支援地址搜尋、篩選排序、建案名稱反查等功能。

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/flask-3.0+-green.svg)
![SQLite](https://img.shields.io/badge/sqlite-3-blue.svg)

## ✨ 功能特色

- 🔍 **關鍵字搜尋** — 地址、區域模糊搜尋，支援全形/半形數字自動轉換
- 📊 **多維篩選** — 總價、單價、年份、坪數、公設比
- 📈 **排序功能** — 交易筆數、年份、面積、公設比、總價、單價
- 🏘️ **建案名稱查詢** — 地址 ↔ 建案社區名稱雙向對照
- 🌐 **Web 前端** — 現代化響應式 UI

## 🚀 快速開始

### 環境需求

- Python 3.8+
- SQLite3

### 安裝

```bash
git clone https://github.com/Cycl0n3-ga/Real-estate-Registry.git
cd Real-estate-Registry

pip install flask flask-cors
```

### 資料準備

1. 將內政部實價登錄 CSV（`ALL_lvr_land_a.csv`）放入專案根目錄
2. 執行資料轉換，建立 SQLite 資料庫：

```bash
cd land_reg
python3 csv_to_sqlite.py
```

### 啟動伺服器

```bash
cd web
python3 server.py
```

瀏覽器開啟 **http://localhost:5001**

## 📂 專案結構

```
land/
├── web/                          # 🌐 Web 應用
│   ├── server.py                 #    Flask 後端伺服器 (port 5001)
│   └── static/
│       └── index.html            #    前端搜尋頁面
├── land_reg/                     # 📦 資料處理
│   ├── csv_to_sqlite.py          #    CSV → SQLite 轉換工具
│   ├── address_search/           #    地址搜尋模組
│   │   └── address_transfer.py   #    核心搜尋引擎
│   └── geodecoding/              #    地理編碼工具
│       ├── geocoder.py           #    地址 → 座標轉換
│       ├── batch_geocode.py      #    批次地理編碼
│       └── build_osm_index.py    #    OSM 門牌索引建立
├── address2com/                  # 🏠 地址 → 建案名稱
│   ├── address2community.py      #    查詢模組
│   ├── build_db.py               #    對照表建立工具
│   └── *.csv                     #    對照資料
├── com2address/                  # 🔄 建案名稱 → 地址
│   ├── community2address.py      #    查詢模組
│   └── 591_api_integration.py    #    591 API 整合
├── Building_Projects_B.csv       # 📋 建案 B 表資料
├── API_使用說明.md                # 📖 API 文件
└── .gitignore
```

## 📊 API 端點

### `GET /api/search`

搜尋交易資料，支援多種篩選條件。

| 參數 | 說明 | 範例 |
|------|------|------|
| `keyword` | 關鍵字（地址/區域） | `keyword=大直` |
| `min_price` / `max_price` | 總價範圍（元） | `min_price=5000000` |
| `min_unit_price` / `max_unit_price` | 單價範圍（元/㎡） | `max_unit_price=300000` |
| `min_year` / `max_year` | 交易年份（民國年） | `min_year=110` |
| `min_area` / `max_area` | 面積範圍（㎡） | `min_area=50&max_area=100` |
| `min_ratio` / `max_ratio` | 公設比（%） | `max_ratio=30` |
| `sort_by` | 排序欄位 | `sort_by=price` |
| `sort_order` | 排序方向 (`asc`/`desc`) | `sort_order=asc` |
| `limit` | 筆數限制 | `limit=50` |

### `GET /api/address2community`

地址反查建案名稱。

| 參數 | 說明 |
|------|------|
| `address` | 查詢地址 |

### `GET /api/stats`

取得資料庫統計資訊。

## 🛠️ 技術架構

- **後端**：Flask + SQLite
- **前端**：原生 HTML/CSS/JavaScript
- **資料來源**：內政部不動產交易實價登錄
- **輔助模組**：address2com（地址→建案）、com2address（建案→地址）、address_match（地址搜尋）

## ⚡ 效能優化

本專案針對 620 萬筆交易資料庫進行系統性效能優化，查詢速度提升 **100–500 倍**。

### 索引架構

`finalize()` 執行後會自動建立 18 個索引：

| 類型 | 索引名稱 | 欄位 | 用途 |
|------|----------|------|------|
| 單欄 | `idx_county_city` | county_city | 縣市篩選 |
| 單欄 | `idx_district` | district | 行政區篩選 |
| 單欄 | `idx_street` | street | 路街搜尋 |
| 單欄 | `idx_lane` | lane | 巷弄搜尋 |
| 單欄 | `idx_number` | number | 門牌搜尋 |
| 單欄 | `idx_floor` | floor | 樓層篩選 |
| 單欄 | `idx_date` | transaction_date | 日期排序 |
| 單欄 | `idx_price` | total_price | 價格排序 |
| 單欄 | `idx_serial` | serial_no | 序號查詢 |
| 單欄 | `idx_dedup_key` | dedup_key | 去重比對 |
| 單欄 | `idx_community` | community_name | 社區查詢 |
| 複合 | `idx_addr_combo` | county_city, district, street, lane, number | 地址精準查詢 |
| 複合 | `idx_community_address` | community_name, address | 社區→地址查詢 |
| 複合 | `idx_street_lane_district` | street, lane, district | 路巷區複合搜尋 |
| 複合 | `idx_search_numbers` | street, lane, district, total_floors, build_date | 進階搜尋 |
| 複合 | `idx_district_street_number` | district, street, number | 區路號搜尋 |
| 複合 | `idx_district_street_lane` | district, street, lane | 區域巷弄搜尋 |
| 複合 | `idx_community_district` | community_name, district | 社區+區域搜尋 |

另有 FTS5 全文檢索索引 (`address_fts`) 作為回退搜尋策略。

### 查詢策略

**address2community（地址→建案）** — 4 層索引查詢 + LIKE 回退：
1. **精確匹配** — district + street + lane + number 完全比對 (< 1ms)
2. **門牌比對** — street + number，忽略巷弄 (< 2ms)
3. **巷弄搜尋** — street + lane 範圍搜尋 (< 5ms)
4. **路段搜尋** — 僅 street 搜尋 (< 10ms)
5. **LIKE 回退** — 原始地址模糊比對（僅在索引都無結果時使用）

**community2address（建案→地址）** — 輕量啟動 + 按需查詢：
- 啟動：僅載入 DISTINCT community_name (0.06s，取代原本 GROUP BY 全掃 3.7s)
- 查詢：使用 `idx_community_address` 索引按需查詢地址

**address_match（地址搜尋）** — 3 層搜尋策略：
1. **結構化搜尋** — 解析地址欄位走索引 (最快)
2. **FTS5 全文搜尋** — 文字比對原始地址
3. **LIKE 變體搜尋** — 數字格式變體模糊比對（最後手段）

### 效能數據

| 操作 | 優化前 | 優化後 | 提升 |
|------|--------|--------|------|
| address2community 啟動 | 4.5s | 0.1s | **45x** |
| address2community 查詢 | 200–1400ms | 0.1–6ms | **300x** |
| community2address 啟動 | 3.7s | 0.06s | **60x** |
| community2address 查詢 | 100–230ms | ~25ms | **5–10x** |
| address_match 結構化搜尋 | N/A (走 LIKE) | < 50ms | **索引直查** |
| convert.py 資料轉換 | ~14,000/s | ~22,000/s | **1.57x** |

### 自動優化

執行 `convert.py` 匯入資料後，會自動執行 `finalize()`：
- 建立所有索引（18 個）
- 建立 FTS5 全文檢索
- 執行 ANALYZE 更新查詢規劃器統計
- 執行 VACUUM 壓縮資料庫（磁碟空間不足時自動跳過）

轉換時使用 PRAGMA 優化批量寫入：
- `journal_mode=WAL` — 寫前日誌，允許讀寫併發
- `synchronous=OFF` — 轉換期間關閉同步（完成後恢復）
- `cache_size=-256000` — 256MB 記憶體快取
- `temp_store=MEMORY` — 暫存使用記憶體
- `locking_mode=EXCLUSIVE` — 獨佔鎖定，減少鎖開銷
- 延遲索引建立 — 轉換完成後才建索引，避免每筆更新索引

## 📄 授權

MIT License
