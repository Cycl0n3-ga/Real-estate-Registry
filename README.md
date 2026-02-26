# 🏠 良富居地產 — 不動產實價登錄查詢系統 v4.3

全台灣不動產交易實價登錄資料查詢系統，整合地圖視覺化、建案名稱反查、OSM 離線地理編碼與智慧地址匹配。

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/flask-3.0+-green.svg)
![SQLite](https://img.shields.io/badge/sqlite-3-blue.svg)
![Leaflet](https://img.shields.io/badge/leaflet-1.9-green.svg)

## ✨ 功能特色

- 🔍 **智慧搜尋** — 地址、建案名稱、區域，支援全形/半形數字自動轉換
- 🗺️ **互動地圖** — Leaflet 地圖 + MarkerCluster，建案群組化標記
- 🎯 **雙圈標記** — 外環＝總價色彩、內圈＝單價色彩，SVG 向量不失真不跑版
- ⚙️ **自訂設定** — 外環/內圈色彩指標可自由切換，設定自動存儲
- 📊 **近兩年分析** — 圈內顯示近兩年均價（排除特殊交易），一目瞭然
- 📊 **多維篩選** — 總價、單價、年份、坪數、公設比、建物型態、特殊交易過濾
- ⚡ **快速篩選列** — 一鍵「近一年」「近兩年」「排除特殊」
- 🏘️ **建案分組** — 同建案/同地址交易自動合併為單一標記，點擊展開列表
- 🔄 **雙向查詢** — 地址↔建案名稱透過 address2com / com2address 雙向查詢
- 📍 **OSM 離線定位** — 900 萬門牌本地索引，批次定位 0.1s/500筆
- 📍 **區域搜尋** — 拖動地圖後「搜此區域」搜尋可視範圍成交紀錄
- 🚗 **車位資訊** — 顯示車位型態與價格
- 📱 **手機友善** — 響應式設計、觸控優化、自動收合側欄
- 🚀 **效能優化** — API gzip壓縮、lat/lng DB索引、伺服器端查詢快取

## 🚀 快速開始

### 環境需求

- Python 3.8+
- SQLite3

### 安裝

```bash
git clone https://github.com/Cycl0n3-ga/Real-estate-Registry.git
cd Real-estate-Registry/land

pip install flask flask-cors flask-compress requests
```

### 資料準備

1. 將內政部實價登錄 CSV（`ALL_lvr_land_a.csv`）放入 `db/` 目錄
2. 執行資料轉換，建立 SQLite 資料庫：

```bash
cd land_reg
python3 csv_to_sqlite.py
```

3. （選擇性）建立 OSM 門牌索引以啟用精確座標定位：

```bash
cd geodecoding
python3 build_osm_index.py
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
│   ├── server.py                 #    Flask 後端 API (545 行)
│   ├── data_utils.py             #    資料格式化與統計模組 (291 行)
│   └── static/
│       └── index.html            #    前端搜尋+地圖頁面 (1313 行)
├── address_utils.py              # 🔧 共用地址工具（正規化、解析、中文數字轉換）
├── address_match/                # 🔍 地址搜尋引擎
│   └── address_match.py          #    搜尋策略：結構化→FTS5→LIKE
├── address2com/                  # 🏠 地址 → 建案名稱
│   └── address2community.py      #    查詢模組
├── com2address/                  # 🔄 建案名稱 → 地址
│   └── community2address.py      #    查詢模組（輕量啟動）
├── geodecoding/                  # 🌍 地理編碼
│   ├── geocoder.py               #    多層策略：OSM索引→快取→API
│   └── build_osm_index.py        #    本地門牌索引建立
├── address_convert/              # 🔤 地址格式轉換
├── db/                           # 💾 資料庫
│   ├── land_data.db              #    SQLite 主資料庫 (~490萬筆)
│   └── osm_addresses.db          #    OSM 門牌索引 (~900萬節點)
└── land_reg/                     # 📦 資料處理工具
    ├── csv_to_sqlite.py          #    CSV → SQLite 轉換（含索引建立）
    └── ...
```

---

## 🗺️ 前端功能說明

### 地圖標記 (v4.2)
- **建案群組化**：同建案或同地址的交易自動合併為單一標記
  - 單筆 → 顯示價格圓圈 + 建案名稱
  - 多筆 → 顯示「N筆 均XX萬/坪」+ 建案名稱
  - 點擊多筆標記 → 側欄顯示完整交易列表
- **單價色階**：
  - 🟢 綠色 ≤ 30萬/坪
  - 🟡 黃色 30~60萬/坪
  - 🟠 橙色 60~90萬/坪
  - 🔴 紅色 > 90萬/坪
- **叢集**（縮小時）：顯示筆數 + 平均單價，同建案顯示名稱
- **位置模式切換**：OSM 精確定位 / DB 快速定位

### 交易列表
- 支援按日期 / 總價 / 單價 / 坪數 / 公設比 / 建案排序
- 每筆交易顯示建案名稱標籤、車位資訊
- 左側色條快速判斷單價等級
- 建案群組可折疊/展開，群組標頭顯示均單價、均坪數、公設比
- 特殊交易標記（親友、法拍等），支援一鍵排除

### 搜尋策略
1. **建案搜尋** — `com2address` 反查 → community_name 索引直查
2. **地址→建案** — `address2com` 找建案 → community_name 索引直查
3. **地址模糊** — `address_match` 結構化→FTS5→LIKE 搜尋
4. **區域搜尋** — 經緯度範圍 + 篩選條件
5. **行政區後過濾** — keyword 含行政區時確保結果只含該區

---

## 📊 API 端點

### `GET /api/search`
搜尋交易資料，支援關鍵字與多維篩選。

| 參數 | 說明 | 範例 |
|------|------|------|
| `keyword` | 關鍵字（地址/建案名稱） | `keyword=信義路三段` |
| `location_mode` | 位置模式 osm/db | `location_mode=osm` |
| `limit` | 回傳筆數上限（最大 2000） | `limit=200` |
| `exclude_special` | 排除特殊交易 | `exclude_special=1` |
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

## 🛠️ 模組化架構 (v4.2)

### `web/data_utils.py` — 資料格式化模組
從 server.py 抽出的純函式模組，無 Flask 依賴：
- `format_tx_row()` — DB row → 前端 JSON（含座標策略、車位、特殊交易）
- `batch_osm_geocode()` — 批次 OSM 定位（單連線、無 API fallback）
- `compute_summary()` / `build_community_summaries()` — 統計計算
- `strip_city()` — 去除地址縣市前綴、修正重複行政區
- `clean_nan()` / `format_roc_date()` / `is_special_transaction()`

### `web/server.py` — Flask API 伺服器
路由定義 + 初始化邏輯：
- 5 個 API 端點
- `get_osm_coords()` — 單筆 OSM 精確定位
- `_build_filter_where()` / `parse_filters_from_request()` — 篩選器
- 背景初始化：com2address、TaiwanGeocoder、建案座標快取

### `geodecoding/geocoder.py`
多層地理編碼策略：
1. **OSM 本地索引** — 900 萬門牌，0.01ms/筆
2. **快取查詢** — 歷史結果 SQLite 快取
3. **Nominatim API** — 線上 API 回退
4. **NLSC / ArcGIS** — 多重 API 回退

---

## ⚡ 效能

### v4.2 效能數據

| 操作 | 時間 | 說明 |
|------|------|------|
| OSM 批次定位 (423 筆) | **0.10s** | 單連線批次查詢 (v4.1: 31.5s) |
| 區域搜尋 (500 筆, OSM) | **0.22s** | DB + OSM + 格式化 |
| 建案搜尋 (78 筆) | **1.17s** | com2address + 格式化 |
| address_match 結構化搜尋 | < 50ms | 走 SQLite 索引 |
| OSM 索引單筆查詢 | 0.01ms | 900 萬門牌 SQLite |

### OSM 批次定位優化 (v4.2 關鍵改善)

| 指標 | v4.1 | v4.2 | 提升 |
|------|------|------|------|
| 426 筆地址定位 | 31.54s | 0.10s | **315x** |
| 瓶頸 | 逐筆開關連線 + API fallback | 單連線 + 僅本地索引 | — |
| OSM 命中率 | ~99% | ~80% (嚴格模式) | 精確度優先 |

### 資料庫規模

| 資料 | 數量 |
|------|------|
| 交易紀錄 | 4,916,327 |
| 有建案名稱 | 1,879,885 |
| 有座標資料 | 4,512,953 |
| 不同建案 | 37,026 |
| OSM 門牌節點 | 9,097,857 |

---

## 📜 版本記錄

### v4.2 (2025-02)
- 🚀 OSM 批次定位加速 315 倍（31.5s → 0.1s）
- 🗺️ 建案/地址群組化標記（同建案合併、不再 spider）
- 📦 模組化重構：`data_utils.py` 抽出資料格式化
- 🏠 地址去縣市前綴、修正重複行政區
- 🐛 修正 OSM 索引路徑錯誤

### v4.1 (2025-02)
- 🚗 車位類型與價格顯示
- 🔍 特殊交易過濾（親友、法拍等）
- 📍 行政區後過濾
- 📊 重疊標記分析

### v4.0 (2025-02)
- 🌍 OSM 離線地理編碼
- 🗺️ 位置模式切換（OSM 精確 / DB 快速）
- 📍 區域搜尋（地圖範圍）
- 🏘️ 建案雙向查詢整合

### v3.0
- 多維篩選
- MarkerCluster 叢集標記
- 建案分組統計

---

## 📄 授權

MIT License
