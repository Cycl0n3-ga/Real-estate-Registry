# 🏠 Real Estate Registry — 不動產實價登錄查詢系統

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
- **輔助模組**：address2com（地址→建案）、com2address（建案→地址）

## 📄 授權

MIT License
