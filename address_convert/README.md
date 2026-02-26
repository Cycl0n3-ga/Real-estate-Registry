# address_convert — 台灣實價登錄多來源資料整合工具

> 最後更新：2026-02-26

## 資料概況 (2026-02-26)

| 資料庫 | 總筆數 | 社區名稱非空筆數 | 社區非空率 |
|--------|-------:|----------------:|----------:|
| `transactions_all_original.db` (API 來源) | 4,561,703 | 1,852,343 | 40.61% |
| `land_data.db` (合併後) | 6,469,055 | 2,360,385 | 36.49% |

> 社區名稱僅 API 來源有收錄，政府 CSV 不含此欄位，故整體約 60% 無社區名為預期結果。

## 概述

`convert.py` (v4) 能**自動識別**任意輸入來源 (CSV / API DB / land_data.db)，清洗後**增量匯入**統一格式的 `land_data.db`。

核心設計原則：
- **丟什麼進來都行** — 自動偵測 CSV 格式或 SQLite schema
- **不破壞既有資料** — 預設增量匯入，已存在的交易會 enrich 而非覆蓋
- **壞資料自動丟棄** — 缺地址、無門牌號的記錄直接 discard

## 架構

```
address_convert/
├── convert.py            # 主轉換腳本 (v4)
├── convert_v3_backup.py  # v3 備份
├── test_convert.py       # 單元測試
└── README.md             # 本文件

../address_utils.py       # 共用模組 (地址解析、正規化、消歧)
../db/
├── ALL_lvr_land_a.csv    # CSV 來源 (政府公開資料)
├── transactions*.db      # API 來源 (LVR API 抓取)
└── land_data.db          # 目標: 統一格式的 SQLite 資料庫
```

### 程式七層架構

```
第一層  安全型別轉換     safe_int / safe_float / parse_price
第二層  資料來源識別     detect_source → SourceType (CSV_LVR / CSV_GENERIC / API_DB / LAND_DB)
第三層  地址工具函式     parse_floor_info / normalize_date / strip_city / strip_floor ...
第四層  LandDataDB 類    schema管理 + 去重 + enrich + 社區回填 + finalize
第五層  record 解析器    _parse_csv_row / _parse_api_row / _parse_land_db_row / _parse_generic_csv_row
第六層  匯入引擎         import_csv_lvr / import_api_db / import_land_db / import_csv_generic
第七層  主流程 + CLI     convert_v4() / import_file() / main()
```

## 用法

### 新版 (v4 推薦用法)

```bash
# 自動偵測並增量匯入 (最常用)
python3 convert.py data.csv                     # 自動識別為 LVR CSV
python3 convert.py transactions.db              # 自動識別為 API DB
python3 convert.py a.csv b.db c.csv             # 多檔依序匯入

# 重建 land_data.db (清空重來)
python3 convert.py --rebuild data.csv transactions.db

# 指定目標 DB
python3 convert.py --target /path/to/land_data.db data.csv

# 無參數: 自動找 ../db/ 下的預設檔案 (等同舊版 --source both)
python3 convert.py
```

### 向後相容 (v3)

```bash
python3 convert.py --source csv
python3 convert.py --source api
python3 convert.py --source both
python3 convert.py --csv-input a.csv --api-input t.db -o out.db
```

### 參數

| 參數 | 說明 |
|------|------|
| `inputs` (positional) | 輸入檔案路徑 (CSV / .db)，可多個 |
| `--target`, `-t` | 目標 land_data.db 路徑 |
| `--rebuild`, `-r` | 重建模式: 刪除舊 DB 重新匯入 |
| `--skip-finalize` | 跳過建索引/FTS/VACUUM |
| `--source`, `-s` | [向後相容] csv / api / both |
| `--csv-input` | [向後相容] CSV 路徑 |
| `--api-input` | [向後相容] API DB 路徑 |
| `--output`, `-o` | [向後相容] 同 --target |

## 自動識別邏輯

```
輸入檔案
├── .csv → 讀取第一行 header
│     ├── 包含 '鄉鎮市區','交易年月日' 等 → CSV_LVR (政府 33 欄格式)
│     └── 包含 '地址','總價' 等其他欄位   → CSV_GENERIC (通用欄位映射)
├── .db / .sqlite → 檢查 SQLite schema
│     ├── 有 land_transaction 表 → LAND_DB (另一個 land_data.db)
│     └── 有 transactions 表     → API_DB (LVR API 抓取)
└── 其他 → 嘗試當 CSV 讀，失敗則 UNKNOWN
```

## 去重 + Enrich 策略

每筆記錄進入 `LandDataDB.upsert_record()` 時：

```
record
  │
  ├── 資料品質檢查: 地址必須包含「號」或「地號」
  │     └── 不合格 → discard
  │
  ├── 計算去重 key
  │     ├── key_addr  = (交易日期前7碼, 正規化地址去縣市)
  │     └── key_price = (交易日期前7碼, 總價)
  │
  ├── 已存在? (addr_key 或 price_key 命中)
  │     ├── 讀取既有記錄的 26 個可補充欄位
  │     ├── 新資料有值且舊資料為空 → UPDATE (enrich)
  │     └── 無新資訊 → 視為重複，跳過
  │
  └── 不存在 → INSERT 新記錄
```

### Enrich 涵蓋的 26 個欄位

| 類別 | 欄位 |
|------|------|
| 地理 | lat, lng |
| 社區 | community_name |
| 基本 | county_city, building_type, main_use, main_material |
| 格局 | rooms, halls, bathrooms |
| 面積 | building_area, land_area, main_area, attached_area, balcony_area |
| 價格 | unit_price |
| 交易 | transaction_type, has_management, elevator |
| 樓層 | floor_level, total_floors |
| 停車 | parking_type, parking_area, parking_price |
| 分區 | urban_zone |
| 其他 | note |

## 資料來源比較

| 特性 | CSV (`ALL_lvr_land_a.csv`) | API DB (`transactions.db`) | land_data.db |
|------|----------------------------|----------------------------|--------------|
| 筆數 | ~470 萬 | ~456 萬 | 合併後 ~647 萬 |
| 縣市資訊 | ❌ (僅有區名) | ✅ (城市代碼 A-Z) | ✅ |
| 經緯度 | ❌ | ✅ | ✅ (enriched) |
| 社區名 | ❌ | ✅ (非空率 40.6%) | ✅ (非空率 36.5%) |
| 建材/車位 | ✅ | ❌ | ✅ |
| 土地面積 | ✅ | ❌ | ✅ |
| 完整原始欄位 | ✅ (33 欄) | 部分 (raw_json 中) | ✅ (47 欄) |
| 可直接匯入 | ✅ | ✅ | ✅ (合併用) |

增量匯入後的 `land_data.db` 結合所有來源優勢。

## 資料庫 Schema

### 主表: `land_transaction` (47 欄)

| # | 欄位 | 型態 | 說明 | CSV | API |
|---|------|------|------|:---:|:---:|
| 1 | `id` | INTEGER PK | 自動遞增 | — | — |
| 2 | `raw_district` | TEXT | 原始鄉鎮市區 | ✅ | town |
| 3 | `transaction_type` | TEXT | 交易標的 | ✅ | json.t |
| 4 | `address` | TEXT | 完整地址 | ✅ | `#` 後半 |
| 5 | `land_area` | REAL | 土地面積(m²) | ✅ | — |
| 6 | `urban_zone` | TEXT | 都市分區 | ✅ | — |
| 7 | `non_urban_zone` | TEXT | 非都市分區 | ✅ | — |
| 8 | `non_urban_use` | TEXT | 非都市使用 | ✅ | — |
| 9 | `transaction_date` | TEXT | 交易日期 | ✅ | date_str |
| 10 | `transaction_count` | TEXT | 交易筆棟數 | ✅ | — |
| 11 | `floor_level` | TEXT | 移轉層次 | ✅ | json.f |
| 12 | `total_floors` | TEXT | 總樓層數 | ✅ | json.f |
| 13 | `building_type` | TEXT | 建物型態 | ✅ | json.b |
| 14 | `main_use` | TEXT | 主要用途 | ✅ | json.pu |
| 15 | `main_material` | TEXT | 主要建材 | ✅ | — |
| 16 | `build_date` | TEXT | 建築完成年月 | ✅ | — |
| 17 | `building_area` | REAL | 建物面積(m²) | ✅ | json.s |
| 18 | `rooms` | INTEGER | 房 | ✅ | json.j |
| 19 | `halls` | INTEGER | 廳 | ✅ | json.k |
| 20 | `bathrooms` | INTEGER | 衛 | ✅ | json.l |
| 21 | `partitioned` | TEXT | 隔間 | ✅ | — |
| 22 | `has_management` | TEXT | 管理組織 | ✅ | json.m |
| 23 | `total_price` | INTEGER | 總價(元) | ✅ | json.tp |
| 24 | `unit_price` | REAL | 單價(元/m²) | ✅ | json.cp |
| 25 | `parking_type` | TEXT | 車位類別 | ✅ | — |
| 26 | `parking_area` | REAL | 車位面積(m²) | ✅ | — |
| 27 | `parking_price` | INTEGER | 車位總價(元) | ✅ | — |
| 28 | `note` | TEXT | 備註 | ✅ | json.note |
| 29 | `serial_no` | TEXT | 編號 | ✅ | `api_` + sq |
| 30 | `main_area` | REAL | 主建物面積 | ✅ | — |
| 31 | `attached_area` | REAL | 附屬建物面積 | ✅ | — |
| 32 | `balcony_area` | REAL | 陽台面積 | ✅ | — |
| 33 | `elevator` | TEXT | 電梯 | ✅ | — |
| 34 | `transfer_no` | TEXT | 移轉編號 | ✅ | — |
| 35 | `county_city` | TEXT | **解析: 縣市** | 推斷 | code→名 |
| 36 | `district` | TEXT | **解析: 鄉鎮市區** | 解析 | 解析 |
| 37 | `village` | TEXT | **解析: 里** | 解析 | 解析 |
| 38 | `street` | TEXT | **解析: 街路(含段)** | 解析 | 解析 |
| 39 | `lane` | TEXT | **解析: 巷** | 解析 | 解析 |
| 40 | `alley` | TEXT | **解析: 弄** | 解析 | 解析 |
| 41 | `number` | TEXT | **解析: 號** | 解析 | 解析 |
| 42 | `floor` | TEXT | **解析: 樓** | 解析 | 解析 |
| 43 | `sub_number` | TEXT | **解析: 之號** | 解析 | 解析 |
| 44 | `community_name` | TEXT | 社區名稱 | — | ✅ |
| 45 | `lat` | REAL | 緯度 | — | ✅ |
| 46 | `lng` | REAL | 經度 | — | ✅ |
| 47 | `dedup_key` | TEXT | 去重鍵 `date7\|addr_norm\|price` | — | — |

### FTS5 全文檢索表

```sql
CREATE VIRTUAL TABLE address_fts USING fts5(
    address, content='land_transaction', content_rowid='id',
    tokenize='unicode61'
);
```

### 索引

| 索引名 | 欄位 | 用途 |
|--------|------|------|
| `idx_county_city` | `county_city` | 縣市篩選 |
| `idx_district` | `district` | 區域篩選 |
| `idx_street` | `street` | 街路查詢 |
| `idx_lane` | `lane` | 巷弄查詢 |
| `idx_number` | `number` | 門牌查詢 |
| `idx_floor` | `floor` | 樓層查詢 |
| `idx_date` | `transaction_date` | 日期篩選 |
| `idx_price` | `total_price` | 價格篩選 |
| `idx_serial` | `serial_no` | 編號查詢 |
| `idx_addr_combo` | `county_city, district, street, lane, number` | 地址複合查詢 |

## 地址解析

### 正規化

1. **全形→半形**: `１８８號` → `188號`
2. **變體字**: `臺` → `台`, `\u5DFF` → `市`
3. **中文數字→阿拉伯**: `十二樓` → `12樓`
4. **段數轉中文**: `3段` → `三段`

### 縣市推斷 / 消歧

| 情境 | 策略 |
|------|------|
| 地址含縣市名 | 直接提取 |
| CSV + 非歧義區 | 查 `DISTRICT_CITY_MAP` (`板橋區` → `新北市`) |
| API + 歧義區 | `city_code` 消歧 (`中山區` + code=`A` → `台北市`) |
| CSV + 歧義區 | fallback 取交易量最大城市 |

歧義區: 中山區、中正區、信義區、大安區、東區、西區、南區、北區、中區、中西區、安平區

## API 資料格式

```
transactions.address: "原始地址#乾淨地址"  → 取 # 後半
transactions.city:    "A"               → CITY_CODE_MAP → "台北市"
transactions.raw_json:
  t=交易類型, f=樓層/總樓, j=房, k=廳, l=衛,
  m=管理, pu=主要用途, tp=總價, cp=單價, s=面積,
  b=建物類型, note=備註, lat/lon=經緯度
```

## v3 → v4 變更摘要

| 項目 | v3 (舊版) | v4 (新版) |
|------|-----------|-----------|
| 匯入模式 | 每次刪除舊 DB 重建 | **增量匯入** (預設保留既有資料) |
| 來源識別 | 手動指定 `--source csv/api/both` | **自動偵測** (丟檔案即可) |
| 支援來源 | CSV + API DB (2 種) | CSV_LVR + CSV_GENERIC + API_DB + LAND_DB (4 種) |
| 去重方式 | 先全量 CSV 再配對 API | **統一 upsert**: 逐筆去重 + enrich |
| enrich 欄位 | 16 個 | **26 個** (新增 land_area, parking 等) |
| 多檔匯入 | ❌ | ✅ `convert.py a.csv b.db c.csv` |
| 核心類別 | 無 (函式散落) | `LandDataDB` 封裝全部 DB 操作 |
| 向後相容 | — | ✅ `--source`/`load_csv`/`load_api` 仍可用 |

## 效能

| 項目 | 規格 |
|------|------|
| CSV 處理速度 | ~100,000 筆/秒 |
| 批次大小 | 10,000 筆 |
| page_size | 4096 bytes |
| journal_mode | WAL (寫入) → DELETE (VACUUM) → WAL |
| enrich 批次 | 5,000 筆/批 |

## 共用模組依賴

`convert.py` 從 `../address_utils.py` 引入:

| 函式/常數 | 用途 |
|-----------|------|
| `normalize_address()` | 地址正規化 |
| `parse_address(addr, city_hint=)` | 地址結構化解析 (含消歧) |
| `fullwidth_to_halfwidth()` | 全形→半形轉換 |
| `CITY_CODE_MAP` | 城市代碼→縣市名 (A→台北市...) |
| `DISTRICT_CITY_MAP` | 鄉鎮市區→縣市映射 |
| `AMBIGUOUS_DISTRICTS` | 歧義區名→候選城市列表 |
