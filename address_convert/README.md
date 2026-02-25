# address_convert — 台灣實價登錄多來源資料整合工具

## 概述

`convert.py` 整合政府實價登錄 CSV 與 LVR API 兩種資料來源，產出統一格式的 SQLite 資料庫。

支援三種資料來源模式，透過 `--source` 參數切換：

| 模式 | 說明 |
|------|------|
| `csv` | 僅從政府 CSV 匯入 |
| `api` | 僅從 LVR API DB 匯入 |
| `both` | **CSV + API 合併**（預設） |

> 本模組**完全取代** `merge_csv_api/merge_and_enrich.py` 的三步驟流程。

## 架構

```
address_convert/
├── convert.py        # 主要轉換腳本 (1143 行)
├── test_convert.py   # 單元測試
└── README.md         # 本文件

../address_utils.py   # 共用模組 (地址解析、正規化、消歧)
../db/
├── ALL_lvr_land_a.csv              # CSV 來源 (政府公開資料)
├── transactions_all_original.db    # API 來源 (LVR API 抓取)
└── land_data.db                    # 輸出: 合併後的 SQLite 資料庫
```

## 用法

```bash
# 預設: 合併兩種來源 (推薦)
python3 convert.py

# 僅 CSV
python3 convert.py --source csv

# 僅 API
python3 convert.py --source api

# 指定路徑
python3 convert.py --csv-input data.csv --api-input trans.db -o out.db
```

### 參數

| 參數 | 預設值 | 說明 |
|------|--------|------|
| `--source`, `-s` | `both` | 資料來源: `csv`, `api`, `both` |
| `--csv-input` | `../db/ALL_lvr_land_a.csv` | CSV 輸入路徑 |
| `--api-input` | `../db/transactions_all_original.db` | API DB 路徑 |
| `--output`, `-o` | `../db/land_data.db` | SQLite 輸出路徑 |

## 合併策略 (`--source both`)

```
Phase 1: CSV 載入
  ├── 讀取 ALL_lvr_land_a.csv (跳過 2 列標頭)
  ├── 地址正規化 + 結構化解析
  └── 批次寫入 land_data.db

Phase 2: API 合併 (3 個子階段)
  ├── Phase A — 去重插入
  │     比對 (日期+地址) 或 (日期+總價)
  │     └── 不存在 → 新增 | 資料缺損 → 丟棄
  │
  ├── Phase B — Enrich 補充
  │     三層匹配: 全址 → 日期+總價 → 去樓層基礎地址
  │     └── 補充: lat/lng, 社區名, 縣市, 建物型態, 房廳衛...
  │
  └── Phase C — 社區名回填
        建立 (縣市, 行政區, 門牌) → 社區名 映射
        └── 批次 UPDATE 匹配的空白記錄

Phase 3: 索引 + FTS5 + ANALYZE + VACUUM
```

### Phase A 去重邏輯

```
API 記錄 → 正規化地址 + 日期
  ├── (日期前7碼, 正規化地址) 已存在 → 跳過 (地址重複)
  ├── (日期前7碼, 總價) 已存在 → 跳過 (價格重複)
  ├── 地址缺 "號" → 丟棄 (資料缺損)
  └── 通過 → INSERT 新記錄 (serial_no = 'api_' + sq)
```

### Phase B Enrich 欄位

| 欄位 | 判空條件 | 來源 |
|------|----------|------|
| `lat` | NULL 或 0 | raw_json.lat |
| `lng` | NULL 或 0 | raw_json.lon |
| `community_name` | 空字串 | transactions.community |
| `county_city` | 空字串 | CITY_CODE_MAP[city] |
| `building_type` | 空字串 | raw_json.b |
| `main_use` | 空字串 | raw_json.pu / AA11 |
| `has_management` | 空字串 | raw_json.m |
| `rooms` | NULL | raw_json.j |
| `halls` | NULL | raw_json.k |
| `bathrooms` | NULL | raw_json.l |
| `building_area` | NULL 或 0 | raw_json.s |
| `unit_price` | NULL 或 0 | raw_json.cp |
| `transaction_type` | 空字串 | raw_json.t |
| `floor_level` | 空字串 | raw_json.f (解析) |
| `total_floors` | 空字串 | raw_json.f (解析) |
| `note` | 空字串 | raw_json.note |

## 資料來源比較

| 特性 | CSV (`ALL_lvr_land_a.csv`) | API DB (`transactions_all_original.db`) |
|------|----------------------------|-----------------------------------------|
| 筆數 | ~470 萬 | ~420 萬 |
| 縣市資訊 | ❌ (僅有區名) | ✅ (城市代碼 A-Z) |
| 經緯度 | ❌ | ✅ |
| 社區名 | ❌ | ✅ (部分) |
| 建材/車位 | ✅ | ❌ |
| 土地面積 | ✅ | ❌ |
| 完整原始欄位 | ✅ (33 欄) | 部分 (raw_json 中) |

合併後的 `land_data.db` 結合兩者優勢，達到最高資料完整度。

## 資料庫 Schema

### 主表: `land_transaction` (45 欄)

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

## 與 merge_csv_api 的對照

| merge_and_enrich.py | convert.py --source both |
|---------------------|--------------------------|
| Step 1: merge (需先有 DB) | Phase A: 去重插入 (同流程自動建) |
| Step 2: enrich (15 欄位) | Phase B: enrich (**16 欄位**, +county_city) |
| Step 3: backfill community | Phase C: 社區回填 (相同邏輯) |
| `serial_no = '591_' + sq` | `serial_no = 'api_' + sq` |
| 自己的 DISTRICT_CITY_MAP | 共用 `address_utils.py` |
| 自己的 norm_addr / 無消歧 | 共用 `parse_address()` + `city_hint` 消歧 |
| 需手動依序執行 3 步 | 一次 `--source both` 全部完成 |

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
