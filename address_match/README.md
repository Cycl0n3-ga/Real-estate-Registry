# 不動產交易地址模糊搜尋工具 (v2 高效能版)

`address_match.py` 是一個專為台灣不動產實價登錄資料設計的極速地址搜尋工具。相較於第一代，v2 版本透過**預先分離地址結構**、**SQLite 數值優化**、以及**三段式智慧搜尋策略**，將原本需要數分鐘的全表掃描搜尋，縮短至 **0.1 秒** 內即可完成。

## 🚀 效能與優化亮點

- **巨幅效能提升**：藉由 B-Tree 複合索引，95% 查詢將於 `0.1s` 秒查回報。
- **三段智慧搜尋 (Fallback Strategy)**：
  1. **結構化索引搜尋 (首選)**：將輸入（如：`信義區松智路1號`）自動拆解為 `district`, `street`, `number` 並走索引比對。
  2. **FTS5 全文檢索**：針對不規則的輸入文字，底層透過倒排索引高速匹配。
  3. **LIKE 變體搜尋 (最後備援)**：自動產生數十種全形/半形、中文數字變體（`５`、`五`、`5`）去撈取極端髒資料。
- **多重「之」與複雜門牌解析**：針對台灣特有複雜門牌規則（如 `53之8號`、`53號12樓之8` 以及 `號之` 等），能準確拆分為 `number` 與 `sub_number`，解決過往黏在一起無法準確比對的問題。
- **路名段數標準化**：路名段數統一使用**國字**（如 `市民大道三段`），無論輸入是 `3段` 還是 `三段` 都能精準匹配，且顯示格式標準統一。
- **精確巷弄匹配**：搜尋 `53號` 不會錯誤匹配到 `143巷53號`，確保留結果屬於同一棟建築或社區。
- **支援多元樓層格式**：支援 `3F`, `3層`, `三樓` 等多種樓層輸入與解析，輸出格式統一為乾淨的 `3F` 格式。
- **乾淨半形表格輸出**：去除實價登錄原始資料中雜亂的全形字元與髒字（如 `５３之８號十二樓`），全面替換為乾淨易讀的格式化地址（如 `53號12F之8`）。
- **儲存優化**：全面改用原生 `INTEGER` / `REAL` 儲存面積與價格，資料庫體積縮減 30% 並大幅降低 I/O 成本。

## 📊 資料庫 Schema (`land_data.db`)

資料表：**`land_transaction`**

### 原始欄位（來自 CSV）

| 欄位名稱 | 型態 | 說明 |
|---|---|---|
| `id` | INTEGER (PK) | 自動遞增主鍵 |
| `raw_district` | TEXT | 原始鄉鎮市區 |
| `transaction_type` | TEXT | 交易標的（房地、車位等） |
| `address` | TEXT | 土地位置或建物門牌地址 |
| `land_area` | REAL | 土地移轉總面積（平方公尺） |
| `urban_zone` | TEXT | 都市土地使用分區 |
| `non_urban_zone` | TEXT | 非都市土地使用分區 |
| `non_urban_use` | TEXT | 非都市土地使用地類別 |
| `transaction_date` | TEXT | 交易年月日（民國） |
| `transaction_count` | TEXT | 交易筆棟數 |
| `floor_level` | TEXT | 移轉層次 |
| `total_floors` | TEXT | 總樓層數 |
| `building_type` | TEXT | 建物型態（公寓、住宅大樓等） |
| `main_use` | TEXT | 主要用途 |
| `main_material` | TEXT | 主要建材 |
| `build_date` | TEXT | 建築完成年月 |
| `building_area` | REAL | 建物移轉總面積（平方公尺） |
| `rooms` | INTEGER | 隔間（房） |
| `halls` | INTEGER | 隔間（廳） |
| `bathrooms` | INTEGER | 隔間（衛） |
| `partitioned` | TEXT | 有無隔間 |
| `has_management` | TEXT | 有無管理組織 |
| `total_price` | INTEGER | 總價（元） |
| `unit_price` | REAL | 單價（元/平方公尺） |
| `parking_type` | TEXT | 車位類別 |
| `parking_area` | REAL | 車位移轉總面積（平方公尺） |
| `parking_price` | INTEGER | 車位總價（元） |
| `note` | TEXT | 備註 |
| `serial_no` | TEXT | 編號 |
| `main_area` | REAL | 主建物面積 |
| `attached_area` | REAL | 附屬建物面積 |
| `balcony_area` | REAL | 陽台面積 |
| `elevator` | TEXT | 有無電梯 |
| `transfer_no` | TEXT | 移轉編號 |

### 解析後地址欄位（由 `address` 自動拆解）

| 欄位名稱 | 型態 | 說明 |
|---|---|---|
| `county_city` | TEXT | 縣市（例：台北市） |
| `district` | TEXT | 鄉鎮市區（例：大安區） |
| `village` | TEXT | 里（例：正義里） |
| `street` | TEXT | 街路名（例：忠孝東路3段） |
| `lane` | TEXT | 巷號 |
| `alley` | TEXT | 弄號 |
| `number` | TEXT | 門牌號（例：12之3） |
| `floor` | TEXT | 樓層 |
| `sub_number` | TEXT | 樓之幾（例：之2） |

### 預留欄位（目前為 NULL）

| 欄位名稱 | 型態 | 說明 |
|---|---|---|
| `community_name` | TEXT | 社區名稱 |
| `lat` | REAL | 緯度 |
| `lng` | REAL | 經度 |

### 虛擬表：`address_fts`（FTS5 全文檢索）

對 `address` 欄位建立倒排索引（tokenize=`unicode61`），對應 `land_transaction.id`，可用 `MATCH` 語法進行地址全文搜尋。

### 索引

| 索引名稱 | 欄位 | 說明 |
|---|---|---|
| `idx_county_city` | `county_city` | 縣市單欄索引 |
| `idx_district` | `district` | 行政區單欄索引 |
| `idx_street` | `street` | 街路單欄索引 |
| `idx_lane` | `lane` | 巷單欄索引 |
| `idx_number` | `number` | 門牌號單欄索引 |
| `idx_floor` | `floor` | 樓層單欄索引 |
| `idx_date` | `transaction_date` | 交易日期索引 |
| `idx_price` | `total_price` | 總價索引 |
| `idx_serial` | `serial_no` | 編號索引 |
| `idx_addr_combo` | `county_city, district, street, lane, number` | 地址複合索引（最常用查詢路徑） |

## 💻 使用方式

### 基本搜尋

自動支援全半形及中文數字轉換：

```bash
cd /home/cyclone/land

# 搜尋特定巷弄
python3 address_match/address_match.py "三民路29巷"

# 包含門牌號碼
python3 address_match/address_match.py "日興一街52號"

# 包含完整縣市區與樓層
python3 address_match/address_match.py "台北市松山區三民路29巷1號3樓"
```

### 🔍 條件篩選 `--XXX`

你可以自由組合多個參數來精準過濾房產：

- `--type` (建物型態)：支援多選、模糊搜尋（例如找 `公寓`、`住宅大樓`）
- `--rooms` (房數)：支援多選（例如 `--rooms 2 3`，找 2 到 3 房）
- `--year` (交易年份)：使用**民國年**，支援區間或單一年份（例如 `--year 110-114`）
- `--ping` (坪數)：總坪數區間（例如 `--ping 30-50`）
- `--price` (總價)：區間設定，單位是**萬元**（例如 `--price 1000-2000`）
- `--unit-price` (單坪價)：區間設定，單位是**萬/坪**
- `--public-ratio` (公設比)：區間設定，單位是 `%`（如 `--public-ratio 0-30`）

### 📌 排序方式 `--sort` 

- `date`：交易日期從新到舊（預設）
- `price`：總價從高到低（最貴優先）
- `unit_price`：單坪價格從高到低
- `count`：同地址交易筆數從高到低（熱門交易社區優先）
- `ping`：坪數從大到小
- `public_ratio`：公設比從小到大（低公設優先）

### 🛠️ 輸出與進階選項

- `--limit`：限制顯示筆數（預設 200）
- `--export <檔名.csv>`：將結果匯出成 CSV 檔供後續分析
- `--show-sql`：顯示底層用到什麼策略與解析結果（除錯用）
- `--no-variants`：不印出程式自動產生的搜尋變體列表，讓畫面更簡潔

---

## 🏆 實用組合範例

**範例一：找尋特定社區/路段有沒有「3房」的「大樓」，且近四年交易**
```bash
python3 address_match/address_match.py "日興一街52號" --type 住宅大樓 --rooms 3 --year 110-114
```

**範例二：在這個路段上，尋找總價在 1000~2000 萬，且坪數在 30~50 坪的公寓**
```bash
python3 address_match/address_match.py "三民路29巷" --type 公寓 --price 1000-2000 --ping 30-50
```

**範例三：找這條路上「最貴」或「單價最高」的交易**
```bash
# 用單坪價高到低排序，限制只看前 10 筆
python3 address_match/address_match.py "三民路" --sort unit_price --limit 10
```

**範例四：找低公設的大房子，並將結果匯出作報告**
```bash
# 找公設比小於 30% ( -30 就是上限 30 的意思 )，超過 40 坪的房子，存到 csv
python3 address_match/address_match.py "光復南路" --public-ratio -30 --ping 40- --export result.csv
```
