# 不動產交易地址模糊搜尋工具

## 功能介紹

`address_transfer.py` 是一個強大的地址搜尋工具，可以自動處理地址中的各種變體，找出可能對應的不動產交易記錄。

### 支援的變體轉換

| 類型 | 範例 |
|------|------|
| 全形/半形數字 | `２９` ↔ `29` |
| 中文/阿拉伯數字 | `二十九` ↔ `29`、`七` ↔ `7` |
| 中文數字寫法 | `二九`、`二十九`、`廿九` |
| 全形/半形括號 | `（）` ↔ `()` |
| 全形/半形空格 | 自動處理 |

## 資料欄位

轉換後的 SQLite 資料庫包含 33 個欄位，包括：

### 核心信息
- **address**: 土地位置建物門牌
- **district**: 行政區（縣市 + 鄉鎮市區）
- **transaction_date**: 交易年月日（民國年）
- **total_price**: 總價（元）
- **unit_price**: 單價（元/平方公尺）

### 建物信息
- **building_type**: 建物型態
- **building_area_sqm**: 建物移轉總面積（平方公尺）
- **total_floors**: 總樓層數
- **floor_level**: 移轉層次（樓層標示）
- **rooms/halls/bathrooms**: 房/廳/衛數量
- **elevator**: 有無電梯

### 土地信息
- **land_area_sqm**: 土地移轉總面積（平方公尺）
- **urban_zone**: 都市土地使用分區
- **non_urban_zone**: 非都市土地使用分區

### 🚗 車位信息（新增）
- **parking_type**: 車位類別（塔式、坡道、路邊…）
- **parking_area_sqm**: 車位移轉面積（平方公尺）
- **parking_price**: 車位總價（元）

### 📝 其他信息
- **note**: 備註（交易特殊說明、增建、親友交易等）
- **transaction_type**: 交易標的（土地/建物/權利…）
- **has_management**: 有無管理組織
- **main_use**: 主要用途
- **main_material**: 主要建材

## 使用方式

### 基本搜尋

```bash
cd /home/cyclone/land/land_reg/address_search
python3 address_transfer.py "三民路29巷"
```

### 進階選項

```bash
# 限制回傳筆數
python3 address_transfer.py "日興一街6號七樓" --limit 50

# 顯示生成的 SQL 語句
python3 address_transfer.py "三民路29巷" --show-sql

# 匯出為 CSV
python3 address_transfer.py "三民路29巷" --export result.csv

# 指定資料庫路徑
python3 address_transfer.py "民生路" --db /path/to/land_a.db

# 不顯示搜尋變體列表
python3 address_transfer.py "三民路29巷" --no-variants
```

## 搜尋結果說明

搜尋結果會以表格形式展示，包含以下列：

- **#**: 序號
- **行政區**: 所在行政區
- **地址**: 完整地址（截斷至32字）
- **日期**: 交易年月日（民國年YYY/MM/DD）
- **樓層**: 移轉層次
- **型態**: 建物型態
- **總價**: 交易總價（萬元/億元格式）
- **單價/㎡**: 每平方公尺單價
- **面積㎡**: 建物面積
- **格局**: 房/廳/衛數量
- **🚗 車位**: 車位類別及價格（若有）
- **📝 備註**: 交易特殊說明（若有）

## 範例

### 例1：搜尋「三民路29巷」
```
$ python3 address_transfer.py "三民路29巷" --limit 5

搜尋地址：三民路29巷
搜尋變體（5 個）：
   • 三民路29巷
   • 三民路２９巷
   • 三民路二十九巷
   • 三民路二九巷
   • 三民路廿九巷

共找到 74 筆交易記錄
```

### 例2：搜尋「日興一街6號七樓」（中英文數字混合）
```
$ python3 address_transfer.py "日興一街6號七樓"

搜尋變體（9 個）：
   • 日興一街6號7樓
   • 日興一街6號七樓
   • 日興一街6號７樓
   • 日興一街六號7樓
   • 日興一街六號七樓
   • 日興一街六號７樓
   • 日興一街６號7樓
   • 日興一街６號七樓
   • 日興一街６號７樓

共找到 1 筆交易記錄
```

## 資料庫統計

| 欄位 | 筆數 | 佔比 |
|------|------|------|
| 有車位類別 | 1,523,275 | 32.6% |
| 有車位價格 | 821,799 | 17.6% |
| 有備註 | 1,751,136 | 37.4% |
| **總筆數** | **4,678,450** | **100%** |

## 技術細節

### 架構
```
land_reg/
├── csv_to_sqlite.py          # CSV → SQLite 轉換腳本
├── land_a.db                 # SQLite 資料庫 (1.9 GB)
└── address_search/
    ├── address_transfer.py   # 主搜尋工具
    └── README.md             # 本文件
```

### 索引

資料庫已建立 5 個索引以加速查詢：

- `idx_address`: 地址欄位（最常用）
- `idx_district`: 行政區欄位
- `idx_date`: 交易日期欄位
- `idx_price`: 總價欄位
- `idx_type`: 交易標的欄位

### 執行效能

- CSV 轉換：~105 秒（460 萬筆）
- 單次搜尋：< 1 秒（5 項變體 × OR 連接）
- 資料庫大小：1.9 GB

## 常見用法

### 找出所有「松山區三民路29巷」的交易
```bash
python3 address_transfer.py "三民路29巷" --limit 200
```

### 查找特定房型（例如「4房2廳」在某條路上）
結果表格會顯示格局欄位，可篩選並參考。

### 分析車位市場
搜尋特定區域並查看車位欄位的價格變化。

### 找出有特殊交易說明的案件
備註欄位會顯示「親友交易」、「增建」等特殊情況。

## 故障排排查

### 找不到資料庫
確保先執行過 `csv_to_sqlite.py`：
```bash
cd /home/cyclone/land/land_reg
python3 csv_to_sqlite.py
```

### 搜尋結果為空
- 嘗試簡化搜尋關鍵字（例如只用「路名」）
- 檢查是否有輸入全形/半形混合
- 使用 `--show-sql` 查看生成的 SQL 語句

### 顯示格式異常
- 安裝 `tabulate` 以獲得更好的表格格式：`pip install tabulate`
- 或不安裝也可以，會自動用基本格式顯示

---

最後更新：2026/02/18
