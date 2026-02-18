# 建案名稱→地址範圍 查詢工具 (com2address)

功能與 `address2com` 完全相反。輸入建案/社區名稱，回傳該建案涵蓋的地址範圍。

## 特色

🏘️ **反向查詢**
- 輸入建案名稱 → 輸出地址範圍
- 例: `健安新城F區` → `三民路29巷1、3、5、7號`

📊 **雙資料來源**
1. `address_community_mapping.csv` (address2com 建立的對照表)
2. `ALL_lvr_land_b.csv` (48.6萬筆建案交易紀錄)
3. `manual_mapping.csv` (手動新增對照)

🔍 **智慧匹配**
- 精確匹配 (100%)
- 模糊匹配 (包含匹配 80%、部分匹配 50%+)
- 自動候選建議

## 安裝

不需要額外套件，僅使用 Python 3 標準庫。

## 使用方式

### 1. 命令列查詢

```bash
python3 community2address.py "健安新城F區"
```

輸出：
```
🏘️  健安新城F區
   → 精確匹配
   📍 區域: 松山區
   📊 交易筆數: 12
   📬 地址數: 4

   🏠 三民路29巷1、3、5、7號
```

### 2. 互動模式

```bash
python3 community2address.py
```

### 3. 詳細地址

```bash
python3 community2address.py --detail "都廳大院"
```

### 4. JSON 輸出

```bash
python3 community2address.py -j "信義星池"
```

### 5. 搜尋建案名稱

```bash
python3 community2address.py --search "健安"
```

### 6. 模組匯入

```python
from community2address import lookup, quick_lookup

# 詳細結果
result = lookup("健安新城F區")
print(result["address_range"]["summary"])  # "三民路29巷1、3、5、7號"

# 簡單結果
print(quick_lookup("健安新城F區"))  # "三民路29巷1、3、5、7號"
```

## API 端點

伺服器啟動後，可使用 HTTP API：

```
GET /api/com2address?name=健安新城F區
GET /api/com2address/search?keyword=健安&limit=10
```

### 回應範例

```json
{
    "success": true,
    "input": "健安新城F區",
    "matched_name": "健安新城F區",
    "match_type": "精確匹配",
    "district": "松山區",
    "transaction_count": 12,
    "address_range": {
        "summary": "三民路29巷1、3、5、7號",
        "road_groups": [
            {
                "road": "三民路29巷",
                "numbers": [1, 3, 5, 7],
                "formatted": "三民路29巷1、3、5、7號",
                "count": 4
            }
        ],
        "total_addresses": 4
    }
}
```
