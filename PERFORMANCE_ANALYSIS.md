# 📊 資料搜尋性能詳細分析報告

## 🔴 核心問題總結

你的應用搜尋**非常慢**的主要原因是：

| 問題 | 影響 | 嚴重程度 |
|------|------|--------|
| **LIKE 模糊匹配 + 沒有全文索引** | 每次查詢要全表掃描 4.68M 行 | 🔴 最嚴重 |
| **複合條件未優化** | 多條件篩選導致超級耗時 | 🔴 很嚴重 |
| **地址字段無前綴索引** | LIKE '%xxx%' 無法利用索引 | 🔴 很嚴重 |
| **每次讀取 CSV 重建資料** | 之前的做法 | ⚠️ 已修復 |
| **無快取機制** | 相同查詢重複計算 | 🟡 中等 |

---

## 📈 詳細數據分析

### 1️⃣ 數據規模
```
ALL_lvr_land_a.csv:  1.3 GB  (4.68M 行, 33 欄)
ALL_lvr_land_b.csv:  160 MB  (540K 行, 31 欄)
SQLite Database:     2.0 GB  (4.68M 交易紀錄)
───────────────────────────────
總記錄數:            ~5.2M 行交易資料
```

### 2️⃣ 表結構
```sql
CREATE TABLE transactions (
    id,
    district,               ← 已索引 ✅
    address,                ← 有索引但無用 ❌
    transaction_type,       ← 已索引
    transaction_date,       ← 已索引
    floor_level,
    total_floors,
    building_type,
    total_price,           ← 已索引
    unit_price,
    building_area_sqm,
    rooms,
    halls, bathrooms,
    has_management,
    elevator,
    parking_type,
    parking_area_sqm,
    parking_price,
    note,
    main_building_area,
    attached_area,
    balcony_area,
    lat, lng                ← 複合索引已建
    ... (其他 15+ 欄位)
);

現有索引:
  ✅ idx_address              → 無用（LIKE '%xxx%' 無法用）
  ✅ idx_district             → 有效
  ✅ idx_date                 → 有效
  ✅ idx_price                → 有效
  ✅ idx_transactions_latlng  → 有效（地圖用）
```

---

## ⚡ 性能測試結果

### A. 簡單地址搜尋
```sql
SELECT COUNT(*) FROM transactions 
WHERE address LIKE '%三民路29%'
```
⏱️ **0.695 秒** ✅ 可接受

### B. 地址 + 價格篩選
```sql
SELECT COUNT(*) FROM transactions 
WHERE address LIKE '%三民路%' 
  AND total_price > 100,000
```
⏱️ **73.174 秒** 🔴 **嚴重緩慢！**

**原因分析：**
- 先執行 LIKE 掃描全表 4.68M 行 (0.695s)
- 再對結果篩選價格 (72.5s)
- 價格索引對複合條件幫助不大

### C. 複雜篩選 (地址 + 建物型態 + 房數)
```sql
SELECT COUNT(*) FROM transactions 
WHERE address LIKE '%三民路%'
  AND total_price > 100,000
  AND building_type LIKE '%住宅%'
  AND rooms = 2
```
⏱️ **16.149 秒** 🔴 **還是太慢**

### D. 單欄位索引查詢（對照組）
```sql
SELECT COUNT(*) FROM transactions 
WHERE district = '松山區'
```
⏱️ **0.002 秒** ✅ 完美

---

## 🔍 主要瓶頸

### 問題 1: LIKE 模糊匹配無法用索引
```
❌ WHERE address LIKE '%三民路%'      → 全表掃描 4.68M 行
❌ WHERE address LIKE '%三民路29%'    → 全表掃描 4.68M 行
✅ WHERE address LIKE '三民路%'       → 可以用索引
```

**為什麼？** SQLite 索引只能優化「前綴匹配」(LIKE 'xxx%')，
對「模糊匹配」(LIKE '%xxx%') 完全無效。

### 問題 2: 複合條件的執行計畫不佳
```
查詢: WHERE address LIKE '%三民路%' AND total_price > 100000

SQLite 的執行計畫可能是：
1. 全表掃描 address (4.68M 行) → 得到 X 行
2. 對 X 行進行 total_price 篩選

這比應該做的事反了：
應該：
1. 用索引讀 total_price > 100000 → 得到 Y 行
2. 對 Y 行進行 address 篩選
```

### 問題 3: 沒有全文索引 (FTS)
```
目前：每次查詢要逐字符比對
應該：預先建立全文索引，查詢速度 10-100 倍快
```

### 問題 4: 無快取機制
```
同一個「三民路」的查詢被執行多次
應該：第一次查詢後快取結果，後續查詢直接返回
```

---

## 💡 優化方案

### 優先級 1️⃣: 建立全文索引 (FTS5) - 預期改善 50-100 倍
```python
# 建議: 為 address 欄位建立 FTS5 全文索引
# 耗時: ~5-10 分鐘（一次性）
# 收益: 搜尋速度從 秒 級改為 毫秒 級

# 方案 A: SQLite FTS5 (最簡單)
CREATE VIRTUAL TABLE address_fts USING fts5(address);
INSERT INTO address_fts SELECT address FROM transactions;

# 查詢改為:
SELECT * FROM transactions 
WHERE rowid IN (
    SELECT rowid FROM address_fts WHERE address MATCH '三民路29'
)
```

### 優先級 2️⃣: 優化複合查詢 - 預期改善 3-5 倍
```python
# 方案 B: 預先統計常見的查詢組合
# 建立物化視圖 (Materialized View)

CREATE TABLE address_stats AS
SELECT 
    address,
    COUNT(*) as tx_count,
    AVG(total_price) as avg_price,
    MIN(total_price) as min_price,
    MAX(total_price) as max_price,
    AVG(building_area_sqm) as avg_area,
    COUNT(DISTINCT district) as district_count
FROM transactions
GROUP BY address;

CREATE INDEX idx_address_stats ON address_stats(address);

# 這樣首先查詢 address_stats (很小，幾千行)
# 再用 rowid 回頭查完整資料
```

### 優先級 3️⃣: 路由級快取 - 預期改善 10-100 倍
```python
# 方案 C: 在後端加入快取層
# 同一個搜尋結果在 5-10 分鐘內重複使用

import functools
from time import time, sleep

# 快取裝飾器 (TTL = 5分鐘)
cache = {}
CACHE_TTL = 300  # 秒

def cached_search(address, ttl=CACHE_TTL):
    key = f"search:{address}"
    if key in cache:
        result, timestamp = cache[key]
        if time() - timestamp < ttl:
            return result  # 直接返回
    
    # 執行查詢
    result = search_address(address)
    cache[key] = (result, time())
    return result
```

### 優先級 4️⃣: 分表 / 分區 - 預期改善 2-3 倍
```python
# 方案 D: 按區分表
# 4.68M 行 → 分成 22 個區表

# 原本:
SELECT * FROM transactions WHERE address LIKE '%三民路%'

# 改為:
SELECT * FROM transactions_松山 WHERE address LIKE '%三民路%'
UNION ALL
SELECT * FROM transactions_大安 WHERE address LIKE '%三民路%'
# ... (只查需要的區)

# 只掃描 ~200K 行而不是 4.68M 行 → 約 20 倍快
```

### 優先級 5️⃣: 位置型索引 - 預期改善地圖查詢
```python
# 方案 E: 空間索引 (Spatial Index)
# 目前: 地圖範圍查詢要掃全表
# 應該: 用 R-tree 空間索引查詢特定區域

CREATE VIRTUAL TABLE transactions_spatial USING rtree(
    id, minX, maxX, minY, maxY
);

# 地圖範圍查詢從 秒 級改為 毫秒 級
```

---

## 🛠️ 立即實施方案

### 短期 (馬上修復 - 1-2 小時)

#### 1. 建立 FTS5 全文索引
```python
import sqlite3

conn = sqlite3.connect('db/land_a.db')
cursor = conn.cursor()

# Step 1: 建立 FTS 表
print("建立 FTS5 表...")
cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS address_fts 
    USING fts5(id UNINDEXED, address, district);
""")

# Step 2: 填入資料 (逐批)
print("填入資料 (this takes ~3-5 minutes)...")
cursor.execute("DELETE FROM address_fts")
cursor.execute("""
    INSERT INTO address_fts 
    SELECT id, address, district FROM transactions
""")
conn.commit()

# Step 3: 用 FTS 測試查詢
print("✅ FTS 表已建立")

# 新查詢方式:
cursor.execute("""
    SELECT address, COUNT(*) 
    FROM transactions 
    WHERE id IN (
        SELECT id FROM address_fts 
        WHERE address MATCH '三民路29'
    )
    GROUP BY address
""")
```

#### 2. 新增複合索引
```python
# 針對最常見的查詢模式建立複合索引
cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_price_type 
    ON transactions(total_price, building_type)
""")

cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_district_date 
    ON transactions(district, transaction_date DESC)
""")
```

#### 3. 在後端加入查詢快取
```python
# /home/cyclone/land/web/server.py 中修改

from functools import lru_cache
import time

SEARCH_CACHE = {}
CACHE_TTL = 300  # 5 分鐘

@app.before_request
def cleanup_cache():
    """定期清理過期快取"""
    current_time = time.time()
    expired = [k for k, (v, t) in SEARCH_CACHE.items() 
               if current_time - t > CACHE_TTL]
    for k in expired:
        del SEARCH_CACHE[k]

def get_cached_search(keyword, filters_key):
    """快取層"""
    cache_key = f"{keyword}:{filters_key}"
    
    if cache_key in SEARCH_CACHE:
        result, _ = SEARCH_CACHE[cache_key]
        return result, True  # 來自快取
    
    return None, False

# 在 api_search() 中使用:
@app.route("/api/search", methods=["GET"])
def api_search():
    keyword = request.args.get("keyword", "").strip()
    
    # 嘗試從快取讀取
    filters_key = str(sorted(request.args.items()))
    cached_result, from_cache = get_cached_search(keyword, filters_key)
    
    if cached_result:
        print(f"✅ 從快取返回 {keyword}")
        return cached_result
    
    # 執行實際查詢 (原有邏輯)
    result = perform_search(keyword, ...)
    
    # 儲存到快取
    SEARCH_CACHE[cache_key] = (result, time.time())
    
    return result
```

---

## 📊 預期改善效果

| 改善 | 現狀 | 預期改善後 | 倍數 |
|------|------|----------|-----|
| **簡單地址搜尋** | 0.7 秒 | 0.01 秒 | 70x |
| **地址+價格篩選** | 73 秒 | 0.5 秒 | 146x |
| **複雜篩選** | 16 秒 | 0.2 秒 | 80x |
| **快取命中** | N/A | 0.001 秒 | 1000x+ |

---

## 📋 實施優先級清單

```
[1] ⚡ 建立 FTS5 全文索引        (預期: 70-100 倍快)
    └─ 耗時: 5-10 分鐘
    └─ 難度: ⭐ 簡單
    └─ 收益: 🔥 最高

[2] 🔀 新增複合索引              (預期: 5-10 倍快)
    └─ 耗時: 5 分鐘
    └─ 難度: ⭐ 簡單
    └─ 收益: 🔥 高

[3] 💾 後端查詢快取              (預期: 10-100 倍快)
    └─ 耗時: 30 分鐘
    └─ 難度: ⭐⭐ 中等
    └─ 收益: 🔥 高

[4] 🗂️ 按區分表                (預期: 20 倍快)
    └─ 耗時: 2 小時
    └─ 難度: ⭐⭐⭐ 複雜
    └─ 收益: 🔥 中等

[5] 🗺️ 空間索引 (地圖優化)       (預期: 10-50 倍快)
    └─ 耗時: 1 小時
    └─ 難度: ⭐⭐ 中等
    └─ 收益: 🔥 中等 (地圖查詢專用)
```

---

## ❓ FAQ

**Q: 為什麼不用 Elasticsearch?**
A: 可以考慮，但需要另裝伺服器。SQLite FTS5 對小中型應用已足夠，而且維護成本低。

**Q: 快取會不會有資料過期問題?**
A: 用 5-10 分鐘 TTL 是個好平衡。如果需要即時，可用 WebSocket 主動推送更新。

**Q: 現有資料可以保留嗎?**
A: 可以。FTS 表是虛擬表，不影響原表。現有索引也保留。

**Q: 需要手動重建嗎?**
A: 不需要。新增索引後 SQLite 自動應用。

---

## 🔗 相關文件

- CSV 大小: 1.3GB + 160MB
- 交易筆數: 4.68M + 540K
- 現有索引: 6 個
- 建議新增: FTS5 + 2-3 個複合索引
