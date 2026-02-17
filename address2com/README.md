# 地址→社區名稱 查詢工具

台灣地址到社區/建案名稱的查詢工具。支援 CSV 本地查詢 + 591房屋交易網即時 API 查詢。

## 特色

✨ **簡單易用**
- 純 CSV 格式，易於編輯和傳輸
- 人類易讀，檔案大小小（3.5 MB）

🚀 **591 API 線上查詢**
- 本地無資料時自動呼叫 591 API
- 提升查詢涵蓋率（台灣全國支援）

📊 **多層匹配**
1. 精確地址匹配 (confidence: 98%)
2. 門牌號匹配 (confidence: 90%)
3. 巷弄匹配 (confidence: 72%)
4. 路段匹配 (confidence: 40%)
5. 591 API 查詢 (confidence: 88%)

## 安裝

不需要額外安裝套件，僅使用 Python 3 標準庫。

```bash
# 首次使用：建立 CSV 資料
python3 build_db.py

# 或直接查詢（自動建立）
python3 address2community.py "三民路29巷5號"
```

## 快速開始

### 1. 命令列查詢

```bash
python3 address2community.py "松山區八德路四段445號八樓"
```

輸出：
```
📍 松山區八德路四段445號八樓
   → 🏘️  信義星池
   信心度: [████████░░] 88%
   匹配: 591 即時查詢
   區域: 信義區
```

### 2. 互動模式

```bash
python3 address2community.py
```

### 3. 批次查詢

```bash
python3 address2community.py --batch addresses.txt
```

### 4. JSON 輸出

```bash
python3 address2community.py -j "三民路29巷5號"
```

### 5. 模組匯入

```python
from address2community import lookup, quick_lookup

# 詳細結果
result = lookup("三民路29巷5號")
print(result["best"])  # "健安新城F區"

# 簡單結果
print(quick_lookup("三民路29巷5號"))  # "健安新城F區"
```

## 查詢流程

```
輸入地址 → 正規化（去城市/區/樓層）
    ↓
Level 1: CSV 精確匹配 (98%)
    ↓ 未找到
Level 2: 門牌號匹配 (90%)
    ↓ 未找到
Level 3: 巷弄匹配 (72%)
    ↓ 未找到
Level 4: 路段匹配 (40%)
    ↓ 信心度 < 70
Level 5: 591 API 線上查詢 (88%)
```

## 資料來源

| 來源 | 說明 | 筆數 |
|------|------|------|
| B 表 | ALL_lvr_land_b.csv (實價登錄預售屋) | 21,026 |
| 手動對照 | manual_mapping.csv (使用者新增) | 2,635+ |
| 591 API | bff.591.com.tw (線上即時查詢) | 無上限 |

## 檔案說明

| 檔案 | 說明 |
|------|------|
| `build_db.py` | 從 B 表建立 CSV 資料 |
| `address2community.py` | 查詢工具（CLI + 模組） |
| `address_community_mapping.csv` | CSV 資料庫（3.5 MB） |
| `manual_mapping.csv` | 手動對照表（可編輯） |

## 效能

| 指標 | 數值 |
|------|------|
| 啟動時間 | ~0.3 秒 |
| 每筆查詢 | ~0.001-0.2 秒 |
| CSV 大小 | 3.5 MB |
| 總記錄數 | 23,661 筆 |

## 重建資料庫

```bash
cd address2com
python3 build_db.py
```

## 指令列選項

```bash
python3 address2community.py [address] [options]

選項：
  -b, --batch FILE          批次查詢（檔案路徑）
  -d, --detail              顯示詳細結果
  --no-api                  停用 591 API（僅本地查詢）
  -v, --verbose             顯示匹配過程
  -j, --json                JSON 輸出
  --add ADDR COMMUNITY      新增手動對照
```

## 常見問題

**Q：查不到某個地址怎麼辦？**
A：程式會自動呼叫 591 API（預設開啟）。若仍找不到，可用 `--add` 手動新增。

**Q：如何更新資料？**
A：執行 `python3 build_db.py` 重新從 B 表建立。

**Q：能否離線使用？**
A：能。用 `--no-api` 關閉 591 API，僅使用本地 CSV。

## 版本歷史

**v2 (2026-02-18)** - CSV + 591 API
- 改用 CSV 格式（易編輯、易傳輸）
- 保留 591 API 線上查詢
- 簡化資料結構

**v1** - SQLite + 591 API
- SQLite 快速查詢
- 自動快取 API 結果

---

**最後更新**：2026-02-18# 完全重建（清除 591 快取）
python3 build_db.py

# 重建但保留 591 快取
python3 build_db.py --keep-cache
```
