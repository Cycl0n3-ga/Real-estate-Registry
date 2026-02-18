# Address2Community 整合說明

## 功能概述

已成功將 `address2com` 地址查詢社區功能整合進 `app.py`，提供一個新的 API endpoint 來查詢地址對應的社區/建案名稱。

## API 端點

### GET /api/address2community

查詢地址對應的社區/建案名稱

**參數：**
- `address` (必填): 要查詢的地址

**回應範例：**
```json
{
  "success": true,
  "input": "敦化北路123號",
  "normalized": "敦化北路123號",
  "best": "環翠名宮",
  "results": [
    {
      "community": "環翠名宮",
      "confidence": 98,
      "match_level": "完整地址精確匹配",
      "district": "松山區",
      "source": "實價登錄",
      "count": 45
    }
  ]
}
```

## 使用方式

### 1. 命令列測試

```bash
# 測試單一地址
python3 test_address2com.py "台北市松山區敦化北路123號"

# 批次測試
python3 test_address2com.py
```

### 2. curl 測試

```bash
curl "http://localhost:5000/api/address2community?address=敦化北路123號"
```

### 3. Python 程式

```python
import urllib.request
import urllib.parse
import json

address = "敦化北路123號"
url = f'http://localhost:5000/api/address2community?address={urllib.parse.quote(address)}'

with urllib.request.urlopen(url) as r:
    data = json.loads(r.read())
    print(f"最佳結果: {data['best']}")
```

### 4. JavaScript (前端)

```javascript
const address = "敦化北路123號";
fetch(`/api/address2community?address=${encodeURIComponent(address)}`)
  .then(res => res.json())
  .then(data => {
    console.log('社區名稱:', data.best);
    console.log('所有結果:', data.results);
  });
```

## 匹配級別說明

系統使用多層級匹配策略，由精確到模糊：

1. **完整地址精確匹配** (信心度 98%)
   - 正規化後的地址完全匹配資料庫記錄

2. **門牌號匹配** (信心度 90%)
   - 路段 + 門牌號匹配（如：民族路25號）

3. **巷弄匹配** (信心度 72%)
   - 路段 + 巷號匹配（如：民族路25巷）

4. **路段匹配** (信心度 40%)
   - 僅路段名稱匹配（如：民族路）

## 資料來源

- **本地 CSV**: `/home/cyclone/land/address2com/address_community_mapping.csv`
- 包含超過 10 萬筆地址與社區的對照關係
- 資料來源：實價登錄、手動標記

## 技術細節

### 地址正規化

系統會自動進行以下處理：
- 去除縣市、區域資訊
- 去除里鄰資訊
- 去除樓層資訊
- 全形轉半形
- 統一格式

例如：
- 輸入: `台北市松山區民生里敦化北路123號5樓`
- 正規化: `敦化北路123號`

### 索引結構

為提升查詢效能，建立了四種索引：
1. `normalized`: 正規化地址索引
2. `to_number`: 到號地址索引（路段+門牌）
3. `to_alley`: 到巷地址索引（路段+巷號）
4. `road`: 路段索引

## 整合內容

在 `app.py` 中新增的功能：

1. **設定區塊**: 新增 `ADDRESS2COM_CSV` 路徑
2. **全域狀態**: 新增 `_address2com_data` 和 `_address2com_indices`
3. **工具函數**:
   - `normalize_address_a2c()`: 地址正規化
   - `extract_road_number_a2c()`: 提取門牌號
   - `extract_road_alley_a2c()`: 提取巷號
   - `extract_road_a2c()`: 提取路段
   - `load_address2com_data()`: 載入 CSV 資料
   - `query_address2community()`: 查詢主函數
4. **API 路由**: `/api/address2community`
5. **初始化**: 在 `init_data()` 中自動載入資料

## 測試結果

```
✅ 敦化北路123號 → 環翠名宮 (98% 信心度)
✅ 英才路443號 → 勝美La ONE (98% 信心度)
✅ 三民路29號 → 久樘花漾天鵝 (40% 信心度，路段匹配)
```

## 注意事項

1. 確保 CSV 檔案存在於指定路徑
2. 如果 CSV 不存在，系統會顯示警告但不會中斷
3. 信心度低於 70% 的結果可能不準確，建議結合其他資訊驗證

## 未來擴展

可考慮整合以下功能：
- 591 API 線上查詢（當本地查不到時）
- 手動標記功能
- 自動學習與更新
- 模糊匹配演算法優化
