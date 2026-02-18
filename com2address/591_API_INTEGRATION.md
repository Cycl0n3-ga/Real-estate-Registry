# 591 API 集成方案

## 當前架構

```
com2address 系統
├── 數據源1: address_community_mapping.csv (23,664 筆)
├── 數據源2: ALL_lvr_land_b.csv (486,305 筆建案交易)
└── 數據源3: manual_mapping.csv (手動對照)
```

## 591 API 集成優勢

| 特性 | 本地CSV | 591 API |
|------|--------|---------|
| **更新速度** | 靜態（需手動更新） | 📡 實時 |
| **建案覆蓋** | 48.6萬筆 | 📊 更多（全臺所有房產） |
| **圖片/介紹** | ❌ 無 | 📸 有 |
| **實時價格** | ❌ 無 | 💰 有 |
| **查詢速度** | ⚡ 毫秒級 | ⏱️ 秒級 |

## 推薦的三層架構

```
用户查詢 "健安新城F區"
       ↓
  【第1層】本地CSV索引
  ├─ 如果找到 → 🚀 立即返回（<1ms）
  └─ 如果未找到 ↓
       ↓
  【第2層】591 API 補充
  ├─ 如果啟用591 → 📡 調用API（~500ms）
  ├─ 快取結果以加速
  └─ 如果找到 → 返回
       ↓
  【第3層】手動對照表
  └─ 如果仍未找到 → 查詢 manual_mapping.csv
```

## 集成步驟

### 方案A：簡單版本（不需修改現有代碼）

```bash
# 1. 安裝依賴
pip install requests

# 2. 測試591 API模組
python3 591_api_integration.py

# 3. 獨立使用
python3 -c "
from 591_api_integration import Api591Client
client = Api591Client()
result = client.search_community('健安新城F區')
print(result)
"
```

### 方案B：完整集成（推薦）

修改 `community2address.py`，在 `lookup()` 函數中添加591 API 支持：

```python
# 在 community2address.py 中添加

from 591_api_integration import Api591Client, HybridLookup

# 初始化混合查詢
hybrid = HybridLookup(local_data=com2address_dict, use_591=True)

# 修改 lookup() 函數
def lookup(name, use_591=True):
    if use_591:
        return hybrid.lookup(name)
    else:
        # 原有邏輯（僅使用本地數據）
        ...
```

### 方案C：API 端點集成

修改 `app.py`，添加新的 API 端點：

```python
@app.route('/api/com2address/hybrid', methods=['GET'])
def com2address_hybrid():
    """混合查詢端點 - 支持本地 + 591 API"""
    name = request.args.get('name', '').strip()
    use_591 = request.args.get('use_591', 'true').lower() == 'true'
    
    result = hybrid_lookup.lookup(name, use_591=use_591)
    
    return jsonify({
        'success': result.get('success', True),
        'name': name,
        'source': result.get('source', '未知'),
        'data': result
    })
```

## 使用範例

### 命令列（添加591支持後）

```bash
# 只使用本地數據
python3 community2address.py "健安新城F區"

# 使用本地 + 591 API（如果本地未找到）
python3 community2address.py --with-591 "新竹科技新貴"

# 優先使用591 API
python3 community2address.py --prefer-591 "台中豪宅"
```

### HTTP API（添加591支持後）

```bash
# 使用本地 + 591 API
curl "http://localhost:5000/api/com2address/hybrid?name=健安新城F區&use_591=true"

# 返回格式
{
    "success": true,
    "name": "健安新城F區",
    "source": "591",  # 或 "本地數據"
    "data": {
        "district": "松山區",
        "address": "三民路29巷1、3、5、7號",
        "transaction_count": 12,
        ...
    }
}
```

## 重要注意事項

### ⚠️ 591 合法性問題

1. **檢查Terms of Service**
   - 訪問 https://www.591.com.tw 的 robots.txt
   - 確認是否允許爬取

2. **反爬蟲防護**
   - 591 可能有IP限流機制
   - 需設置合理的延遲和User-Agent
   - 建議使用快取避免頻繁請求

3. **資料授權**
   - 確認591允許數據在本系統中使用
   - 必要時聯絡591取得授權

### 🔒 最佳實踐

```python
# 使用快取減少API調用
client = Api591Client(cache_dir='/tmp/591_cache')

# 設置請求延遲
time.sleep(0.5)  # 每個請求間隔500ms

# 監控失敗率
success_rate = successful_requests / total_requests
if success_rate < 0.5:
    print("⚠️  591 API 可用性低，切換到本地模式")
    hybrid.use_591 = False
```

## 效能分析

### 查詢時間對比

| 場景 | 時間 | 說明 |
|------|------|------|
| 本地CSV命中 | ~1ms | 最快 |
| 本地CSV未命中，591 API | ~500ms | 可接受 |
| 本地CSV未命中，591 API超時 | ~5s後降級到本地 | 優雅降級 |

### 快取效果

- 首次591查詢: ~500ms
- 後續快取查詢: ~1ms
- **推薦快取有效期**: 24-48小時

## 替代方案對比

| 方案 | 優勢 | 劣勢 |
|------|------|------|
| **純本地CSV** ✅ | 快速、穩定、無依賴 | 更新遲滯、覆蓋不全 |
| **純591 API** | 實時、覆蓋廣 | ❌ 慢、反爬風險、依賴網路 |
| **混合方案** ✅ | 結合兩者優勢 | ✅ 複雜度稍高 |

## 後續改進

- [ ] 591 API 認證機制（如需要）
- [ ] 數據同步排程任務
- [ ] 591與本地數據版本控制
- [ ] 錯誤恢復機制

---

**當前狀態**: ✅ 591 模組已創建，可隨時集成
