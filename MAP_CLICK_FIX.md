# 地圖點擊功能修復說明

## 問題描述
點擊地圖標記後顯示「無交易紀錄」，因為地址無法正確轉換成建案名稱。

## 解決方案

### 整合 address2community API
在地圖的點擊處理函數中加入三層查詢邏輯：

1. **查詢建案名稱** (address2community API)
   - 將點擊的地址轉換成社區/建案名稱
   - 使用本地 CSV 資料庫（23,664 筆記錄）
   - 支援多層級匹配（精確→門牌→巷弄→路段）

2. **搜尋建案資料** (projects API)
   - 優先使用建案名稱搜尋
   - 備用方案：直接用地址搜尋
   - 確保能找到交易資料

3. **顯示交易詳情**
   - 載入該建案的所有交易記錄
   - 顯示建案名稱作為標題
   - 展示完整的交易統計和明細

## 修改檔案

### 1. liangfu_map.html
- 修改 `selectProject()` 函數，傳入建案名稱參數
- 重寫 `showProjectDetail()` 函數，整合 address2community 查詢
- 更新 `renderProjectDetail()` 函數，支援自訂標題

### 2. liangfu_map_v4.html
- 同樣的修改應用到 v4 版本

### 3. app.py (已完成)
- 新增 `/api/address2community` endpoint
- 載入並索引 address2com 資料

## 查詢流程

```
用戶點擊地圖標記
  ↓
取得地址: "新北市板橋區民族路25號"
  ↓
[address2community API] → 建案名稱: "豐馥"
  ↓
[projects API] → 搜尋 "豐馥" → 找到建案資料
  ↓
[project detail API] → 載入 255 筆交易資料
  ↓
顯示完整交易紀錄
```

## 測試結果

✅ **成功案例**:
- 新北市板橋區民族路25號 → 豐馥 (255 筆交易)
- 台中市西屯區文華路100號 → 富宇富美圖 (31 筆交易)

⚠️ **部分案例**:
- 某些建案名稱可能在資料庫中記錄方式不同
- 使用地址搜尋作為備用方案解決

❌ **失敗處理**:
- 顯示友善的「暫無交易紀錄」訊息
- 顯示查詢的地址和建案名稱供參考

## 技術細節

### 前端邏輯 (JavaScript)
```javascript
async function showProjectDetail(projectId, address, projectName) {
    // 1. 查詢建案名稱
    if (!projectName) {
        const a2c = await fetch(`/api/address2community?address=...`);
        finalProjectName = a2c.best;
    }
    
    // 2. 搜尋建案
    let result = await fetch(`/api/projects?keyword=${finalProjectName}`);
    if (!result) {
        result = await fetch(`/api/projects?keyword=${address}`); // 備用
    }
    
    // 3. 顯示詳情
    const detail = await fetch(`/api/project/${result.id}`);
    renderProjectDetail(detail.project);
}
```

### 後端 API (Python)
```python
@app.route('/api/address2community')
def api_address2community():
    address = request.args.get('address')
    result = query_address2community(address)
    return jsonify({
        'best': result['best'],
        'results': result['results']
    })
```

## 優勢

1. **高匹配率**: 23,664 筆建案記錄涵蓋全台主要建案
2. **智能備用**: 多層查詢策略確保最大成功率
3. **快速響應**: 本地 CSV 索引，毫秒級查詢
4. **友善提示**: 失敗時提供詳細的除錯資訊

## 未來改進

- [ ] 整合 591 API 作為線上備用查詢
- [ ] 建案名稱模糊匹配（相似度演算法）
- [ ] 手動標記功能（用戶反饋）
- [ ] 自動學習與資料更新機制
