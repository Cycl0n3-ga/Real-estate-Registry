# 房地產價格地圖查詢系統

## 📋 系統說明

這是一個房地產交易資訊視覺化系統，可以在Google地圖上標記房地產交易位置並顯示價格資訊。

## 🚀 使用方式

### 方案一：Flask 後端版本（推薦）

這個方案使用 Python Flask + DuckDB 提供高效的查詢服務。

#### 1. 安裝必要套件

```bash
pip install flask flask-cors duckdb
```

#### 2. 啟動服務器

```bash
cd /home/cyclone/land
python3 map_server.py
```

服務器會在 http://localhost:5000 啟動

#### 3. 設定 Google Maps API Key

編輯 `real_estate_map_flask.html` 文件，找到最後一行：

```html
<script src="https://maps.googleapis.com/maps/api/js?key=YOUR_GOOGLE_MAPS_API_KEY&callback=initMap&language=zh-TW" async defer></script>
```

將 `YOUR_GOOGLE_MAPS_API_KEY` 替換為你的 Google Maps API Key。

#### 4. 開啟瀏覽器

在瀏覽器中訪問：http://localhost:5000

#### 5. 搜尋房地產

輸入地址關鍵字（例如：日興一街６號），點擊搜尋按鈕。

### 方案二：純前端版本

如果不想使用後端服務器，可以直接開啟 `real_estate_map.html`。

**注意**：這個版本需要瀏覽器允許讀取本地 CSV 文件，可能需要使用本地 HTTP 服務器。

## 🎨 功能特色

### ✨ 主要功能

1. **地圖標記**
   - 在 Google 地圖上顯示房地產位置
   - 使用顏色區分價格範圍
   - 點擊標記查看詳細資訊

2. **價格分類**
   - 🟢 綠色：< 500萬
   - 🟠 橙色：500萬 - 1000萬
   - 🟠 深橙：1000萬 - 2000萬
   - 🔴 紅色：> 2000萬

3. **統計資訊**
   - 物件數量
   - 平均總價
   - 平均單價
   - 價格區間

4. **物件列表**
   - 顯示所有搜尋結果
   - 點擊卡片定位到地圖
   - 顯示詳細交易資訊

5. **資料匯出**
   - 匯出 CSV 格式
   - 包含所有查詢結果

## 🔑 取得 Google Maps API Key

1. 前往 [Google Cloud Console](https://console.cloud.google.com/)
2. 創建新專案或選擇現有專案
3. 啟用 "Maps JavaScript API" 和 "Geocoding API"
4. 創建 API 憑證（API Key）
5. 複製 API Key 並貼到 HTML 文件中

**免費額度**：Google Maps 每月提供 $200 美元的免費額度，通常足夠個人使用。

## 📊 資料欄位說明

系統會顯示以下房地產資訊：

- **土地位置建物門牌**：物件地址
- **總價元**：交易總價
- **單價元平方公尺**：每平方公尺單價
- **建物移轉總面積平方公尺**：建物面積
- **建物型態**：公寓、華廈、住宅大樓等
- **交易年月日**：交易日期
- **移轉層次**：樓層資訊
- **建物現況格局**：房廳衛數量

## 🛠️ 技術架構

### 後端（Flask版本）
- **Flask**：Web 框架
- **DuckDB**：高效能 CSV 查詢
- **Flask-CORS**：跨域請求支援

### 前端
- **Google Maps JavaScript API**：地圖顯示
- **Geocoding API**：地址轉座標
- **原生 JavaScript**：互動功能

## 📝 使用範例

### 搜尋範例關鍵字

- 街道名稱：`日興一街`、`信義路`
- 區域：`信義區`、`大安區`
- 完整地址：`日興一街６號`

### API 端點（Flask版本）

#### 搜尋房地產
```
GET /api/search?location=日興一街６號
```

#### 回應格式
```json
{
  "success": true,
  "count": 10,
  "data": [...],
  "stats": {
    "total_count": 10,
    "avg_price": 15000000,
    "max_price": 25000000,
    "min_price": 8000000,
    "avg_unit_price": 350000
  }
}
```

## ⚠️ 注意事項

1. **Google Maps API 限制**
   - 每天有請求次數限制
   - 建議不要一次查詢太多物件（系統限制最多50筆）

2. **地理編碼準確性**
   - 依賴 Google Geocoding API
   - 某些地址可能無法精確定位

3. **CSV 文件路徑**
   - 確保 `ALL_lvr_land_a.csv` 文件存在
   - Flask 版本預設路徑：`/home/cyclone/land/ALL_lvr_land_a.csv`

4. **瀏覽器相容性**
   - 建議使用 Chrome、Firefox、Edge 等現代瀏覽器
   - 需要支援 ES6+ JavaScript

## 🐛 常見問題

### Q: 地圖無法顯示？
A: 檢查 Google Maps API Key 是否正確設定。

### Q: 搜尋沒有結果？
A: 
- 確認地址關鍵字是否正確
- 檢查 CSV 文件是否包含該地址資料

### Q: 標記位置不準確？
A: 這是 Google Geocoding API 的限制，某些地址可能無法精確定位。

### Q: Flask 服務器啟動失敗？
A: 
- 確認已安裝所有必要套件
- 檢查 5000 端口是否被佔用
- 確認 CSV 文件路徑正確

## 📦 文件清單

- `real_estate_map_flask.html` - Flask 版本的前端頁面（推薦）
- `map_server.py` - Flask 後端服務器
- `real_estate_map.html` - 純前端版本（不需要後端）
- `README.md` - 本說明文件

## 🔄 未來改進

- [ ] 支援多個 CSV 文件
- [ ] 加入進階篩選功能（價格範圍、面積範圍等）
- [ ] 增加圖表統計視覺化
- [ ] 支援歷史交易趨勢分析
- [ ] 加入地區熱力圖
- [ ] 支援比較功能

## 📧 技術支援

如有問題或建議，請參考：
- Google Maps API 文件：https://developers.google.com/maps
- DuckDB 文件：https://duckdb.org/docs/
- Flask 文件：https://flask.palletsprojects.com/

---

**版本**：1.0.0  
**更新日期**：2026-02-16
