# 🏠 良富居地產 - 專業房地產地圖系統

一個專業房地產交易地圖系統，提供建案查詢、價格分析、銷控面板等功能。

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/flask-3.0+-green.svg)

## ✨ 功能特色

### 🗺️ 互動式地圖
- **建案標記**：在 Google Maps 上顯示所有建案位置
- **價格分層**：使用不同顏色標記不同價格範圍
  - 🟢 綠色：< 1000萬
  - 🔵 藍色：1000萬 - 2000萬
  - 🟠 橙色：2000萬 - 5000萬
  - 🔴 紅色：> 5000萬
- **集群展示**：相近建案自動聚合顯示

### 📊 銷控面板
- **樓層銷售狀態**：視覺化顯示每個樓層的交易狀態
- **交易記錄**：完整的歷史交易資料
- **統計資訊**：
  - 總交易數
  - 平均總價
  - 平均單價
  - 平均面積

### 🔄 單位切換
- **㎡/坪切換**：一鍵切換平方公尺與坪數顯示
- **自動轉換**：所有面積和單價自動轉換
  - 1 坪 = 3.3058 平方公尺
  - 單價自動換算（元/㎡ ↔ 萬元/坪）

### 🎯 進階篩選
- **總價範圍**：設定最低/最高總價
- **單價範圍**：依單價篩選（支援㎡和坪）
- **關鍵字搜尋**：搜尋地址、建案名稱

### 📱 響應式設計
- 現代化 UI 設計
- 流暢的動畫效果
- 直覺的操作介面

## 🚀 快速開始

### 環境需求

```bash
pip install flask flask-cors duckdb
```

### 環境需求

- Python 3.8+
- pip

### 安裝步驟

1. **克隆專案**
```bash
git clone https://github.com/YOUR_USERNAME/real-estate-map.git
cd real-estate-map
```

2. **安裝依賴**
```bash
pip install flask flask-cors duckdb python-dotenv
```

3. **設定環境變數**

創建 `.env` 文件：
```bash
GOOGLE_MAPS_API_KEY=你的_API_KEY
```

4. **準備 CSV 數據**

確保 `ALL_lvr_land_a.csv` 文件在專案目錄中。

5. **啟動服務器**
```bash
python3 app.py
```

6. **開啟瀏覽器**

訪問 http://localhost:5000

## 📖 使用說明

### 基本操作

1. **瀏覽建案**
   - 地圖上的彩色圓點代表不同建案
   - 圓點顏色表示價格範圍
   - 點擊圓點查看建案詳情

2. **搜尋功能**
   - 在頂部搜尋框輸入關鍵字
   - 支援地址、建案名稱、地區搜尋
   - 按 Enter 或點擊搜尋按鈕

3. **單位切換**
   - 點擊右上角 ㎡/坪 按鈕
   - 所有面積和單價自動轉換

4. **進階篩選**
   - 設定總價範圍（萬元）
   - 設定單價範圍（依當前單位）
   - 點擊「套用篩選」

5. **查看銷控面板**
   - 點擊建案卡片或地圖標記
   - 查看完整交易記錄
   - 瀏覽樓層銷售狀態

### 快捷鍵

- `Enter` - 執行搜尋
- `Esc` - 關閉銷控面板

## 🔑 取得 Google Maps API Key

1. 前往 [Google Cloud Console](https://console.cloud.google.com/)
2. 創建新專案或選擇現有專案
3. 啟用以下 API：
   - Maps JavaScript API
   - Geocoding API
4. 創建 API 憑證（API Key）
5. 複製 API Key 到 `.env` 文件

**免費額度**：Google Maps 每月提供 $200 美元的免費額度。

## 📂 專案結構

```
land/
├── app.py                      # Flask 後端主程式
├── leju_map.html              # 前端主頁面
├── .env                       # 環境變數（不提交到 git）
├── .gitignore                 # Git 忽略文件
├── .gitattributes             # Git LFS 設定
├── ALL_lvr_land_a.csv        # 房地產交易數據（git lfs）
├── README.md                  # 本文件
└── SETUP_GITHUB.md           # GitHub 設定說明
```

## 🛠️ 技術架構

### 後端
- **Flask** - Web 框架
- **DuckDB** - 高效能 CSV 查詢引擎
- **Flask-CORS** - 跨域請求支援
- **python-dotenv** - 環境變數管理

### 前端
- **Google Maps JavaScript API** - 地圖顯示
- **Geocoding API** - 地址轉座標
- **原生 JavaScript** - 互動功能
- **CSS3** - 現代化樣式

### 數據
- 內政部不動產交易實價登錄資料
- CSV 格式存儲
- Git LFS 管理大文件

## 📊 API 端點

### GET /api/projects
獲取所有建案列表（聚合數據）

**回應範例：**
```json
{
  "success": true,
  "count": 200,
  "projects": [
    {
      "id": 123456,
      "name": "信義區豪宅",
      "address": "台北市信義區...",
      "avg_price": 35000000,
      "avg_unit_price": 450000,
      "transaction_count": 15
    }
  ]
}
```

### GET /api/project/{project_id}
獲取建案詳細資訊和銷控數據

**參數：**
- `address` - 建案地址

### GET /api/search
搜尋建案

**參數：**
- `keyword` - 搜尋關鍵字
- `min_price` - 最低總價
- `max_price` - 最高總價
- `min_unit_price` - 最低單價（元/㎡）
- `max_unit_price` - 最高單價（元/㎡）

## 🎨 自訂設定

### 修改預設地圖中心
編輯 `leju_map.html`：
```javascript
map = new google.maps.Map(document.getElementById('map'), {
    center: { lat: 25.0330, lng: 121.5654 }, // 修改此處
    zoom: 12
});
```

### 調整價格顏色範圍
編輯 `leju_map.html` 中的 `getPriceColor` 函數：
```javascript
function getPriceColor(price) {
    if (price < 10000000) return '#4caf50';  // < 1000萬
    if (price < 20000000) return '#2196f3';  // 1000萬 - 2000萬
    if (price < 50000000) return '#ff9800';  // 2000萬 - 5000萬
    return '#f44336';                         // > 5000萬
}
```

## 🐛 常見問題

### Q: 搜尋沒有結果？
A: 
- 確認關鍵字拼寫正確
- 嘗試使用更短的關鍵字
- 檢查 CSV 文件是否包含該地區數據

### Q: 地圖無法顯示？
A: 
- 檢查 Google Maps API Key 是否正確
- 確認已啟用 Maps JavaScript API
- 檢查瀏覽器控制台錯誤訊息

### Q: 單位轉換不準確？
A: 
- 系統使用 1 坪 = 3.3058 平方公尺
- 這是標準建築單位換算

### Q: CSV 文件太大無法上傳？
A: 
- 已使用 Git LFS 管理大文件
- 確保已安裝 git-lfs：`sudo apt-get install git-lfs`
- 執行：`git lfs install`

## 🔄 更新日誌

### v2.0.0 (2026-02-16)
- ✨ 新增單位切換功能（㎡/坪）
- ✨ 整合建案地圖和銷控面板
- ✨ 重新設計篩選功能（總價+單價）
- 🐛 修復搜尋功能 SQL 注入問題
- 🎨 改進 UI/UX 設計
- 📝 完善文檔

### v1.0.0 (2026-02-15)
- 🎉 初始版本發布
- 基本地圖功能
- 建案列表展示

## 📄 授權

MIT License - 詳見 [LICENSE](LICENSE)

## 👥 貢獻

歡迎提交 Pull Request 或開 Issue！

## 📧 聯絡

如有問題或建議，請開 Issue 或聯絡：
- GitHub: [@YOUR_USERNAME](https://github.com/YOUR_USERNAME)

## 🙏 致謝

- 內政部不動產交易實價登錄資料
- Google Maps Platform
- DuckDB 團隊
- Flask 社群

---

**⭐ 如果這個專案對你有幫助，請給個星星！**
