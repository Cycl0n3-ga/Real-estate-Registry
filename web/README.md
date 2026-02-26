# 良富居地產 v3.0 — Web API 伺服器

Flask 後端 API 伺服器，整合房價搜尋、建案名對照地址、地址對照建案等功能。
---

# 架構總覽

本系統採用模組化設計，所有地址、數字正規化、全半形轉換、變體產生等邏輯，皆集中於 `address_utils.py`，並由各功能模組（如 `address_match`、`address2com`、`com2address`、`geocoder` 等）共用。搜尋 API 會自動結合「地址模糊比對」與「建案名查詢」以提升查全率。

- **核心模組**：
  - `address_utils.py`：全形/半形、數字、地址正規化、變體產生等共用工具。
  - `address_match/`：地址/建案模糊搜尋，主搜尋引擎。
  - `address2com/`：地址 → 建案名查詢。
  - `com2address/`：建案名 → 地址查詢。
  - `geodecoding/`：OSM 地理編碼。
- **API 伺服器**：`web/server.py`，整合所有查詢、靜態檔案服務、錯誤處理。
- **前端**：`web/static/`，Leaflet.js 地圖，顯示總價、聚合、摘要。

## 環境需求

- Python 3.8+
- 虛擬環境：`/home/cyclone/.venv`
- 資料庫：`land/db/land_data.db`（SQLite）

## 啟動方式

```bash
cd /home/cyclone/land
source /home/cyclone/.venv/bin/activate
python web/server.py
```

伺服器預設監聽 **http://localhost:5001**

## API 端點

### `GET /`
回傳前端靜態首頁 (`static/index.html`)。

---

### `GET /api/search`
統一房價搜尋（支援地址、建案名稱）。

| 參數 | 說明 | 預設 |
|---|---|---|
| `keyword` | 搜尋關鍵字（地址或建案名，必填） | — |
| `limit` | 回傳上限，最多 2000 | `500` |
| `building_type` | 建物型態，逗號分隔（如 `住宅大樓,透天厝`） | — |
| `rooms` | 房數，逗號分隔（如 `2,3`） | — |
| `public_ratio` | 公設比範圍（如 `0-35`） | — |
| `year` | 民國年份範圍（如 `110-114`） | — |
| `ping` | 坪數範圍（如 `20-40`） | — |
| `unit_price` | 單坪價範圍，萬/坪（如 `60-120`） | — |
| `price` | 總價範圍，萬元（如 `1000-3000`） | — |

**回應範例：**
```json
{
  "success": true,
  "keyword": "信義路",
  "search_type": "address",
  "community_name": null,
  "transactions": [...],
  "summary": {
    "total": 42,
    "avg_price": 2500,
    "avg_unit_price_ping": 85.3,
    "avg_ping": 30.1,
    "avg_ratio": 28.5
  },
  "total": 42
}
```

---

### `GET /api/community2address`
建案名稱 → 對應地址範圍查詢。

| 參數 | 說明 |
|---|---|
| `community` | 建案名稱（必填），如 `健安新城B區` |

---

### `GET /api/address2community`
地址 → 對應建案名稱查詢。

| 參數 | 說明 |
|---|---|
| `address` | 地址字串（必填） |

---

### `GET /api/stats`
查詢系統狀態與資料庫統計資訊。

---

## 目錄結構

```
land/web/
├── server.py        # Flask 主程式
├── server.log       # 執行紀錄
├── README.md        # 本文件
└── static/          # 前端靜態檔案
```

## 主要模組與職責
---

## 開發與維護建議

- **統一邏輯**：所有正規化、轉換、變體產生請集中於 `address_utils.py`，避免重複。
- **API 回應格式**：所有 API 皆回傳 JSON，錯誤時也應回傳 JSON 格式（避免前端出現 Unexpected token '<' 錯誤）。
- **搜尋流程**：API 會自動結合地址模糊比對、建案名查詢，並回傳摘要統計。
- **地圖顯示**：前端地圖標記顯示「總價」而非單價。
- **測試與除錯**：可直接執行各模組下的測試腳本，或用 API 測試工具（如 curl, Postman）驗證。
- **資料庫維護**：如需更新資料，請先備份 `land/db/` 目錄。

## 常見問題

- **前端顯示 Unexpected token '<'**：
  - 可能原因：API 回傳 HTML（如 404/500 錯誤頁）而非 JSON。
  - 解法：請確認 Flask API 路由與錯誤處理皆正確回傳 JSON 格式。

- **搜尋結果不完整**：
  - 請確認 `address_utils.py` 是否有最新正規化邏輯，並已被各模組正確引用。

- **地圖未顯示總價**：
  - 請確認前端 JS 取用 `price` 欄位而非 `unit_price`。

---

## 版本紀錄

- v3.0
  - 全面模組化，address_utils.py 中心化所有正規化/轉換邏輯
  - 搜尋 API 統一整合地址、建案名查詢
  - 前端地圖顯示總價、聚合、摘要
  - 完善 API 文件與開發指引

| 模組            | 路徑                    | 功能說明 |
|-----------------|-------------------------|---------|
| `address_utils` | `land/address_utils.py` | 地址/數字正規化、全半形轉換、變體產生，所有模組共用 |
| `address_match` | `land/address_match/`   | 地址/建案模糊搜尋，主搜尋引擎，完全依賴 address_utils |
| `address2com`   | `land/address2com/`     | 地址 → 建案名查詢，依賴 address_utils |
| `com2address`   | `land/com2address/`     | 建案名 → 地址查詢，依賴 address_utils |
| `geocoder`      | `land/geodecoding/`     | OSM 地理編碼，依賴 address_utils |
| `web`           | `land/web/`             | Flask API 與前端靜態檔案服務 |
