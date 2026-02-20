# 內政部不動產交易實價查詢 — 逆向工程筆記 & API 使用說明

> 研究對象：https://lvr.land.moi.gov.tw/

---

## 1. 網站架構分析

### 1.1 系統概覽

```
使用者瀏覽器
    │
    ├─ 前端 SPA (JSP + webpack bundled Vue/React)
    │       ├─ common.bundle.js   ← 核心查詢邏輯 (AES加密)
    │       ├─ qt_ajax.js         ← 地址/路段輔助查詢
    │       └─ index.bundle.js    ← 主頁面
    │
    ├─ lvr.land.moi.gov.tw  ← 查詢網站 (Java/JSP)
    │       ├─ /SERVICE/CITY           → 縣市清單 (公開)
    │       ├─ /SERVICE/CITY/{code}/   → 鄉鎮清單 (公開)
    │       ├─ /SERVICE/QueryPrice/SaleBuild/{town}/{keyword}  → 社區名稱搜尋 (需Cookie)
    │       ├─ /SERVICE/QueryPrice/{hash}?q={encrypted}        → 主查詢 (AES加密)
    │       ├─ /SERVICE/QueryPrice/community/{hash}/{encrypted} → 社區歷史成交
    │       └─ /SERVICE/QueryPrice/SaleData/{hash}?q={encrypted}
    │
    ├─ plvr.land.moi.gov.tw ← Open Data 下載
    │       └─ /Download?type=zip&fileName=lvr_landcsv.zip     → 全國CSV壓縮檔
    │           /Download?fileName={city}_lvr_land_{type}.csv  → 單一城市檔案
    │
    └─ api.nlsc.gov.tw      ← 地籍圖資服務 (NLSC)
            ├─ /idc/ListRoadLaneAlley/{city}/{town}/{road}     → 巷弄清單
            └─ /idc/TextQueryAddress/{address}/{limit}/{city}  → 門牌查詢
```

---

## 2. 為什麼網站可以搜尋「社區名稱」

### 2.1 兩種資料類型

| 類型 | 代碼 | 欄位 |
|------|------|------|
| 不動產買賣 (成屋) | A | 無直接社區名稱欄位，門牌地址 |
| 預售屋買賣 | B | **有 `建案名稱` 欄位** |
| 不動產租賃 | C | 無直接社區名稱欄位 |

### 2.2 社區名稱來源

網站的「社區名稱」有兩個來源：

1. **預售屋 (Type B)**：直接使用 CSV 中的 `建案名稱` 欄位
2. **成屋 (Type A)**：後端維護了一個**建物名稱對應表**，將大樓/社區名稱對應到地號/建號。這個資料來自**建物登記資料**，不在 Open Data CSV 中。

### 2.3 `SaleBuild` 搜尋 API（已驗證可用）

網站前端在輸入社區名稱時，會呼叫此 API 做自動完成：

```
GET /SERVICE/QueryPrice/SaleBuild/{town_code}/{keyword}
```

**範例**：搜尋台北市大安區 `信義` 相關建案
```
GET https://lvr.land.moi.gov.tw/SERVICE/QueryPrice/SaleBuild/A02/%E4%BF%A1%E7%BE%A9
```

**回傳**：
```json
{
  "CHK": "Y",
  "LIST": [
    {"buildname": "元利信義聯勤-北棟"},
    {"buildname": "信義CASA"},
    {"buildname": "信義御邸"}
  ]
}
```

---

## 3. AES 加密機制（逆向工程）

主要查詢 API 使用 CryptoJS AES 加密，**加密金鑰已找到**：

```javascript
var g = window.location.host;  // "lvr.land.moi.gov.tw"
```

加密流程（來自 `common.bundle.js`）：
```javascript
var n = CryptoJS.enc.Base64.parse(
    CryptoJS.AES.encrypt(JSON.stringify(query_params), g).toString()
);
var encrypted_query = CryptoJS.enc.Base64.stringify(n);
// URL: /SERVICE/QueryPrice/{md5_hash_of_params}?q={encrypted_query}
```

**Python 實作（OpenSSL EVP_BytesToKey 衍生法）**：
```python
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib, os, base64

KEY_PASSPHRASE = b"lvr.land.moi.gov.tw"

def cryptojs_aes_encrypt(data: str) -> str:
    """模擬 CryptoJS.AES.encrypt(data, passphrase)"""
    salt = os.urandom(8)
    key, iv = evp_bytes_to_key(KEY_PASSPHRASE, salt, 32, 16)
    # ... (詳見 moi_client.py)
```

---

## 4. Open Data 下載

### 4.1 直接下載 URL

```
# 全國所有資料（ZIP，約 100-500MB）
https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip

# 單一縣市單一類別
https://plvr.land.moi.gov.tw/Download?fileName={city_code}_lvr_land_{type}.csv
```

### 4.2 縣市代碼

| 代碼 | 縣市 | 代碼 | 縣市 |
|------|------|------|------|
| A | 臺北市 | D | 臺南市 |
| B | 臺中市 | E | 高雄市 |
| C | 基隆市 | F | 新北市 |
| G | 宜蘭縣 | H | 桃園市 |
| I | 嘉義市 | J | 新竹縣 |
| K | 苗栗縣 | M | 南投縣 |
| N | 彰化縣 | O | 新竹市 |
| P | 雲林縣 | Q | 嘉義縣 |
| T | 屏東縣 | U | 花蓮縣 |
| V | 臺東縣 | W | 金門縣 |
| X | 澎湖縣 | Z | 連江縣 |

### 4.3 交易類別

| 代碼 | 說明 | 社區名稱欄位 |
|------|------|------|
| A | 不動產買賣（成屋） | ❌ 無 |
| B | 預售屋買賣 | ✅ `建案名稱` |
| C | 不動產租賃 | ❌ 無 |

---

## 5. 工具使用

### 5.1 下載 Open Data

```bash
python3 download_opendata.py --city A --type A  # 下載台北市買賣
python3 download_opendata.py --city all --type B # 下載全國預售屋
python3 download_opendata.py --all               # 下載全國ZIP
```

### 5.2 搜尋社區名稱

```python
from moi_client import MoiClient

client = MoiClient()
# 搜尋台北市大安區的信義相關社區
results = client.search_building("A02", "信義")
# [{"buildname": "信義CASA"}, ...]
```

### 5.3 查詢城市/鄉鎮清單

```python
cities = client.get_cities()
towns = client.get_towns("A")  # 台北市各區
```

---

## 6. 資料欄位說明

### Type A (買賣) 欄位
`鄉鎮市區`, `交易標的`, `土地位置建物門牌`, `土地移轉總面積平方公尺`, `都市土地使用分區`, `交易年月日`, `交易筆棟數`, `移轉層次`, `總樓層數`, `建物型態`, `主要用途`, `主要建材`, `建築完成年月`, `建物移轉總面積平方公尺`, `建物現況格局-房/廳/衛`, `有無管理組織`, `總價元`, `單價元平方公尺`, `車位類別`, `車位總價元`, `備註`, `編號`, `主建物面積`, `附屬建物面積`, `陽台面積`, `電梯`, `移轉編號`

### Type B (預售屋) 額外欄位
+ **`建案名稱`**（社區名稱！）, `棟及號`, `解約情形`
