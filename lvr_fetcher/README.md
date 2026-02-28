# fetch com from lvr

從內政部實價登錄網站（[lvr.land.moi.gov.tw](https://lvr.land.moi.gov.tw)）爬取台灣全台建案社區名稱與不動產買賣交易資料，並存入 SQLite 資料庫。

---

## 檔案結構

```
fetch com from lvr/
├── __init__.py              # 套件入口，匯出 LvrApiClient、download_all_communities
├── client.py                # API 客戶端（身分驗證、加密、各 API 封裝）
├── fetch_communities.py     # 爬取全台建案社區名稱
├── fetch_transactions.py    # 爬取全台不動產買賣交易資料
└── README.md
```

---

## 模組說明

### `client.py` — `LvrApiClient`

封裝對 `lvr.land.moi.gov.tw` 的底層 HTTP 請求，包含：

- **登入流程**：取得 `JSESSIONID` → 訪問 `list.jsp` 初始化 session
- **Token 機制**：每次查詢前呼叫 `setToken.jsp` 取得一次性 token
- **查詢加密**：複製瀏覽器端的 `getEncodeStr()`，以 CryptoJS AES + Base64 雙重加密查詢參數
- **SSL 相容**：降低 TLS 安全等級以相容台灣政府網站

| 方法 | 說明 |
|---|---|
| `login()` | 建立 Session |
| `get_token()` | 取得一次性查詢 Token |
| `get_cities()` | 取得縣市清單 |
| `get_towns(city_code)` | 取得指定縣市的行政區清單 |
| `search_communities_raw(town_code, keyword)` | 搜尋建案名稱（自動完成 API） |
| `query_price(city, town, starty, startm, endy, endm, ...)` | 查詢不動產買賣交易清單 |

---

### `fetch_communities.py` — 建案社區名稱爬蟲

**策略**：對每個行政區，用約 200 個常見中文字元（`SWEEP_CHARS`）逐一呼叫建案自動完成 API，藉此涵蓋絕大多數建案名稱。

**資料存至**：`db/community_address.db`

#### 資料表：`community_mapping`

| 欄位 | 說明 |
|---|---|
| `city_code` | 縣市代碼 |
| `town_code` | 行政區代碼 |
| `address` | 建案地址 |
| `community_name` | 建案/社區名稱（`buildname`） |
| `short_name` | 建案簡稱（`sq_name`） |
| `raw_data` | API 回傳原始 JSON（完整保留） |

> 僅儲存 `community_name` 或 `address` 至少一個有值的資料；空白資料直接丟棄。

#### 資料表：`crawl_progress`（斷點續傳）

記錄已爬完的 `(town_code, keyword)` 組合，重跑時自動跳過。

#### 使用方式

```python
from fetch_communities import download_all_communities

# 使用預設參數（全台、間隔 0.5 秒）
download_all_communities()

# 自訂掃描字元與間隔
download_all_communities(delay=1.0, chars=['台', '大', '美'])
```

---

### `fetch_transactions.py` — 不動產交易資料爬蟲

**策略**：對每個縣市，按時間段切割查詢（大城市按月、其餘按季），避免單次回傳量過大。

**資料存至**：`db/transactions.db`

#### 大城市（月查詢）
臺北市、臺中市、臺南市、高雄市、新北市、桃園市

#### 資料表：`transactions`

| 欄位 | 說明 |
|---|---|
| `city` | 縣市代碼 |
| `town` | 行政區 |
| `address` | 地址 |
| `build_type` | 建物種類 |
| `community` | 建案名稱 |
| `date_str` | 交易日期 |
| `floor` | 樓層 |
| `area` | 建物面積 |
| `total_price` | 總價 |
| `unit_price` | 單價 |
| `lat` / `lon` | 緯度 / 經度 |
| `sq` | 唯一識別碼（unique key，防重複） |
| `raw_json` | API 回傳原始 JSON（完整保留） |

#### 資料表：`fetch_progress`（斷點續傳）

記錄已完成的 `(city, period)` 組合（period 格式：`民國年-月`，例如 `114-01`）。

#### 使用方式（程式呼叫）

```python
from fetch_transactions import download_all

# 全台，民國 101 年 1 月 ～ 115 年 2 月
download_all(starty=101, startm=1, endy=115, endm=2)

# 只下載特定縣市
download_all(starty=110, startm=1, endy=115, endm=2, cities=['A', 'F'])
```

#### 使用方式（命令列）

```bash
# 全台下載
python -m "fetch com from lvr".fetch_transactions --all \
    --starty 101 --startm 1 --endy 115 --endm 2

# 單一縣市（基隆市）
python -m "fetch com from lvr".fetch_transactions \
    --city C --starty 110 --startm 1 --endy 115 --endm 2

# 完整參數
python -m "fetch com from lvr".fetch_transactions --all \
    --starty 101 --startm 1 --endy 115 --endm 2 \
    --db ./db/transactions.db \
    --delay 0.5 \
    --ptype 1,2,3,4,5
```

#### `--ptype` 交易標的說明

| 代碼 | 說明 |
|---|---|
| `1` | 房地（土地+建物） |
| `2` | 房地（含車位） |
| `3` | 土地 |
| `4` | 建物（單純） |
| `5` | 車位 |

---

## 依賴套件

```bash
pip install pycryptodome
```

---

## 注意事項

- 爬蟲對政府網站發出請求，建議 `delay` 不要設太低（預設 0.5 秒），避免被封鎖。
- Session 有效期有限，`LvrApiClient` 會自動在 401 時重新登入。
- 兩個爬蟲均支援**斷點續傳**，中途中斷後重跑會從上次停止的地方繼續。
