# 台灣地址地理編碼工具 (geodecoding)

高效批次將台灣地址轉換為 WGS84 經緯度座標，專為 `land_a.db` 數百萬筆不動產交易地址設計。

## 特色

🏠 **門牌級精度（v2 新功能）**  
整合 OSM 本地索引（約 900 萬筆門牌節點），精度從路段中心（±800-1500m）提升至**門牌精確（±10-50m）**。

🚀 **三層加速策略**
1. **本地 OSM 索引** — 900 萬筆門牌節點離線查詢（7-20ms，門牌級）
2. **SQLite 永久快取** — 查過的地址永遠不用再查（微秒級）
3. **路段級備援** — Nominatim API 路段座標（找不到時降級）

📊 **處理規模**
| 層級 | 不同項目數 | 說明 |
|------|-----------|------|
| 原始交易 | ~4,678,000 | land_a.db 總筆數 |
| 有效地址 | ~2,927,000 | 含路/街/大道+門牌號 |
| 基本地址 | ~1,480,000 | 去除樓層後不同地址 |
| **OSM 門牌索引** | **~9,000,000** | **全台門牌精確座標** |

## 精度比較

| 方法 | 精度 | 誤差 | 說明 |
|------|------|------|------|
| 舊版（路段級）| road | ±200-1500m | Nominatim 只到路段中心 |
| **新版（門牌級）** | **exact** | **±10-50m** | **OSM 9M 門牌節點** |

## 安裝

```bash
# 核心功能不需額外套件（純 Python 標準庫）

# 選用：進度條
pip install tqdm
```

## 快速開始

### 步驟 0：建立 OSM 門牌索引（推薦，一次性）

```bash
cd /home/cyclone/land/land_reg/geodecoding

# 下載全台門牌資料（約 15-25 分鐘，1.5-2 GB）
python3 build_osm_index.py

# 查看下載進度
python3 build_osm_index.py --status

# 只下載指定縣市（快速測試）
python3 build_osm_index.py --cities 臺北市,新北市
```

### 1. 查看目前狀態

```bash
cd /home/cyclone/land/land_reg/geodecoding
python3 batch_geocode.py --status
```

### 2. 匯入既有快取

```bash
# 匯入之前已建立的 geocode_cache.json
python3 batch_geocode.py --import-cache ../../geocode_cache.json
```

### 3. 路段級批次處理（推薦第一步）

```bash
# 先測試少量
python3 batch_geocode.py --strategy road --limit 100

# 處理特定區域
python3 batch_geocode.py --strategy road --district 松山區

# 全部處理（公開 Nominatim 約需 ~22 小時）
python3 batch_geocode.py --strategy road
```

### 4. 寫回 land_a.db

```bash
python3 batch_geocode.py --write-back
```

### 5. 匯出 CSV

```bash
python3 batch_geocode.py --export geocoded_addresses.csv
```

## 程式碼使用

### 單一地址查詢

```python
from geocoder import TaiwanGeocoder

gc = TaiwanGeocoder()

# 完整地址
result = gc.geocode("臺北市大安區和平東路三段1號")
print(result)
# {'lat': 25.026, 'lng': 121.543, 'level': 'exact', 'source': 'nominatim', ...}

# 需要補全縣市時
result = gc.geocode("三民路29巷5號", district="松山區")
```

### 批次查詢

```python
from geocoder import TaiwanGeocoder

gc = TaiwanGeocoder()

addresses = [
    ("大安區", "臺北市大安區和平東路三段1號"),
    ("松山區", "三民路29巷5號"),
    ("板橋區", "新北市板橋區文化路一段100號"),
]

results = gc.batch_geocode(addresses, strategy='road')
for addr, result in results.items():
    print(f"  {addr} → ({result['lat']:.5f}, {result['lng']:.5f}) [{result['level']}]")
```

### 便利函式

```python
from geocoder import quick_geocode

coords = quick_geocode("臺北市大安區和平東路三段1號")
print(coords)  # (25.026, 121.543)
```

## 加速方案：本地 Nominatim

公開 Nominatim 限制 1 req/sec，81K 路段約需 22 小時。
架設本地實例可提升至 **數千 req/sec**。

### Docker 快速架設

```bash
# 下載台灣 OSM 資料 (~150MB)
wget https://download.geofabrik.de/asia/taiwan-latest.osm.pbf

# 啟動 Nominatim 容器
docker run -it \
  -e PBF_PATH=/nominatim/data/taiwan-latest.osm.pbf \
  -e REPLICATION_URL=https://download.geofabrik.de/asia/taiwan-updates/ \
  -p 8080:8080 \
  -v $(pwd)/taiwan-latest.osm.pbf:/nominatim/data/taiwan-latest.osm.pbf \
  -v nominatim-data:/var/lib/postgresql/14/main \
  --name nominatim \
  mediagis/nominatim:4.4

# 使用本地 Nominatim（速度飛升！）
python3 batch_geocode.py --strategy road \
  --nominatim-url http://localhost:8080/search
```

本地實例預計處理時間: **< 10 分鐘**（對比公開 API 的 22 小時）

## 地址正規化

工具會自動處理以下台灣地址特殊狀況：

| 原始地址 | 正規化後 |
|---------|---------|
| `臺北市大安區和平東路三段１號５樓` | `臺北市大安區和平東路三段1號` |
| `null豐原區水源路中坑巷２８號四樓` | `臺中市豐原區水源路中坑巷28號` |
| `三民路29巷5號等共用部分` | `臺北市松山區三民路29巷5號` |
| `&２１４１４；門街81巷45號三樓` | `門街81巷45號` |

## 精度等級

| Level | 說明 | 精度 |
|-------|------|------|
| `exact` | 精確門牌定位 | ±50m |
| `road` | 路段級定位 | ±200m |
| `district` | 區域級定位 | ±2km |

## 檔案結構

```
geodecoding/
├── geocoder.py        # 核心引擎（地址正規化、快取、API Provider）
├── batch_geocode.py   # 批次處理 CLI 工具
├── README.md          # 本文件
└── cache/
    └── geocode_cache.db  # SQLite 永久快取（自動建立）
```

## API Reference

### TaiwanGeocoder

```python
gc = TaiwanGeocoder(
    cache_dir=None,          # 快取目錄（預設: ./cache/）
    provider='nominatim',    # 'nominatim' 或 'nlsc'
    nominatim_url=None,      # 本地 Nominatim URL
    concurrency=1,           # 並行數
)

gc.geocode(address, district='')          # 單一查詢
gc.batch_geocode(address_list, strategy)  # 批次查詢
gc.stats()                                # 快取統計
```

### GeoCache

```python
cache = GeoCache('path/to/cache.db')
cache.get(address_key)                    # 查詢單一
cache.get_batch([key1, key2, ...])        # 批次查詢
cache.put(key, lat, lng, level, source)   # 寫入單一
cache.put_batch(records)                  # 批次寫入
cache.import_json_cache('old_cache.json') # 匯入 JSON
cache.stats()                             # 統計
```

### batch_geocode.py CLI

```bash
python3 batch_geocode.py [OPTIONS]

選項:
  --status                顯示進度
  --strategy {smart,road,exact}  geocoding 策略
  --district DISTRICT     指定區域
  --limit N               限制筆數
  --provider {nominatim,nlsc}  API provider
  --nominatim-url URL     本地 Nominatim URL
  --write-back            寫回 land_a.db
  --export CSV            匯出 CSV
  --import-cache JSON     匯入舊快取
  --verbose               詳細輸出
```
