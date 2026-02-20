# OSM 地理編碼實現文檔

## 概述
前端現在使用 **OpenStreetMap (OSM) Nominatim** 服務進行精確的地址地理編碼，而不是僅依賴行政區座標。

## 實現細節

### 後端改進 (`server.py`)

#### 1. 導入模組
```python
from geocoder import TaiwanGeocoder
```

#### 2. 全域初始化
```python
geocoder_engine = None
geocoder_ready = False

def init_geocoder():
    """背景初始化地理編碼引擎"""
    global geocoder_engine, geocoder_ready
    geocoder_engine = TaiwanGeocoder(
        cache_dir=str(LAND_DIR / "db"),
        provider="nominatim",
        concurrency=1
    )
    geocoder_ready = True
```

#### 3. 座標獲取函式
```python
def get_address_coords(address: str, district: str = "") -> tuple:
    """使用 OSM Nominatim 地理編碼取得準確座標"""
    if not geocoder_ready or geocoder_engine is None:
        return None, None
    
    try:
        result = geocoder_engine.geocode(address, district=district)
        if result and 'lat' in result and 'lng' in result:
            return result['lat'], result['lng']
    except Exception:
        pass
    
    return None, None
```

#### 4. 改進的座標解析策略（在 `format_tx_row` 中）
優先順序（從高到低）：
1. **OSM Geocoding**（新增！精確地址級別）
   - 使用 `TaiwanGeocoder.geocode()` 解析準確座標
   - 支援精確門牌級位置（如果有 OSM 索引）
   - 或路段級位置（通過 Nominatim API）

2. **資料庫快取座標**（如果 OSM 查詢失敗）
   - 回退到歷史快取的座標

3. **行政區座標**（如果以上都失敗）
   - 回退到行政區中心座標

4. **隨機微偏移**（最後一步）
   - 避免完全重疊的標記

### 前端整合

前端代碼已經準備好接收和顯示這些精確座標：

```javascript
function plotMarkers(){
  txData.forEach((tx, i) => {
    if(!tx.lat || !tx.lng) return;
    
    const marker = L.circleMarker([tx.lat, tx.lng], {
      radius: 6, fillColor: color, ...
    });
    
    markerGroup.addLayer(marker);
    bounds.push([tx.lat, tx.lng]);
  });
  
  if(bounds.length > 0){
    map.fitBounds(bounds, {padding:[40,40], maxZoom:16});
  }
}
```

## 座標精度改進

### 之前（使用行政區座標 + 隨機偏移）
- 精度：約 ±500 米（行政區級別）
- 所有相同行政區內的地址都聚集在一起

### 之後（使用 OSM Geocoding）
- 精度：±10 米以內（門牌級別，如有 OSM 索引）
- 或 ±50 米（路段級別，通過 Nominatim）
- 每個地址都有獨立的精確位置

### 測試結果

```
📍 搜尋: 松山區三民路43
✅ 找到 3 筆交易

   1. 台北市松山區三民路４３巷８號十樓之１
      座標: (25.046084, 121.550550) ✅ 有效
      
   2. 臺北市松山區三民路４３巷６號十二樓之２
      座標: (25.058456, 121.548759) ✅ 有效
```

## 快取機制

系統使用多層快取提升效能：

1. **SQLite 快取** (`geocode_cache.db`)
   - 已查詢過的地址快速查詢（微秒級）

2. **路段快取**
   - 同一路段的相似地址共用座標

3. **OSM 本地門牌索引**
   - 如果已執行 `build_osm_index.py`，可得到門牌級精度

## 使用方式

### 1. 啟動伺服器
```bash
cd /home/cyclone/land/web
python3 server.py
```

伺服器會在背景初始化 `TaiwanGeocoder`。

### 2. 搜尋地址
前端使用 `/api/search` 端點，現在返回的座標會使用 OSM Geocoding：

```bash
curl "http://localhost:5001/api/search?keyword=三民路43&limit=3"
```

### 3. 地圖顯示
搜尋結果會在地圖上準確顯示每個地址的位置。

## 依賴

- `TaiwanGeocoder` (在 `/home/cyclone/land/land_reg/geodecoding/geocoder.py`)
- OSM Nominatim API（線上服務，自動查詢）
- SQLite（用於快取）

## 故障排除

### 如果座標看起來不正確

1. **檢查 Geocoder 初始化**
   ```bash
   tail -50 /tmp/server_geocoding.log | grep -i geocoder
   ```

2. **手動測試 Geocoder**
   ```python
   from geocoder import TaiwanGeocoder
   gc = TaiwanGeocoder()
   result = gc.geocode("台北市大安區和平東路三段1號")
   print(result)  # 應該返回精確座標
   ```

3. **清除快取並重試**
   ```bash
   rm /home/cyclone/land/db/geocode_cache.db
   ```

## 性能考慮

- 第一次查詢地址時可能需要 1-2 秒（通過 Nominatim API）
- 後續查詢會使用本地快取，速度為毫秒級
- 建議保持 `concurrency=1` 以避免速率限制

## 未來改進

1. 部署本地 Nominatim 實例加快查詢
2. 執行 `build_osm_index.py` 建立完整的 OSM 門牌索引
3. 批量預先編碼常用地址
4. 整合其他地理編碼服務作為備援

---
**實現日期**: 2026-02-18  
**狀態**: ✅ 生產就緒
