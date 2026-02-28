-- ══════════════════════════════════════════════════════════════
-- optimize_indexes.sql — 不動產實價登錄資料庫索引優化
-- ══════════════════════════════════════════════════════════════
-- 使用方式:
--   sqlite3 land_data.db < optimize_indexes.sql
--
-- 或在 Python 中:
--   conn.executescript(open('optimize_indexes.sql').read())
-- ══════════════════════════════════════════════════════════════

-- 建案名稱 + 交易日期（加速建案搜尋 + 時間排序）
CREATE INDEX IF NOT EXISTS idx_community_date
ON land_transaction(community_name, transaction_date DESC);

-- 區域搜尋覆蓋索引（lat/lng 範圍查詢 + 日期排序）
CREATE INDEX IF NOT EXISTS idx_lat_lng_date
ON land_transaction(lat, lng, transaction_date DESC)
WHERE lat IS NOT NULL AND lng IS NOT NULL;

-- 建物型態篩選（加速 building_type 過濾）
CREATE INDEX IF NOT EXISTS idx_building_type
ON land_transaction(building_type);

-- 行政區 + 交易日期（加速區域篩選）
CREATE INDEX IF NOT EXISTS idx_district_date
ON land_transaction(district, transaction_date DESC);

-- 交易日期索引（加速時間範圍查詢）
CREATE INDEX IF NOT EXISTS idx_transaction_date
ON land_transaction(transaction_date DESC);

-- 複合索引: 行政區 + 建物型態 + 日期（常見篩選組合）
CREATE INDEX IF NOT EXISTS idx_district_type_date
ON land_transaction(district, building_type, transaction_date DESC);

-- 分析統計更新
ANALYZE;

-- 顯示結果
SELECT 'Indexes optimized successfully' AS status;
