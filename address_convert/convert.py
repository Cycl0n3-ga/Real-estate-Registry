#!/usr/bin/env python3
"""
台灣不動產實價登錄資料轉換腳本 v4

核心功能:
  1. 自動識別輸入資料來源 (CSV / API DB / 其他 .db)
  2. 清洗、正規化、結構化
  3. 增量匯入 land_data.db (去重 + enrich + 新增)

用法:
  python3 convert.py <input_file>                    # 自動識別
  python3 convert.py data.csv                        # CSV 匯入
  python3 convert.py transactions.db                 # API DB 匯入
  python3 convert.py a.csv b.db c.csv                # 多檔依序匯入
  python3 convert.py --rebuild a.csv b.db            # 重建 land_data.db
  python3 convert.py --target /path/to/land_data.db  # 指定目標 DB

去重策略:
  以 (交易日期前7碼 + 正規化地址 + 總價) 三鍵判斷是否為同一筆交易。
  同一天、同地址但不同價格視為不同交易。
    - 已存在且新資料有額外欄位 → enrich (補充)
    - 不存在 → 新增
    - 資料缺損 (無地址/無號) → 丟棄
"""

import csv
import json
import sqlite3
import os
import sys
import argparse
import re
import time
import hashlib
import math
from enum import Enum
from typing import Optional, Dict, List, Tuple, Any

# ── 共用模組 ──────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

# 全域 verbose 旗標 (由 main() 設定)
_VERBOSE = False
_VERBOSE_MAX = float('inf')  # 不限制：所有範例都印出並寫入 log

# 日誌檔案句柄與函式
_LOG_FILE = None

def log_print(*args, **kwargs):
    """同時輸出到 stdout 和日誌檔案"""
    msg = ' '.join(str(a) for a in args)
    print(*args, **kwargs)
    if _LOG_FILE:
        print(msg, file=_LOG_FILE, flush=True)

def init_logging(log_path: str):
    """初始化日誌檔案"""
    global _LOG_FILE
    try:
        _LOG_FILE = open(log_path, 'w', encoding='utf-8', buffering=1)
        log_print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] 開始匯入')
    except Exception as e:
        print(f'⚠️ 無法開啟日誌檔案: {e}', flush=True)
        _LOG_FILE = None

def close_logging():
    """關閉日誌檔案"""
    global _LOG_FILE
    if _LOG_FILE:
        log_print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] 匯入完成')
        _LOG_FILE.close()
        _LOG_FILE = None

from address_utils import (
    normalize_address,
    parse_address,
    chinese_numeral_to_int,
    fullwidth_to_halfwidth,
    CITY_CODE_MAP,
    AMBIGUOUS_DISTRICTS,
    DISTRICT_CITY_MAP,
)

# 向後相容別名 (供 test_convert.py 等使用)
normalize_address_numbers = normalize_address


# ═══════════════════════════════════════════════════════════════════════════════
# 第一層: 安全型別轉換
# ═══════════════════════════════════════════════════════════════════════════════

def safe_int(val, default=None):
    if val is None or val == '':
        return default
    if isinstance(val, int):
        return val
    try:
        # 快速路徑: 純數字字串
        return int(val)
    except (ValueError, TypeError):
        try:
            return int(float(str(val).replace(',', '').replace(' ', '')))
        except (ValueError, TypeError):
            return default


def safe_float(val, default=None):
    if val is None or val == '':
        return default
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        try:
            return float(str(val).replace(',', ''))
        except (ValueError, TypeError):
            return default


def parse_price(val):
    """'39,380,000' → int"""
    if not val:
        return None
    try:
        return int(str(val).replace(',', '').replace(' ', ''))
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 第二層: 資料來源自動識別
# ═══════════════════════════════════════════════════════════════════════════════

class SourceType(Enum):
    """資料來源類型"""
    CSV_LVR = 'csv_lvr'        # 政府實價登錄 CSV (33 欄, 雙行表頭)
    CSV_GENERIC = 'csv_generic' # 其他 CSV (嘗試欄位名映射)
    API_DB = 'api_db'          # transactions.db (LVR API 抓取)
    LAND_DB = 'land_db'        # 已存在的 land_data.db
    UNKNOWN = 'unknown'


# LVR CSV 的已知中文標頭關鍵字 (前8欄)
_LVR_CSV_KEYWORDS = {'鄉鎮市區', '交易標的', '土地位置建物門牌', '交易年月日', '總價元'}


def detect_source(filepath: str) -> SourceType:
    """
    自動偵測輸入來源類型。

    偵測邏輯:
      .csv  → 讀前2行標頭 → LVR CSV or generic CSV
      .db   → 查 schema → transactions 表 → API_DB
                        → land_transaction 表 → LAND_DB
      其他  → UNKNOWN
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext == '.csv':
        return _detect_csv_type(filepath)
    elif ext in ('.db', '.sqlite', '.sqlite3'):
        return _detect_db_type(filepath)
    else:
        # 嘗試當 CSV 讀
        try:
            return _detect_csv_type(filepath)
        except Exception:
            return SourceType.UNKNOWN


def _detect_csv_type(filepath: str) -> SourceType:
    """偵測 CSV 子類型"""
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline().strip()
    except Exception:
        return SourceType.UNKNOWN

    # 用逗號拆開看有沒有 LVR 關鍵字
    fields = set(first_line.split(','))
    if fields & _LVR_CSV_KEYWORDS:
        return SourceType.CSV_LVR

    # 看有沒有其他可辨識的欄位
    known_cols = {'address', '地址', 'total_price', '總價', 'transaction_date', '交易日期'}
    if fields & known_cols:
        return SourceType.CSV_GENERIC

    # fallback: 如果欄數 >= 28 且第一行看起來像中文標頭，視為 LVR
    if len(fields) >= 28:
        return SourceType.CSV_LVR

    return SourceType.CSV_GENERIC


def _detect_db_type(filepath: str) -> SourceType:
    """偵測 SQLite DB 子類型"""
    try:
        conn = sqlite3.connect(filepath)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception:
        return SourceType.UNKNOWN

    if 'land_transaction' in tables:
        return SourceType.LAND_DB
    if 'transactions' in tables:
        return SourceType.API_DB

    return SourceType.UNKNOWN


# ═══════════════════════════════════════════════════════════════════════════════
# 第三層: 地址/日期/樓層 工具函式
# ═══════════════════════════════════════════════════════════════════════════════

CHINESE_FLOOR = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7,
    '八': 8, '九': 9, '十': 10, '十一': 11, '十二': 12, '十三': 13,
    '十四': 14, '十五': 15, '十六': 16, '十七': 17, '十八': 18,
    '十九': 19, '二十': 20, '二十一': 21, '二十二': 22, '二十三': 23,
    '二十四': 24, '二十五': 25, '二十六': 26, '二十七': 27,
    '二十八': 28, '二十九': 29, '三十': 30,
    '地下一': -1, '地下二': -2, '地下三': -3,
}


def parse_floor_info(floor_str):
    """解析樓層欄位: '九層/十五層' → ('9', '15')"""
    if not floor_str:
        return '', ''
    parts = floor_str.split('/')
    if len(parts) == 2:
        fl, tf = parts[0].strip(), parts[1].strip()
        for s, attr in ((fl, 'fl'), (tf, 'tf')):
            stripped = s.rstrip('層')
            if stripped in CHINESE_FLOOR:
                if attr == 'fl':
                    fl = str(CHINESE_FLOOR[stripped])
                else:
                    tf = str(CHINESE_FLOOR[stripped])
        return fl, tf
    return floor_str.strip(), ''


def normalize_date(date_str):
    """'101/01/09' → '1010109'"""
    if not date_str:
        return ''
    return date_str.replace('/', '')


def clean_trans_addr(addr_raw):
    """取 transactions.db 地址 '#' 後半部的乾淨地址"""
    if addr_raw and '#' in addr_raw:
        return addr_raw.split('#', 1)[1]
    return addr_raw or ''


def norm_addr_simple(addr):
    """簡單正規化: 全形→半形、臺→台、去空白"""
    return fullwidth_to_halfwidth(addr or '').replace('臺', '台').replace(' ', '')


def strip_city(addr):
    """移除地址開頭的縣市名"""
    for city in CITY_CODE_MAP.values():
        if addr.startswith(city):
            return addr[len(city):]
    for old in ('台北縣', '桃園縣', '台中縣', '台南縣', '高雄縣'):
        if addr.startswith(old):
            return addr[len(old):]
    return addr


def strip_floor(addr):
    """去除尾端樓層資訊，取得建物基礎地址"""
    addr = re.sub(r'(-\d+|地下\d+|\d+)樓[之\d]*$', '', addr)
    return addr.rstrip('之号號 ')


# ═══════════════════════════════════════════════════════════════════════════════
# 第四層: land_data.db 管理 (schema + 去重 + enrich)
# ═══════════════════════════════════════════════════════════════════════════════

# —— land_transaction 所有欄位名 (不含 id) ——
LAND_COLUMNS = [
    'raw_district', 'transaction_type', 'address', 'land_area',
    'urban_zone', 'non_urban_zone', 'non_urban_use',
    'transaction_date', 'transaction_count', 'floor_level', 'total_floors',
    'building_type', 'main_use', 'main_material', 'build_date',
    'building_area', 'rooms', 'halls', 'bathrooms', 'partitioned',
    'has_management', 'total_price', 'unit_price',
    'parking_type', 'parking_area', 'parking_price',
    'note', 'serial_no', 'main_area', 'attached_area', 'balcony_area',
    'elevator', 'transfer_no',
    'county_city', 'district', 'village', 'street', 'lane', 'alley',
    'number', 'floor', 'sub_number',
    'community_name', 'lat', 'lng',
]

INSERT_SQL = (
    'INSERT INTO land_transaction ('
    + ', '.join(LAND_COLUMNS)
    + ') VALUES ('
    + ', '.join(['?'] * len(LAND_COLUMNS))
    + ')'
)

# 判定「空」的欄位名 → 判空函數
_EMPTY_NUMERIC = lambda v: v is None or v == 0
_EMPTY_TEXT = lambda v: not v

# 哪些欄位在比較時算「有資訊」（用於 enrich 判斷）
ENRICH_FIELDS = [
    ('lat',              _EMPTY_NUMERIC),
    ('lng',              _EMPTY_NUMERIC),
    ('community_name',   _EMPTY_TEXT),
    ('county_city',      _EMPTY_TEXT),
    ('building_type',    _EMPTY_TEXT),
    ('main_use',         _EMPTY_TEXT),
    ('main_material',    _EMPTY_TEXT),
    ('has_management',   _EMPTY_TEXT),
    ('rooms',            lambda v: v is None),
    ('halls',            lambda v: v is None),
    ('bathrooms',        lambda v: v is None),
    ('building_area',    _EMPTY_NUMERIC),
    ('unit_price',       _EMPTY_NUMERIC),
    ('transaction_type', _EMPTY_TEXT),
    ('floor_level',      _EMPTY_TEXT),
    ('total_floors',     _EMPTY_TEXT),
    ('note',             _EMPTY_TEXT),
    ('land_area',        _EMPTY_NUMERIC),
    ('urban_zone',       _EMPTY_TEXT),
    ('parking_type',     _EMPTY_TEXT),
    ('parking_area',     _EMPTY_NUMERIC),
    ('parking_price',    _EMPTY_NUMERIC),
    ('main_area',        _EMPTY_NUMERIC),
    ('attached_area',    _EMPTY_NUMERIC),
    ('balcony_area',     _EMPTY_NUMERIC),
    ('elevator',         _EMPTY_TEXT),
]

INSERT_DEDUP_SQL = (
    'INSERT INTO land_transaction ('
    + ', '.join(LAND_COLUMNS + ['dedup_key'])
    + ') VALUES ('
    + ', '.join(['?'] * (len(LAND_COLUMNS) + 1))
    + ')'
)


class _BloomFilter:
    """Compact bloom filter for dedup key existence checking.

    For 5M items at 0.1% false-positive rate:
      - size ≈ 72M bits ≈ 9 MB
      - num_hashes ≈ 10
    Memory is O(1) regardless of item count (fixed-size bytearray).
    """
    __slots__ = ('size', 'num_hashes', 'bits')

    def __init__(self, expected_items: int = 5_000_000, fp_rate: float = 0.001):
        self.size = int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        self.num_hashes = max(1, int((self.size / expected_items) * math.log(2)))
        self.bits = bytearray((self.size + 7) // 8)

    def _hashes(self, key: str):
        h = hashlib.md5(key.encode('utf-8')).digest()
        h1 = int.from_bytes(h[:8], 'little')
        h2 = int.from_bytes(h[8:], 'little')
        size = self.size
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % size

    def add(self, key: str):
        bits = self.bits
        for pos in self._hashes(key):
            bits[pos >> 3] |= (1 << (pos & 7))

    def __contains__(self, key: str) -> bool:
        bits = self.bits
        return all(bits[pos >> 3] & (1 << (pos & 7)) for pos in self._hashes(key))

    def memory_mb(self) -> float:
        return len(self.bits) / 1024 / 1024


class LandDataDB:
    """
    管理 land_data.db 的讀寫、去重與 enrich。

    使用方式:
        db = LandDataDB('/path/to/land_data.db')
        db.open()
        db.upsert_record(record_dict)   # 自動去重 + enrich
        db.finalize()                    # 建索引 + FTS + VACUUM
        db.close()
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._bloom = _BloomFilter(expected_items=5_000_000, fp_rate=0.001)
        self._batch_keys: set = set()  # 當前批次的 dedup_key (bounded to BATCH_SIZE)
        self._insert_batch: list = []
        self._enrich_batch: list = []
        self._init_stats()
        self.BATCH_SIZE = 50000

    def _init_stats(self):
        self._stats = {
            'inserted': 0, 'enriched': 0,
            'duplicated': 0, 'discarded': 0, 'total_scanned': 0,
            'discard_no_addr': 0,
            'discard_no_number': 0,
            'discard_parse_err': 0,
        }
        self._verbose_count = {'discarded': 0, 'enriched': 0, 'duplicated': 0}

    def open(self, rebuild=False, load_dedup=True):
        """
        開啟 (或建立) land_data.db。
        rebuild=True 時會刪除舊 DB 重建。
        load_dedup=False 時跳過去重鍵載入（僅做 backfill 時使用）。
        """
        if rebuild and os.path.exists(self.db_path):
            os.remove(self.db_path)
            log_print(f'  🗑  已刪除舊資料庫: {self.db_path}')

        is_new = not os.path.exists(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        cur = self.conn.cursor()

        # 批量匯入效能設定 (finalize 時會恢復)
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('PRAGMA synchronous=OFF')        # 匯入期間關閉同步 (finalize 恢復)
        cur.execute('PRAGMA cache_size=-256000')      # 256MB cache
        cur.execute('PRAGMA temp_store=MEMORY')
        cur.execute('PRAGMA locking_mode=EXCLUSIVE')  # 獨佔鎖定避免鎖開銷
        cur.execute('PRAGMA page_size=8192')           # 較大頁面提升大表效能

        self._create_tables(cur)
        cur.execute('CREATE INDEX IF NOT EXISTS idx_dedup_key ON land_transaction(dedup_key)')
        self.conn.commit()

        if is_new:
            log_print(f'  ✨ 建立新資料庫: {self.db_path}')
        else:
            count = cur.execute('SELECT COUNT(*) FROM land_transaction').fetchone()[0]
            log_print(f'  📂 開啟既有資料庫: {self.db_path} ({count:,} 筆)')
            # 增量匯入時，先暫時移除非必要索引以加速寫入
            self._drop_non_essential_indexes(cur)

        # 載入去重鍵值
        if load_dedup:
            self._load_dedup_keys()

    def _create_tables(self, cursor):
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS land_transaction (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_district    TEXT,
                transaction_type TEXT,
                address         TEXT,
                land_area       REAL,
                urban_zone      TEXT,
                non_urban_zone  TEXT,
                non_urban_use   TEXT,
                transaction_date TEXT,
                transaction_count TEXT,
                floor_level     TEXT,
                total_floors    TEXT,
                building_type   TEXT,
                main_use        TEXT,
                main_material   TEXT,
                build_date      TEXT,
                building_area   REAL,
                rooms           INTEGER,
                halls           INTEGER,
                bathrooms       INTEGER,
                partitioned     TEXT,
                has_management  TEXT,
                total_price     INTEGER,
                unit_price      REAL,
                parking_type    TEXT,
                parking_area    REAL,
                parking_price   INTEGER,
                note            TEXT,
                serial_no       TEXT,
                main_area       REAL,
                attached_area   REAL,
                balcony_area    REAL,
                elevator        TEXT,
                transfer_no     TEXT,
                county_city     TEXT,
                district        TEXT,
                village         TEXT,
                street          TEXT,
                lane            TEXT,
                alley           TEXT,
                number          TEXT,
                floor           TEXT,
                sub_number      TEXT,
                community_name  TEXT,
                lat             REAL,
                lng             REAL,
                dedup_key       TEXT
            )
        ''')

    def _load_dedup_keys(self):
        """從既有資料載入 dedup_key 到 Bloom filter (~9 MB)"""
        cur = self.conn.cursor()
        # 檢查是否有 dedup_key 欄位 (向後相容)
        cur.execute('PRAGMA table_info(land_transaction)')
        cols = {row[1] for row in cur.fetchall()}
        if 'dedup_key' not in cols:
            log_print('    ⚠ 舊版 DB 無 dedup_key 欄位，跳過載入')
            return

        cur.execute('SELECT dedup_key FROM land_transaction WHERE dedup_key IS NOT NULL')
        count = 0
        for (key,) in cur:
            self._bloom.add(key)
            count += 1
        log_print(f'    Bloom filter: {count:,} 既有鍵值 (~{self._bloom.memory_mb():.1f} MB)')

    def _drop_non_essential_indexes(self, cursor):
        """暫時移除非去重索引，大幅加速批量寫入"""
        # 保留 idx_dedup_key (去重必需)，其餘在 finalize() 重建
        drop_indexes = [
            'idx_county_city', 'idx_district', 'idx_street', 'idx_lane',
            'idx_number', 'idx_floor', 'idx_date', 'idx_price', 'idx_serial',
            'idx_community',
            'idx_addr_combo', 'idx_community_address', 'idx_street_lane_district',
            'idx_search_numbers', 'idx_district_street_number',
            'idx_district_street_lane', 'idx_community_district',
        ]
        dropped = 0
        for idx_name in drop_indexes:
            try:
                cursor.execute(f'DROP INDEX IF EXISTS {idx_name}')
                dropped += 1
            except Exception:
                pass
        if dropped:
            self.conn.commit()
            log_print(f'    🗑  暫移 {dropped} 個索引 (finalize 時重建)')

    def upsert_record(self, rec: dict):
        """
        智慧匯入一筆記錄。

        邏輯:
          1. 檢驗資料品質 → 不合格 → discard
          2. 計算 dedup_key = "date7|addr_norm|price"
          3. 檢查 batch_keys → bloom filter → DB
          4. 已存在 → enrich (補充空欄位) 或 duplicate
          5. 不存在 → insert
        """
        self._stats['total_scanned'] += 1

        # —— 資料品質驗證 ——
        addr = rec.get('address', '')
        if not addr:
            self._stats['discarded'] += 1
            self._stats['discard_no_addr'] += 1
            return
        if not re.search(r'號|地號', addr):
            self._stats['discarded'] += 1
            self._stats['discard_no_number'] += 1
            if _VERBOSE and self._verbose_count['discarded'] < _VERBOSE_MAX:
                log_print(f'    [丟棄] 無號: {addr}')
                self._verbose_count['discarded'] += 1
            return

        # —— 計算 dedup key (三鍵: 日期 + 地址 + 總價) ——
        date_str = rec.get('transaction_date', '') or ''
        d = date_str.replace('/', '')[:7]
        addr_norm = strip_city(norm_addr_simple(addr))
        price = rec.get('total_price') or 0
        try:
            price = int(price)
        except (ValueError, TypeError):
            price = 0

        if not addr_norm:
            # 無法正規化地址 → 直接插入 (不做去重)
            values = tuple(rec.get(col) for col in LAND_COLUMNS)
            self._insert_batch.append((*values, None))
            self._stats['inserted'] += 1
            if len(self._insert_batch) >= self.BATCH_SIZE:
                self._flush_inserts()
            return

        dedup_key = f"{d}|{addr_norm}|{price}"

        # —— Level 1: 檢查當前批次 (O(1), set 最多 BATCH_SIZE 個) ——
        if dedup_key in self._batch_keys:
            self._stats['duplicated'] += 1
            if _VERBOSE and self._verbose_count['duplicated'] < _VERBOSE_MAX:
                log_print(f'    [重複-batch] serial={rec.get("serial_no","?")} key={dedup_key}: {addr}')
                self._verbose_count['duplicated'] += 1
            return

        # —— Level 2: 檢查 Bloom filter (~9 MB, O(k)) ——
        if dedup_key in self._bloom:
            # Bloom filter hit → 可能是重複，查 DB 確認 (0.1% 偽陽性)
            row = self.conn.execute(
                'SELECT id FROM land_transaction WHERE dedup_key = ?',
                (dedup_key,)
            ).fetchone()
            if row:
                existing_id = row[0]
                enriched = self._try_enrich(existing_id, rec)
                if enriched:
                    self._stats['enriched'] += 1
                    if _VERBOSE and self._verbose_count['enriched'] < _VERBOSE_MAX:
                        detail = ', '.join(f'{k}={v}' for k, v in enriched.items())
                        log_print(f'    [補充] exist_id={existing_id} serial={rec.get("serial_no","?")} {detail}: {addr}')
                        self._verbose_count['enriched'] += 1
                else:
                    self._stats['duplicated'] += 1
                    if _VERBOSE and self._verbose_count['duplicated'] < _VERBOSE_MAX:
                        log_print(f'    [重複] exist_id={existing_id} serial={rec.get("serial_no","?")} key={dedup_key}: {addr}')
                        self._verbose_count['duplicated'] += 1
                return
            # Bloom false positive → fall through to insert

        # —— 新記錄 → 插入 ——
        values = tuple(rec.get(col) for col in LAND_COLUMNS)
        self._insert_batch.append((*values, dedup_key))
        self._batch_keys.add(dedup_key)
        self._bloom.add(dedup_key)
        self._stats['inserted'] += 1

        if len(self._insert_batch) >= self.BATCH_SIZE:
            self._flush_inserts()

    def _try_enrich(self, row_id: int, new_rec: dict) -> list:
        """
        嘗試用新資料補充既有記錄的空欄位。
        回傳補充的欄位名列表 (空列表=沒更新)。
        """
        # 讀取既有欄位
        cols_to_check = [col for col, _ in ENRICH_FIELDS]
        col_sql = ', '.join(cols_to_check)
        cur = self.conn.cursor()
        row = cur.execute(
            f'SELECT {col_sql} FROM land_transaction WHERE id = ?',
            (row_id,)
        ).fetchone()
        if not row:
            return False

        updates = {}
        for i, (col_name, is_empty) in enumerate(ENRICH_FIELDS):
            current_val = row[i]
            if is_empty(current_val):
                new_val = new_rec.get(col_name)
                if new_val is not None and new_val != '' and new_val != 0:
                    updates[col_name] = new_val

        if not updates:
            return []

        self._enrich_batch.append((updates, row_id))
        if len(self._enrich_batch) >= self.BATCH_SIZE:
            self._flush_enriches()
        return updates

    def _flush_inserts(self):
        if not self._insert_batch:
            return
        self.conn.executemany(INSERT_DEDUP_SQL, self._insert_batch)
        self.conn.commit()
        self._insert_batch = []
        self._batch_keys.clear()

    def _flush_enriches(self):
        if not self._enrich_batch:
            return
        for updates, row_id in self._enrich_batch:
            set_clauses = ', '.join(f'{col} = ?' for col in updates)
            values = list(updates.values()) + [row_id]
            self.conn.execute(
                f'UPDATE land_transaction SET {set_clauses} WHERE id = ?',
                values
            )
        self.conn.commit()
        self._enrich_batch = []

    def flush_all(self):
        """強制寫入所有待處理批次"""
        self._flush_inserts()
        self._flush_enriches()

    def fast_insert_records(self, records):
        """
        批次快速插入 (跳過逐筆 upsert 的 Python 開銷)。

        適用於: rebuild 模式或確認無需 enrich 的場景。
        邏輯:
          1. 使用預計算的 _dedup_key (若有)
          2. 批次 bloom filter 檢查 (Python set 去重同批重複)
          3. 一次 executemany 插入

        比 upsert_record 快 3-4 倍 (減少 per-record Python 開銷)。
        """
        batch_insert = []
        _norm = norm_addr_simple
        _strip = strip_city
        _bloom = self._bloom
        _batch_keys = self._batch_keys
        stats = self._stats

        for rec in records:
            stats['total_scanned'] += 1

            addr = rec.get('address', '')
            if not addr:
                stats['discarded'] += 1
                stats['discard_no_addr'] += 1
                continue
            if '號' not in addr and '地號' not in addr:
                stats['discarded'] += 1
                stats['discard_no_number'] += 1
                continue

            # 使用預計算的 _dedup_key (若 parser 已提供)
            dedup_key = rec.get('_dedup_key')
            if dedup_key is None:
                # fallback: 動態計算
                date_str = rec.get('transaction_date', '') or ''
                d = date_str.replace('/', '')[:7]
                addr_norm = _strip(_norm(addr))
                price = rec.get('total_price') or 0
                try:
                    price = int(price)
                except (ValueError, TypeError):
                    price = 0
                dedup_key = f"{d}|{addr_norm}|{price}" if addr_norm else None

            if dedup_key:
                # 快速去重: set + bloom (不查 DB)
                if dedup_key in _batch_keys:
                    stats['duplicated'] += 1
                    continue

                if dedup_key in _bloom:
                    stats['duplicated'] += 1
                    continue

                _batch_keys.add(dedup_key)
                _bloom.add(dedup_key)

            values = tuple(rec.get(col) for col in LAND_COLUMNS)
            batch_insert.append((*values, dedup_key))
            stats['inserted'] += 1

        # 批量插入
        if batch_insert:
            self.conn.executemany(INSERT_DEDUP_SQL, batch_insert)

        # 避免 batch_keys 無限成長
        if len(self._batch_keys) > 100000:
            self._batch_keys.clear()

    def fast_insert_tuples(self, tuples_list):
        """
        極速批次插入 (直接接收 tuple 列表，跳過所有 dict 開銷)。

        每個 tuple 格式: (*LAND_COLUMNS_values, dedup_key)
        address 欄位在 tuple[2]，dedup_key 在 tuple[-1]。
        """
        batch_insert = []
        _bloom = self._bloom
        _batch_keys = self._batch_keys
        stats = self._stats

        for tup in tuples_list:
            stats['total_scanned'] += 1
            addr = tup[2]  # address 是第 3 個欄位

            if not addr:
                stats['discarded'] += 1
                stats['discard_no_addr'] += 1
                continue
            if '號' not in addr and '地號' not in addr:
                stats['discarded'] += 1
                stats['discard_no_number'] += 1
                continue

            dedup_key = tup[-1]  # 最後一個欄位

            if dedup_key:
                if dedup_key in _batch_keys:
                    stats['duplicated'] += 1
                    continue
                if dedup_key in _bloom:
                    stats['duplicated'] += 1
                    continue
                _batch_keys.add(dedup_key)
                _bloom.add(dedup_key)

            batch_insert.append(tup)
            stats['inserted'] += 1

        if batch_insert:
            self.conn.executemany(INSERT_DEDUP_SQL, batch_insert)

        if len(self._batch_keys) > 100000:
            self._batch_keys.clear()

    def backfill_community(self, api_db_path: str):
        """
        從 API DB 回填 community_name。

        演算法（O(N) 單次掃描，不用 LIKE）:
          Phase 1: 從 API DB 建立 地址鍵值(去縣市+去樓層+半形) → community 映射
          Phase 2: 掃描 land_transaction，對無社區記錄做 Python dict 比對
                   → batch UPDATE
          ※ 全形/半形地址統一在 Python 正規化後比對，不再依賴 SQL LIKE
        """
        if not os.path.exists(api_db_path):
            return 0

        print('  回填社區名...', flush=True)
        conn_t = sqlite3.connect(api_db_path)
        conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
        rows = conn_t.execute(
            "SELECT city, address, community FROM transactions "
            "WHERE community != '' AND community IS NOT NULL AND address != ''"
        ).fetchall()
        conn_t.close()

        # Phase 1: addr_key → {community: vote_count}
        # addr_key = 去縣市 + 去樓層 + 半形正規化
        votes: dict = {}
        for _city_code, addr_raw, community in rows:
            addr = strip_floor(strip_city(norm_addr_simple(clean_trans_addr(addr_raw))))
            if not addr or '號' not in addr:
                continue
            bucket = votes.setdefault(addr, {})
            bucket[community] = bucket.get(community, 0) + 1

        comm_map = {addr: max(v, key=v.get) for addr, v in votes.items()}
        print(f'    社區映射: {len(comm_map):,} 個地址鍵值', flush=True)

        # Phase 2: 單次掃描，比對無社區的記錄
        cur = self.conn.cursor()
        updates: list = []
        updated = 0

        for row_id, addr in cur.execute(
            "SELECT id, address FROM land_transaction "
            "WHERE community_name IS NULL OR community_name = ''"
        ):
            # 全形→半形正規化後比對，解決 CSV 全形與 API 半形不一致的問題
            norm = strip_floor(strip_city(norm_addr_simple(addr or '')))
            community = comm_map.get(norm)
            if community:
                updates.append((community, row_id))
                if len(updates) >= 5000:
                    self.conn.executemany(
                        "UPDATE land_transaction SET community_name = ? WHERE id = ?",
                        updates
                    )
                    self.conn.commit()
                    updated += len(updates)
                    updates = []

        if updates:
            self.conn.executemany(
                "UPDATE land_transaction SET community_name = ? WHERE id = ?",
                updates
            )
            self.conn.commit()
            updated += len(updates)

        return updated

    def finalize(self):
        """建索引 + FTS5 + ANALYZE + VACUUM，並恢復安全的 PRAGMA 設定"""
        self.flush_all()
        cur = self.conn.cursor()

        # 恢復安全的同步設定
        cur.execute('PRAGMA synchronous=NORMAL')
        self.conn.commit()

        # 單欄索引
        log_print('  📇 建立索引...')
        indexes = [
            ('idx_county_city', 'county_city'),
            ('idx_district', 'district'),
            ('idx_street', 'street'),
            ('idx_lane', 'lane'),
            ('idx_number', 'number'),
            ('idx_floor', 'floor'),
            ('idx_date', 'transaction_date'),
            ('idx_price', 'total_price'),
            ('idx_serial', 'serial_no'),
            ('idx_dedup_key', 'dedup_key'),
            ('idx_community', 'community_name'),
        ]
        for name, col in indexes:
            cur.execute(f'CREATE INDEX IF NOT EXISTS {name} ON land_transaction({col})')

        # 複合索引（加速查詢服務）
        composite_indexes = [
            ('idx_addr_combo', 'county_city, district, street, lane, number'),
            ('idx_community_address', 'community_name, address'),
            ('idx_street_lane_district', 'street, lane, district'),
            ('idx_search_numbers', 'street, lane, district, total_floors, build_date'),
            ('idx_district_street_number', 'district, street, number'),
            ('idx_district_street_lane', 'district, street, lane'),
            ('idx_community_district', 'community_name, district'),
        ]
        for name, cols in composite_indexes:
            cur.execute(f'CREATE INDEX IF NOT EXISTS {name} ON land_transaction({cols})')
        self.conn.commit()

        # FTS5
        log_print('  🔍 建立 FTS5 全文檢索...')
        cur.execute('DROP TABLE IF EXISTS address_fts')
        cur.execute('''
            CREATE VIRTUAL TABLE address_fts USING fts5(
                address,
                content='land_transaction',
                content_rowid='id',
                tokenize='unicode61'
            )
        ''')
        cur.execute('''
            INSERT INTO address_fts(rowid, address)
            SELECT id, address FROM land_transaction WHERE address != ''
        ''')
        self.conn.commit()

        # ANALYZE
        log_print('  📊 更新統計資訊...')
        self.conn.execute('ANALYZE')
        self.conn.commit()

        # VACUUM (需要約等同 DB 大小的額外磁碟空間)
        log_print('  🗜  壓縮資料庫...')
        try:
            self.conn.execute('PRAGMA journal_mode=DELETE')
            self.conn.commit()
            self.conn.execute('VACUUM')
        except sqlite3.OperationalError as e:
            log_print(f'  ⚠️  VACUUM 失敗 ({e})，跳過壓縮 (不影響資料完整性)')
        finally:
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA locking_mode=NORMAL')  # 恢復正常鎖定模式
            self.conn.execute('PRAGMA synchronous=NORMAL')    # 確保安全同步
            self.conn.commit()

    def print_stats(self):
        """印出匯入統計"""
        s = self._stats
        cur = self.conn.cursor()
        total = cur.execute('SELECT COUNT(*) FROM land_transaction').fetchone()[0]
        has_city = cur.execute(
            'SELECT COUNT(*) FROM land_transaction '
            'WHERE county_city IS NOT NULL AND county_city != ""'
        ).fetchone()[0]
        has_geo = cur.execute(
            'SELECT COUNT(*) FROM land_transaction '
            'WHERE lat IS NOT NULL AND lat != 0'
        ).fetchone()[0]
        has_comm = cur.execute(
            'SELECT COUNT(*) FROM land_transaction '
            'WHERE community_name IS NOT NULL AND community_name != ""'
        ).fetchone()[0]
        has_street = cur.execute(
            'SELECT COUNT(*) FROM land_transaction '
            'WHERE street IS NOT NULL AND street != ""'
        ).fetchone()[0]

        pct = lambda n: n / total * 100 if total else 0
        db_size = os.path.getsize(self.db_path) / 1024 / 1024

        log_print(f'\n📊 本次匯入統計:')
        log_print(f'  掃描:    {s["total_scanned"]:,}')
        log_print(f'  新增:    {s["inserted"]:,}')
        log_print(f'  補充:    {s["enriched"]:,}')
        log_print(f'  重複:    {s["duplicated"]:,}')
        log_print(f'  丟棄:    {s["discarded"]:,}'
              + (f'  (無地址={s["discard_no_addr"]:,} / 缺號={s["discard_no_number"]:,} / 例外={s["discard_parse_err"]:,})'
                 if s['discarded'] else ''))
        if _VERBOSE:
            log_print(f'  (verbose 樣本已在上方即時輸出，共印出: '
                      f'丟棄={self._verbose_count["discarded"]} '
                      f'補充={self._verbose_count["enriched"]} '
                      f'重複={self._verbose_count["duplicated"]})')
        
        # 最後顯示資料庫總覽
        log_print(f'\n📦 資料庫總覽:')
        log_print(f'  總筆數:        {total:,}')
        log_print(f'  有縣市名:      {has_city:,} ({pct(has_city):.1f}%)')
        log_print(f'  地址解析成功:  {has_street:,} ({pct(has_street):.1f}%)')
        log_print(f'  有經緯度:      {has_geo:,} ({pct(has_geo):.1f}%)')
        log_print(f'  有社區名:      {has_comm:,} ({pct(has_comm):.1f}%)')
        log_print(f'  資料庫大小:    {db_size:.1f} MB')
        print(f'\n📦 資料庫總覽:')
        print(f'  總筆數:        {total:,}')
        print(f'  有縣市名:      {has_city:,} ({pct(has_city):.1f}%)')
        print(f'  地址解析成功:  {has_street:,} ({pct(has_street):.1f}%)')
        print(f'  有經緯度:      {has_geo:,} ({pct(has_geo):.1f}%)')
        print(f'  有社區名:      {has_comm:,} ({pct(has_comm):.1f}%)')
        print(f'  資料庫大小:    {db_size:.1f} MB')

    def reset_stats(self):
        """重置本次統計 (多檔匯入時可在每檔之間呼叫)"""
        self._init_stats()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


# ═══════════════════════════════════════════════════════════════════════════════
# 第五層: 各來源的 record 解析器
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_csv_row(row: list) -> Optional[dict]:
    """
    將一列 LVR CSV → 標準 record dict。
    回傳 None 表示跳過。
    """
    while len(row) < 33:
        row.append('')

    raw_address = row[2]
    parsed = parse_address(raw_address, row[0])

    # 預計算 dedup key (避免 fast_insert_records 重複正規化)
    addr_norm = strip_city(norm_addr_simple(raw_address)) if raw_address else ''
    date_str = row[7]
    d = date_str.replace('/', '')[:7] if date_str else ''
    price = safe_int(row[21]) or 0
    _dedup_key = f"{d}|{addr_norm}|{price}" if addr_norm else None

    return {
        'raw_district':      row[0],
        'transaction_type':  row[1],
        'address':           row[2],
        'land_area':         safe_float(row[3]),
        'urban_zone':        row[4],
        'non_urban_zone':    row[5],
        'non_urban_use':     row[6],
        'transaction_date':  row[7],
        'transaction_count': row[8],
        'floor_level':       row[9],
        'total_floors':      row[10],
        'building_type':     row[11],
        'main_use':          row[12],
        'main_material':     row[13],
        'build_date':        row[14],
        'building_area':     safe_float(row[15]),
        'rooms':             safe_int(row[16]),
        'halls':             safe_int(row[17]),
        'bathrooms':         safe_int(row[18]),
        'partitioned':       row[19],
        'has_management':    row[20],
        'total_price':       safe_int(row[21]),
        'unit_price':        safe_float(row[22]),
        'parking_type':      row[23],
        'parking_area':      safe_float(row[24]),
        'parking_price':     safe_int(row[25]),
        'note':              row[26],
        'serial_no':         row[27],
        'main_area':         safe_float(row[28]),
        'attached_area':     safe_float(row[29]),
        'balcony_area':      safe_float(row[30]),
        'elevator':          row[31],
        'transfer_no':       row[32] if len(row) > 32 else '',
        'county_city':       parsed['county_city'],
        'district':          parsed['district'],
        'village':           parsed['village'],
        'street':            parsed['street'],
        'lane':              parsed['lane'],
        'alley':             parsed['alley'],
        'number':            parsed['number'],
        'floor':             parsed['floor'],
        'sub_number':        parsed['sub_number'],
        'community_name':    None,
        'lat':               None,
        'lng':               None,
        '_dedup_key':        _dedup_key,
    }


def _parse_csv_row_fast(row: list):
    """
    將一列 LVR CSV → (values_tuple, dedup_key) 快速版。
    直接產生 INSERT 用的 tuple，避免 dict 創建 + 再提取的開銷。
    回傳 None 表示跳過。
    """
    while len(row) < 33:
        row.append('')

    raw_address = row[2]
    parsed = parse_address(raw_address, row[0])

    # 預計算 dedup key
    addr_norm = strip_city(norm_addr_simple(raw_address)) if raw_address else ''
    d = row[7].replace('/', '')[:7] if row[7] else ''
    price = safe_int(row[21]) or 0
    dedup_key = f"{d}|{addr_norm}|{price}" if addr_norm else None

    # 直接建立與 LAND_COLUMNS + ['dedup_key'] 對應的 tuple
    return (
        row[0],                          # raw_district
        row[1],                          # transaction_type
        row[2],                          # address
        safe_float(row[3]),              # land_area
        row[4],                          # urban_zone
        row[5],                          # non_urban_zone
        row[6],                          # non_urban_use
        row[7],                          # transaction_date
        row[8],                          # transaction_count
        row[9],                          # floor_level
        row[10],                         # total_floors
        row[11],                         # building_type
        row[12],                         # main_use
        row[13],                         # main_material
        row[14],                         # build_date
        safe_float(row[15]),             # building_area
        safe_int(row[16]),               # rooms
        safe_int(row[17]),               # halls
        safe_int(row[18]),               # bathrooms
        row[19],                         # partitioned
        row[20],                         # has_management
        safe_int(row[21]),               # total_price
        safe_float(row[22]),             # unit_price
        row[23],                         # parking_type
        safe_float(row[24]),             # parking_area
        safe_int(row[25]),               # parking_price
        row[26],                         # note
        row[27],                         # serial_no
        safe_float(row[28]),             # main_area
        safe_float(row[29]),             # attached_area
        safe_float(row[30]),             # balcony_area
        row[31],                         # elevator
        row[32] if len(row) > 32 else '',  # transfer_no
        parsed['county_city'],           # county_city
        parsed['district'],              # district
        parsed['village'],               # village
        parsed['street'],                # street
        parsed['lane'],                  # lane
        parsed['alley'],                 # alley
        parsed['number'],                # number
        parsed['floor'],                 # floor
        parsed['sub_number'],            # sub_number
        None,                            # community_name
        None,                            # lat
        None,                            # lng
        dedup_key,                       # dedup_key
    )


def _parse_api_row(row) -> Optional[dict]:
    """
    將 transactions.db 一列 → 標準 record dict。
    回傳 None 表示資料缺損。
    """
    _id, city_code, town, addr_raw, build_type, community, \
        date_str, floor_col, area_col, tp_raw, up_raw, \
        lat, lon, sq, rj_text = row

    j = {}
    if rj_text:
        try:
            j = json.loads(rj_text)
        except Exception:
            pass

    # 地址清洗
    addr_clean = clean_trans_addr(addr_raw)
    if not addr_clean or '號' not in addr_clean:
        return None

    # 用 city_code 取得 city_hint → 精確消歧
    city_hint = CITY_CODE_MAP.get(city_code, '')
    parsed = parse_address(addr_clean, city_hint=city_hint)

    # 日期
    transaction_date = normalize_date(date_str)

    # 樓層
    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    # JSON 欄位
    transaction_type = j.get('t', '') or ''
    rooms = safe_int(j.get('j'))
    halls = safe_int(j.get('k'))
    bathrooms = safe_int(j.get('l'))
    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''
    building_type_j = build_type or j.get('b', '') or ''
    note = j.get('note', '') or ''

    total_price = parse_price(tp_raw) or parse_price(j.get('tp'))
    unit_price = safe_float(up_raw) or safe_float(j.get('cp'))
    building_area = safe_float(area_col) or safe_float(j.get('s'))

    serial_no = f'api_{sq}' if sq else None

    lat_val = lat if (lat and lat != 0) else None
    lng_val = lon if (lon and lon != 0) else None
    if not lat_val and j.get('lat'):
        lat_val = j['lat']
    if not lng_val and j.get('lon'):
        lng_val = j['lon']

    floor_parsed = parsed['floor']
    if not floor_parsed and floor_level:
        try:
            int(floor_level)
            floor_parsed = floor_level
        except ValueError:
            pass

    return {
        'raw_district':      parsed.get('district') or town or '',
        'transaction_type':  transaction_type,
        'address':           addr_clean,
        'land_area':         None,
        'urban_zone':        '',
        'non_urban_zone':    '',
        'non_urban_use':     '',
        'transaction_date':  transaction_date,
        'transaction_count': '',
        'floor_level':       floor_level,
        'total_floors':      total_floors,
        'building_type':     building_type_j,
        'main_use':          main_use,
        'main_material':     '',
        'build_date':        '',
        'building_area':     building_area,
        'rooms':             rooms,
        'halls':             halls,
        'bathrooms':         bathrooms,
        'partitioned':       '',
        'has_management':    has_management,
        'total_price':       total_price,
        'unit_price':        unit_price,
        'parking_type':      '',
        'parking_area':      None,
        'parking_price':     None,
        'note':              note,
        'serial_no':         serial_no,
        'main_area':         None,
        'attached_area':     None,
        'balcony_area':      None,
        'elevator':          '',
        'transfer_no':       '',
        'county_city':       parsed['county_city'],
        'district':          parsed['district'],
        'village':           parsed['village'],
        'street':            parsed['street'],
        'lane':              parsed['lane'],
        'alley':             parsed['alley'],
        'number':            parsed['number'],
        'floor':             floor_parsed,
        'sub_number':        parsed['sub_number'],
        'community_name':    community or '',
        'lat':               lat_val,
        'lng':               lng_val,
    }


# 舊版相容: tuple 格式的 API 解析 (供 load_api 使用)
def _parse_api_record(row):
    """[向後相容] 回傳 tuple 格式"""
    rec = _parse_api_row(row)
    if rec is None:
        return None
    return tuple(rec.get(col) for col in LAND_COLUMNS)


def _parse_land_db_row(row, col_names: list) -> Optional[dict]:
    """
    將另一個 land_data.db 的一列 → 標準 record dict。
    (用於合併兩個 land_data.db)
    """
    rec = {}
    for i, col in enumerate(col_names):
        if col == 'id':
            continue
        rec[col] = row[i]
    return rec


def _parse_generic_csv_row(row: list, header_map: dict) -> Optional[dict]:
    """
    將通用 CSV 一列 → 標準 record dict。
    header_map: {csv欄位名 → land_data欄位名}
    """
    rec = {col: None for col in LAND_COLUMNS}

    for csv_col, land_col in header_map.items():
        if csv_col == '_indices':
            continue
        idx = header_map['_indices'].get(csv_col)
        if idx is not None and idx < len(row):
            val = row[idx]
            # 依據欄位類型轉換
            if land_col in ('land_area', 'building_area', 'unit_price',
                            'parking_area', 'main_area', 'attached_area',
                            'balcony_area'):
                rec[land_col] = safe_float(val)
            elif land_col in ('rooms', 'halls', 'bathrooms', 'total_price',
                              'parking_price'):
                rec[land_col] = safe_int(val)
            else:
                rec[land_col] = val or ''

    # 如果有地址，做結構化解析
    addr = rec.get('address', '')
    if addr:
        district_hint = rec.get('raw_district', '') or rec.get('district', '') or ''
        parsed = parse_address(addr, district_hint)
        for k in ('county_city', 'district', 'village', 'street',
                   'lane', 'alley', 'number', 'floor', 'sub_number'):
            if not rec.get(k):
                rec[k] = parsed.get(k, '')

    return rec


# 通用 CSV 欄位名稱映射 (csv header → land_data column)
_GENERIC_CSV_MAP = {
    # 中文
    '鄉鎮市區': 'raw_district', '交易標的': 'transaction_type',
    '土地位置建物門牌': 'address', '地址': 'address', '門牌': 'address',
    '土地移轉總面積平方公尺': 'land_area', '土地面積': 'land_area',
    '都市土地使用分區': 'urban_zone', '非都市土地使用分區': 'non_urban_zone',
    '非都市土地使用編定': 'non_urban_use',
    '交易年月日': 'transaction_date', '交易日期': 'transaction_date',
    '交易筆棟數': 'transaction_count',
    '移轉層次': 'floor_level', '總樓層數': 'total_floors',
    '建物型態': 'building_type', '主要用途': 'main_use',
    '主要建材': 'main_material', '建築完成年月': 'build_date',
    '建物移轉總面積平方公尺': 'building_area', '建物面積': 'building_area',
    '建物現況格局-房': 'rooms', '房': 'rooms',
    '建物現況格局-廳': 'halls', '廳': 'halls',
    '建物現況格局-衛': 'bathrooms', '衛': 'bathrooms',
    '建物現況格局-隔間': 'partitioned', '有無管理組織': 'has_management',
    '總價元': 'total_price', '總價': 'total_price',
    '單價元平方公尺': 'unit_price', '單價': 'unit_price',
    '車位類別': 'parking_type',
    '車位移轉總面積(平方公尺)': 'parking_area', '車位面積': 'parking_area',
    '車位總價元': 'parking_price', '車位總價': 'parking_price',
    '備註': 'note', '編號': 'serial_no',
    '主建物面積': 'main_area', '附屬建物面積': 'attached_area',
    '陽台面積': 'balcony_area', '電梯': 'elevator', '移轉編號': 'transfer_no',
    '縣市': 'county_city', '區': 'district', '社區': 'community_name',
    '緯度': 'lat', '經度': 'lng',
    # 英文
    'address': 'address', 'total_price': 'total_price',
    'unit_price': 'unit_price', 'transaction_date': 'transaction_date',
    'district': 'district', 'county_city': 'county_city',
    'community': 'community_name', 'lat': 'lat', 'lng': 'lng', 'lon': 'lng',
}


def _build_generic_csv_map(headers: list) -> dict:
    """從 CSV header 建立欄位映射"""
    mapping = {}
    indices = {}
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if h_clean in _GENERIC_CSV_MAP:
            land_col = _GENERIC_CSV_MAP[h_clean]
            mapping[h_clean] = land_col
            indices[h_clean] = i
    mapping['_indices'] = indices
    return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# 第六層: 匯入引擎 (讀取各來源 → 呼叫 db.upsert_record)
# ═══════════════════════════════════════════════════════════════════════════════

def import_csv_lvr(db: LandDataDB, csv_path: str):
    """匯入 LVR 實價登錄 CSV (使用極速 tuple 插入)"""
    log_print(f'\n📄 [CSV-LVR] 匯入: {csv_path}')
    t0 = time.time()

    batch = []
    batch_size = db.BATCH_SIZE
    total = 0

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)  # 中文標頭
        next(reader, None)  # 英文標頭

        for row in reader:
            tup = _parse_csv_row_fast(row)
            if tup:
                batch.append(tup)

            total += 1
            if len(batch) >= batch_size:
                db.fast_insert_tuples(batch)
                db.conn.commit()
                batch = []

                elapsed = time.time() - t0
                rate = total / elapsed if elapsed > 0 else 0
                s = db._stats
                log_print(f'  ⏳ {total:,} 筆 | 新增 {s["inserted"]:,} | '
                      f'補充 {s["enriched"]:,} | 重複 {s["duplicated"]:,} | '
                      f'丟棄 {s["discarded"]:,} ({rate:,.0f}/s)',
                      flush=True)

    if batch:
        db.fast_insert_tuples(batch)
        db.conn.commit()

    elapsed = time.time() - t0
    log_print(f'  ✅ CSV-LVR 完成: {elapsed:.1f}s')


def import_csv_generic(db: LandDataDB, csv_path: str):
    """匯入通用 CSV"""
    log_print(f'\n📄 [CSV-Generic] 匯入: {csv_path}')
    t0 = time.time()

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        header_map = _build_generic_csv_map(headers)

        if not header_map.get('_indices'):
            log_print(f'  ⚠️  無法識別欄位映射，跳過此檔案')
            log_print(f'     偵測到的欄位: {headers[:10]}...')
            return

        mapped = {k: v for k, v in header_map.items() if k != '_indices'}
        log_print(f'  欄位映射: {mapped}')

        for i, row in enumerate(reader, 1):
            rec = _parse_generic_csv_row(row, header_map)
            if rec:
                db.upsert_record(rec)

            if i % 10000 == 0:
                db.flush_all()
                elapsed = time.time() - t0
                s = db._stats
                log_print(f'  ⏳ {i:,} 筆 | 新增 {s["inserted"]:,} | '
                      f'補充 {s["enriched"]:,} | 重複 {s["duplicated"]:,} | '
                      f'丟棄 {s["discarded"]:,} ({elapsed:.0f}s)',
                      flush=True)

    db.flush_all()
    elapsed = time.time() - t0
    log_print(f'  ✅ CSV-Generic 完成: {elapsed:.1f}s')


def import_api_db(db: LandDataDB, api_db_path: str):
    """匯入 API transactions DB (使用批次快速插入)"""
    log_print(f'\n🌐 [API-DB] 匯入: {api_db_path}')
    t0 = time.time()

    conn_t = sqlite3.connect(api_db_path)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
    ct = conn_t.cursor()
    ct.execute(
        'SELECT id, city, town, address, build_type, community, date_str, '
        'floor, area, total_price, unit_price, lat, lon, sq, raw_json '
        'FROM transactions'
    )

    batch = []
    batch_size = db.BATCH_SIZE
    total = 0

    for row in ct:
        total += 1
        try:
            rec = _parse_api_row(row)
        except Exception:
            rec = None

        if rec:
            batch.append(rec)
        else:
            db._stats['discarded'] += 1
            db._stats['discard_parse_err'] += 1
            db._stats['total_scanned'] += 1

        if len(batch) >= batch_size:
            db.fast_insert_records(batch)
            db.conn.commit()
            batch = []

            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            s = db._stats
            log_print(f'  ⏳ {total:,} 筆 | 新增 {s["inserted"]:,} | '
                  f'補充 {s["enriched"]:,} | 重複 {s["duplicated"]:,} | '
                  f'丟棄 {s["discarded"]:,} ({rate:,.0f}/s)',
                  flush=True)

    if batch:
        db.fast_insert_records(batch)
        db.conn.commit()

    conn_t.close()
    elapsed = time.time() - t0
    log_print(f'  ✅ API-DB 完成: {elapsed:.1f}s')


def import_land_db(db: LandDataDB, source_db_path: str):
    """從另一個 land_data.db 匯入 (合併兩個 land_data.db)"""
    print(f'\n📦 [LAND-DB] 匯入: {source_db_path}')
    t0 = time.time()

    conn_s = sqlite3.connect(source_db_path)
    cur_s = conn_s.cursor()

    # 取得來源的欄位名
    cur_s.execute('PRAGMA table_info(land_transaction)')
    col_names = [row[1] for row in cur_s.fetchall()]

    cur_s.execute('SELECT * FROM land_transaction')

    for i, row in enumerate(cur_s, 1):
        try:
            rec = _parse_land_db_row(row, col_names)
        except Exception:
            rec = None

        if rec:
            db.upsert_record(rec)
        else:
            db._stats['discarded'] += 1
            db._stats['total_scanned'] += 1

        if i % 10000 == 0:
            db.flush_all()
            elapsed = time.time() - t0
            s = db._stats
            print(f'  ⏳ {i:,} 筆 | 新增 {s["inserted"]:,} | '
                  f'補充 {s["enriched"]:,} | 重複 {s["duplicated"]:,} | '
                  f'丟棄 {s["discarded"]:,} ({elapsed:.0f}s)',
                  flush=True)

    db.flush_all()
    conn_s.close()
    elapsed = time.time() - t0
    print(f'  ✅ LAND-DB 完成: {elapsed:.1f}s')


# ═══════════════════════════════════════════════════════════════════════════════
# 第七層: 主流程
# ═══════════════════════════════════════════════════════════════════════════════

def import_file(db: LandDataDB, filepath: str):
    """
    自動偵測並匯入單一檔案。
    """
    if not os.path.exists(filepath):
        log_print(f'  ❌ 檔案不存在: {filepath}')
        return

    source_type = detect_source(filepath)
    log_print(f'  🔍 偵測到來源類型: {source_type.value}')

    if source_type == SourceType.CSV_LVR:
        import_csv_lvr(db, filepath)
    elif source_type == SourceType.CSV_GENERIC:
        import_csv_generic(db, filepath)
    elif source_type == SourceType.API_DB:
        import_api_db(db, filepath)
    elif source_type == SourceType.LAND_DB:
        # 防止自己匯入自己
        target_real = os.path.realpath(db.db_path)
        source_real = os.path.realpath(filepath)
        if target_real == source_real:
            log_print(f'  ⚠️  來源與目標是同一個檔案，跳過')
            return
        import_land_db(db, filepath)
    else:
        log_print(f'  ❌ 無法識別的資料來源格式: {filepath}')
        return


def convert_v4(input_files: List[str], target_path: str,
               rebuild: bool = False, skip_finalize: bool = False,
               verbose: bool = False):
    """
    主要轉換流程 (v4)。

    Args:
        input_files:    要匯入的檔案路徑列表
        target_path:    目標 land_data.db 路徑
        rebuild:        是否重建 (刪除舊 DB)
        skip_finalize:  跳過索引/FTS/VACUUM (多批匯入時最後再做)
    """
    global _VERBOSE
    _VERBOSE = verbose

    log_path = os.path.join(os.path.dirname(target_path), 'land_data_import.log')
    init_logging(log_path)

    log_print(f'\n{"=" * 60}')
    log_print(f'  目標:  {target_path}')
    log_print(f'  模式:  {"重建" if rebuild else "增量匯入"}')
    log_print(f'  輸入:  {len(input_files)} 個檔案')
    for f in input_files:
        log_print(f'         • {f}')
    log_print(f'  Verbose 模式: {verbose} (全域 _VERBOSE={_VERBOSE})')
    if _VERBOSE:
        log_print(f'  詳細log: 開啟 (每種類型前 {_VERBOSE_MAX} 筆範例)')
    log_print(f'{"=" * 60}')

    db = LandDataDB(target_path)
    db.open(rebuild=rebuild)

    t0 = time.time()

    # 追蹤是否有 API DB (用於後續社區回填)
    api_db_files = []

    for filepath in input_files:
        db.reset_stats()
        import_file(db, filepath)
        # 確保 flush all samples before printing stats
        db.flush_all()
        db.print_stats()

        # 記下 API DB 路徑供社區回填
        st = detect_source(filepath)
        if st == SourceType.API_DB:
            api_db_files.append(filepath)

    # 社區回填 (若有 API DB 來源)
    for api_path in api_db_files:
        t_bf = time.time()
        bf_count = db.backfill_community(api_path)
        log_print(f'  ✅ 社區回填: {bf_count:,} 筆 ({time.time() - t_bf:.1f}s)')

    # 索引/FTS/壓縮
    if not skip_finalize:
        db.finalize()

    elapsed = time.time() - t0
    log_print(f'\n🎉 全部完成! 耗時 {elapsed:.1f}s')

    # 最終總覽
    db.reset_stats()
    db.print_stats()
    db.close()
    
    close_logging()
    log_print(f'📝 日誌已保存: {log_path}')


# ── 向後相容: 舊版 v3 API ─────────────────────────────────────────────────────

def load_csv(conn, csv_path):
    """[向後相容] 舊版 CSV 載入 (直接 INSERT，不做去重)"""
    print(f'\n📄 [CSV] 載入: {csv_path}')
    cursor = conn.cursor()
    batch, total, parsed_ok = [], 0, 0
    t0 = time.time()

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader, None)
        next(reader, None)
        for row in reader:
            total += 1
            while len(row) < 33:
                row.append('')
            parsed = parse_address(row[2], row[0])
            if parsed['street']:
                parsed_ok += 1
            values = (
                row[0], row[1], row[2], safe_float(row[3]),
                row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                row[11], row[12], row[13], row[14],
                safe_float(row[15]), safe_int(row[16]), safe_int(row[17]),
                safe_int(row[18]), row[19], row[20],
                safe_int(row[21]), safe_float(row[22]),
                row[23], safe_float(row[24]), safe_int(row[25]),
                row[26], row[27], safe_float(row[28]),
                safe_float(row[29]), safe_float(row[30]),
                row[31], row[32] if len(row) > 32 else '',
                parsed['county_city'], parsed['district'], parsed['village'],
                parsed['street'], parsed['lane'], parsed['alley'],
                parsed['number'], parsed['floor'], parsed['sub_number'],
                None, None, None,
            )
            batch.append(values)
            if len(batch) >= 10000:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                batch = []

    if batch:
        cursor.executemany(INSERT_SQL, batch)
        conn.commit()

    elapsed = time.time() - t0
    pct = parsed_ok / total * 100 if total else 0
    print(f'\n  ✅ CSV 載入完成: {total:,} 筆, '
          f'地址解析率 {pct:.1f}%, {elapsed:.1f}s')
    return total


def load_api(conn, api_db_path):
    """[向後相容] 舊版 API 載入 (直接 INSERT，不做去重)"""
    print(f'\n🌐 [API] 載入: {api_db_path}')
    cursor = conn.cursor()
    conn_t = sqlite3.connect(api_db_path)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')
    ct = conn_t.cursor()
    ct.execute(
        'SELECT id, city, town, address, build_type, community, date_str, '
        'floor, area, total_price, unit_price, lat, lon, sq, raw_json '
        'FROM transactions'
    )
    batch, total, inserted, skipped = [], 0, 0, 0
    t0 = time.time()
    for row in ct:
        total += 1
        try:
            rec = _parse_api_record(row)
        except Exception:
            skipped += 1
            continue
        if rec is None:
            skipped += 1
            continue
        batch.append(rec)
        inserted += 1
        if len(batch) >= 10000:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()
            batch = []
    if batch:
        cursor.executemany(INSERT_SQL, batch)
        conn.commit()
    conn_t.close()
    elapsed = time.time() - t0
    print(f'\n  ✅ API 載入完成: 掃描 {total:,}, '
          f'插入 {inserted:,}, 略過 {skipped:,}, {elapsed:.1f}s')
    return inserted


def create_tables(cursor):
    """[向後相容] 建立資料表"""
    db = LandDataDB.__new__(LandDataDB)
    db._create_tables(cursor)


def create_indexes(cursor):
    """[向後相容] 建立索引"""
    print('  📇 建立索引...')
    indexes = [
        ('idx_county_city', 'county_city'),
        ('idx_district', 'district'),
        ('idx_street', 'street'),
        ('idx_lane', 'lane'),
        ('idx_number', 'number'),
        ('idx_floor', 'floor'),
        ('idx_date', 'transaction_date'),
        ('idx_price', 'total_price'),
        ('idx_serial', 'serial_no'),
    ]
    for name, col in indexes:
        cursor.execute(f'CREATE INDEX IF NOT EXISTS {name} ON land_transaction({col})')
    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_addr_combo
        ON land_transaction(county_city, district, street, lane, number)''')
    # com2address 查詢用索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_community_address ON land_transaction(community_name, address) WHERE community_name IS NOT NULL AND address IS NOT NULL')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_street_lane_district ON land_transaction(street, lane, district)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_search_numbers ON land_transaction(street, lane, district, total_floors, build_date) WHERE number IS NOT NULL')


def create_fts(cursor):
    """[向後相容] 建立 FTS5"""
    print('  🔍 建立 FTS5 全文檢索...')
    cursor.execute('DROP TABLE IF EXISTS address_fts')
    cursor.execute('''
        CREATE VIRTUAL TABLE address_fts USING fts5(
            address, content='land_transaction', content_rowid='id',
            tokenize='unicode61'
        )
    ''')
    cursor.execute('''
        INSERT INTO address_fts(rowid, address)
        SELECT id, address FROM land_transaction WHERE address != ''
    ''')


def convert(source, csv_path=None, api_path=None, output_path=None):
    """[向後相容] v3 轉換流程 — 會刪除舊 DB 重建"""
    input_files = []
    if source in ('csv', 'both') and csv_path:
        input_files.append(csv_path)
    if source in ('api', 'both') and api_path:
        input_files.append(api_path)
    convert_v4(input_files, output_path, rebuild=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='台灣實價登錄資料轉換 v4 — 自動識別 + 增量匯入',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""範例:
  # 自動偵測並增量匯入 (最常用)
  python3 convert.py data.csv
  python3 convert.py transactions.db
  python3 convert.py a.csv b.db c.csv

  # 重建 land_data.db (清空重來)
  python3 convert.py --rebuild data.csv transactions.db

  # 指定目標
  python3 convert.py --target /path/to/land_data.db data.csv

  # 向後相容: 不帶 input 時走預設路徑 (csv + api → both)
  python3 convert.py
  python3 convert.py --source csv
  python3 convert.py --source api
  python3 convert.py --source both
        """
    )
    parser.add_argument('inputs', nargs='*',
                        help='輸入檔案路徑 (CSV / .db)，可多個')
    parser.add_argument('--target', '-t', default=None,
                        help='目標 land_data.db 路徑')
    parser.add_argument('--rebuild', '-r', action='store_true',
                        help='重建模式: 刪除舊 DB 重新匯入')
    parser.add_argument('--skip-finalize', action='store_true',
                        help='跳過建索引/FTS/VACUUM (多批時最後再做)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='詳細 log: 顯示丟棄/補充/重複的範例記錄')

    # 向後相容參數
    parser.add_argument('--source', '-s',
                        choices=['csv', 'api', 'both'], default=None,
                        help='[向後相容] 資料來源模式')
    parser.add_argument('--csv-input', default=None,
                        help='[向後相容] CSV 輸入路徑')
    parser.add_argument('--api-input', default=None,
                        help='[向後相容] API DB 路徑')
    parser.add_argument('--output', '-o', default=None,
                        help='[向後相容] 同 --target')

    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)

    # 目標路徑
    target_path = (args.target or args.output
                   or os.path.join(project_dir, 'db', 'land_data.db'))

    # —— 向後相容模式: --source csv/api/both ——
    if args.source and not args.inputs:
        csv_path = args.csv_input or os.path.join(
            project_dir, 'db', 'ALL_lvr_land_a.csv')
        api_path = args.api_input or os.path.join(
            project_dir, 'db', 'transactions_all_original.db')

        input_files = []
        if args.source in ('csv', 'both'):
            if not os.path.exists(csv_path):
                print(f'❌ 找不到 CSV 檔案: {csv_path}')
                sys.exit(1)
            input_files.append(csv_path)
        if args.source in ('api', 'both'):
            if not os.path.exists(api_path):
                print(f'❌ 找不到 API DB: {api_path}')
                sys.exit(1)
            input_files.append(api_path)

        # 向後相容: --source 模式預設 rebuild
        convert_v4(input_files, target_path, rebuild=True, verbose=args.verbose)
        return

    # —— 新版模式: positional inputs ——
    if not args.inputs:
        # 無輸入 → 預設 both
        csv_path = os.path.join(project_dir, 'db', 'ALL_lvr_land_a.csv')
        api_path = os.path.join(project_dir, 'db', 'transactions_all_original.db')
        input_files = []
        if os.path.exists(csv_path):
            input_files.append(csv_path)
        if os.path.exists(api_path):
            input_files.append(api_path)
        if not input_files:
            print('❌ 找不到預設輸入檔案，請指定輸入路徑')
            parser.print_help()
            sys.exit(1)
        convert_v4(input_files, target_path, rebuild=True, verbose=args.verbose)
    else:
        # 有明確 inputs → 增量匯入 (除非 --rebuild)
        for f in args.inputs:
            if not os.path.exists(f):
                print(f'❌ 檔案不存在: {f}')
                sys.exit(1)
        convert_v4(args.inputs, target_path,
                   rebuild=args.rebuild,
                   skip_finalize=args.skip_finalize,
                   verbose=args.verbose)


if __name__ == '__main__':
    main()
