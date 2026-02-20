#!/usr/bin/env python3
"""
address_match.py — 不動產交易地址搜尋工具 v2
==============================================
針對新版 land_data.db (含解析後地址欄位 + FTS5) 優化。

搜尋策略（依序嘗試）:
  1. 結構化搜尋: 利用解析後欄位 (county_city, district, street, lane, ...)
     精準匹配，走索引，極快
  2. FTS5 全文搜尋: 文字比對原始地址
  3. LIKE 後備: 所有數字格式變體 LIKE 匹配

用法:
    python3 address_match.py "三民路29巷"
    python3 address_match.py "日興一街52號"
    python3 address_match.py "松山區三民路29巷1號" --year 110-114
    python3 address_match.py "三民路29巷" --type 公寓 住宅大樓 --sort unit_price

篩選:
    --type 公寓 華廈        建物型態 (模糊, 可多選)
    --rooms 2 3             房數
    --public-ratio 0-35     公設比 (%)
    --year 110-114          民國年
    --ping 20-40            坪數
    --unit-price 60-120     單坪萬元
    --price 1000-5000       總價萬元

排序 (--sort):
    date / price / count / unit_price / ping / public_ratio
"""

import sqlite3
import sys
import os
import re
import argparse
from itertools import product

# ── 路徑 ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(SCRIPT_DIR, '..', 'db', 'land_data.db')

# ── 數字常數 ──────────────────────────────────────────────────────────────────
FULLWIDTH_DIGITS = '０１２３４５６７８９'
HALFWIDTH_DIGITS = '0123456789'
CN_BASIC = {
    '零': 0, '一': 1, '二': 2, '兩': 2, '三': 3, '四': 4,
    '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10, '百': 100,
}
CN_DIGIT_MAP = ['零','一','二','三','四','五','六','七','八','九']
CHINESE_NUM_CHARS = '○零一壹二貳兩三參叁四肆五伍六陸七柒八捌九玖十拾百佰千仟'


# ═══════════════════════════════════════════════════════════════════════════════
# 數字轉換
# ═══════════════════════════════════════════════════════════════════════════════

def fullwidth_to_halfwidth(text):
    result = []
    for ch in text:
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        elif code == 0x3000:
            result.append(' ')
        else:
            result.append(ch)
    return ''.join(result)

def halfwidth_to_fullwidth(text):
    result = []
    for ch in text:
        idx = HALFWIDTH_DIGITS.find(ch)
        result.append(FULLWIDTH_DIGITS[idx] if idx >= 0 else ch)
    return ''.join(result)

def _cn_str_to_int(s):
    if not s:
        return None
    if all(c in CN_DIGIT_MAP for c in s):
        try:
            return int(''.join(str(CN_DIGIT_MAP.index(c)) for c in s))
        except:
            pass
    try:
        result = 0
        current = 0
        for ch in s:
            if ch in ('零', '〇'):
                continue
            elif ch == '十':
                if current == 0: current = 1
                result += current * 10
                current = 0
            elif ch == '百':
                result += current * 100
                current = 0
            else:
                v = CN_BASIC.get(ch)
                if v is None: return None
                current = v
        result += current
        return result if result > 0 else None
    except:
        return None

def arabic_to_chinese(n):
    if n <= 0 or n > 9999: return []
    results = set()
    results.add(''.join(CN_DIGIT_MAP[int(d)] for d in str(n)))
    # 標準中文
    parts = []
    tens = (n % 100) // 10
    units = n % 10
    hundreds = (n % 1000) // 100
    thousands = n // 1000
    if thousands: parts.append(CN_DIGIT_MAP[thousands] + '千')
    if hundreds:
        parts.append(CN_DIGIT_MAP[hundreds] + '百')
    elif thousands and (tens or units):
        parts.append('零')
    if tens:
        if tens == 1 and not thousands and not hundreds:
            parts.append('十')
        else:
            parts.append(CN_DIGIT_MAP[tens] + '十')
    elif (thousands or hundreds) and units:
        parts.append('零')
    if units:
        parts.append(CN_DIGIT_MAP[units])
    results.add(''.join(parts))
    if 10 <= n <= 19:
        results.add('一十' + (CN_DIGIT_MAP[n%10] if n%10 else ''))
        results.add('十' + (CN_DIGIT_MAP[n%10] if n%10 else ''))
    return list(results)

def generate_number_variants(num_str):
    variants = set()
    normalized = fullwidth_to_halfwidth(num_str)
    try:
        n = int(normalized)
    except:
        n = None
    variants.add(normalized)
    variants.add(halfwidth_to_fullwidth(normalized))
    if n is not None:
        for cn in arabic_to_chinese(n):
            variants.add(cn)
        if 20 <= n <= 29:
            variants.add('廿' + (CN_DIGIT_MAP[n%10] if n%10 else ''))
    return [v for v in variants if v]


# ═══════════════════════════════════════════════════════════════════════════════
# 地址解析與變體產生
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_query(text):
    """正規化查詢字串"""
    text = fullwidth_to_halfwidth(text.strip())
    text = text.replace('\u5DFF', '市').replace('臺', '台')
    # 中文數字→阿拉伯 (在特定後綴前)
    pattern = re.compile(rf'([{CHINESE_NUM_CHARS}]+)(樓|層|號|巷|弄|之|鄰|F|f)')
    def _repl(m):
        num = _cn_str_to_int(m.group(1))
        if num is not None:
            return f'{num}{m.group(2)}'
        return m.group(0)
    text = pattern.sub(_repl, text)

    # 將數字段統一轉為國字段 (e.g. 3段 -> 三段)
    arabic_to_cn = {
        '1': '一', '2': '二', '3': '三', '4': '四', '5': '五',
        '6': '六', '7': '七', '8': '八', '9': '九', '10': '十'
    }
    def _repl_sec(m):
        n = m.group(1)
        cn = arabic_to_cn.get(n) if len(n) <= 2 else None
        if cn:
            return f"{cn}段"
        return m.group(0)
    text = re.sub(r'(\d+)段', _repl_sec, text)

    return text


CITY_PATTERN = re.compile(
    r'^(台北市|新北市|桃園市|台中市|台南市|高雄市|'
    r'基隆市|新竹(?:市|縣)|嘉義(?:市|縣)|'
    r'苗栗縣|彰化縣|南投縣|雲林縣|屏東縣|'
    r'台東縣|花蓮縣|宜蘭縣|澎湖縣|金門縣|連江縣)'
)


def parse_query(query):
    """
    解析使用者查詢, 萃取結構化條件。
    回傳 dict: county_city, district, street, lane, alley, number, floor, sub_number
    """
    addr = normalize_query(query)
    result = {k: '' for k in
              ['county_city', 'district', 'street', 'lane', 'alley',
               'number', 'floor', 'sub_number']}

    # 縣市
    m = CITY_PATTERN.match(addr)
    if m:
        result['county_city'] = m.group(1)
        addr = addr[m.end():]

    # 鄉鎮市區
    m = re.match(r'^(.{1,4}?(?:區|鄉|鎮|市))(?=.)', addr)
    if m:
        result['district'] = m.group(1)
        addr = addr[m.end():]

    # 里
    m = re.match(r'^(.{1,5}?里)(?=[^\d]*(?:路|街|大道|\d))', addr)
    if m:
        addr = addr[m.end():]

    # 鄰
    m = re.match(r'^(\d+鄰)', addr)
    if m:
        addr = addr[m.end():]

    # 街路名 (含段)
    m = re.match(r'^(.+?(?:路|街|大道))([一二三四五六七八九十\d]+段)?', addr)
    if m:
        result['street'] = m.group(1) + (m.group(2) or '')
        addr = addr[m.end():]

    # 巷
    m = re.match(r'^(\d+)巷', addr)
    if m:
        result['lane'] = m.group(1)
        addr = addr[len(m.group(0)):]

    # 弄
    m = re.match(r'^(\d+)弄', addr)
    if m:
        result['alley'] = m.group(1)
        addr = addr[len(m.group(0)):]

    # 號 — X之Y號 → number=X, sub_number=Y;  X號 → number=X
    m = re.match(r'^(\d+)(?:之(\d+))?號', addr)
    if m:
        result['number'] = m.group(1)
        if m.group(2):
            result['sub_number'] = m.group(2)
        addr = addr[len(m.group(0)):]

    # 號之Y (如 53號之3)
    m2 = re.match(r'^之(\d+)', addr)
    if m2:
        if not result['sub_number']:
            result['sub_number'] = m2.group(1)
        addr = addr[len(m2.group(0)):]

    # 樓
    m = re.match(r'^(\d+)(?:樓|層|[Ff])', addr)
    if m:
        result['floor'] = m.group(1)
        addr = addr[len(m.group(0)):]

    # 之 (樓之X, 如 53號12樓之8)
    m = re.match(r'^之(\d+)', addr)
    if m:
        if not result['sub_number']:
            result['sub_number'] = m.group(1)

    return result


def parse_address_tokens(address):
    """解析地址字串為 token 列表 (用於產生變體)"""
    normalized = fullwidth_to_halfwidth(address)
    tokens = []
    pattern = re.compile(r'(\d+|[^\d]+)')
    raw_tokens = []
    for m in pattern.finditer(normalized):
        val = m.group()
        if val.isdigit():
            raw_tokens.append({'type': 'num', 'val': val})
        else:
            raw_tokens.append({'type': 'text', 'val': val})

    CN_ADDR_UNIT = r'(?=[樓層號巷弄段之]|F(?:\d|$))'
    CN_NUM_PAT = re.compile(r'([零〇一兩二三四五六七八九十百千]+)' + CN_ADDR_UNIT)

    for tok in raw_tokens:
        if tok['type'] != 'text':
            tokens.append(tok)
            continue
        text = tok['val']
        pos = 0
        for m in CN_NUM_PAT.finditer(text):
            start, end = m.start(), m.end()
            cn_str = m.group(1)
            arabic_val = _cn_str_to_int(cn_str)
            if start > pos:
                tokens.append({'type': 'text', 'val': text[pos:start]})
            if arabic_val and arabic_val > 0:
                tokens.append({'type': 'cn_num', 'val': cn_str, 'arabic': arabic_val})
            else:
                tokens.append({'type': 'text', 'val': cn_str})
            pos = end
        if pos < len(text):
            tokens.append({'type': 'text', 'val': text[pos:]})
    return tokens


def generate_address_variants(address):
    """產生地址搜尋變體"""
    tokens = parse_address_tokens(address)
    candidates = []
    for tok in tokens:
        if tok['type'] == 'num':
            candidates.append(generate_number_variants(tok['val']))
        elif tok['type'] == 'cn_num':
            vs = set()
            vs.add(tok['val'])
            vs.add(str(tok['arabic']))
            vs.add(halfwidth_to_fullwidth(str(tok['arabic'])))
            for cn in arabic_to_chinese(tok['arabic']):
                vs.add(cn)
            candidates.append(list(vs))
        else:
            candidates.append([tok['val']])

    all_v = set()
    for combo in product(*candidates):
        all_v.add(''.join(combo))
    all_v.add(address.strip())
    all_v.add(halfwidth_to_fullwidth(fullwidth_to_halfwidth(address.strip())))
    return sorted(all_v)


# ═══════════════════════════════════════════════════════════════════════════════
# 篩選工具
# ═══════════════════════════════════════════════════════════════════════════════

def parse_range(s):
    if not s: return (None, None)
    s = s.strip()
    if '-' in s:
        parts = s.split('-', 1)
        lo = float(parts[0]) if parts[0].strip() else None
        hi = float(parts[1]) if parts[1].strip() else None
        return (lo, hi)
    else:
        val = float(s)
        return (val, val)

SORT_OPTIONS = {
    'date':         'transaction_date DESC, id DESC',
    'price':        'total_price DESC NULLS LAST',
    'count':        'addr_count DESC, transaction_date DESC',
    'unit_price':   'unit_price_per_ping DESC NULLS LAST',
    'ping':         'ping DESC NULLS LAST',
    'public_ratio': 'public_ratio ASC NULLS LAST',
}


# ═══════════════════════════════════════════════════════════════════════════════
# 搜尋引擎
# ═══════════════════════════════════════════════════════════════════════════════

def _build_computed_cols():
    """CTE 計算欄位 SQL"""
    return """
        CASE WHEN building_area > 0
             THEN ROUND(building_area / 3.30579, 1) ELSE NULL END AS ping,
        CASE WHEN building_area > 0 AND main_area > 0 AND building_area > main_area
             THEN ROUND(
                    (building_area - COALESCE(main_area,0) - COALESCE(attached_area,0) - COALESCE(balcony_area,0))
                    / building_area * 100, 1)
             ELSE NULL END AS public_ratio,
        CASE WHEN building_area > 0 AND total_price > 0
             THEN ROUND(total_price / 10000.0 / (building_area / 3.30579), 1)
             ELSE NULL END AS unit_price_per_ping,
        CAST(SUBSTR(transaction_date, 1, LENGTH(transaction_date) - 4) AS INTEGER) AS roc_year
    """


def _build_filter_sql(filters, params):
    """建立篩選條件 SQL"""
    clauses = []
    btype = filters.get('building_types') or []
    if btype:
        tc = ' OR '.join(['building_type LIKE ?' for _ in btype])
        clauses.append(f'({tc})')
        params.extend([f'%{t}%' for t in btype])

    rooms = filters.get('rooms') or []
    if rooms:
        rc = ' OR '.join(['rooms = ?' for _ in rooms])
        clauses.append(f'({rc})')
        params.extend([int(r) for r in rooms])

    for field, col in [
        ('public_ratio_min', 'public_ratio'), ('public_ratio_max', 'public_ratio'),
        ('year_min', 'roc_year'), ('year_max', 'roc_year'),
        ('ping_min', 'ping'), ('ping_max', 'ping'),
        ('unit_price_min', 'unit_price_per_ping'), ('unit_price_max', 'unit_price_per_ping'),
    ]:
        v = filters.get(field)
        if v is not None:
            op = '>=' if field.endswith('min') else '<='
            clauses.append(f'{col} IS NOT NULL AND {col} {op} ?')
            params.append(v)

    if filters.get('price_min') is not None:
        clauses.append('total_price IS NOT NULL AND total_price >= ?')
        params.append(int(filters['price_min'] * 10000))
    if filters.get('price_max') is not None:
        clauses.append('total_price IS NOT NULL AND total_price <= ?')
        params.append(int(filters['price_max'] * 10000))

    return ' AND '.join(clauses) if clauses else ''


def search_structured(conn, parsed, filters, sort_by, limit):
    """策略 1: 結構化搜尋 (走索引, 最快)"""
    where_parts = []
    params = []

    for field in ['county_city', 'district', 'street', 'number', 'floor', 'sub_number']:
        val = parsed.get(field)
        if val:
            where_parts.append(f'{field} = ?')
            params.append(val)

    # 針對巷、弄的精準比對邏輯
    # 如果使用者有指定門牌號碼 (number)，則要求巷、弄必須完全符合 (即如果輸入沒巷弄，資料庫也不能有巷弄)
    if parsed.get('number'):
        for field in ['lane', 'alley']:
            val = parsed.get(field, '')
            if val:
                where_parts.append(f'{field} = ?')
                params.append(val)
            else:
                where_parts.append(f"({field} = '' OR {field} IS NULL)")
    else:
        # 沒有指定門牌時，如果使用者有給巷弄，就過濾巷弄
        for field in ['lane', 'alley']:
            val = parsed.get(field, '')
            if val:
                where_parts.append(f'{field} = ?')
                params.append(val)

    if not where_parts:
        return []

    where_addr = ' AND '.join(where_parts)
    computed = _build_computed_cols()
    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])

    sql = f"""
    WITH base AS (
        SELECT *, {computed}
        FROM land_transaction
        WHERE {where_addr} AND address != ''
    ),
    counted AS (
        SELECT *, COUNT(*) OVER (PARTITION BY address) AS addr_count
        FROM base
    )
    SELECT * FROM counted
    """
    filter_sql = _build_filter_sql(filters, params)
    if filter_sql:
        sql += f' WHERE {filter_sql}'
    sql += f' ORDER BY {order_sql} LIMIT {limit}'

    cursor = conn.execute(sql, params)
    return [dict(r) for r in cursor.fetchall()]


def search_fts(conn, query, filters, sort_by, limit):
    """策略 2: FTS5 全文搜尋"""
    computed = _build_computed_cols()
    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])
    params = [f'"{query}"']

    sql = f"""
    WITH base AS (
        SELECT t.*, {computed}
        FROM land_transaction t
        WHERE t.id IN (SELECT rowid FROM address_fts WHERE address MATCH ?)
          AND t.address != ''
    ),
    counted AS (
        SELECT *, COUNT(*) OVER (PARTITION BY address) AS addr_count
        FROM base
    )
    SELECT * FROM counted
    """
    filter_sql = _build_filter_sql(filters, params)
    if filter_sql:
        sql += f' WHERE {filter_sql}'
    sql += f' ORDER BY {order_sql} LIMIT {limit}'

    try:
        cursor = conn.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []


def search_like(conn, variants, filters, sort_by, limit):
    """策略 3: LIKE 後備搜尋"""
    computed = _build_computed_cols()
    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])

    like_cond = ' OR '.join(['address LIKE ?' for _ in variants])
    params = [f'%{v}%' for v in variants]

    sql = f"""
    WITH base AS (
        SELECT *, {computed}
        FROM land_transaction
        WHERE ({like_cond}) AND address != ''
    ),
    counted AS (
        SELECT *, COUNT(*) OVER (PARTITION BY address) AS addr_count
        FROM base
    )
    SELECT * FROM counted
    """
    filter_sql = _build_filter_sql(filters, params)
    if filter_sql:
        sql += f' WHERE {filter_sql}'
    sql += f' ORDER BY {order_sql} LIMIT {limit}'

    cursor = conn.execute(sql, params)
    return [dict(r) for r in cursor.fetchall()]


def search_address(address, db_path=DEFAULT_DB, filters=None,
                   sort_by='date', limit=200, show_sql=False):
    """
    主搜尋函式。依序嘗試:
      1. 結構化搜尋 (解析後欄位, 走索引)
      2. FTS5 全文搜尋
      3. LIKE 變體搜尋
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"找不到資料庫: {db_path}")

    filters = filters or {}
    parsed = parse_query(address)
    variants = generate_address_variants(address)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA cache_size=-50000')  # 50MB cache
    conn.execute('PRAGMA mmap_size=268435456')  # 256MB mmap

    method = ''
    rows = []

    try:
        # 策略 1: 結構化搜尋
        if parsed.get('street'):
            rows = search_structured(conn, parsed, filters, sort_by, limit)
            method = '結構化索引'

        # 策略 2: FTS5
        if not rows:
            normalized = normalize_query(address)
            rows = search_fts(conn, normalized, filters, sort_by, limit)
            method = 'FTS5 全文'

        # 策略 3: LIKE 變體
        if not rows:
            rows = search_like(conn, variants, filters, sort_by, limit)
            method = 'LIKE 變體'

    finally:
        conn.close()

    if show_sql:
        print(f'\n  🔧 搜尋策略: {method}')
        print(f'  📌 解析結果: {parsed}')

    return {
        'query': address,
        'variants': variants,
        'parsed': parsed,
        'method': method,
        'filters': filters,
        'sort_by': sort_by,
        'total': len(rows),
        'results': rows,
        'show_sql': show_sql,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 顯示 / 輸出
# ═══════════════════════════════════════════════════════════════════════════════

def format_price(price):
    if price is None: return '-'
    try:
        p = int(price)
        if p >= 100_000_000: return f'{p/100_000_000:.2f}億'
        elif p >= 10_000: return f'{p/10_000:.0f}萬'
        else: return f'{p:,}'
    except: return str(price)

def format_date(d):
    if not d: return '-'
    s = str(d)
    if len(s) < 7: return s
    return f"{s[:-4]}/{s[-4:-2]}/{s[-2:]}"

def format_address(r):
    """從解析後欄位組合乾淨地址，fallback 到清理過的 raw address"""
    street = r.get('street') or ''
    if not street:
        # fallback: 清理 raw address
        raw = r.get('address') or ''
        raw = fullwidth_to_halfwidth(raw).replace('臺', '台')
        return raw[:35]

    parts = [street]
    if r.get('lane'):
        parts.append(f"{r['lane']}巷")
    if r.get('alley'):
        parts.append(f"{r['alley']}弄")
    if r.get('number'):
        parts.append(f"{r['number']}號")
    if r.get('floor'):
        parts.append(f"{r['floor']}F")
    if r.get('sub_number'):
        parts.append(f"之{r['sub_number']}")
    return ''.join(parts)


def print_results(result, show_variants=True):
    print(f"\n{'═'*72}")
    print(f"🔍 搜尋地址：{result['query']}")
    print(f"{'═'*72}")

    if result.get('method'):
        print(f"⚡ 搜尋策略：{result['method']}")

    parsed = result.get('parsed', {})
    active_parsed = {k: v for k, v in parsed.items() if v}
    if active_parsed:
        parts = [f"{k}={v}" for k, v in active_parsed.items()]
        print(f"📋 解析結果：{', '.join(parts)}")

    # 只有當結構化搜尋失敗，或使用者有要求顯示細節時，才顯示變體
    is_structured = result.get('method') == '結構化索引'
    force_show = result.get('show_sql', False)
    
    if show_variants and (not is_structured or force_show):
        vars_list = result.get('variants', [])
        if vars_list and len(vars_list) <= 20:
            print(f"📝 搜尋變體（{len(vars_list)} 個）：")
            for v in vars_list:
                print(f"   • {v}")
            print()

    # 篩選提示
    filters = result.get('filters', {})
    active = []
    if filters.get('building_types'): active.append(f"型態:{'/'.join(filters['building_types'])}")
    if filters.get('rooms'): active.append(f"房數:{'+'.join(str(r) for r in filters['rooms'])}房")
    for label, lo_key, hi_key, unit in [
        ('公設比', 'public_ratio_min', 'public_ratio_max', '%'),
        ('年份', 'year_min', 'year_max', ''),
        ('坪數', 'ping_min', 'ping_max', '坪'),
        ('單坪', 'unit_price_min', 'unit_price_max', '萬'),
        ('總價', 'price_min', 'price_max', '萬'),
    ]:
        lo = filters.get(lo_key)
        hi = filters.get(hi_key)
        if lo is not None or hi is not None:
            active.append(f"{label}:{lo or ''}~{hi or ''}{unit}")
    if active:
        print(f"🔧 篩選條件：{' | '.join(active)}")

    sort_label = {
        'date': '成交日期↓', 'price': '總價↓', 'count': '筆數↓',
        'unit_price': '單坪價↓', 'ping': '坪數↓', 'public_ratio': '公設比↑',
    }
    print(f"📌 排序：{sort_label.get(result.get('sort_by','date'), '')}")
    print()

    total = result['total']
    rows = result['results']
    print(f"📊 共找到 {total} 筆交易記錄\n")

    if not rows:
        print("  （無資料）")
        return

    # 統計摘要
    prices = [r['total_price'] for r in rows if r.get('total_price') and r['total_price'] > 0]
    pings = [r['ping'] for r in rows if r.get('ping')]
    upps = [r['unit_price_per_ping'] for r in rows if r.get('unit_price_per_ping')]
    prs = [r['public_ratio'] for r in rows if r.get('public_ratio') and r['public_ratio'] > 0]

    if prices:
        avg_p = sum(prices)/len(prices)
        med_p = sorted(prices)[len(prices)//2]
        print(f"  💰 總價   均值 {format_price(avg_p)}  中位 {format_price(med_p)}"
              f"  最低 {format_price(min(prices))}  最高 {format_price(max(prices))}")
    if upps:
        avg_u = sum(upps)/len(upps)
        med_u = sorted(upps)[len(upps)//2]
        print(f"  📐 單坪   均值 {avg_u:.1f}萬  中位 {med_u:.1f}萬"
              f"  最低 {min(upps):.1f}萬  最高 {max(upps):.1f}萬")
    if pings:
        avg_pg = sum(pings)/len(pings)
        print(f"  📏 坪數   均值 {avg_pg:.1f}坪  最小 {min(pings):.1f}坪  最大 {max(pings):.1f}坪")
    if prs:
        avg_pr = sum(prs)/len(prs)
        print(f"  🏢 公設比 均值 {avg_pr:.1f}%  最低 {min(prs):.1f}%  最高 {max(prs):.1f}%")
    print()

    # 表格輸出
    try:
        from tabulate import tabulate
        headers = ['#', '行政區', '地址', '日期', '樓層', '型態',
                   '總價', '單坪萬', '坪數', '公設%', '格局', '車位', '備註']
        table_data = []
        for i, r in enumerate(rows, 1):
            layout = ''
            if r.get('rooms'):  layout += f"{r['rooms']}房"
            if r.get('halls'):  layout += f"{r['halls']}廳"
            if r.get('bathrooms'): layout += f"{r['bathrooms']}衛"
            pk = ''
            if r.get('parking_type'):
                pk = (r['parking_type'] or '')[:6]
                if r.get('parking_price') and r['parking_price'] > 0:
                    pk += f" {format_price(r['parking_price'])}"
            btype = re.sub(r'\s*\([^)]*\)', '', r.get('building_type') or '-').strip()
            pub_r = f"{r['public_ratio']:.0f}%" if r.get('public_ratio') and r['public_ratio'] > 0 else '-'
            unit_p = f"{r['unit_price_per_ping']:.1f}" if r.get('unit_price_per_ping') else '-'
            ping = f"{r['ping']:.1f}" if r.get('ping') else '-'
            dist = r.get('district') or r.get('raw_district') or ''
            table_data.append([
                i, dist, format_address(r)[:30],
                format_date(r.get('transaction_date')),
                (r.get('floor_level') or '-')[:6],
                btype[:8],
                format_price(r.get('total_price')),
                unit_p, ping, pub_r,
                layout or '-', pk or '-',
                (r.get('note') or '')[:18] or '-',
            ])
        print(tabulate(table_data, headers=headers, tablefmt='simple'))
    except ImportError:
        header = f"{'#':>4}  {'行政區':6}  {'地址':<30}  {'日期':9}  {'總價':>8}  {'單坪萬':>6}  {'坪數':>6}  {'公設%':>5}  {'格局':8}"
        print(header)
        print('─' * len(header))
        for i, r in enumerate(rows, 1):
            layout = ''
            if r.get('rooms'):    layout += f"{r['rooms']}房"
            if r.get('halls'):    layout += f"{r['halls']}廳"
            if r.get('bathrooms'):layout += f"{r['bathrooms']}衛"
            pub_r = f"{r['public_ratio']:.0f}%" if r.get('public_ratio') and r['public_ratio'] > 0 else '-'
            unit_p = f"{r['unit_price_per_ping']:.1f}" if r.get('unit_price_per_ping') else '-'
            ping = f"{r['ping']:.1f}" if r.get('ping') else '-'
            dist = r.get('district') or r.get('raw_district') or ''
            print(
                f"{i:>4}  {dist:<6}  "
                f"{format_address(r)[:30]:<30}  "
                f"{format_date(r.get('transaction_date')):9}  "
                f"{format_price(r.get('total_price')):>8}  "
                f"{unit_p:>6}  {ping:>6}  {pub_r:>5}  {layout or '-':8}"
            )
            if r.get('note'):
                print(f"       📝 {r['note'][:70]}")

    print(f"\n{'─'*72}")


def export_csv(result, output_path):
    import csv as csv_mod
    rows = result['results']
    if not rows:
        print("無資料可匯出。")
        return
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv_mod.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ 已匯出 {len(rows)} 筆 → {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='不動產交易地址搜尋 v2 (結構化 + FTS5 + LIKE)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
╔══════════════════════════════════════════════════════════════╗
║  範例                                                        ║
╠══════════════════════════════════════════════════════════════╣
║  address_match.py "三民路29巷"                               ║
║  address_match.py "日興一街52號" --limit 50                   ║
║  address_match.py "三民路29巷" --type 住宅大樓 --sort count   ║
║  address_match.py "三民路29巷" --year 110-114 --ping 20-40   ║
║  address_match.py "三民路29巷" --export result.csv            ║
╚══════════════════════════════════════════════════════════════╝
        """
    )
    parser.add_argument('address', help='要搜尋的地址片段')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite 資料庫路徑')
    parser.add_argument('--limit', type=int, default=200, help='最多回傳筆數 (預設200)')
    parser.add_argument('--show-sql', action='store_true', help='顯示搜尋策略')
    parser.add_argument('--export', metavar='FILE', help='匯出 CSV')
    parser.add_argument('--no-variants', action='store_true', help='不顯示變體列表')

    fg = parser.add_argument_group('篩選')
    fg.add_argument('--type', nargs='+', metavar='TYPE', dest='building_types')
    fg.add_argument('--rooms', nargs='+', type=int, metavar='N')
    fg.add_argument('--public-ratio', metavar='MIN-MAX', dest='public_ratio_range')
    fg.add_argument('--year', metavar='MIN-MAX', dest='year_range')
    fg.add_argument('--ping', metavar='MIN-MAX', dest='ping_range')
    fg.add_argument('--unit-price', metavar='MIN-MAX', dest='unit_price_range')
    fg.add_argument('--price', metavar='MIN-MAX', dest='price_range')

    sg = parser.add_argument_group('排序')
    sg.add_argument('--sort', choices=list(SORT_OPTIONS.keys()), default='date')

    args = parser.parse_args()

    filters = {}
    if args.building_types: filters['building_types'] = args.building_types
    if args.rooms: filters['rooms'] = args.rooms
    for attr, lo_key, hi_key in [
        ('public_ratio_range', 'public_ratio_min', 'public_ratio_max'),
        ('year_range', 'year_min', 'year_max'),
        ('ping_range', 'ping_min', 'ping_max'),
        ('unit_price_range', 'unit_price_min', 'unit_price_max'),
        ('price_range', 'price_min', 'price_max'),
    ]:
        lo, hi = parse_range(getattr(args, attr, None))
        if lo is not None: filters[lo_key] = lo
        if hi is not None: filters[hi_key] = hi

    try:
        result = search_address(
            args.address, db_path=args.db, filters=filters,
            sort_by=args.sort, limit=args.limit, show_sql=args.show_sql,
        )
        print_results(result, show_variants=not args.no_variants)
        if args.export:
            export_csv(result, args.export)
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 執行錯誤: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
