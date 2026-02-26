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

# ── 共用模組 ──────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from address_utils import (
    fullwidth_to_halfwidth,
    halfwidth_to_fullwidth,
    chinese_numeral_to_int,
    arabic_to_chinese,
    normalize_address,
    parse_query,
    CHINESE_NUM_CHARS,
    CN_DIGIT_MAP,
)

# 向後相容別名 (供 web/server.py 等使用)
normalize_query = lambda text: normalize_address(text, for_query=True)

# ── 路徑 ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(SCRIPT_DIR, '..', 'db', 'land_data.db')


# ═══════════════════════════════════════════════════════════════════════════════
# 數字變體產生 (搜尋專用)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_number_variants(num_str):
    """產生數字的所有表示變體（半形/全形/中文）"""
    variants = set()
    normalized = fullwidth_to_halfwidth(num_str)
    try:
        n = int(normalized)
    except (ValueError, TypeError):
        n = None
    variants.add(normalized)
    variants.add(halfwidth_to_fullwidth(normalized))
    if n is not None:
        for cn in arabic_to_chinese(n):
            variants.add(cn)
        if 20 <= n <= 29:
            variants.add('廿' + (CN_DIGIT_MAP[n % 10] if n % 10 else ''))
    return [v for v in variants if v]


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
            arabic_val = chinese_numeral_to_int(cn_str)
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
    """策略 1: 結構化搜尋 (走索引, 最快)

    查詢策略 (由精確到寬鬆):
      Level 1: district + street + lane + number  精確地址
      Level 2: street + number                    門牌比對
      Level 3: district + street + lane           巷弄搜尋
      Level 4: street (+ lane if given)           路段搜尋
    """
    street = parsed.get('street')
    if not street:
        return []

    district = parsed.get('district')
    lane = parsed.get('lane', '')
    alley = parsed.get('alley', '')
    number = parsed.get('number')
    floor_val = parsed.get('floor')
    sub_number = parsed.get('sub_number')

    # 構建查詢層級
    levels = []

    # Level 1: 最精確 — district + street + lane + number
    if district and number:
        w = ['district = ?', 'street = ?', 'number = ?']
        p = [district, street, number]
        if lane:
            w.append('lane = ?'); p.append(lane)
        else:
            w.append("(lane = '' OR lane IS NULL)")
        if alley:
            w.append('alley = ?'); p.append(alley)
        else:
            w.append("(alley = '' OR alley IS NULL)")
        if floor_val:
            w.append('floor = ?'); p.append(floor_val)
        if sub_number:
            w.append('sub_number = ?'); p.append(sub_number)
        levels.append((w, p))

    # Level 2: street + number (跨區搜尋)
    if number:
        w = ['street = ?', 'number = ?']
        p = [street, number]
        if lane:
            w.append('lane = ?'); p.append(lane)
        else:
            w.append("(lane = '' OR lane IS NULL)")
        if alley:
            w.append('alley = ?'); p.append(alley)
        else:
            w.append("(alley = '' OR alley IS NULL)")
        levels.append((w, p))

    # Level 3: district + street + lane (巷弄範圍)
    if district and lane:
        w = ['district = ?', 'street = ?', 'lane = ?']
        p = [district, street, lane]
        if alley:
            w.append('alley = ?'); p.append(alley)
        levels.append((w, p))

    # Level 4: street + lane (路段+巷)
    if lane:
        levels.append((['street = ?', 'lane = ?'], [street, lane]))

    # Level 5: district + street
    if district:
        levels.append((['district = ?', 'street = ?'], [district, street]))

    # Level 6: 僅 street
    levels.append((['street = ?'], [street]))

    computed = _build_computed_cols()
    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])

    for where_parts, base_params in levels:
        params = list(base_params)
        where_addr = ' AND '.join(where_parts)

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
        rows = [dict(r) for r in cursor.fetchall()]
        if rows:
            return rows

    return []


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
    """策略 3: LIKE 後備搜尋 (限制變體數量避免全表掃描)"""
    computed = _build_computed_cols()
    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])

    # 限制最多 8 個變體，避免大量 OR 導致效能問題
    limited = variants[:8] if len(variants) > 8 else variants

    like_cond = ' OR '.join(['address LIKE ?' for _ in limited])
    params = [f'%{v}%' for v in limited]

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


def _get_connection(db_path):
    """建立已優化的 SQLite 連線"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA cache_size=-50000')   # 50MB cache
    conn.execute('PRAGMA mmap_size=268435456') # 256MB mmap
    conn.execute('PRAGMA query_only=ON')       # 唯讀提示，避免 journal 開銷
    return conn


# 模組級連線快取 (同一 db_path 共用)
_conn_cache = {}

def _get_cached_connection(db_path):
    """取得快取連線（避免重複開關連線）"""
    real_path = os.path.realpath(db_path)
    conn = _conn_cache.get(real_path)
    if conn is not None:
        try:
            conn.execute('SELECT 1')
            return conn
        except sqlite3.Error:
            _conn_cache.pop(real_path, None)
    conn = _get_connection(db_path)
    _conn_cache[real_path] = conn
    return conn


def search_address(address, db_path=DEFAULT_DB, filters=None,
                   sort_by='date', limit=200, show_sql=False, conn=None):
    """
    主搜尋函式。依序嘗試:
      1. 結構化搜尋 (解析後欄位, 走索引)
      2. FTS5 全文搜尋
      3. LIKE 變體搜尋

    Args:
        conn: 可選的已開啟連線 (避免重複開關)
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"找不到資料庫: {db_path}")

    filters = filters or {}
    parsed = parse_query(address)

    own_conn = conn is None
    if own_conn:
        conn = _get_cached_connection(db_path)

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
            variants = generate_address_variants(address)
            rows = search_like(conn, variants, filters, sort_by, limit)
            method = 'LIKE 變體'

    except sqlite3.Error:
        # 連線可能已失效，清除快取
        real_path = os.path.realpath(db_path)
        _conn_cache.pop(real_path, None)
        raise

    if show_sql:
        print(f'\n  🔧 搜尋策略: {method}')
        print(f'  📌 解析結果: {parsed}')

    return {
        'query': address,
        'variants': generate_address_variants(address) if not rows or method != '結構化索引' else [],
        'parsed': parsed,
        'method': method,
        'filters': filters,
        'sort_by': sort_by,
        'total': len(rows),
        'results': rows,
        'show_sql': show_sql,
    }


def search_address_batch(addresses, db_path=DEFAULT_DB, filters=None,
                         sort_by='date', limit=100):
    """
    批次搜尋多個地址 (共用連線，效能大幅提升)。

    Args:
        addresses: 地址列表
        db_path: 資料庫路徑
        filters: 共用篩選條件
        sort_by: 排序方式
        limit: 每個地址的最大結果數

    Returns:
        list of search result dicts
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"找不到資料庫: {db_path}")

    conn = _get_cached_connection(db_path)
    results = []

    for addr in addresses:
        try:
            result = search_address(
                addr, db_path=db_path, filters=filters,
                sort_by=sort_by, limit=limit, conn=conn
            )
            results.append(result)
        except Exception as e:
            results.append({
                'query': addr, 'error': str(e),
                'total': 0, 'results': []
            })

    return results


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
        headers = ['#', '行政區', '地址', '社區', '日期', '樓層', '型態',
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
            community = (r.get('community_name') or '')[:10] or '-'
            table_data.append([
                i, dist, format_address(r)[:30],
                community,
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
        header = f"{'#':>4}  {'行政區':6}  {'地址':<30}  {'社區':<10}  {'日期':9}  {'總價':>8}  {'單坪萬':>6}  {'坪數':>6}  {'公設%':>5}  {'格局':8}"
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
            community = (r.get('community_name') or '')[:10] or '-'
            print(
                f"{i:>4}  {dist:<6}  "
                f"{format_address(r)[:30]:<30}  "
                f"{community:<10}  "
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
