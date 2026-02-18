#!/usr/bin/env python3
"""
address_transfer.py
===================
給定一個地址片段，從不動產交易 SQLite 資料庫找出所有可能對應的交易紀錄。
支援多種篩選條件與排序方式，適合實際房地產研究使用。

處理範圍：
  - 全形 ↔ 半形數字  (２９ ↔ 29)
  - 阿拉伯數字 ↔ 中文數字 (29 ↔ 二十九)
  - 常見地址縮寫變體  (巷/弄/號/樓/之)

用法：
    python3 address_transfer.py "三民路29巷"
    python3 address_transfer.py "三民路29巷" --type 公寓 住宅大樓
    python3 address_transfer.py "三民路29巷" --rooms 2 3 --ping 20-40
    python3 address_transfer.py "三民路29巷" --year 110-114 --price 1000-3000
    python3 address_transfer.py "三民路29巷" --public-ratio 0-35 --sort unit_price
    python3 address_transfer.py "三民路29巷" --sort count --export result.csv

篩選參數（預設全選）：
    --type 公寓 華廈        建物型態（關鍵字，可多個，模糊匹配）
    --rooms 2 3             房數（多選）
    --public-ratio 0-35     公設比範圍（%）
    --year 110-114          成交年份範圍（民國年），也可填單年如 --year 113
    --ping 20-40            建物坪數範圍
    --unit-price 60-120     單坪價格範圍（萬/坪）
    --price 1000-5000       總價範圍（萬元）

排序參數（--sort）：
    date        成交日期降冪（預設）
    price       總價降冪
    count       同地址交易筆數降冪（熱門地址優先）
    unit_price  單坪價格降冪
    ping        坪數降冪
    public_ratio 公設比升冪（低公設優先）

環境需求：
    pip install tabulate  （選用，讓表格更美觀）
"""

import sqlite3
import sys
import os
import re
import argparse
from itertools import product

# ── 路徑設定 ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(SCRIPT_DIR, '..', 'land_a.db')

# ── 數字對照表 ─────────────────────────────────────────────────────────────────
FULLWIDTH_DIGITS = '０１２３４５６７８９'
HALFWIDTH_DIGITS = '0123456789'

# 中文數字
CN_BASIC = {
    '零': 0, '一': 1, '二': 2, '兩': 2, '三': 3, '四': 4,
    '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '十': 10, '百': 100,
}
CN_DIGIT_MAP = ['零','一','二','三','四','五','六','七','八','九']

# 地址關鍵字（搜尋時需保留）
ADDR_KEYWORDS = ['路', '街', '大道', '巷', '弄', '號', '樓', '之', '段',
                 '區', '市', '縣', '鎮', '鄉', '里', '鄰', 'F', 'B']


# ═══════════════════════════════════════════════════════════════════════════════
# 數字轉換工具
# ═══════════════════════════════════════════════════════════════════════════════

def fullwidth_to_halfwidth(text: str) -> str:
    """全形數字 → 半形數字"""
    result = []
    for ch in text:
        idx = FULLWIDTH_DIGITS.find(ch)
        result.append(HALFWIDTH_DIGITS[idx] if idx >= 0 else ch)
    return ''.join(result)

def halfwidth_to_fullwidth(text: str) -> str:
    """半形數字 → 全形數字"""
    result = []
    for ch in text:
        idx = HALFWIDTH_DIGITS.find(ch)
        result.append(FULLWIDTH_DIGITS[idx] if idx >= 0 else ch)
    return ''.join(result)

def arabic_to_chinese(n: int) -> list[str]:
    """
    阿拉伯整數 → 中文數字，回傳可能的表示方式列表
    例如: 29 → ['二十九', '二九']
        1  → ['一', '一樓']（呼叫者自行處理後綴）
        10 → ['十', '一十']
    """
    if n == 0:
        return ['零']
    if n < 0 or n > 9999:
        return []

    results = set()

    # 方式一：逐位對應（流水號型，如門牌號、樓層）
    digits_str = ''.join(CN_DIGIT_MAP[int(d)] for d in str(n))
    results.add(digits_str)

    # 方式二：十進位中文（一般數量）
    def to_cn_standard(num):
        if num == 0:
            return '零'
        if num < 10:
            return CN_DIGIT_MAP[num]
        parts = []
        thousands = num // 1000
        hundreds  = (num % 1000) // 100
        tens      = (num % 100) // 10
        units     = num % 10
        if thousands:
            parts.append(CN_DIGIT_MAP[thousands] + '千')
            if hundreds == 0 and (tens or units):
                parts.append('零')
        if hundreds:
            parts.append(CN_DIGIT_MAP[hundreds] + '百')
            if tens == 0 and units:
                parts.append('零')
        if tens:
            if tens == 1 and not thousands and not hundreds:
                parts.append('十')  # 10~19 可省略「一」
            else:
                parts.append(CN_DIGIT_MAP[tens] + '十')
        elif units and (thousands or hundreds):
            parts.append('零')
        if units:
            parts.append(CN_DIGIT_MAP[units])
        return ''.join(parts)

    results.add(to_cn_standard(n))

    # 十幾：一十x 和 十x 都可能出現
    if 10 <= n <= 19:
        results.add('一十' + CN_DIGIT_MAP[n % 10] if n % 10 else '一十')
        results.add('十' + CN_DIGIT_MAP[n % 10] if n % 10 else '十')

    return list(results)

def chinese_to_arabic(text: str) -> list[int]:
    """
    從中文字串中提取中文數字並轉換為阿拉伯整數列表。
    例如: '二十九巷' → [29]
    """
    # 找出連續的中文數字片段
    pattern = r'[零一兩二三四五六七八九十百千]+'
    results = []
    for m in re.finditer(pattern, text):
        s = m.group()
        val = _cn_str_to_int(s)
        if val is not None and val > 0:
            results.append(val)
    return results

def _cn_str_to_int(s: str) -> int | None:
    """中文數字字串 → 整數"""
    if not s:
        return None
    # 純逐位型：二九 → 29
    if all(c in CN_DIGIT_MAP for c in s):
        try:
            return int(''.join(str(CN_DIGIT_MAP.index(c)) for c in s))
        except:
            pass
    # 標準中文：二十九 → 29
    try:
        result = 0
        current = 0
        for ch in s:
            if ch in ('零', '〇'):
                continue
            elif ch == '十':
                if current == 0:
                    current = 1
                result += current * 10
                current = 0
            elif ch == '百':
                result += current * 100
                current = 0
            elif ch == '千':
                result += current * 1000
                current = 0
            else:
                v = CN_BASIC.get(ch)
                if v is None:
                    return None
                current = v
        result += current
        return result if result > 0 else None
    except:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 地址變體產生器
# ═══════════════════════════════════════════════════════════════════════════════

def generate_number_variants(num_str: str) -> list[str]:
    """
    給定一個數字字串（半形），產生所有可能的表示形式。
    例如 '29' → ['29', '２９', '二十九', '二九', '廿九']
    """
    variants = set()
    n = None

    # 嘗試解析為整數（支援半形/全形）
    normalized = fullwidth_to_halfwidth(num_str)
    try:
        n = int(normalized)
    except:
        pass

    # 半形
    variants.add(normalized)
    # 全形
    variants.add(halfwidth_to_fullwidth(normalized))

    if n is not None:
        # 中文變體
        for cn in arabic_to_chinese(n):
            variants.add(cn)
        # 廿系列
        if 20 <= n <= 29:
            tens_cn = '廿' + (CN_DIGIT_MAP[n % 10] if n % 10 else '')
            variants.add(tens_cn)
        if 30 <= n <= 39:
            tens_cn = '卅' + (CN_DIGIT_MAP[n % 10] if n % 10 else '')
            variants.add(tens_cn)

    return [v for v in variants if v]


def parse_address_tokens(address: str) -> list[dict]:
    """
    解析地址字串，切分為 (文字部分, 數字部分) 的 token 列表。
    同時處理：
      - 半形/全形阿拉伯數字 → {'type':'num'}
      - 中文數字（緊鄰地址關鍵字之前）→ {'type':'cn_num'}
    例如 '日興一街6號七樓' →
        [{'type':'text',   'val':'日興一街'},
         {'type':'num',    'val':'6'},
         {'type':'text',   'val':'號'},
         {'type':'cn_num', 'val':'七', 'arabic':7},
         {'type':'text',   'val':'樓'}]
    """
    # 先全形數字轉半形
    normalized = fullwidth_to_halfwidth(address)

    tokens = []
    # 先切出阿拉伯數字
    pattern = re.compile(r'(\d+|[^\d]+)')
    raw_tokens = []
    for m in pattern.finditer(normalized):
        val = m.group()
        if val.isdigit():
            raw_tokens.append({'type': 'num', 'val': val})
        else:
            raw_tokens.append({'type': 'text', 'val': val})

    # 再對文字 token 內部切分中文數字（緊接在地址單位前的數字）
    # 地址單位：樓、層、F（樓層）；號（門牌）；巷、弄（前面的數字）
    # 中文數字後接地址單位的模式：[零一兩二三四五六七八九十百千]+ + 地址單位
    CN_ADDR_UNIT = r'(?=[樓層號巷弄之]|F(?:\d|$))'
    CN_NUM_PAT   = re.compile(
        r'([零〇一兩二三四五六七八九十百千]+)' + CN_ADDR_UNIT
    )

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
            # 保留前面的文字
            if start > pos:
                tokens.append({'type': 'text', 'val': text[pos:start]})
            if arabic_val and arabic_val > 0:
                tokens.append({'type': 'cn_num', 'val': cn_str, 'arabic': arabic_val})
            else:
                # 無法解析則當一般文字
                tokens.append({'type': 'text', 'val': cn_str})
            pos = end
        if pos < len(text):
            tokens.append({'type': 'text', 'val': text[pos:]})

    return tokens


def generate_address_variants(address: str) -> list[str]:
    """
    給定一個地址，產生所有可能的搜尋變體。
    核心邏輯：將地址中每個數字替換為所有可能的表示形式，
    組合產生多個候選字串。
    """
    tokens = parse_address_tokens(address)

    # 每個 token 的候選值
    candidates_per_token = []
    for tok in tokens:
        if tok['type'] == 'num':
            candidates_per_token.append(generate_number_variants(tok['val']))
        elif tok['type'] == 'cn_num':
            # 中文數字：產生阿拉伯數字、全形、半形、其他中文寫法
            arabic = tok['arabic']
            variants_set = set()
            # 保留原中文
            variants_set.add(tok['val'])
            # 阿拉伯半形
            variants_set.add(str(arabic))
            # 阿拉伯全形
            variants_set.add(halfwidth_to_fullwidth(str(arabic)))
            # 其他中文變體
            for cn in arabic_to_chinese(arabic):
                variants_set.add(cn)
            candidates_per_token.append(list(variants_set))
        else:
            # 文字 token：只保留原始值（全形轉半形後的）
            candidates_per_token.append([tok['val']])

    # 笛卡爾積
    all_variants = set()
    for combo in product(*candidates_per_token):
        all_variants.add(''.join(combo))

    # 另外加上：原始輸入（含全形）
    all_variants.add(address.strip())
    # 全形版本
    all_variants.add(halfwidth_to_fullwidth(fullwidth_to_halfwidth(address.strip())))

    return sorted(all_variants)


# ═══════════════════════════════════════════════════════════════════════════════
# 篩選參數解析工具
# ═══════════════════════════════════════════════════════════════════════════════

def parse_range(s: str | None) -> tuple:
    """
    解析範圍字串，回傳 (min, max)，None 表示不限。
    支援格式：'20-40'  → (20, 40)
              '-40'    → (None, 40)
              '20-'    → (20, None)
              '113'    → (113, 113)  （單一值）
    """
    if not s:
        return (None, None)
    s = s.strip()
    if '-' in s:
        parts = s.split('-', 1)
        lo = float(parts[0]) if parts[0].strip() else None
        hi = float(parts[1]) if parts[1].strip() else None
        return (lo, hi)
    else:
        val = float(s)
        return (val, val)


# ═══════════════════════════════════════════════════════════════════════════════
# 資料庫搜尋
# ═══════════════════════════════════════════════════════════════════════════════

# 排序選項對應的 SQL ORDER BY
SORT_OPTIONS = {
    'date':         'transaction_date DESC, id DESC',
    'price':        'total_price DESC NULLS LAST',
    'count':        'addr_count DESC, transaction_date DESC',
    'unit_price':   'unit_price_per_ping DESC NULLS LAST',
    'ping':         'ping DESC NULLS LAST',
    'public_ratio': 'public_ratio ASC NULLS LAST',
}

def build_search_query(variants: list[str],
                       filters: dict,
                       sort_by: str = 'date',
                       limit: int = 200) -> tuple[str, list]:
    """
    建立帶篩選與排序的 CTE SQL 查詢。

    filters 支援的 key：
        building_types  : list[str]  建物型態關鍵字（模糊匹配）
        rooms           : list[int]  房數
        public_ratio_min/max : float 公設比 %
        year_min/max    : int        民國年
        ping_min/max    : float      坪數
        unit_price_min/max: float    單坪萬元
        price_min/max   : float      總價萬元
    """
    # ── 地址比對條件 ─────────────────────────────────────────────────────────
    addr_conds = ' OR '.join(['address LIKE ?' for _ in variants])
    params: list = [f'%{v}%' for v in variants]

    # ── CTE：base：原始欄位 + 計算欄位 ──────────────────────────────────────
    cte_base = f"""
    base AS (
        SELECT
            id,
            district,
            address,
            transaction_type,
            transaction_date,
            floor_level,
            total_floors,
            building_type,
            total_price,
            unit_price,
            building_area_sqm,
            rooms,
            halls,
            bathrooms,
            has_management,
            elevator,
            parking_type,
            parking_area_sqm,
            parking_price,
            note,
            main_building_area,
            attached_area,
            balcony_area,
            -- 計算坪數（1坪=3.30579㎡）
            CASE WHEN building_area_sqm > 0
                 THEN ROUND(building_area_sqm / 3.30579, 1)
                 ELSE NULL END                                     AS ping,
            -- 計算公設比
            CASE WHEN building_area_sqm > 0 AND main_building_area > 0
                      AND building_area_sqm > main_building_area
                 THEN ROUND(
                        (building_area_sqm
                            - COALESCE(main_building_area, 0)
                            - COALESCE(attached_area, 0)
                            - COALESCE(balcony_area, 0))
                        / building_area_sqm * 100, 1)
                 ELSE NULL END                                     AS public_ratio,
            -- 計算單坪價格（萬/坪）
            CASE WHEN building_area_sqm > 0 AND total_price > 0
                 THEN ROUND(
                        total_price / 10000.0
                        / (building_area_sqm / 3.30579), 1)
                 ELSE NULL END                                     AS unit_price_per_ping,
            -- 民國年（transaction_date 格式 YYYMMDD，7碼）
            CAST(SUBSTR(transaction_date, 1, LENGTH(transaction_date) - 4)
                 AS INTEGER)                                       AS roc_year
        FROM transactions
        WHERE ({addr_conds})
          AND address != ''
          AND address NOT LIKE '%land sector%'
    )"""

    # ── CTE：counted：加入同地址交易筆數 ────────────────────────────────────
    cte_counted = """
    counted AS (
        SELECT *,
               COUNT(*) OVER (PARTITION BY address) AS addr_count
        FROM base
    )"""

    # ── 篩選條件 ─────────────────────────────────────────────────────────────
    filter_clauses = []

    # 建物型態（模糊多選）
    btype = filters.get('building_types') or []
    if btype:
        tc = ' OR '.join(['building_type LIKE ?' for _ in btype])
        filter_clauses.append(f'({tc})')
        params.extend([f'%{t}%' for t in btype])

    # 房數（精確多選）
    rooms = filters.get('rooms') or []
    if rooms:
        rc = ' OR '.join(['rooms = ?' for _ in rooms])
        filter_clauses.append(f'({rc})')
        params.extend([int(r) for r in rooms])

    # 公設比
    if filters.get('public_ratio_min') is not None:
        filter_clauses.append('public_ratio IS NOT NULL AND public_ratio >= ?')
        params.append(filters['public_ratio_min'])
    if filters.get('public_ratio_max') is not None:
        filter_clauses.append('public_ratio IS NOT NULL AND public_ratio <= ?')
        params.append(filters['public_ratio_max'])

    # 民國年份
    if filters.get('year_min') is not None:
        filter_clauses.append('roc_year >= ?')
        params.append(int(filters['year_min']))
    if filters.get('year_max') is not None:
        filter_clauses.append('roc_year <= ?')
        params.append(int(filters['year_max']))

    # 坪數
    if filters.get('ping_min') is not None:
        filter_clauses.append('ping IS NOT NULL AND ping >= ?')
        params.append(filters['ping_min'])
    if filters.get('ping_max') is not None:
        filter_clauses.append('ping IS NOT NULL AND ping <= ?')
        params.append(filters['ping_max'])

    # 單坪價格（萬/坪）
    if filters.get('unit_price_min') is not None:
        filter_clauses.append('unit_price_per_ping IS NOT NULL AND unit_price_per_ping >= ?')
        params.append(filters['unit_price_min'])
    if filters.get('unit_price_max') is not None:
        filter_clauses.append('unit_price_per_ping IS NOT NULL AND unit_price_per_ping <= ?')
        params.append(filters['unit_price_max'])

    # 總價（萬元）
    if filters.get('price_min') is not None:
        filter_clauses.append('total_price IS NOT NULL AND total_price >= ?')
        params.append(int(filters['price_min'] * 10000))
    if filters.get('price_max') is not None:
        filter_clauses.append('total_price IS NOT NULL AND total_price <= ?')
        params.append(int(filters['price_max'] * 10000))

    where_str = ('WHERE ' + ' AND '.join(filter_clauses)) if filter_clauses else ''

    order_sql = SORT_OPTIONS.get(sort_by, SORT_OPTIONS['date'])

    sql = f"""
    WITH
    {cte_base},
    {cte_counted}
    SELECT * FROM counted
    {where_str}
    ORDER BY {order_sql}
    LIMIT {limit}
    """
    return sql, params


def search_address(address: str,
                   db_path: str = DEFAULT_DB,
                   filters: dict | None = None,
                   sort_by: str = 'date',
                   limit: int = 200,
                   show_sql: bool = False) -> dict:
    """
    主搜尋函式。
    回傳 dict: {
        'query':    原始輸入,
        'variants': 所有搜尋變體,
        'filters':  使用的篩選條件,
        'sort_by':  排序欄位,
        'total':    回傳筆數,
        'results':  [交易紀錄 dict, ...]
    }
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"找不到資料庫: {db_path}\n"
            f"請先執行: python3 {os.path.join(os.path.dirname(db_path), 'csv_to_sqlite.py')}"
        )

    variants = generate_address_variants(address)
    filters  = filters or {}

    sql, params = build_search_query(variants, filters, sort_by=sort_by, limit=limit)

    if show_sql:
        print("\n─── 產生的 SQL ─────────────────────────────────────────")
        print(sql)
        print("參數:")
        for i, p in enumerate(params):
            print(f"  [{i+1}] {p}")
        print("────────────────────────────────────────────────────────\n")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()

    return {
        'query':    address,
        'variants': variants,
        'filters':  filters,
        'sort_by':  sort_by,
        'total':    len(rows),
        'results':  rows,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 顯示 / 輸出
# ═══════════════════════════════════════════════════════════════════════════════

def format_price(price) -> str:
    if price is None:
        return '-'
    try:
        p = int(price)
        if p >= 100_000_000:
            return f'{p/100_000_000:.2f}億'
        elif p >= 10_000:
            return f'{p/10_000:.0f}萬'
        else:
            return f'{p:,}'
    except:
        return str(price)

def format_date(d) -> str:
    """民國 YYYYMMDD → 民國YYY/MM/DD"""
    if not d or len(str(d)) != 8:
        return str(d) if d else '-'
    s = str(d)
    return f"{s[:3]}/{s[3:5]}/{s[5:7]}"

def print_results(result: dict, show_variants: bool = True):
    print(f"\n{'═'*72}")
    print(f"🔍 搜尋地址：{result['query']}")
    print(f"{'═'*72}")

    if show_variants and len(result['variants']) <= 20:
        print(f"📝 搜尋變體（{len(result['variants'])} 個）：")
        for v in result['variants']:
            print(f"   • {v}")
        print()

    # 顯示篩選條件
    filters = result.get('filters', {})
    active = []
    if filters.get('building_types'):
        active.append(f"型態:{'/'.join(filters['building_types'])}")
    if filters.get('rooms'):
        active.append(f"房數:{'+'.join(str(r) for r in filters['rooms'])}房")
    pr_lo = filters.get('public_ratio_min')
    pr_hi = filters.get('public_ratio_max')
    if pr_lo is not None or pr_hi is not None:
        active.append(f"公設比:{pr_lo or ''}~{pr_hi or ''}%")
    y_lo = filters.get('year_min')
    y_hi = filters.get('year_max')
    if y_lo is not None or y_hi is not None:
        active.append(f"年份:民國{y_lo or ''}~{y_hi or ''}年")
    pg_lo = filters.get('ping_min')
    pg_hi = filters.get('ping_max')
    if pg_lo is not None or pg_hi is not None:
        active.append(f"坪數:{pg_lo or ''}~{pg_hi or ''}坪")
    up_lo = filters.get('unit_price_min')
    up_hi = filters.get('unit_price_max')
    if up_lo is not None or up_hi is not None:
        active.append(f"單坪:{up_lo or ''}~{up_hi or ''}萬")
    p_lo  = filters.get('price_min')
    p_hi  = filters.get('price_max')
    if p_lo is not None or p_hi is not None:
        active.append(f"總價:{p_lo or ''}~{p_hi or ''}萬")
    if active:
        print(f"🔧 篩選條件：{' | '.join(active)}")
    sort_label = {
        'date': '成交日期↓', 'price': '總價↓', 'count': '筆數↓',
        'unit_price': '單坪價↓', 'ping': '坪數↓', 'public_ratio': '公設比↑',
    }
    print(f"📌 排序：{sort_label.get(result.get('sort_by','date'), result.get('sort_by',''))}")
    print()

    total = result['total']
    rows  = result['results']
    print(f"📊 共找到 {total} 筆交易記錄\n")

    if not rows:
        print("  （無資料）")
        return

    # ── 統計摘要 ─────────────────────────────────────────────────────────────
    prices     = [r['total_price']        for r in rows if r.get('total_price') and r['total_price'] > 0]
    pings      = [r['ping']               for r in rows if r.get('ping')]
    unit_prices= [r['unit_price_per_ping'] for r in rows if r.get('unit_price_per_ping')]
    pub_ratios = [r['public_ratio']        for r in rows if r.get('public_ratio') and r['public_ratio'] > 0]

    if prices:
        avg_p = sum(prices) / len(prices)
        med_p = sorted(prices)[len(prices)//2]
        print(f"  💰 總價   均值 {format_price(avg_p)}  中位 {format_price(med_p)}"
              f"  最低 {format_price(min(prices))}  最高 {format_price(max(prices))}")
    if unit_prices:
        avg_u = sum(unit_prices) / len(unit_prices)
        med_u = sorted(unit_prices)[len(unit_prices)//2]
        print(f"  📐 單坪   均值 {avg_u:.1f}萬  中位 {med_u:.1f}萬"
              f"  最低 {min(unit_prices):.1f}萬  最高 {max(unit_prices):.1f}萬")
    if pings:
        avg_pg = sum(pings) / len(pings)
        print(f"  📏 坪數   均值 {avg_pg:.1f}坪"
              f"  最小 {min(pings):.1f}坪  最大 {max(pings):.1f}坪")
    if pub_ratios:
        avg_pr = sum(pub_ratios) / len(pub_ratios)
        print(f"  🏢 公設比 均值 {avg_pr:.1f}%"
              f"  最低 {min(pub_ratios):.1f}%  最高 {max(pub_ratios):.1f}%")
    print()

    # ── 表格輸出 ─────────────────────────────────────────────────────────────
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

            parking_info = ''
            if r.get('parking_type'):
                parking_info = r['parking_type'][:6]
                if r.get('parking_price') and r['parking_price'] > 0:
                    parking_info += f" {format_price(r['parking_price'])}"

            btype = (r.get('building_type') or '-')
            # 去除括號說明（如「住宅大樓(11層含以上有電梯)」→「住宅大樓」）
            btype = re.sub(r'\s*\([^)]*\)', '', btype).strip()

            pub_r = f"{r['public_ratio']:.0f}%" if r.get('public_ratio') and r['public_ratio'] > 0 else '-'
            unit_p = f"{r['unit_price_per_ping']:.1f}" if r.get('unit_price_per_ping') else '-'
            ping   = f"{r['ping']:.1f}" if r.get('ping') else '-'
            note   = (r.get('note') or '')[:18]

            table_data.append([
                i,
                r.get('district', ''),
                r.get('address', '')[:30],
                format_date(r.get('transaction_date')),
                (r.get('floor_level') or '-')[:6],
                btype[:8],
                format_price(r.get('total_price')),
                unit_p,
                ping,
                pub_r,
                layout or '-',
                parking_info or '-',
                note or '-',
            ])
        print(tabulate(table_data, headers=headers, tablefmt='simple'))
    except ImportError:
        # 無 tabulate 時的精簡格式
        header = f"{'#':>4}  {'行政區':6}  {'地址':<30}  {'日期':9}  {'總價':>8}  {'單坪萬':>6}  {'坪數':>6}  {'公設%':>5}  {'格局':8}"
        print(header)
        print('─' * len(header))
        for i, r in enumerate(rows, 1):
            layout = ''
            if r.get('rooms'):    layout += f"{r['rooms']}房"
            if r.get('halls'):    layout += f"{r['halls']}廳"
            if r.get('bathrooms'):layout += f"{r['bathrooms']}衛"
            pub_r  = f"{r['public_ratio']:.0f}%" if r.get('public_ratio') and r['public_ratio'] > 0 else '-'
            unit_p = f"{r['unit_price_per_ping']:.1f}" if r.get('unit_price_per_ping') else '-'
            ping   = f"{r['ping']:.1f}" if r.get('ping') else '-'
            print(
                f"{i:>4}  "
                f"{(r.get('district') or ''):<6}  "
                f"{(r.get('address') or '')[:30]:<30}  "
                f"{format_date(r.get('transaction_date')):9}  "
                f"{format_price(r.get('total_price')):>8}  "
                f"{unit_p:>6}  "
                f"{ping:>6}  "
                f"{pub_r:>5}  "
                f"{layout or '-':8}"
            )
            if r.get('note'):
                print(f"       📝 {r['note'][:70]}")

    print(f"\n{'─'*72}")


def export_csv(result: dict, output_path: str):
    import csv
    rows = result['results']
    if not rows:
        print("無資料可匯出。")
        return
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ 已匯出 {len(rows)} 筆 → {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='不動產交易地址模糊搜尋，支援篩選與排序',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
╔══════════════════════════════════════════════════════════════╗
║  範例用法                                                    ║
╠══════════════════════════════════════════════════════════════╣
║ 基本搜尋                                                     ║
║   address_transfer.py "三民路29巷"                           ║
║                                                              ║
║ 篩選建物型態（支援關鍵字模糊，可多選）                       ║
║   ... --type 公寓                                            ║
║   ... --type 住宅大樓 華廈                                   ║
║                                                              ║
║ 篩選房數                                                     ║
║   ... --rooms 3                                              ║
║   ... --rooms 2 3                                            ║
║                                                              ║
║ 篩選公設比（%）                                              ║
║   ... --public-ratio 0-35      （0%~35%）                    ║
║   ... --public-ratio -30       （上限30%）                   ║
║                                                              ║
║ 篩選成交年份（民國年）                                       ║
║   ... --year 110-114                                         ║
║   ... --year 113               （單年）                      ║
║                                                              ║
║ 篩選坪數                                                     ║
║   ... --ping 25-45                                           ║
║                                                              ║
║ 篩選單坪價格（萬/坪）                                        ║
║   ... --unit-price 60-120                                    ║
║                                                              ║
║ 篩選總價（萬元）                                             ║
║   ... --price 1000-3000                                      ║
║                                                              ║
║ 排序（date/price/count/unit_price/ping/public_ratio）        ║
║   ... --sort unit_price        （單坪價格高到低）            ║
║   ... --sort count             （成交最多地址優先）          ║
║   ... --sort public_ratio      （低公設優先）                ║
║                                                              ║
║ 組合範例                                                     ║
║   ... --type 住宅大樓 --rooms 3 --year 110-114               ║
║       --ping 30-50 --public-ratio -35 --sort unit_price      ║
║   ... --price 2000-5000 --sort price --export result.csv     ║
╚══════════════════════════════════════════════════════════════╝
        """
    )
    parser.add_argument('address', help='要搜尋的地址片段')

    # ── 基本選項 ──────────────────────────────────────────────────────────────
    parser.add_argument('--db',          default=DEFAULT_DB,  help='SQLite 資料庫路徑')
    parser.add_argument('--limit',       type=int, default=200, help='最多回傳筆數（預設200）')
    parser.add_argument('--show-sql',    action='store_true', help='印出 SQL 語句')
    parser.add_argument('--export',      metavar='FILE',       help='將結果匯出為 CSV')
    parser.add_argument('--no-variants', action='store_true', help='不顯示搜尋變體列表')

    # ── 篩選選項 ──────────────────────────────────────────────────────────────
    filter_group = parser.add_argument_group('篩選條件（預設全選）')
    filter_group.add_argument('--type',
        nargs='+', metavar='TYPE', dest='building_types',
        help='建物型態關鍵字（可多選，模糊匹配）\n'
             '  常用值: 公寓 華廈 住宅大樓 透天厝 套房 店面 辦公')
    filter_group.add_argument('--rooms',
        nargs='+', type=int, metavar='N',
        help='房數（可多選，如 --rooms 2 3）')
    filter_group.add_argument('--public-ratio',
        metavar='MIN-MAX', dest='public_ratio_range',
        help='公設比範圍（%%），如 0-35 或 -30（上限30%%）')
    filter_group.add_argument('--year',
        metavar='MIN-MAX', dest='year_range',
        help='民國成交年份，如 110-114 或 113（單年）')
    filter_group.add_argument('--ping',
        metavar='MIN-MAX', dest='ping_range',
        help='坪數範圍，如 25-45')
    filter_group.add_argument('--unit-price',
        metavar='MIN-MAX', dest='unit_price_range',
        help='單坪價格範圍（萬/坪），如 60-120')
    filter_group.add_argument('--price',
        metavar='MIN-MAX', dest='price_range',
        help='總價範圍（萬元），如 1000-3000')

    # ── 排序選項 ──────────────────────────────────────────────────────────────
    sort_group = parser.add_argument_group('排序')
    sort_group.add_argument('--sort',
        choices=list(SORT_OPTIONS.keys()), default='date',
        metavar='FIELD',
        help='排序欄位（預設: date）\n'
             '  date         成交日期降冪\n'
             '  price        總價降冪\n'
             '  count        同地址成交筆數降冪\n'
             '  unit_price   單坪價格降冪\n'
             '  ping         坪數降冪\n'
             '  public_ratio 公設比升冪（低公設優先）')

    args = parser.parse_args()

    # ── 組裝 filters dict ────────────────────────────────────────────────────
    filters: dict = {}

    if args.building_types:
        filters['building_types'] = args.building_types
    if args.rooms:
        filters['rooms'] = args.rooms

    pr_lo, pr_hi = parse_range(args.public_ratio_range)
    if pr_lo is not None: filters['public_ratio_min'] = pr_lo
    if pr_hi is not None: filters['public_ratio_max'] = pr_hi

    y_lo, y_hi = parse_range(args.year_range)
    if y_lo is not None: filters['year_min'] = y_lo
    if y_hi is not None: filters['year_max'] = y_hi

    pg_lo, pg_hi = parse_range(args.ping_range)
    if pg_lo is not None: filters['ping_min'] = pg_lo
    if pg_hi is not None: filters['ping_max'] = pg_hi

    up_lo, up_hi = parse_range(args.unit_price_range)
    if up_lo is not None: filters['unit_price_min'] = up_lo
    if up_hi is not None: filters['unit_price_max'] = up_hi

    p_lo, p_hi = parse_range(args.price_range)
    if p_lo is not None: filters['price_min'] = p_lo
    if p_hi is not None: filters['price_max'] = p_hi

    # ── 執行搜尋 ─────────────────────────────────────────────────────────────
    try:
        result = search_address(
            args.address,
            db_path=args.db,
            filters=filters,
            sort_by=args.sort,
            limit=args.limit,
            show_sql=args.show_sql,
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
