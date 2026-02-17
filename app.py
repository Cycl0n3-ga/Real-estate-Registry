#!/usr/bin/env python3
"""
良富居地產 v3.0 — 專業房地產地圖系統
修正：全形→半形、中文樓層→數字、地址搜尋、篩選UX、建案比對精準化
技術：Flask + DuckDB + Leaflet.js + OpenStreetMap
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
import os
import re
import json
import math
import time
import hashlib
import threading
from collections import defaultdict
from urllib.parse import quote_plus
from urllib.request import urlopen, Request

app = Flask(__name__)
CORS(app)

# ============================================================
# 設定
# ============================================================
CSV_PATH = '/home/cyclone/land/ALL_lvr_land_a.csv'
BUILDING_CSV_PATH = '/home/cyclone/land/Building_Projects_B.csv'
GEOCODE_CACHE_PATH = '/home/cyclone/land/geocode_cache.json'
DB_PATH = '/home/cyclone/land/land_data.duckdb'
PING_TO_SQM = 3.30579

# ============================================================
# 全域狀態
# ============================================================
_building_projects = []          # list[dict]  — Building_Projects_B
_building_index = {}             # id -> project
_address_index = {}              # id -> {address_raw, address, district}
_geocode_cache = {}
_geocode_lock = threading.Lock()
_data_ready = False
_db = None

# ============================================================
# 特殊交易類型
# ============================================================
SPECIAL_TX_PATTERNS = [
    ('親友特殊關係', ['親友', '特殊關係'],              '⚠️', '#e74c3c'),
    ('親等交易',     ['親等', '等親'],                  '👥', '#e67e22'),
    ('預售屋',       ['預售'],                          '🏗', '#3498db'),
    ('含增建',       ['增建'],                          '🏠', '#9b59b6'),
    ('車位交易',     ['車位交易', '單獨車位'],           '🅿', '#607d8b'),
    ('法拍',         ['拍賣', '法拍'],                  '⚖', '#c0392b'),
    ('信託',         ['信託'],                          '📋', '#8e44ad'),
    ('含裝潢',       ['裝潢'],                          '🔨', '#27ae60'),
    ('債務相關',     ['債'],                            '💰', '#d35400'),
    ('含頂樓加蓋',   ['頂樓'],                          '🔝', '#795548'),
    ('共有',         ['共有'],                          '👫', '#00897b'),
]


def detect_special_transaction(note):
    if not note or not isinstance(note, str):
        return []
    results = []
    for label, keywords, icon, color in SPECIAL_TX_PATTERNS:
        if any(kw in note for kw in keywords):
            results.append({'label': label, 'icon': icon, 'color': color})
    return results


# ============================================================
# 文字 / 數字轉換工具
# ============================================================
def fullwidth_to_halfwidth(text):
    """全形數字、英文字母轉半形"""
    if not text:
        return text
    out = []
    for ch in str(text):
        c = ord(ch)
        if 0xFF10 <= c <= 0xFF19:      # ０-９
            out.append(chr(c - 0xFEE0))
        elif 0xFF21 <= c <= 0xFF3A:    # Ａ-Ｚ
            out.append(chr(c - 0xFEE0))
        elif 0xFF41 <= c <= 0xFF5A:    # ａ-ｚ
            out.append(chr(c - 0xFEE0))
        else:
            out.append(ch)
    return ''.join(out)


def halfwidth_to_fullwidth(text):
    """半形數字、英文字母轉全形"""
    if not text:
        return text
    out = []
    for ch in str(text):
        c = ord(ch)
        if 0x30 <= c <= 0x39:          # 0-9
            out.append(chr(c + 0xFEE0))
        elif 0x41 <= c <= 0x5A:        # A-Z
            out.append(chr(c + 0xFEE0))
        elif 0x61 <= c <= 0x7A:        # a-z
            out.append(chr(c + 0xFEE0))
        else:
            out.append(ch)
    return ''.join(out)


_CN = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
       '六': 6, '七': 7, '八': 8, '九': 9}


def chinese_to_number(text):
    """中文數字→阿拉伯  十一→11  二十三→23  三十八→38"""
    if not text:
        return 0
    s = re.sub(r'[層樓Ff\s]', '', str(text).strip())
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        pass
    if s == '十':
        return 10
    if s == '百':
        return 100
    if '百' in s:
        h_part, rest = s.split('百', 1)
        h = _CN.get(h_part, 1) if h_part else 1
        return h * 100 + (chinese_to_number(rest) if rest else 0)
    if '十' in s:
        t_part, o_part = s.split('十', 1)
        tens = _CN.get(t_part, 1) if t_part else 1
        ones = _CN.get(o_part, 0) if o_part else 0
        return tens * 10 + ones
    return _CN.get(s, 0)


def _fmt_one_floor(part):
    """格式化單一樓層片段"""
    part = part.strip()
    if not part:
        return ''
    bm = re.search(r'地下([一二三四五六七八九十百\d]+)', part)
    if bm:
        return f'B{chinese_to_number(bm.group(1))}F'
    fm = re.search(r'([一二三四五六七八九十百\d]+)\s*層', part)
    if fm:
        n = chinese_to_number(fm.group(1))
        extra = re.sub(r'[一二三四五六七八九十百\d]+\s*層', '', part).strip()
        return f'{n}F' + (f'+{extra}' if extra else '')
    if '全' in part:
        return '全棟'
    if '夾層' in part:
        return '夾層'
    if '頂' in part:
        return '頂樓'
    return part


def format_floor(floor_str, total_str=None):
    """完整樓層格式化  '七層'+'十層' → '7F/10F'"""
    if not floor_str or str(floor_str).strip() in ('', 'nan', 'None'):
        return '—'
    s = fullwidth_to_halfwidth(str(floor_str))
    parts = re.split(r'[，,]', s)
    fmts = [_fmt_one_floor(p) for p in parts if p.strip()]
    fmts = [f for f in fmts if f]
    if not fmts:
        return '—'
    result = ','.join(fmts)
    if total_str:
        t = _fmt_total_floors(total_str)
        if t != '—':
            result = f'{result}/{t}'
    return result


def _fmt_total_floors(s):
    if not s or str(s).strip() in ('', 'nan', 'None'):
        return '—'
    s = fullwidth_to_halfwidth(str(s).strip())
    fm = re.search(r'([一二三四五六七八九十百\d]+)\s*層', s)
    if fm:
        return f'{chinese_to_number(fm.group(1))}F'
    try:
        return f'{int(s)}F'
    except ValueError:
        pass
    return s


# ============================================================
# 地址比對：從代表地址抽取多層級 pattern
# ============================================================
def extract_address_patterns(address):
    """回傳 [最精確, ..., 最寬鬆] 的 LIKE pattern 列表"""
    if not address:
        return []
    addr = fullwidth_to_halfwidth(str(address))
    patterns = []
    # road + section + lane + alley
    m = re.search(
        r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?\d+巷\d+弄)',
        addr)
    if m:
        patterns.append(m.group(1))
    # road + section + lane
    m = re.search(
        r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?\d+巷)',
        addr)
    if m and m.group(1) not in patterns:
        patterns.append(m.group(1))
    # road + section (有門牌)
    m = re.search(
        r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?)\d+號',
        addr)
    if m and m.group(1) not in patterns:
        patterns.append(m.group(1))
    # road + section only
    m = re.search(
        r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?)',
        addr)
    if m and m.group(1) not in patterns:
        patterns.append(m.group(1))
    return patterns


def extract_road_name(address):
    pats = extract_address_patterns(address)
    return pats[-1] if pats else None


# ============================================================
# 台灣鄉鎮市區座標
# ============================================================
DISTRICT_COORDINATES = {
    '中壢區': (24.9696, 120.9843), '桃園區': (25.0330, 121.3167),
    '新竹市': (24.8026, 120.9693), '北屯區': (24.2169, 120.7901),
    '淡水區': (25.1654, 121.4529), '板橋區': (25.0121, 121.4627),
    '西屯區': (24.1884, 120.6350), '新莊區': (25.0568, 121.4315),
    '竹北市': (24.8363, 120.9863), '中和區': (25.0049, 121.4935),
    '北投區': (25.1370, 121.5130), '苗栗市': (24.5595, 120.8196),
    '中山區': (25.0455, 121.5149), '大安區': (25.0330, 121.5254),
    '松山區': (25.0487, 121.5623), '南港區': (25.0543, 121.6090),
    '信義區': (25.0330, 121.5654), '內湖區': (25.0850, 121.5788),
    '士林區': (25.1122, 121.5254), '大同區': (25.0737, 121.5149),
    '文山區': (25.0035, 121.5674), '南屯區': (24.1003, 120.6684),
    '烏日區': (24.0630, 120.6717), '龍井區': (24.2507, 120.5690),
    '霧峰區': (24.0580, 120.8225), '太平區': (24.1456, 120.9383),
    '潭子區': (24.1995, 120.8610), '大雅區': (24.2575, 120.7870),
    '神岡區': (24.2456, 120.8080), '清水區': (24.2583, 120.5689),
    '梧棲區': (24.2495, 120.5439), '大肚區': (24.2250, 120.5519),
    '沙鹿區': (24.2330, 120.5699), '基隆市': (25.1276, 121.7347),
    '宜蘭縣': (24.7599, 121.7497), '花蓮縣': (24.0046, 121.5743),
    '台東縣': (22.7696, 121.1446), '屏東縣': (22.5442, 120.4886),
    '雲林縣': (23.7071, 120.4334), '嘉義市': (23.4788, 120.4432),
    '嘉義縣': (23.4534, 120.6081), '新北市': (25.0170, 121.4627),
    '三重區': (25.0617, 121.4879), '蘆洲區': (25.0855, 121.4738),
    '汐止區': (25.0626, 121.6610), '永和區': (25.0076, 121.5138),
    '三峽區': (24.9340, 121.3687), '土城區': (24.9723, 121.4437),
    '鶯歌區': (24.9519, 121.3517), '泰山區': (25.0500, 121.4300),
    '林口區': (25.0786, 121.3919), '五股區': (25.0787, 121.4380),
    '八里區': (25.1400, 121.4000), '樹林區': (24.9909, 121.4200),
    '深坑區': (25.0020, 121.6155), '瑞芳區': (25.1092, 121.8100),
    '萬里區': (25.1792, 121.6891), '金山區': (25.2220, 121.6370),
    '左營區': (22.6847, 120.2940), '前鎮區': (22.5955, 120.3268),
    '三民區': (22.6467, 120.3165), '鼓山區': (22.6555, 120.2710),
    '苓雅區': (22.6200, 120.3260), '楠梓區': (22.7308, 120.3262),
    '小港區': (22.5647, 120.3456), '鳳山區': (22.6268, 120.3595),
    '大寮區': (22.5965, 120.3987), '鳥松區': (22.6620, 120.3647),
    '仁武區': (22.7002, 120.3520), '岡山區': (22.7906, 120.2953),
    '路竹區': (22.8561, 120.2617), '橋頭區': (22.7575, 120.3058),
    '龍潭區': (24.8642, 121.2163), '楊梅區': (24.9077, 121.1449),
    '大溪區': (24.8832, 121.2863), '蘆竹區': (25.0439, 121.2917),
    '大園區': (25.0647, 121.2333), '龜山區': (25.0287, 121.3453),
    '八德區': (24.9456, 121.2900), '平鎮區': (24.9459, 121.2182),
    '觀音區': (25.0349, 121.1417), '新屋區': (24.9736, 121.1067),
    '竹東鎮': (24.7310, 121.0900), '新豐鄉': (24.8900, 120.9700),
    '湖口鄉': (24.9023, 121.0400), '永康區': (22.9896, 120.2440),
    '仁德區': (22.9385, 120.2545), '歸仁區': (22.9049, 120.3027),
    '善化區': (23.1310, 120.2978), '新化區': (23.0383, 120.3119),
    '安南區': (23.0468, 120.1853), '安平區': (22.9927, 120.1659),
    '東區': (22.9798, 120.2252),   '北區': (23.0030, 120.2080),
    '南區': (22.9600, 120.1980),   '中西區': (22.9920, 120.2000),
    '彰化市': (24.0827, 120.5417), '員林市': (23.9590, 120.5740),
    '鹿港鎮': (24.0585, 120.4325), '花壇鄉': (24.0937, 120.5146),
    '南投市': (23.9120, 120.6672), '草屯鎮': (23.9740, 120.6800),
    '埔里鎮': (23.9610, 120.9660), '竹山鎮': (23.7599, 120.6861),
    '鹽埕區': (22.6230, 120.2836), '前金區': (22.6266, 120.2952),
    '新興區': (22.6296, 120.3090), '旗津區': (22.5898, 120.2653),
    '林園區': (22.5100, 120.3927), '大樹區': (22.7240, 120.4300),
    '新營區': (23.3032, 120.3031), '麻豆區': (23.1793, 120.2411),
    '佳里區': (23.1602, 120.1808), '后里區': (24.3185, 120.7436),
    '豐原區': (24.2543, 120.7182), '東勢區': (24.2569, 120.7920),
    '旗山區': (22.8861, 120.4839), '美濃區': (22.8982, 120.5421),
}


def get_district_coordinates(district):
    if district in DISTRICT_COORDINATES:
        return DISTRICT_COORDINATES[district]
    for key in DISTRICT_COORDINATES:
        if district in key or key in district:
            return DISTRICT_COORDINATES[key]
    return (24.5, 121.0)


# ============================================================
# Geocoding（Nominatim + 檔案快取）
# ============================================================
def load_geocode_cache():
    global _geocode_cache
    if os.path.exists(GEOCODE_CACHE_PATH):
        try:
            with open(GEOCODE_CACHE_PATH, 'r', encoding='utf-8') as f:
                _geocode_cache = json.load(f)
            print(f"📍 已載入 {len(_geocode_cache)} 筆座標快取")
        except Exception:
            _geocode_cache = {}


def save_geocode_cache():
    with _geocode_lock:
        try:
            with open(GEOCODE_CACHE_PATH, 'w', encoding='utf-8') as f:
                json.dump(_geocode_cache, f, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️  快取儲存失敗: {e}")


def clean_address_for_geocoding(address):
    addr = str(address)
    addr = re.sub(r'\d+樓.*$', '', addr)
    addr = re.sub(r'[A-Za-z]\d*棟.*$', '', addr)
    addr = re.sub(r'店[A-Z].*$', '', addr)
    addr = re.sub(r'(?:和|與|及).+?(?:交叉|路口).*$', '', addr)
    if not addr.startswith(('台灣', '臺灣')):
        addr = '台灣 ' + addr
    return addr.strip()


def nominatim_geocode(address):
    clean_addr = clean_address_for_geocoding(address)
    try:
        url = (
            'https://nominatim.openstreetmap.org/search'
            f'?q={quote_plus(clean_addr)}'
            '&format=json&limit=1&countrycodes=tw'
        )
        req = Request(url, headers={'User-Agent': 'LiangFuEstate/3.0'})
        with urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data:
                return [float(data[0]['lat']), float(data[0]['lon'])]
    except Exception:
        pass
    return None


def get_coordinates(address, district):
    if address in _geocode_cache:
        c = _geocode_cache[address]
        return (c[0], c[1])
    return get_district_coordinates(district)


def background_geocoder():
    time.sleep(5)
    uncached = [
        p for p in _building_projects
        if p['representative_address'] not in _geocode_cache
    ]
    if not uncached:
        print("📍 所有建案座標已快取")
        return
    uncached.sort(key=lambda x: x['transaction_count'], reverse=True)
    print(f"📍 背景 Geocoding：{len(uncached)} 筆待處理")
    success = 0
    for i, proj in enumerate(uncached):
        addr = proj['representative_address']
        result = nominatim_geocode(addr)
        if result:
            with _geocode_lock:
                _geocode_cache[addr] = result
            success += 1
        time.sleep(1.1)
        if (i + 1) % 50 == 0:
            save_geocode_cache()
            print(f"  📍 進度 {i+1}/{len(uncached)}，成功 {success}")
    save_geocode_cache()
    print(f"📍 Geocoding 完成：{success}/{len(uncached)}")


# ============================================================
# 初始化
# ============================================================
def init_database():
    global _db
    csv_mtime = os.path.getmtime(CSV_PATH) if os.path.exists(CSV_PATH) else 0
    db_exists = os.path.exists(DB_PATH)
    db_mtime = os.path.getmtime(DB_PATH) if db_exists else 0
    need_rebuild = not db_exists or csv_mtime > db_mtime

    if need_rebuild:
        print("📦 建立 DuckDB 資料庫（首次需約 30 秒）…")
        if db_exists:
            os.remove(DB_PATH)
        con = duckdb.connect(DB_PATH)
        con.execute(f"""
            CREATE TABLE transactions AS
            SELECT * FROM read_csv_auto('{CSV_PATH}')
            WHERE "鄉鎮市區" IS NOT NULL
              AND "鄉鎮市區" != 'The villages and towns urban district'
              AND "鄉鎮市區" != ''
        """)
        con.execute('CREATE INDEX idx_district ON transactions("鄉鎮市區")')
        row_count = con.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
        con.close()
        print(f"✅ DuckDB 建立完成：{row_count:,} 筆交易")
    else:
        print("✅ 使用既有 DuckDB 資料庫")

    _db = duckdb.connect(DB_PATH, read_only=True)


def get_db():
    global _db
    if _db is None:
        _db = duckdb.connect(DB_PATH, read_only=True)
    return _db


def init_data():
    global _building_projects, _building_index, _data_ready

    print("🏗️  載入建案資料庫…")
    init_database()
    load_geocode_cache()

    try:
        con = duckdb.connect()
        df = con.execute(
            f"SELECT * FROM read_csv_auto('{BUILDING_CSV_PATH}')"
        ).fetchdf()
        con.close()

        projects = []
        for _, row in df.iterrows():
            name = str(row.get('建案名稱', '')).strip()
            if not name or name == '建案名稱':
                continue
            pid = hashlib.md5(name.encode()).hexdigest()[:12]
            district = str(row.get('鄉鎮市區', '')).strip()
            addr_raw = str(row.get('代表地址', '')).strip()
            addr = fullwidth_to_halfwidth(addr_raw)
            lat, lng = get_coordinates(addr_raw, district)

            avg_price = 0
            try:
                avg_price = float(row.get('平均成交價元', 0) or 0)
            except (ValueError, TypeError):
                pass

            avg_area = 0
            try:
                avg_area = float(row.get('平均面積平方公尺', 0) or 0)
            except (ValueError, TypeError):
                pass

            tx_count = 0
            try:
                tx_count = int(row.get('交易筆數', 0) or 0)
            except (ValueError, TypeError):
                pass

            max_floors_raw = str(row.get('最高樓層', ''))
            max_floors = _fmt_total_floors(max_floors_raw)

            proj = {
                'id': pid,
                'name': name,
                'address': addr,
                'district': district,
                'transaction_count': tx_count,
                'building_type': str(row.get('建物型態', '')),
                'max_floors': max_floors,
                'avg_price': avg_price,
                'avg_area_sqm': avg_area,
                'avg_ping': round(avg_area / PING_TO_SQM, 2) if avg_area else 0,
                'avg_unit_price_ping': round(avg_price / (avg_area / PING_TO_SQM), 0) if avg_area > 0 else 0,
                'address_count': int(row.get('地址數量', 0) or 0),
                'representative_address': addr,
                'address_patterns': extract_address_patterns(addr),
                'year_range': str(row.get('交易年份範圍', '')),
                'lat': lat,
                'lng': lng,
                'is_address_result': False,
            }
            projects.append(proj)
            _building_index[pid] = proj

        _building_projects = sorted(
            projects, key=lambda x: x['transaction_count'], reverse=True)
        _data_ready = True
        print(f"✅ 載入 {len(_building_projects)} 個建案")
    except Exception as e:
        print(f"❌ 載入失敗: {e}")
        import traceback
        traceback.print_exc()
        _data_ready = True


# ============================================================
# 工具
# ============================================================
def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return 0
    return obj


def format_roc_date(roc_date):
    if not roc_date:
        return None
    ds = str(roc_date).strip()
    if len(ds) < 7:
        return None
    try:
        y = int(ds[:3]) + 1911
        return f"{y}/{ds[3:5]}/{ds[5:7]}"
    except Exception:
        return None


def _make_tx_record(row):
    """從 DuckDB row 組出格式化交易記錄 dict"""
    note = str(row.get('備註', '') or '')
    specials = detect_special_transaction(note)

    price = 0
    try:
        price = float(row.get('總價元', 0) or 0)
    except (ValueError, TypeError):
        pass

    unit_price_sqm = 0
    try:
        unit_price_sqm = float(row.get('單價元平方公尺', 0) or 0)
    except (ValueError, TypeError):
        pass

    area_sqm = 0
    try:
        area_sqm = float(row.get('建物移轉總面積平方公尺', 0) or 0)
    except (ValueError, TypeError):
        pass

    main_area = 0
    try:
        main_area = float(row.get('主建物面積', 0) or 0)
    except (ValueError, TypeError):
        pass

    ping = area_sqm / PING_TO_SQM if area_sqm else 0
    upping = unit_price_sqm * PING_TO_SQM if unit_price_sqm else 0
    ratio = ((area_sqm - main_area) / area_sqm * 100) \
        if area_sqm > 0 and main_area > 0 else None

    parking_price = 0
    try:
        parking_price = float(row.get('車位總價元', 0) or 0)
    except (ValueError, TypeError):
        pass
    parking_area = 0
    try:
        parking_area = float(row.get('車位移轉總面積(平方公尺)', 0) or 0)
    except (ValueError, TypeError):
        pass

    floor_raw = str(row.get('移轉層次', '') or '')
    total_floors_raw = str(row.get('總樓層數', '') or '')

    return {
        'date': format_roc_date(row.get('交易年月日')),
        'date_raw': str(row.get('交易年月日', '')),
        'address': fullwidth_to_halfwidth(str(row.get('土地位置建物門牌', ''))),
        'floor': format_floor(floor_raw, total_floors_raw),
        'total_floors': _fmt_total_floors(total_floors_raw),
        'rooms': str(row.get('建物現況格局-房', '') or ''),
        'halls': str(row.get('建物現況格局-廳', '') or ''),
        'baths': str(row.get('建物現況格局-衛', '') or ''),
        'building_type': str(row.get('建物型態', '')),
        'price': price,
        'unit_price_ping': round(upping, 0),
        'area_ping': round(ping, 2),
        'ratio': round(ratio, 1) if ratio is not None else None,
        'parking_type': str(row.get('車位類別', '')),
        'parking_price': parking_price,
        'parking_area': parking_area,
        'note': note,
        'special': specials,
        'has_elevator': str(row.get('電梯', '')),
        'has_management': str(row.get('有無管理組織', '')),
        'main_use': str(row.get('主要用途', '')),
        'main_material': str(row.get('主要建材', '')),
        'build_date': str(row.get('建築完成年月', '')),
        'transaction_target': str(row.get('交易標的', '')),
    }


# ============================================================
# DuckDB 地址搜尋
# ============================================================
def search_addresses_in_db(keyword, limit=60):
    """在 DuckDB 搜尋含關鍵字的地址，聚合後回傳虛擬建案列表"""
    kw = fullwidth_to_halfwidth(keyword)  # 轉成半形
    kw_fullwidth = halfwidth_to_fullwidth(kw)  # 生成全形版本
    try:
        con = get_db()
        
        # 使用 OR 直接合併兩個模式
        df = con.execute("""
            SELECT
                "鄉鎮市區" AS district,
                "土地位置建物門牌" AS address,
                COUNT(*) AS tx_count,
                AVG(TRY_CAST("總價元" AS DOUBLE)) AS avg_price,
                AVG(TRY_CAST("建物移轉總面積平方公尺" AS DOUBLE)) AS avg_area,
                AVG(TRY_CAST("單價元平方公尺" AS DOUBLE)) AS avg_unit_sqm,
                MAX("總樓層數") AS max_floors,
                MAX("建物型態") AS building_type
            FROM transactions
            WHERE ("土地位置建物門牌" LIKE ? OR "土地位置建物門牌" LIKE ?)
              AND TRY_CAST("總價元" AS DOUBLE) > 0
            GROUP BY "鄉鎮市區", "土地位置建物門牌"
            ORDER BY tx_count DESC, "鄉鎮市區", "土地位置建物門牌"
            LIMIT ?
        """, [f'%{kw}%', f'%{kw_fullwidth}%', limit * 2]).fetchdf()

        results = []
        for _, row in df.iterrows():
            addr_raw = str(row['address'])
            addr = fullwidth_to_halfwidth(addr_raw)
            district = str(row['district'])
            pid = 'addr_' + hashlib.md5(
                f"{district}_{addr_raw}".encode()
            ).hexdigest()[:12]

            avg_price = float(row['avg_price'] or 0)
            avg_area = float(row['avg_area'] or 0)
            avg_unit_sqm = float(row['avg_unit_sqm'] or 0)
            ping = avg_area / PING_TO_SQM if avg_area else 0
            upping = avg_unit_sqm * PING_TO_SQM if avg_unit_sqm else 0

            proj = {
                'id': pid,
                'name': addr,
                'address': addr,
                'address_raw': addr_raw,
                'representative_address': addr,
                'district': district,
                'transaction_count': int(row['tx_count']),
                'building_type': str(row['building_type'] or ''),
                'max_floors': _fmt_total_floors(str(row['max_floors'] or '')),
                'avg_price': avg_price,
                'avg_area': ping,
                'avg_area_sqm': avg_area,
                'unit_price_ping': upping,
                'address_count': 1,
                'year_range': '',
                'lat': get_district_coordinates(district)[0],
                'lng': get_district_coordinates(district)[1],
                'is_address_result': True,
            }
            _address_index[pid] = {
                'address_raw': addr_raw,
                'address': addr,
                'district': district,
            }
            results.append(proj)
        return results
    except Exception as e:
        print(f"search_addresses_in_db error: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================
# Flask 路由
# ============================================================

@app.route('/')
def index():
    with open('liangfu_map.html', 'r', encoding='utf-8') as f:
        return f.read()



@app.route('/api/projects', methods=['GET'])
def api_projects():
    """建案列表（含地址搜尋 fallback）"""
    keyword = request.args.get('keyword', '').strip()
    district = request.args.get('district', '').strip()
    building_type = request.args.get('building_type', '').strip()
    min_price = request.args.get('min_price', type=float)
    max_price = request.args.get('max_price', type=float)
    min_ping = request.args.get('min_ping', type=float)
    max_ping = request.args.get('max_ping', type=float)
    sort_by = request.args.get('sort_by', 'transaction_count')
    sort_order = request.args.get('sort_order', 'desc')
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 200, type=int)

    results = list(_building_projects)

    if keyword:
        kw = fullwidth_to_halfwidth(keyword).lower()
        results = [
            p for p in results
            if kw in p['name'].lower()
            or kw in p['district'].lower()
            or kw in p['representative_address'].lower()
            or kw in p['building_type'].lower()
        ]
    if district:
        results = [p for p in results if district in p['district']]
    if building_type:
        results = [p for p in results if building_type in p['building_type']]
    if min_price is not None:
        results = [p for p in results if p['avg_price'] >= min_price]
    if max_price is not None:
        results = [p for p in results if p['avg_price'] <= max_price]
    if min_ping is not None:
        results = [p for p in results if p['avg_area_sqm'] >= min_ping * PING_TO_SQM]
    if max_ping is not None:
        results = [p for p in results if p['avg_area_sqm'] <= max_ping * PING_TO_SQM]

    # --- 關鍵字 >= 2 字，補充 DuckDB 地址搜尋 ---
    addr_results = []
    if keyword and len(keyword) >= 2:
        addr_results = search_addresses_in_db(keyword, limit=60)
        existing = {p['representative_address'] for p in results}
        addr_results = [a for a in addr_results if a['representative_address'] not in existing]
        if district:
            addr_results = [a for a in addr_results if district in a['district']]
        if building_type:
            addr_results = [a for a in addr_results if building_type in a['building_type']]

    merged = results + addr_results

    sort_keys = {
        'transaction_count': 'transaction_count',
        'price': 'avg_price',
        'area': 'avg_area_sqm',
        'unit_price': 'avg_unit_price_ping',
        'name': 'name',
    }
    sk = sort_keys.get(sort_by, 'transaction_count')
    rev = sort_order != 'asc'
    try:
        merged.sort(key=lambda x: x.get(sk, 0) or 0, reverse=rev)
    except Exception:
        pass

    total = len(merged)
    start = (page - 1) * limit
    page_results = merged[start:start + limit]

    for p in page_results:
        addr = p.get('representative_address', '')
        if addr in _geocode_cache:
            c = _geocode_cache[addr]
            p['lat'], p['lng'] = c[0], c[1]

    return jsonify(clean_nan({
        'success': True,
        'total': total,
        'page': page,
        'limit': limit,
        'building_count': len(results),
        'address_count': len(addr_results),
        'projects': page_results,
    }))


@app.route('/api/project/<project_id>', methods=['GET'])
def api_project_detail(project_id):
    """建案詳情（支援建案表 & 地址搜尋結果）"""

    if project_id.startswith('addr_'):
        # ── 地址搜尋結果 ──
        info = _address_index.get(project_id)
        if not info:
            return jsonify({'success': False, 'error': '找不到此地址（請重新搜尋）'}), 404
        district = info['district']
        addr_raw = info['address_raw']
        addr = info['address']
        proj = {
            'id': project_id,
            'name': addr,
            'district': district,
            'representative_address': addr,
            'building_type': '',
            'max_floors': '',
            'is_address_result': True,
        }
        try:
            con = get_db()
            df = con.execute("""
                SELECT * FROM transactions
                WHERE "鄉鎮市區" = ? AND "土地位置建物門牌" = ?
                  AND TRY_CAST("總價元" AS DOUBLE) > 0
                ORDER BY "交易年月日" DESC LIMIT 500
            """, [district, addr_raw]).fetchdf()
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        # ── 建案表 ──
        proj = _building_index.get(project_id)
        if not proj:
            return jsonify({'success': False, 'error': '找不到此建案'}), 404
        district = proj['district']
        patterns = proj.get('address_patterns', [])

        try:
            con = get_db()
            df = None
            for pat in patterns:
                test_df = con.execute("""
                    SELECT * FROM transactions
                    WHERE "鄉鎮市區" = ? AND "土地位置建物門牌" LIKE ?
                      AND TRY_CAST("總價元" AS DOUBLE) > 0
                    ORDER BY "交易年月日" DESC LIMIT 500
                """, [district, f'%{pat}%']).fetchdf()
                if len(test_df) >= 3:
                    df = test_df
                    break
            if df is None and patterns:
                df = con.execute("""
                    SELECT * FROM transactions
                    WHERE "鄉鎮市區" = ? AND "土地位置建物門牌" LIKE ?
                      AND TRY_CAST("總價元" AS DOUBLE) > 0
                    ORDER BY "交易年月日" DESC LIMIT 500
                """, [district, f'%{patterns[-1]}%']).fetchdf()
            if df is None:
                df = con.execute("""
                    SELECT * FROM transactions
                    WHERE "鄉鎮市區" = ?
                      AND TRY_CAST("總價元" AS DOUBLE) > 0
                    ORDER BY "交易年月日" DESC LIMIT 200
                """, [district]).fetchdf()
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'error': str(e)}), 500

    # ── 組合交易紀錄 ──
    transactions = []
    special_count = 0
    total_price = total_unit = total_area = 0
    count = 0

    for _, row in df.iterrows():
        tx = _make_tx_record(row)
        if tx['special']:
            special_count += 1
        total_price += tx['price']
        total_unit += tx['unit_price_ping']
        total_area += tx['area_ping']
        count += 1
        transactions.append(tx)

    summary = {
        'total_transactions': count,
        'special_count': special_count,
        'avg_price': round(total_price / count, 0) if count else 0,
        'avg_unit_price': round(total_unit / count, 0) if count else 0,
        'avg_area': round(total_area / count, 2) if count else 0,
    }

    return jsonify(clean_nan({
        'success': True,
        'project': proj,
        'summary': summary,
        'transactions': transactions,
    }))


@app.route('/api/stats', methods=['GET'])
def api_stats():
    total_projects = len(_building_projects)
    total_tx = sum(p['transaction_count'] for p in _building_projects)
    districts = defaultdict(int)
    for p in _building_projects:
        districts[p['district']] += p['transaction_count']
    top_districts = sorted(districts.items(), key=lambda x: -x[1])[:20]
    return jsonify({
        'success': True,
        'total_projects': total_projects,
        'total_transactions': total_tx,
        'top_districts': [{'district': d, 'count': c} for d, c in top_districts],
    })


@app.route('/api/districts', methods=['GET'])
def api_districts():
    districts = sorted(set(p['district'] for p in _building_projects if p['district']))
    try:
        con = get_db()
        db_d = con.execute(
            'SELECT DISTINCT "鄉鎮市區" FROM transactions '
            'WHERE "鄉鎮市區" IS NOT NULL ORDER BY 1'
        ).fetchdf()['鄉鎮市區'].tolist()
        return jsonify({'success': True, 'districts': sorted(set(districts + db_d))})
    except Exception:
        return jsonify({'success': True, 'districts': districts})


# ============================================================
if __name__ == '__main__':
    print("=" * 60)
    print("🏢 良富居地產 v3.0")
    print("=" * 60)
    print(f"📁 交易 CSV: {CSV_PATH}")
    print(f"📁 建案 CSV: {BUILDING_CSV_PATH}")
    print("=" * 60)

    init_data()

    t = threading.Thread(target=background_geocoder, daemon=True)
    t.start()

    print(f"🖥️  http://localhost:5000")
    app.run(debug=False, host='0.0.0.0', port=5000)
