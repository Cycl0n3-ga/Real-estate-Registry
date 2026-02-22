#!/usr/bin/env python3
"""
merge_transactions.py
=====================
把 transactions.db（591 API）獨有的交易資料填入 land_data.db。

判斷邏輯：
  1. 以 (normalized_date, normalized_addr) 為主鍵，已存在者跳過
  2. 主鍵不符但 (date, total_price) 相同者視為重複，跳過
  3. 其餘全部插入，serial_no = "591_" + sq
"""

import sqlite3
import re
import json
import time
import os

DB_LAND = os.path.join(os.path.dirname(__file__), 'land_data.db')
DB_TRANS = os.path.join(os.path.dirname(__file__), 'transactions.db')

# ── DISTRICT_CITY_MAP（縮減版，含所有行政區）──────────────────────────────────
DISTRICT_CITY_MAP = {
    '中正區': '台北市', '大同區': '台北市', '中山區': '台北市', '松山區': '台北市',
    '大安區': '台北市', '萬華區': '台北市', '信義區': '台北市', '士林區': '台北市',
    '北投區': '台北市', '內湖區': '台北市', '南港區': '台北市', '文山區': '台北市',
    '板橋區': '新北市', '三重區': '新北市', '中和區': '新北市', '永和區': '新北市',
    '新莊區': '新北市', '新店區': '新北市', '樹林區': '新北市', '鶯歌區': '新北市',
    '三峽區': '新北市', '淡水區': '新北市', '汐止區': '新北市', '瑞芳區': '新北市',
    '土城區': '新北市', '蘆洲區': '新北市', '五股區': '新北市', '泰山區': '新北市',
    '林口區': '新北市', '深坑區': '新北市', '石碇區': '新北市', '坪林區': '新北市',
    '三芝區': '新北市', '石門區': '新北市', '八里區': '新北市', '平溪區': '新北市',
    '雙溪區': '新北市', '貢寮區': '新北市', '金山區': '新北市', '萬里區': '新北市',
    '烏來區': '新北市',
    '桃園區': '桃園市', '中壢區': '桃園市', '平鎮區': '桃園市', '八德區': '桃園市',
    '楊梅區': '桃園市', '蘆竹區': '桃園市', '大溪區': '桃園市', '龍潭區': '桃園市',
    '龜山區': '桃園市', '大園區': '桃園市', '觀音區': '桃園市', '新屋區': '桃園市',
    '復興區': '桃園市',
    '豐原區': '台中市', '大里區': '台中市', '太平區': '台中市', '清水區': '台中市',
    '沙鹿區': '台中市', '梧棲區': '台中市', '后里區': '台中市', '神岡區': '台中市',
    '潭子區': '台中市', '大雅區': '台中市', '新社區': '台中市', '石岡區': '台中市',
    '外埔區': '台中市', '大甲區': '台中市', '大肚區': '台中市', '龍井區': '台中市',
    '霧峰區': '台中市', '烏日區': '台中市', '和平區': '台中市', '西屯區': '台中市',
    '南屯區': '台中市', '北屯區': '台中市', '北區': '台中市', '南區': '台中市',
    '東區': '台中市', '西區': '台中市', '中區': '台中市',
    '新營區': '台南市', '鹽水區': '台南市', '白河區': '台南市', '柳營區': '台南市',
    '後壁區': '台南市', '麻豆區': '台南市', '下營區': '台南市', '六甲區': '台南市',
    '官田區': '台南市', '大內區': '台南市', '佳里區': '台南市', '學甲區': '台南市',
    '西港區': '台南市', '七股區': '台南市', '將軍區': '台南市', '北門區': '台南市',
    '新化區': '台南市', '善化區': '台南市', '新市區': '台南市', '安定區': '台南市',
    '山上區': '台南市', '玉井區': '台南市', '楠西區': '台南市', '南化區': '台南市',
    '左鎮區': '台南市', '仁德區': '台南市', '歸仁區': '台南市', '關廟區': '台南市',
    '龍崎區': '台南市', '永康區': '台南市', '安南區': '台南市', '安平區': '台南市',
    '東山區': '台南市',
    '鳳山區': '高雄市', '林園區': '高雄市', '大寮區': '高雄市', '大樹區': '高雄市',
    '大社區': '高雄市', '仁武區': '高雄市', '鳥松區': '高雄市', '岡山區': '高雄市',
    '橋頭區': '高雄市', '燕巢區': '高雄市', '田寮區': '高雄市', '阿蓮區': '高雄市',
    '路竹區': '高雄市', '湖內區': '高雄市', '茄萣區': '高雄市', '永安區': '高雄市',
    '彌陀區': '高雄市', '梓官區': '高雄市', '旗山區': '高雄市', '美濃區': '高雄市',
    '六龜區': '高雄市', '甲仙區': '高雄市', '杉林區': '高雄市', '內門區': '高雄市',
    '茂林區': '高雄市', '桃源區': '高雄市', '那瑪夏區': '高雄市', '楠梓區': '高雄市',
    '左營區': '高雄市', '鼓山區': '高雄市', '三民區': '高雄市', '苓雅區': '高雄市',
    '前鎮區': '高雄市', '旗津區': '高雄市', '小港區': '高雄市', '前金區': '高雄市',
    '鹽埕區': '高雄市', '新興區': '高雄市',
    '仁愛區': '基隆市', '安樂區': '基隆市', '暖暖區': '基隆市', '七堵區': '基隆市',
    '中山區': '基隆市',  # 衝突：台北市中山 vs 基隆中山 => 用順序覆蓋，台北優先（前面先設了）
    '香山區': '新竹市',
    '竹北市': '新竹縣', '竹東鎮': '新竹縣', '新埔鎮': '新竹縣', '關西鎮': '新竹縣',
    '湖口鄉': '新竹縣', '新豐鄉': '新竹縣', '芎林鄉': '新竹縣', '橫山鄉': '新竹縣',
    '北埔鄉': '新竹縣', '寶山鄉': '新竹縣', '峨眉鄉': '新竹縣', '尖石鄉': '新竹縣',
    '五峰鄉': '新竹縣',
    '苗栗市': '苗栗縣', '頭份市': '苗栗縣', '竹南鎮': '苗栗縣', '後龍鎮': '苗栗縣',
    '通霄鎮': '苗栗縣', '苑裡鎮': '苗栗縣', '卓蘭鎮': '苗栗縣', '大湖鄉': '苗栗縣',
    '公館鄉': '苗栗縣', '銅鑼鄉': '苗栗縣', '南庄鄉': '苗栗縣', '頭屋鄉': '苗栗縣',
    '三義鄉': '苗栗縣', '西湖鄉': '苗栗縣', '造橋鄉': '苗栗縣', '三灣鄉': '苗栗縣',
    '獅潭鄉': '苗栗縣', '泰安鄉': '苗栗縣',
    '彰化市': '彰化縣', '員林市': '彰化縣', '鹿港鎮': '彰化縣', '和美鎮': '彰化縣',
    '溪湖鎮': '彰化縣', '北斗鎮': '彰化縣', '田中鎮': '彰化縣', '二林鎮': '彰化縣',
    '線西鄉': '彰化縣', '伸港鄉': '彰化縣', '福興鄉': '彰化縣', '秀水鄉': '彰化縣',
    '花壇鄉': '彰化縣', '芬園鄉': '彰化縣', '大村鄉': '彰化縣', '埔鹽鄉': '彰化縣',
    '埔心鄉': '彰化縣', '永靖鄉': '彰化縣', '社頭鄉': '彰化縣', '二水鄉': '彰化縣',
    '田尾鄉': '彰化縣', '埤頭鄉': '彰化縣', '芳苑鄉': '彰化縣', '大城鄉': '彰化縣',
    '竹塘鄉': '彰化縣', '溪州鄉': '彰化縣',
    '南投市': '南投縣', '埔里鎮': '南投縣', '草屯鎮': '南投縣', '竹山鎮': '南投縣',
    '集集鎮': '南投縣', '名間鄉': '南投縣', '鹿谷鄉': '南投縣', '中寮鄉': '南投縣',
    '魚池鄉': '南投縣', '國姓鄉': '南投縣', '水里鄉': '南投縣', '信義鄉': '南投縣',
    '仁愛鄉': '南投縣',
    '斗六市': '雲林縣', '斗南鎮': '雲林縣', '虎尾鎮': '雲林縣', '西螺鎮': '雲林縣',
    '土庫鎮': '雲林縣', '北港鎮': '雲林縣', '古坑鄉': '雲林縣', '大埤鄉': '雲林縣',
    '莿桐鄉': '雲林縣', '林內鄉': '雲林縣', '二崙鄉': '雲林縣', '崙背鄉': '雲林縣',
    '麥寮鄉': '雲林縣', '東勢鄉': '雲林縣', '褒忠鄉': '雲林縣', '台西鄉': '雲林縣',
    '元長鄉': '雲林縣', '四湖鄉': '雲林縣', '口湖鄉': '雲林縣', '水林鄉': '雲林縣',
    '太保市': '嘉義縣', '朴子市': '嘉義縣', '布袋鎮': '嘉義縣', '大林鎮': '嘉義縣',
    '民雄鄉': '嘉義縣', '溪口鄉': '嘉義縣', '新港鄉': '嘉義縣', '六腳鄉': '嘉義縣',
    '東石鄉': '嘉義縣', '義竹鄉': '嘉義縣', '鹿草鄉': '嘉義縣', '水上鄉': '嘉義縣',
    '中埔鄉': '嘉義縣', '竹崎鄉': '嘉義縣', '梅山鄉': '嘉義縣', '番路鄉': '嘉義縣',
    '大埔鄉': '嘉義縣', '阿里山鄉': '嘉義縣',
    '屏東市': '屏東縣', '潮州鎮': '屏東縣', '東港鎮': '屏東縣', '恆春鎮': '屏東縣',
    '萬丹鄉': '屏東縣', '長治鄉': '屏東縣', '麟洛鄉': '屏東縣', '九如鄉': '屏東縣',
    '里港鄉': '屏東縣', '鹽埔鄉': '屏東縣', '高樹鄉': '屏東縣', '萬巒鄉': '屏東縣',
    '內埔鄉': '屏東縣', '竹田鄉': '屏東縣', '新埤鄉': '屏東縣', '枋寮鄉': '屏東縣',
    '新園鄉': '屏東縣', '崁頂鄉': '屏東縣', '林邊鄉': '屏東縣', '南州鄉': '屏東縣',
    '佳冬鄉': '屏東縣', '琉球鄉': '屏東縣', '車城鄉': '屏東縣', '滿州鄉': '屏東縣',
    '枋山鄉': '屏東縣', '霧台鄉': '屏東縣', '瑪家鄉': '屏東縣', '泰武鄉': '屏東縣',
    '來義鄉': '屏東縣', '春日鄉': '屏東縣', '獅子鄉': '屏東縣', '牡丹鄉': '屏東縣',
    '三地門鄉': '屏東縣',
    '宜蘭市': '宜蘭縣', '羅東鎮': '宜蘭縣', '蘇澳鎮': '宜蘭縣', '頭城鎮': '宜蘭縣',
    '礁溪鄉': '宜蘭縣', '壯圍鄉': '宜蘭縣', '員山鄉': '宜蘭縣', '冬山鄉': '宜蘭縣',
    '五結鄉': '宜蘭縣', '三星鄉': '宜蘭縣', '大同鄉': '宜蘭縣', '南澳鄉': '宜蘭縣',
    '花蓮市': '花蓮縣', '鳳林鎮': '花蓮縣', '玉里鎮': '花蓮縣', '新城鄉': '花蓮縣',
    '吉安鄉': '花蓮縣', '壽豐鄉': '花蓮縣', '光復鄉': '花蓮縣', '豐濱鄉': '花蓮縣',
    '瑞穗鄉': '花蓮縣', '富里鄉': '花蓮縣', '秀林鄉': '花蓮縣', '萬榮鄉': '花蓮縣',
    '卓溪鄉': '花蓮縣',
    '台東市': '台東縣', '成功鎮': '台東縣', '關山鎮': '台東縣', '卑南鄉': '台東縣',
    '大武鄉': '台東縣', '太麻里鄉': '台東縣', '東河鄉': '台東縣', '長濱鄉': '台東縣',
    '鹿野鄉': '台東縣', '池上鄉': '台東縣', '綠島鄉': '台東縣', '延平鄉': '台東縣',
    '海端鄉': '台東縣', '達仁鄉': '台東縣', '金峰鄉': '台東縣', '蘭嶼鄉': '台東縣',
    '馬公市': '澎湖縣', '湖西鄉': '澎湖縣', '白沙鄉': '澎湖縣', '西嶼鄉': '澎湖縣',
    '望安鄉': '澎湖縣', '七美鄉': '澎湖縣',
    '金城鎮': '金門縣', '金湖鎮': '金門縣', '金沙鎮': '金門縣', '金寧鄉': '金門縣',
    '烈嶼鄉': '金門縣', '烏坵鄉': '金門縣',
    '南竿鄉': '連江縣', '北竿鄉': '連江縣', '莒光鄉': '連江縣', '東引鄉': '連江縣',
    '新竹市': '新竹市', '嘉義市': '嘉義市',
}

CHINESE_FLOOR = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '十': 10, '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
    '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
    '二十一': 21, '二十二': 22, '二十三': 23, '二十四': 24, '二十五': 25,
    '二十六': 26, '二十七': 27, '二十八': 28, '二十九': 29, '三十': 30,
    '地下一': -1, '地下二': -2, '地下三': -3,
}


def norm_addr(addr):
    """全形→半形、臺→台、中文樓層→數字"""
    result = []
    for ch in (addr or ''):
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(ch)
    addr = ''.join(result).replace('臺', '台').replace(' ', '')
    addr = re.sub(
        r'(地下[一二三]|二十[一二三四五六七八九]|三十|二十|十[一二三四五六七八九]|[一二三四五六七八九十])(樓|層)',
        lambda m: str(CHINESE_FLOOR.get(m.group(1), m.group(1))) + m.group(2),
        addr
    )
    return addr


def strip_city(addr):
    for prefix in [
        '台北市', '新北市', '桃園市', '台中市', '台南市', '高雄市',
        '基隆市', '新竹市', '嘉義市', '新竹縣', '苗栗縣', '彰化縣',
        '南投縣', '雲林縣', '嘉義縣', '屏東縣', '宜蘭縣', '花蓮縣',
        '台東縣', '澎湖縣', '金門縣', '連江縣', '桃園縣', '台北縣',
        '台中縣', '台南縣', '高雄縣',
    ]:
        if addr.startswith(prefix):
            return addr[len(prefix):]
    return addr


def extract_district(addr):
    """從地址取出 district（行政區），查 DISTRICT_CITY_MAP"""
    for length in (4, 3, 2):
        candidate = addr[:length]
        if candidate in DISTRICT_CITY_MAP:
            return candidate
    return ''


def parse_price(val):
    if not val:
        return None
    try:
        return int(str(val).replace(',', '').replace(' ', ''))
    except Exception:
        return None


def parse_area(val):
    if not val:
        return None
    try:
        return float(str(val).replace(',', ''))
    except Exception:
        return None


def parse_floor_info(floor_str):
    """'九層/十五層' → (floor_level='九層', total_floors='十五層')"""
    if not floor_str:
        return '', ''
    parts = floor_str.split('/')
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return floor_str.strip(), ''


def build_land_data_keys(cl):
    """建立 land_data.db 的 (date, norm_addr) 和 (date, price) key set"""
    print('  讀取 land_data.db 鍵值...', flush=True)
    cl.execute('SELECT transaction_date, address, total_price FROM land_transaction WHERE address LIKE "%號%"')
    addr_date_keys = set()
    date_price_keys = set()
    for r in cl.fetchall():
        date = r[0].replace('/', '')[:7]
        addr = strip_city(norm_addr(r[1]))
        addr_date_keys.add((date, addr))
        price = parse_price(r[2])
        if price:
            date_price_keys.add((date, price))
    print(f'  land_data (date+addr) keys: {len(addr_date_keys):,}', flush=True)
    return addr_date_keys, date_price_keys


INSERT_SQL = """
INSERT INTO land_transaction (
    raw_district, transaction_type, address,
    land_area, urban_zone, non_urban_zone, non_urban_use,
    transaction_date, transaction_count, floor_level, total_floors,
    building_type, main_use, main_material, build_date,
    building_area, rooms, halls, bathrooms, partitioned,
    has_management, total_price, unit_price,
    parking_type, parking_area, parking_price,
    note, serial_no, main_area, attached_area,
    balcony_area, elevator, transfer_no,
    county_city, district, village, street, lane, alley,
    number, floor, sub_number,
    community_name, lat, lng
) VALUES (
    ?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,  ?,?,?,?,?,
    ?,?,?,  ?,?,?,  ?,?,?,?,  ?,?,?,
    ?,?,?,?,?,?,  ?,?,?,  ?,?,?
)
"""


def map_record(row, rj):
    """將 transactions.db 一列 + raw_json → land_transaction 欄位 tuple"""
    tid, city, town, addr_raw, build_type, community, date_str, floor_col, area_col, tp_raw, up_raw, lat, lon, sq, _ = row

    j = {}
    try:
        if rj:
            j = json.loads(rj)
    except Exception:
        pass

    # 地址：取 # 後半（半形規範版）
    if addr_raw and '#' in addr_raw:
        addr_clean = addr_raw.split('#', 1)[1]
    else:
        addr_clean = addr_raw or ''
    addr_norm = norm_addr(addr_clean)

    # district
    district = extract_district(addr_norm)
    county_city = DISTRICT_CITY_MAP.get(district, '')

    # 日期
    transaction_date = date_str.replace('/', '') if date_str else ''

    # 樓層
    floor_json = j.get('f', '') or floor_col or ''
    floor_level, total_floors = parse_floor_info(floor_json)

    # 交易標的
    transaction_type = j.get('t', '') or ''

    # 房廳衛
    rooms = None
    halls = None
    bathrooms = None
    try:
        rooms = int(j.get('j', '')) if j.get('j', '') != '' else None
    except Exception:
        pass
    try:
        halls = int(j.get('k', '')) if j.get('k', '') != '' else None
    except Exception:
        pass
    try:
        bathrooms = int(j.get('l', '')) if j.get('l', '') != '' else None
    except Exception:
        pass

    has_management = j.get('m', '') or ''
    main_use = j.get('pu', '') or j.get('AA11', '') or ''

    total_price = parse_price(tp_raw) or parse_price(j.get('tp'))
    unit_price = parse_area(up_raw) or parse_area(j.get('cp'))
    building_area = parse_area(area_col) or parse_area(j.get('s'))

    serial_no = f'591_{sq}' if sq else None

    return (
        town or '',           # raw_district
        transaction_type,     # transaction_type
        addr_clean,           # address
        None,                 # land_area
        '', '', '',           # urban_zone, non_urban_zone, non_urban_use
        transaction_date,     # transaction_date
        '',                   # transaction_count
        floor_level,          # floor_level
        total_floors,         # total_floors
        build_type or '',     # building_type
        main_use,             # main_use
        '',                   # main_material
        '',                   # build_date
        building_area,        # building_area
        rooms,                # rooms
        halls,                # halls
        bathrooms,            # bathrooms
        '',                   # partitioned
        has_management,       # has_management
        total_price,          # total_price
        unit_price,           # unit_price
        '',                   # parking_type
        None,                 # parking_area
        None,                 # parking_price
        '',                   # note
        serial_no,            # serial_no
        None,                 # main_area
        None,                 # attached_area
        None,                 # balcony_area
        '',                   # elevator
        '',                   # transfer_no
        county_city,          # county_city
        district,             # district
        '',                   # village
        '',                   # street
        '',                   # lane
        '',                   # alley
        '',                   # number
        '',                   # floor
        '',                   # sub_number
        community or '',      # community_name
        lat,                  # lat
        lon,                  # lng
    )


def rebuild_fts(conn):
    """重建 FTS5 address_fts 索引"""
    print('\n  重建 FTS5 索引...', flush=True)
    conn.execute('DROP TABLE IF EXISTS address_fts')
    conn.commit()
    conn.execute("""
        CREATE VIRTUAL TABLE address_fts USING fts5(
            address,
            content='land_transaction',
            content_rowid='id',
            tokenize='unicode61'
        )
    """)
    conn.commit()
    print('  填充 FTS...', flush=True)
    conn.execute("""
        INSERT INTO address_fts(rowid, address)
        SELECT id, address FROM land_transaction WHERE address != ''
    """)
    conn.commit()
    print('  FTS 重建完成', flush=True)


def main():
    t0 = time.time()

    conn_l = sqlite3.connect(DB_LAND)
    conn_l.text_factory = lambda b: b.decode('utf-8', errors='replace')
    conn_l.execute('PRAGMA journal_mode=WAL')
    conn_l.execute('PRAGMA synchronous=NORMAL')
    conn_l.execute('PRAGMA cache_size=-65536')

    conn_t = sqlite3.connect(DB_TRANS)
    conn_t.text_factory = lambda b: b.decode('utf-8', errors='replace')

    cl = conn_l.cursor()
    ct = conn_t.cursor()

    addr_date_keys, date_price_keys = build_land_data_keys(cl)

    print('  掃描 transactions.db 並插入獨有資料...', flush=True)
    ct.execute('SELECT id, city, town, address, build_type, community, date_str, floor, area, total_price, unit_price, lat, lon, sq, raw_json FROM transactions')

    BATCH = 10_000
    batch = []
    total_scanned = inserted = skipped_addr = skipped_price = 0

    for row in ct:
        total_scanned += 1
        date_str = row[6] or ''
        addr_raw = row[3] or ''
        tp_raw = row[9]
        sq = row[13]
        rj = row[14]

        date = date_str.replace('/', '')
        addr = norm_addr(addr_raw.split('#', 1)[1] if '#' in addr_raw else addr_raw)

        # 主鍵已存在 → 跳過
        if (date, addr) in addr_date_keys:
            skipped_addr += 1
            continue

        # 同日期+總價 → 視為重複
        price = parse_price(tp_raw)
        if price and (date, price) in date_price_keys:
            skipped_price += 1
            continue

        # 新筆資料
        try:
            rec = map_record(row, rj)
        except Exception as e:
            print(f'\n  [WARN] 跳過 id={row[0]}: {e}', flush=True)
            continue

        batch.append(rec)
        inserted += 1

        if len(batch) >= BATCH:
            cl.executemany(INSERT_SQL, batch)
            conn_l.commit()
            elapsed = time.time() - t0
            print(f'\r  已掃描 {total_scanned:,} | 插入 {inserted:,} | 跳過(地址重複) {skipped_addr:,} | 跳過(總價重複) {skipped_price:,}  ({elapsed:.0f}s)', end='', flush=True)
            batch.clear()

    if batch:
        cl.executemany(INSERT_SQL, batch)
        conn_l.commit()

    elapsed = time.time() - t0
    print(f'\n\n✅ 插入完成')
    print(f'   掃描總計: {total_scanned:,}')
    print(f'   已插入:   {inserted:,}')
    print(f'   跳過(date+addr重複): {skipped_addr:,}')
    print(f'   跳過(date+price重複): {skipped_price:,}')
    print(f'   耗時: {elapsed:.1f}s')

    # 重建 FTS
    rebuild_fts(conn_l)

    # ANALYZE
    print('  ANALYZE...', flush=True)
    conn_l.execute('ANALYZE')
    conn_l.commit()

    # 驗證
    cl.execute('SELECT COUNT(*) FROM land_transaction')
    total = cl.fetchone()[0]
    db_size = os.path.getsize(DB_LAND) / 1024 / 1024
    print(f'\n📊 land_data.db 最終統計')
    print(f'   總筆數: {total:,}')
    print(f'   DB 大小: {db_size:.1f} MB')
    print(f'   總耗時: {time.time() - t0:.1f}s')

    conn_l.close()
    conn_t.close()


if __name__ == '__main__':
    main()
