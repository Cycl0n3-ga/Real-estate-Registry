#!/usr/bin/env python3
"""
address_utils.py — 台灣地址正規化 / 解析共用模組
=================================================
從 address_convert/convert.py 與 address_match/address_match.py 中
提取的共用常數、轉換函式與地址解析器。

提供:
  - 全形/半形轉換
  - 中文數字 ↔ 阿拉伯數字轉換
  - 台灣地址正規化
  - 台灣地址結構化解析（縣市/區/街路/巷弄/號/樓）
  - 鄉鎮市區 → 縣市映射表
"""

import re

# ============================================================
# 常數
# ============================================================

FULLWIDTH_DIGITS = '０１２３４５６７８９'
HALFWIDTH_DIGITS = '0123456789'

# 中文數字映射（含大寫/變體）
CHINESE_DIGITS = {
    '○': 0, '零': 0, '〇': 0,
    '一': 1, '壹': 1,
    '二': 2, '貳': 2, '兩': 2,
    '三': 3, '參': 3, '叁': 3,
    '四': 4, '肆': 4,
    '五': 5, '伍': 5,
    '六': 6, '陸': 6,
    '七': 7, '柒': 7,
    '八': 8, '捌': 8,
    '九': 9, '玖': 9,
}

CHINESE_UNITS = {
    '十': 10, '拾': 10,
    '百': 100, '佰': 100,
    '千': 1000, '仟': 1000,
}

CHINESE_NUM_CHARS = '○零一壹二貳兩三參叁四肆五伍六陸七柒八捌九玖十拾百佰千仟'

# 基本中文數字（0~9）順序表
CN_DIGIT_MAP = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九']

# 段數: 阿拉伯→中文
ARABIC_TO_CN_SECTION = {
    '1': '一', '2': '二', '3': '三', '4': '四', '5': '五',
    '6': '六', '7': '七', '8': '八', '9': '九', '10': '十',
}


# ============================================================
# 全形半形轉換
# ============================================================

def fullwidth_to_halfwidth(text: str) -> str:
    """全形字元轉半形（涵蓋 ASCII 全形區間 + 全形空白）"""
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


def halfwidth_to_fullwidth(text: str) -> str:
    """半形數字轉全形"""
    result = []
    for ch in text:
        idx = HALFWIDTH_DIGITS.find(ch)
        result.append(FULLWIDTH_DIGITS[idx] if idx >= 0 else ch)
    return ''.join(result)


# ============================================================
# 中文數字 ↔ 阿拉伯數字
# ============================================================

def chinese_numeral_to_int(text: str):
    """
    中文數字字串轉為整數。

    支援:
      - 阿拉伯數字直接轉 (e.g. '123')
      - 位置式中文 (e.g. '一二三' → 123)
      - 標準中文  (e.g. '二十三' → 23, '一百二十三' → 123)
      - 大寫/變體 (e.g. '貳拾參' → 23)

    回傳 int 或 None（無法解析時）。
    """
    if not text:
        return None

    # 嘗試直接轉為 int
    try:
        return int(text)
    except (ValueError, TypeError):
        pass

    # 嘗試位置式中文 (每字代表一個十進位位數)
    # e.g. '一二三' → 1,2,3 → 123
    if all(ch in CN_DIGIT_MAP for ch in text):
        try:
            return int(''.join(str(CN_DIGIT_MAP.index(ch)) for ch in text))
        except (ValueError, IndexError):
            pass

    # 標準中文數字 (含十/百/千單位)
    total = 0
    current = 0
    for ch in text:
        if ch in CHINESE_DIGITS:
            current = CHINESE_DIGITS[ch]
        elif ch in CHINESE_UNITS:
            unit = CHINESE_UNITS[ch]
            if current == 0:
                current = 1
            total += current * unit
            current = 0
        else:
            return None
    total += current

    if total > 0:
        return total
    # 明確的零
    if text.strip() in ('零', '○', '〇'):
        return 0
    return None


def arabic_to_chinese(n: int) -> list:
    """
    阿拉伯數字轉中文數字（回傳所有變體字串列表）。
    用於產生搜尋變體。
    """
    if n <= 0 or n > 9999:
        return []
    results = set()

    # 位置式: 123 → 一二三
    results.add(''.join(CN_DIGIT_MAP[int(d)] for d in str(n)))

    # 標準中文
    parts = []
    tens = (n % 100) // 10
    units = n % 10
    hundreds = (n % 1000) // 100
    thousands = n // 1000
    if thousands:
        parts.append(CN_DIGIT_MAP[thousands] + '千')
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

    # 十幾 的變體
    if 10 <= n <= 19:
        results.add('一十' + (CN_DIGIT_MAP[n % 10] if n % 10 else ''))
        results.add('十' + (CN_DIGIT_MAP[n % 10] if n % 10 else ''))

    return list(results)


# ============================================================
# 地址正規化
# ============================================================

# 預設地址後綴 pattern（樓/層/號/巷/弄/之/鄰）
_ADDR_SUFFIXES_BASE = '樓|層|號|巷|弄|之|鄰'
_ADDR_SUFFIXES_QUERY = '樓|層|號|巷|弄|之|鄰|F|f'  # 查詢時額外支援 F/f


def normalize_address(text: str, *, for_query: bool = False) -> str:
    """
    台灣地址正規化。

    步驟:
      1. 全形→半形
      2. 變體字修正 (臺→台, \u5DFF→市)
      3. 中文數字→阿拉伯數字 (在 樓/層/號/巷/弄/之/鄰 前)
      4. 阿拉伯數字段 → 中文段 (e.g. '3段' → '三段')

    Args:
        text: 地址字串
        for_query: True 時額外處理 F/f 後綴（查詢模式）
    """
    if not text:
        return text or ''

    text = fullwidth_to_halfwidth(text.strip() if for_query else text)
    text = text.replace('\u5DFF', '市')
    text = text.replace('臺', '台')

    suffixes = _ADDR_SUFFIXES_QUERY if for_query else _ADDR_SUFFIXES_BASE
    pattern = re.compile(rf'([{CHINESE_NUM_CHARS}]+)({suffixes})')

    def _replace(m):
        num = chinese_numeral_to_int(m.group(1))
        if num is not None:
            return f'{num}{m.group(2)}'
        return m.group(0)

    text = pattern.sub(_replace, text)

    # 將數字段統一轉為中文段 (e.g. '3段' → '三段')
    def _repl_section(m):
        n = m.group(1)
        cn = ARABIC_TO_CN_SECTION.get(n) if len(n) <= 2 else None
        if cn:
            return f'{cn}段'
        return m.group(0)

    text = re.sub(r'(\d+)段', _repl_section, text)
    return text


# ============================================================
# 鄉鎮市區 → 縣市映射
# ============================================================

DISTRICT_CITY_MAP = {
    # 台北市
    '松山區': '台北市', '萬華區': '台北市', '文山區': '台北市',
    '南港區': '台北市', '內湖區': '台北市', '士林區': '台北市',
    '北投區': '台北市', '大同區': '台北市',
    # 新北市
    '板橋區': '新北市', '三重區': '新北市', '中和區': '新北市',
    '永和區': '新北市', '新莊區': '新北市', '新店區': '新北市',
    '樹林區': '新北市', '鶯歌區': '新北市', '三峽區': '新北市',
    '淡水區': '新北市', '汐止區': '新北市', '瑞芳區': '新北市',
    '土城區': '新北市', '蘆洲區': '新北市', '五股區': '新北市',
    '泰山區': '新北市', '林口區': '新北市', '深坑區': '新北市',
    '石碇區': '新北市', '坪林區': '新北市', '三芝區': '新北市',
    '石門區': '新北市', '八里區': '新北市', '平溪區': '新北市',
    '雙溪區': '新北市', '貢寮區': '新北市', '金山區': '新北市',
    '萬里區': '新北市', '烏來區': '新北市',
    # 桃園市
    '桃園區': '桃園市', '中壢區': '桃園市', '平鎮區': '桃園市',
    '八德區': '桃園市', '楊梅區': '桃園市', '蘆竹區': '桃園市',
    '大溪區': '桃園市', '龍潭區': '桃園市', '龜山區': '桃園市',
    '大園區': '桃園市', '觀音區': '桃園市', '新屋區': '桃園市',
    '復興區': '桃園市',
    # 台中市
    '豐原區': '台中市', '大里區': '台中市', '太平區': '台中市',
    '清水區': '台中市', '沙鹿區': '台中市', '梧棲區': '台中市',
    '后里區': '台中市', '神岡區': '台中市', '潭子區': '台中市',
    '大雅區': '台中市', '新社區': '台中市', '石岡區': '台中市',
    '外埔區': '台中市', '大甲區': '台中市', '大肚區': '台中市',
    '龍井區': '台中市', '霧峰區': '台中市', '烏日區': '台中市',
    '和平區': '台中市', '西屯區': '台中市', '南屯區': '台中市',
    '北屯區': '台中市',
    # 台南市
    '新營區': '台南市', '鹽水區': '台南市', '白河區': '台南市',
    '柳營區': '台南市', '後壁區': '台南市', '東山區': '台南市',
    '麻豆區': '台南市', '下營區': '台南市', '六甲區': '台南市',
    '官田區': '台南市', '大內區': '台南市', '佳里區': '台南市',
    '學甲區': '台南市', '西港區': '台南市', '七股區': '台南市',
    '將軍區': '台南市', '北門區': '台南市', '新化區': '台南市',
    '善化區': '台南市', '新市區': '台南市', '安定區': '台南市',
    '山上區': '台南市', '玉井區': '台南市', '楠西區': '台南市',
    '南化區': '台南市', '左鎮區': '台南市', '仁德區': '台南市',
    '歸仁區': '台南市', '關廟區': '台南市', '龍崎區': '台南市',
    '永康區': '台南市', '安南區': '台南市', '安平區': '台南市',
    # 高雄市
    '鳳山區': '高雄市', '林園區': '高雄市', '大寮區': '高雄市',
    '大樹區': '高雄市', '大社區': '高雄市', '仁武區': '高雄市',
    '鳥松區': '高雄市', '岡山區': '高雄市', '橋頭區': '高雄市',
    '燕巢區': '高雄市', '田寮區': '高雄市', '阿蓮區': '高雄市',
    '路竹區': '高雄市', '湖內區': '高雄市', '茄萣區': '高雄市',
    '永安區': '高雄市', '彌陀區': '高雄市', '梓官區': '高雄市',
    '旗山區': '高雄市', '美濃區': '高雄市', '六龜區': '高雄市',
    '甲仙區': '高雄市', '杉林區': '高雄市', '內門區': '高雄市',
    '茂林區': '高雄市', '桃源區': '高雄市', '那瑪夏區': '高雄市',
    '楠梓區': '高雄市', '左營區': '高雄市', '鼓山區': '高雄市',
    '三民區': '高雄市', '苓雅區': '高雄市', '前鎮區': '高雄市',
    '旗津區': '高雄市', '小港區': '高雄市', '前金區': '高雄市',
    '鹽埕區': '高雄市', '新興區': '高雄市',
    # 基隆市
    '仁愛區': '基隆市', '安樂區': '基隆市', '暖暖區': '基隆市',
    '七堵區': '基隆市',
    # 新竹市/新竹縣
    '香山區': '新竹市',
    '竹北市': '新竹縣', '竹東鎮': '新竹縣', '新埔鎮': '新竹縣',
    '關西鎮': '新竹縣', '湖口鄉': '新竹縣', '新豐鄉': '新竹縣',
    '芎林鄉': '新竹縣', '橫山鄉': '新竹縣', '北埔鄉': '新竹縣',
    '寶山鄉': '新竹縣', '峨眉鄉': '新竹縣', '尖石鄉': '新竹縣',
    '五峰鄉': '新竹縣',
    # 苗栗縣
    '苗栗市': '苗栗縣', '頭份市': '苗栗縣', '竹南鎮': '苗栗縣',
    '後龍鎮': '苗栗縣', '通霄鎮': '苗栗縣', '苑裡鎮': '苗栗縣',
    '卓蘭鎮': '苗栗縣', '大湖鄉': '苗栗縣', '公館鄉': '苗栗縣',
    '銅鑼鄉': '苗栗縣', '南庄鄉': '苗栗縣', '頭屋鄉': '苗栗縣',
    '三義鄉': '苗栗縣', '西湖鄉': '苗栗縣', '造橋鄉': '苗栗縣',
    '三灣鄉': '苗栗縣', '獅潭鄉': '苗栗縣', '泰安鄉': '苗栗縣',
    # 彰化縣
    '彰化市': '彰化縣', '員林市': '彰化縣', '鹿港鎮': '彰化縣',
    '和美鎮': '彰化縣', '溪湖鎮': '彰化縣', '北斗鎮': '彰化縣',
    '田中鎮': '彰化縣', '二林鎮': '彰化縣', '線西鄉': '彰化縣',
    '伸港鄉': '彰化縣', '福興鄉': '彰化縣', '秀水鄉': '彰化縣',
    '花壇鄉': '彰化縣', '芬園鄉': '彰化縣', '大村鄉': '彰化縣',
    '埔鹽鄉': '彰化縣', '埔心鄉': '彰化縣', '永靖鄉': '彰化縣',
    '社頭鄉': '彰化縣', '二水鄉': '彰化縣', '田尾鄉': '彰化縣',
    '埤頭鄉': '彰化縣', '芳苑鄉': '彰化縣', '大城鄉': '彰化縣',
    '竹塘鄉': '彰化縣', '溪州鄉': '彰化縣',
    # 南投縣
    '南投市': '南投縣', '埔里鎮': '南投縣', '草屯鎮': '南投縣',
    '竹山鎮': '南投縣', '集集鎮': '南投縣', '名間鄉': '南投縣',
    '鹿谷鄉': '南投縣', '中寮鄉': '南投縣', '魚池鄉': '南投縣',
    '國姓鄉': '南投縣', '水里鄉': '南投縣', '信義鄉': '南投縣',
    '仁愛鄉': '南投縣',
    # 雲林縣
    '斗六市': '雲林縣', '斗南鎮': '雲林縣', '虎尾鎮': '雲林縣',
    '西螺鎮': '雲林縣', '土庫鎮': '雲林縣', '北港鎮': '雲林縣',
    '古坑鄉': '雲林縣', '大埤鄉': '雲林縣', '莿桐鄉': '雲林縣',
    '林內鄉': '雲林縣', '二崙鄉': '雲林縣', '崙背鄉': '雲林縣',
    '麥寮鄉': '雲林縣', '東勢鄉': '雲林縣', '褒忠鄉': '雲林縣',
    '台西鄉': '雲林縣', '元長鄉': '雲林縣', '四湖鄉': '雲林縣',
    '口湖鄉': '雲林縣', '水林鄉': '雲林縣',
    # 嘉義縣
    '太保市': '嘉義縣', '朴子市': '嘉義縣', '布袋鎮': '嘉義縣',
    '大林鎮': '嘉義縣', '民雄鄉': '嘉義縣', '溪口鄉': '嘉義縣',
    '新港鄉': '嘉義縣', '六腳鄉': '嘉義縣', '東石鄉': '嘉義縣',
    '義竹鄉': '嘉義縣', '鹿草鄉': '嘉義縣', '水上鄉': '嘉義縣',
    '中埔鄉': '嘉義縣', '竹崎鄉': '嘉義縣', '梅山鄉': '嘉義縣',
    '番路鄉': '嘉義縣', '大埔鄉': '嘉義縣', '阿里山鄉': '嘉義縣',
    # 屏東縣
    '屏東市': '屏東縣', '潮州鎮': '屏東縣', '東港鎮': '屏東縣',
    '恆春鎮': '屏東縣', '萬丹鄉': '屏東縣', '長治鄉': '屏東縣',
    '麟洛鄉': '屏東縣', '九如鄉': '屏東縣', '里港鄉': '屏東縣',
    '鹽埔鄉': '屏東縣', '高樹鄉': '屏東縣', '萬巒鄉': '屏東縣',
    '內埔鄉': '屏東縣', '竹田鄉': '屏東縣', '新埤鄉': '屏東縣',
    '枋寮鄉': '屏東縣', '新園鄉': '屏東縣', '崁頂鄉': '屏東縣',
    '林邊鄉': '屏東縣', '南州鄉': '屏東縣', '佳冬鄉': '屏東縣',
    '琉球鄉': '屏東縣', '車城鄉': '屏東縣', '滿州鄉': '屏東縣',
    '枋山鄉': '屏東縣', '霧台鄉': '屏東縣', '瑪家鄉': '屏東縣',
    '泰武鄉': '屏東縣', '來義鄉': '屏東縣', '春日鄉': '屏東縣',
    '獅子鄉': '屏東縣', '牡丹鄉': '屏東縣', '三地門鄉': '屏東縣',
    # 宜蘭/花蓮/台東/離島
    '宜蘭市': '宜蘭縣', '羅東鎮': '宜蘭縣', '蘇澳鎮': '宜蘭縣',
    '頭城鎮': '宜蘭縣', '礁溪鄉': '宜蘭縣', '壯圍鄉': '宜蘭縣',
    '員山鄉': '宜蘭縣', '冬山鄉': '宜蘭縣', '五結鄉': '宜蘭縣',
    '三星鄉': '宜蘭縣', '大同鄉': '宜蘭縣', '南澳鄉': '宜蘭縣',
    '花蓮市': '花蓮縣', '鳳林鎮': '花蓮縣', '玉里鎮': '花蓮縣',
    '新城鄉': '花蓮縣', '吉安鄉': '花蓮縣', '壽豐鄉': '花蓮縣',
    '光復鄉': '花蓮縣', '豐濱鄉': '花蓮縣', '瑞穗鄉': '花蓮縣',
    '富里鄉': '花蓮縣', '秀林鄉': '花蓮縣', '萬榮鄉': '花蓮縣',
    '卓溪鄉': '花蓮縣',
    '台東市': '台東縣', '成功鎮': '台東縣', '關山鎮': '台東縣',
    '卑南鄉': '台東縣', '大武鄉': '台東縣', '太麻里鄉': '台東縣',
    '東河鄉': '台東縣', '長濱鄉': '台東縣', '鹿野鄉': '台東縣',
    '池上鄉': '台東縣', '綠島鄉': '台東縣', '延平鄉': '台東縣',
    '海端鄉': '台東縣', '達仁鄉': '台東縣', '金峰鄉': '台東縣',
    '蘭嶼鄉': '台東縣',
    '馬公市': '澎湖縣', '湖西鄉': '澎湖縣', '白沙鄉': '澎湖縣',
    '西嶼鄉': '澎湖縣', '望安鄉': '澎湖縣', '七美鄉': '澎湖縣',
    '金城鎮': '金門縣', '金湖鎮': '金門縣', '金沙鎮': '金門縣',
    '金寧鄉': '金門縣', '烈嶼鄉': '金門縣', '烏坵鄉': '金門縣',
    '南竿鄉': '連江縣', '北竿鄉': '連江縣', '莒光鄉': '連江縣',
    '東引鄉': '連江縣',
    # 省轄市本身
    '新竹市': '新竹市', '嘉義市': '嘉義市', '基隆市': '基隆市',
}


# ============================================================
# 地址解析
# ============================================================

CITY_PATTERN = re.compile(
    r'^(台北市|新北市|桃園(?:市|縣)|台中(?:市|縣)|台南(?:市|縣)|'
    r'高雄(?:市|縣)|基隆市|新竹(?:市|縣)|嘉義(?:市|縣)|'
    r'苗栗縣|彰化縣|南投縣|雲林縣|屏東縣|'
    r'台東縣|花蓮縣|宜蘭縣|澎湖縣|金門縣|連江縣|台北縣)'
)

OLD_TO_NEW = {
    '台北縣': '新北市', '桃園縣': '桃園市',
    '台中縣': '台中市', '台南縣': '台南市', '高雄縣': '高雄市',
}


def parse_address(raw_address, district_col=''):
    """
    解析台灣地址為各組成部分。

    Args:
        raw_address: 原始地址字串
        district_col: CSV 中獨立的鄉鎮市區欄值（可選，用作 fallback）

    Returns:
        dict with keys: county_city, district, village, neighborhood,
                        street, lane, alley, number, floor, sub_number
    """
    empty = {
        'county_city': '', 'district': '', 'village': '', 'neighborhood': '',
        'street': '', 'lane': '', 'alley': '', 'number': '', 'floor': '',
        'sub_number': '',
    }
    if not raw_address or not isinstance(raw_address, str):
        return empty
    if '地號' in raw_address:
        return empty

    addr = normalize_address(raw_address.strip())
    result = dict(empty)

    # 縣市
    m = CITY_PATTERN.match(addr)
    if m:
        result['county_city'] = OLD_TO_NEW.get(m.group(1), m.group(1))
        addr = addr[m.end():]
        m2 = CITY_PATTERN.match(addr)
        if m2:
            addr = addr[m2.end():]

    # 鄉鎮市區
    m = re.match(r'^(.{1,4}?(?:區|鄉|鎮|市))', addr)
    if m:
        result['district'] = m.group(1)
        addr = addr[m.end():]

    if not result['district'] and district_col:
        result['district'] = normalize_address(district_col.strip())

    if not result['county_city'] and result['district']:
        result['county_city'] = DISTRICT_CITY_MAP.get(result['district'], '')

    # 里
    m = re.match(r'^(.{1,5}?里)(?=[^\d]*(?:路|街|大道|\d+鄰))', addr)
    if m:
        result['village'] = m.group(1)
        addr = addr[m.end():]

    # 鄰
    m = re.match(r'^(\d+鄰)', addr)
    if m:
        result['neighborhood'] = m.group(1)
        addr = addr[m.end():]

    # 街路名 (含段)
    m = re.match(r'^(.+?(?:路|街|大道))([一二三四五六七八九十\d]+段)?', addr)
    if m:
        result['street'] = m.group(1) + (m.group(2) or '')
        addr = addr[m.end():]
    else:
        m = re.match(r'^([^\d]+?)(?=\d)', addr)
        if m and m.group(1):
            result['street'] = m.group(1)
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

    # 號之Y (如 基隆市中正區新豐街486號之5  2樓)
    m2 = re.match(r'^之(\d+)', addr)
    if m2:
        if not result['sub_number']:
            result['sub_number'] = m2.group(1)
        addr = addr[len(m2.group(0)):]

    # 樓
    m = re.match(r'^[,，]?\s*(\d+)(?:樓|層)', addr)
    if m:
        result['floor'] = m.group(1)
        addr = addr[len(m.group(0)):]

    # 之 (樓之X, 如 53號12樓之8)
    m = re.match(r'^之(\d+)', addr)
    if m:
        if not result['sub_number']:
            result['sub_number'] = m.group(1)

    return result


def parse_query(query: str) -> dict:
    """
    解析使用者搜尋查詢為結構化條件。

    與 parse_address 的差異:
      - 使用 for_query=True 正規化（額外處理 F/f）
      - 不保留里、鄰（查詢不需要）
      - 不使用 DISTRICT_CITY_MAP 推算縣市
      - 支援 F/f 作為樓層後綴

    Args:
        query: 使用者輸入的地址查詢字串

    Returns:
        dict with keys: county_city, district, street, lane, alley,
                        number, floor, sub_number
    """
    addr = normalize_address(query, for_query=True)
    result = {k: '' for k in
              ['county_city', 'district', 'street', 'lane', 'alley',
               'number', 'floor', 'sub_number']}

    # 縣市
    m = CITY_PATTERN.match(addr)
    if m:
        result['county_city'] = OLD_TO_NEW.get(m.group(1), m.group(1))
        addr = addr[m.end():]

    # 鄉鎮市區
    m = re.match(r'^(.{1,4}?(?:區|鄉|鎮|市))(?=.)', addr)
    if m:
        result['district'] = m.group(1)
        addr = addr[m.end():]

    # 里 (略過不儲存)
    m = re.match(r'^(.{1,5}?里)(?=[^\d]*(?:路|街|大道|\d))', addr)
    if m:
        addr = addr[m.end():]

    # 鄰 (略過不儲存)
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

    # 號之Y
    m2 = re.match(r'^之(\d+)', addr)
    if m2:
        if not result['sub_number']:
            result['sub_number'] = m2.group(1)
        addr = addr[len(m2.group(0)):]

    # 樓 (支援 F/f)
    m = re.match(r'^(\d+)(?:樓|層|[Ff])', addr)
    if m:
        result['floor'] = m.group(1)
        addr = addr[len(m.group(0)):]

    # 之 (樓之X)
    m = re.match(r'^之(\d+)', addr)
    if m:
        if not result['sub_number']:
            result['sub_number'] = m.group(1)

    return result
