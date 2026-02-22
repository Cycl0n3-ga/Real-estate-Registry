#!/usr/bin/env python3
"""
Taiwan Address Geocoder - 台灣地址轉座標核心引擎
================================================

高效地將台灣地址批次轉換為 WGS84 經緯度座標。

設計策略（由快到慢）:
  1. SQLite 快取查詢（微秒級）
  2. 路段級座標近似（本地計算，毫秒級）
  3. Nominatim API 查詢（線上，~1秒/筆）
  4. NLSC 國土測繪 API（備援）

使用方式:
    from geocoder import TaiwanGeocoder

    gc = TaiwanGeocoder()

    # 單一查詢
    result = gc.geocode("台北市大安區和平東路三段1號")
    print(result)  # {'lat': 25.026, 'lng': 121.543, 'source': 'nominatim', ...}

    # 批次查詢
    addresses = ["台北市大安區和平東路三段1號", "新北市板橋區文化路一段100號"]
    results = gc.batch_geocode(addresses)

依賴: 僅使用 Python 標準庫 + requests（選用）+ tqdm（選用）
"""

import sqlite3
import re
import time
import json
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import logging
import os
import hashlib
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any

logger = logging.getLogger(__name__)

# =====================================================================
# 台灣區域 → 縣市對照表
# =====================================================================
DISTRICT_TO_CITY = {
    # 臺北市
    '松山區': '臺北市', '信義區': '臺北市', '大安區': '臺北市',
    '中山區': '臺北市', '中正區': '臺北市', '大同區': '臺北市',
    '萬華區': '臺北市', '文山區': '臺北市', '南港區': '臺北市',
    '內湖區': '臺北市', '士林區': '臺北市', '北投區': '臺北市',
    # 新北市
    '板橋區': '新北市', '中和區': '新北市', '永和區': '新北市',
    '新莊區': '新北市', '三重區': '新北市', '蘆洲區': '新北市',
    '土城區': '新北市', '樹林區': '新北市', '鶯歌區': '新北市',
    '三峽區': '新北市', '汐止區': '新北市', '金山區': '新北市',
    '萬里區': '新北市', '淡水區': '新北市', '瑞芳區': '新北市',
    '貢寮區': '新北市', '平溪區': '新北市', '雙溪區': '新北市',
    '新店區': '新北市', '深坑區': '新北市', '石碇區': '新北市',
    '坪林區': '新北市', '烏來區': '新北市', '五股區': '新北市',
    '泰山區': '新北市', '林口區': '新北市', '八里區': '新北市',
    '石門區': '新北市', '三芝區': '新北市',
    # 桃園市
    '桃園區': '桃園市', '中壢區': '桃園市', '平鎮區': '桃園市',
    '八德區': '桃園市', '楊梅區': '桃園市', '蘆竹區': '桃園市',
    '大溪區': '桃園市', '龍潭區': '桃園市', '龜山區': '桃園市',
    '大園區': '桃園市', '觀音區': '桃園市', '新屋區': '桃園市',
    '復興區': '桃園市',
    # 臺中市
    '中區': '臺中市', '東區': '臺中市', '南區': '臺中市',
    '西區': '臺中市', '北區': '臺中市', '西屯區': '臺中市',
    '南屯區': '臺中市', '北屯區': '臺中市', '豐原區': '臺中市',
    '大里區': '臺中市', '太平區': '臺中市', '清水區': '臺中市',
    '沙鹿區': '臺中市', '大甲區': '臺中市', '東勢區': '臺中市',
    '梧棲區': '臺中市', '烏日區': '臺中市', '神岡區': '臺中市',
    '大肚區': '臺中市', '大雅區': '臺中市', '后里區': '臺中市',
    '霧峰區': '臺中市', '潭子區': '臺中市', '龍井區': '臺中市',
    '外埔區': '臺中市', '和平區': '臺中市', '石岡區': '臺中市',
    '大安區(中)': '臺中市', '新社區': '臺中市',
    # 臺南市
    '新營區': '臺南市', '鹽水區': '臺南市', '白河區': '臺南市',
    '柳營區': '臺南市', '後壁區': '臺南市', '東山區': '臺南市',
    '麻豆區': '臺南市', '下營區': '臺南市', '六甲區': '臺南市',
    '官田區': '臺南市', '大內區': '臺南市', '佳里區': '臺南市',
    '學甲區': '臺南市', '西港區': '臺南市', '七股區': '臺南市',
    '將軍區': '臺南市', '北門區': '臺南市', '新化區': '臺南市',
    '善化區': '臺南市', '新市區': '臺南市', '安定區': '臺南市',
    '山上區': '臺南市', '玉井區': '臺南市', '楠西區': '臺南市',
    '南化區': '臺南市', '左鎮區': '臺南市', '仁德區': '臺南市',
    '歸仁區': '臺南市', '關廟區': '臺南市', '龍崎區': '臺南市',
    '永康區': '臺南市', '東區(南)': '臺南市', '南區(南)': '臺南市',
    '北區(南)': '臺南市', '安南區': '臺南市', '安平區': '臺南市',
    '中西區': '臺南市',
    # 高雄市
    '鳳山區': '高雄市', '三民區': '高雄市', '前鎮區': '高雄市',
    '苓雅區': '高雄市', '左營區': '高雄市', '楠梓區': '高雄市',
    '小港區': '高雄市', '鼓山區': '高雄市', '旗津區': '高雄市',
    '鹽埕區': '高雄市', '前金區': '高雄市', '新興區': '高雄市',
    '鳥松區': '高雄市', '大社區': '高雄市', '仁武區': '高雄市',
    '大樹區': '高雄市', '岡山區': '高雄市', '路竹區': '高雄市',
    '橋頭區': '高雄市', '梓官區': '高雄市', '彌陀區': '高雄市',
    '永安區': '高雄市', '湖內區': '高雄市', '茄萣區': '高雄市',
    '阿蓮區': '高雄市', '田寮區': '高雄市', '燕巢區': '高雄市',
    '林園區': '高雄市', '大寮區': '高雄市', '旗山區': '高雄市',
    '美濃區': '高雄市', '內門區': '高雄市', '杉林區': '高雄市',
    '甲仙區': '高雄市', '六龜區': '高雄市', '桃源區': '高雄市',
    '那瑪夏區': '高雄市', '茂林區': '高雄市',
    # 基隆市
    '仁愛區': '基隆市', '中正區(基)': '基隆市', '信義區(基)': '基隆市',
    '中山區(基)': '基隆市', '安樂區': '基隆市', '暖暖區': '基隆市',
    '七堵區': '基隆市',
    # 新竹市（不分區）
    '新竹市': '新竹市', '東區(竹)': '新竹市', '北區(竹)': '新竹市',
    '香山區': '新竹市',
    # 新竹縣
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
    '和美鎮': '彰化縣', '北斗鎮': '彰化縣', '溪湖鎮': '彰化縣',
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
    '莿桐鄉': '雲林縣', '林內鄉': '雲林縣', '古坑鄉': '雲林縣',
    '大埤鄉': '雲林縣', '崙背鄉': '雲林縣', '二崙鄉': '雲林縣',
    '麥寮鄉': '雲林縣', '臺西鄉': '雲林縣', '東勢鄉': '雲林縣',
    '褒忠鄉': '雲林縣', '四湖鄉': '雲林縣', '口湖鄉': '雲林縣',
    '水林鄉': '雲林縣', '元長鄉': '雲林縣',
    # 嘉義市（不分區）
    '嘉義市': '嘉義市', '東區(嘉)': '嘉義市', '西區(嘉)': '嘉義市',
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
    '枋山鄉': '屏東縣', '霧臺鄉': '屏東縣', '瑪家鄉': '屏東縣',
    '泰武鄉': '屏東縣', '來義鄉': '屏東縣', '春日鄉': '屏東縣',
    '獅子鄉': '屏東縣', '牡丹鄉': '屏東縣', '三地門鄉': '屏東縣',
    # 宜蘭縣
    '宜蘭市': '宜蘭縣', '羅東鎮': '宜蘭縣', '蘇澳鎮': '宜蘭縣',
    '頭城鎮': '宜蘭縣', '礁溪鄉': '宜蘭縣', '壯圍鄉': '宜蘭縣',
    '員山鄉': '宜蘭縣', '冬山鄉': '宜蘭縣', '五結鄉': '宜蘭縣',
    '三星鄉': '宜蘭縣', '大同鄉': '宜蘭縣', '南澳鄉': '宜蘭縣',
    # 花蓮縣
    '花蓮市': '花蓮縣', '鳳林鎮': '花蓮縣', '玉里鎮': '花蓮縣',
    '新城鄉': '花蓮縣', '吉安鄉': '花蓮縣', '壽豐鄉': '花蓮縣',
    '光復鄉': '花蓮縣', '豐濱鄉': '花蓮縣', '瑞穗鄉': '花蓮縣',
    '富里鄉': '花蓮縣', '秀林鄉': '花蓮縣', '萬榮鄉': '花蓮縣',
    '卓溪鄉': '花蓮縣',
    # 臺東縣
    '臺東市': '臺東縣', '台東市': '臺東縣', '成功鎮': '臺東縣',
    '關山鎮': '臺東縣', '卑南鄉': '臺東縣', '大武鄉': '臺東縣',
    '太麻里鄉': '臺東縣', '東河鄉': '臺東縣', '長濱鄉': '臺東縣',
    '鹿野鄉': '臺東縣', '池上鄉': '臺東縣', '綠島鄉': '臺東縣',
    '延平鄉': '臺東縣', '海端鄉': '臺東縣', '達仁鄉': '臺東縣',
    '金峰鄉': '臺東縣', '蘭嶼鄉': '臺東縣',
    # 澎湖縣
    '馬公市': '澎湖縣', '湖西鄉': '澎湖縣', '白沙鄉': '澎湖縣',
    '西嶼鄉': '澎湖縣', '望安鄉': '澎湖縣', '七美鄉': '澎湖縣',
    # 金門縣
    '金城鎮': '金門縣', '金湖鎮': '金門縣', '金沙鎮': '金門縣',
    '金寧鄉': '金門縣', '烈嶼鄉': '金門縣', '烏坵鄉': '金門縣',
    # 連江縣
    '南竿鄉': '連江縣', '北竿鄉': '連江縣', '莒光鄉': '連江縣',
    '東引鄉': '連江縣',
}

# 有歧義的區名：多個城市共用（需靠 district 欄位判斷）
AMBIGUOUS_DISTRICTS = {
    '東區', '西區', '南區', '北區', '中區',
    '中山區', '中正區', '信義區', '大安區', '仁愛區',
}

# 縣市別名：台→臺 等
CITY_ALIASES = {
    '台北市': '臺北市', '台中市': '臺中市', '台南市': '臺南市',
    '台東市': '臺東市', '台東縣': '臺東縣',
    '桃園縣': '桃園市',  # 舊制
}

# 已升格縣（舊縣名 → 新市名），鄉/鎮/市後綴統一改「區」
# 桃園縣2014升格、台北縣/台中縣/台南縣/高雄縣2010升格
_COUNTY_UPGRADE = {
    '桃園縣': '桃園市',
    '台北縣': '新北市',
    '臺北縣': '新北市',
    '台中縣': '臺中市',
    '臺中縣': '臺中市',
    '台南縣': '臺南市',
    '臺南縣': '臺南市',
    '高雄縣': '高雄市',
}

# 特殊鄉鎮更名（不遵循一般「X鎮/鄉/市 → X區」規則）
_SPECIAL_DISTRICT_UPGRADE = {
    '高雄縣三民鄉': '高雄市那瑪夏區',
}

# =====================================================================
# 全形半形數字對照
# =====================================================================
FULLWIDTH_DIGITS = '０１２３４５６７８９'
HALFWIDTH_DIGITS = '0123456789'

_FW2HW = str.maketrans(FULLWIDTH_DIGITS, HALFWIDTH_DIGITS)
_HW2FW = str.maketrans(HALFWIDTH_DIGITS, FULLWIDTH_DIGITS)


# =====================================================================
# AddressNormalizer - 地址正規化
# =====================================================================
class AddressNormalizer:
    """
    台灣地址正規化器

    主要功能:
    - 全形數字→半形
    - 去除樓層/共用部分等後綴
    - 統一台/臺
    - 提取路段名稱
    - 補全縣市前綴
    """

    @staticmethod
    def fullwidth_to_halfwidth(text: str) -> str:
        return text.translate(_FW2HW)

    @staticmethod
    def halfwidth_to_fullwidth(text: str) -> str:
        return text.translate(_HW2FW)

    @classmethod
    def normalize(cls, address: str) -> Optional[str]:
        """
        完整正規化地址字串

        1. 全形數字→半形
        2. 台→臺
        3. 去除樓層/共用/地下室等後綴
        4. 清理特殊字元
        """
        if not address:
            return None

        addr = str(address).strip()

        # 去掉常見雜訊前綴
        if addr.startswith('null'):
            addr = addr[4:]

        # 去除 HTML entities (如 &２１４１４；或 &21414；)
        addr = re.sub(r'&[^;；]+[;；]', '', addr)
        # 去除殘留的 & 編碼
        addr = re.sub(r'&\w+;', '', addr)

        # 全形數字→半形
        addr = addr.translate(_FW2HW)

        # 統一 台→臺
        addr = addr.replace('台北市', '臺北市')
        addr = addr.replace('台中市', '臺中市')
        addr = addr.replace('台南市', '臺南市')
        addr = addr.replace('台東', '臺東')

        # 處理異體字「巿」（U+5DFF）→「市」（U+5E02）
        addr = addr.replace('\u5dff', '市')

        # 去除重複縣市前綴（如 "新竹市新竹市" → "新竹市"，"桃園縣中壢市桃園縣中壢市" → "桃園縣中壢市"）
        addr = re.sub(r'^([\u4e00-\u9fff]{2,8}[市縣])\1', r'\1', addr)

        # 舊制升格縣轉換（如 "桃園縣中壢市..." → "桃園市中壢區..."）
        for old_county, new_city in _COUNTY_UPGRADE.items():
            if addr.startswith(old_county):
                rest = addr[len(old_county):]
                # 嘗試找後接的鄉/鎮/市名稱（2-4字）
                m = re.match(r'^([\u4e00-\u9fff]{2,4})[市鎮鄉]', rest)
                if m:
                    old_key = old_county + m.group(0)
                    if old_key in _SPECIAL_DISTRICT_UPGRADE:
                        addr = _SPECIAL_DISTRICT_UPGRADE[old_key] + rest[m.end():]
                    else:
                        # 一般規則：X鎮/鄉/市 → X區
                        addr = new_city + m.group(1) + '區' + rest[m.end():]
                else:
                    # 找不到鄉鎮市，只轉換縣→市
                    addr = new_city + rest
                break

        # 去除樓層資訊
        addr = re.sub(r'[一二三四五六七八九十百]+樓.*$', '', addr)
        addr = re.sub(r'\d+樓.*$', '', addr)
        addr = re.sub(r'\d+F.*$', '', addr, flags=re.IGNORECASE)

        # 去除「等共用部分」等後綴
        addr = re.sub(r'等?共用.*$', '', addr)
        addr = re.sub(r'房屋.*$', '', addr)
        addr = re.sub(r'地下.*$', '', addr)

        # 去除「店」等商業後綴
        addr = re.sub(r'店.*$', '', addr)

        # 去除里鄰資訊（僅去除行政單位[區鎮鄉市縣]後方的里名，避免誤刪地名）
        addr = re.sub(r'(?<=[區鎮鄉市縣])[\u4e00-\u9fff]{2,4}里\d*鄰?', '', addr)

        # 去除多餘空白
        addr = re.sub(r'\s+', '', addr)

        return addr.strip() if addr.strip() else None

    @classmethod
    def extract_base_address(cls, address: str) -> Optional[str]:
        """
        提取基本地址（到門牌號為止，不含樓層）

        "臺北市大安區和平東路三段1號5樓" → "臺北市大安區和平東路三段1號"
        """
        addr = cls.normalize(address)
        if not addr:
            return None

        # 匹配到「號」為止（包含之X）
        m = re.search(r'^(.+?\d+(?:之\d+)?號)', addr)
        return m.group(1) if m else addr

    @classmethod
    def extract_road(cls, address: str) -> Optional[str]:
        """
        提取路段名稱（含巷弄）

        "臺北市大安區和平東路三段1號" → "和平東路三段"
        "松山區三民路29巷5號"         → "三民路29巷"
        """
        addr = cls.normalize(address)
        if not addr:
            return None

        # 先去掉縣市區
        stripped = re.sub(r'^.*?[市縣]', '', addr, count=1)
        stripped = re.sub(r'^[^路街道]*?[區鎮鄉市]', '', stripped, count=1)

        # 匹配路段（含段+巷+弄）
        m = re.search(
            r'([\u4e00-\u9fff]+(?:路|街|大道)'
            r'(?:[一二三四五六七八九十]+段)?'
            r'(?:\d+巷)?'
            r'(?:\d+弄)?)',
            stripped
        )
        return m.group(1) if m else None

    @classmethod
    def extract_road_base(cls, address: str) -> Optional[str]:
        """
        提取路段基本名稱（不含巷弄）

        "三民路29巷5號" → "三民路"
        "和平東路三段"  → "和平東路三段"
        """
        addr = cls.normalize(address)
        if not addr:
            return None

        stripped = re.sub(r'^.*?[市縣]', '', addr, count=1)
        stripped = re.sub(r'^[^路街道]*?[區鎮鄉市]', '', stripped, count=1)

        m = re.search(
            r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]+段)?)',
            stripped
        )
        return m.group(1) if m else None

    @classmethod
    def build_full_address(cls, address: str, district: str = '') -> Optional[str]:
        """
        補全地址的縣市前綴

        ("和平東路三段1號", "大安區") → "臺北市大安區和平東路三段1號"
        """
        addr = cls.normalize(address)
        if not addr:
            return None

        # 如果地址已有縣市前綴
        if re.match(r'^(臺|台|新|桃|高|基|宜|花|屏|雲|嘉|苗|彰|南投|澎|金|連)', addr):
            # 嘗試統一名稱
            for old, new in CITY_ALIASES.items():
                if addr.startswith(old):
                    addr = new + addr[len(old):]
                    break
            return addr

        # 需要從 district 推斷城市
        if district:
            city = cls._district_to_city(district)
            if city:
                # 檢查 district 是否已在地址中
                if district in addr:
                    return f"{city}{addr}"
                else:
                    return f"{city}{district}{addr}"

        return addr

    @staticmethod
    def _district_to_city(district: str) -> Optional[str]:
        """從區域名找對應縣市"""
        if district in DISTRICT_TO_CITY:
            return DISTRICT_TO_CITY[district]

        # 歧義區名嘗試直接匹配
        for d, c in DISTRICT_TO_CITY.items():
            if d.startswith(district.rstrip('區鎮鄉市')):
                return c

        return None


# =====================================================================
# GeoCache - SQLite 快取
# =====================================================================
class GeoCache:
    """
    SQLite 永久快取，儲存地址→座標對照。

    表結構:
        geocode_cache(
            address_key TEXT PRIMARY KEY,  -- 正規化後的地址
            lat REAL,                       -- 緯度 (WGS84)
            lng REAL,                       -- 經度 (WGS84)
            level TEXT,                     -- 精度: exact/road/district
            source TEXT,                    -- 來源: nominatim/nlsc/cache/import
            raw_address TEXT,               -- 原始地址
            created_at TIMESTAMP
        )
    """

    def __init__(self, cache_db_path: str):
        self.db_path = cache_db_path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        con = sqlite3.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                address_key TEXT PRIMARY KEY,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                level TEXT DEFAULT 'exact',
                source TEXT DEFAULT 'unknown',
                raw_address TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_geocache_level
            ON geocode_cache(level)
        """)
        con.commit()
        con.close()

    def get(self, address_key: str) -> Optional[Dict]:
        """查詢單一地址"""
        con = sqlite3.connect(self.db_path)
        cur = con.execute(
            "SELECT lat, lng, level, source FROM geocode_cache WHERE address_key = ?",
            (address_key,)
        )
        row = cur.fetchone()
        con.close()
        if row:
            return {'lat': row[0], 'lng': row[1], 'level': row[2], 'source': row[3]}
        return None

    def get_batch(self, address_keys: List[str]) -> Dict[str, Dict]:
        """批次查詢快取"""
        results = {}
        con = sqlite3.connect(self.db_path)
        for i in range(0, len(address_keys), 900):
            batch = address_keys[i:i+900]
            placeholders = ','.join(['?'] * len(batch))
            cur = con.execute(
                f"SELECT address_key, lat, lng, level, source "
                f"FROM geocode_cache WHERE address_key IN ({placeholders})",
                batch
            )
            for row in cur:
                results[row[0]] = {
                    'lat': row[1], 'lng': row[2],
                    'level': row[3], 'source': row[4]
                }
        con.close()
        return results

    def put(self, address_key: str, lat: float, lng: float,
            level: str = 'exact', source: str = 'unknown', raw_address: str = ''):
        """寫入單一快取"""
        with self._lock:
            con = sqlite3.connect(self.db_path)
            con.execute(
                "INSERT OR REPLACE INTO geocode_cache "
                "(address_key, lat, lng, level, source, raw_address) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (address_key, lat, lng, level, source, raw_address)
            )
            con.commit()
            con.close()

    def put_batch(self, records: List[Tuple]):
        """
        批次寫入快取
        records: [(address_key, lat, lng, level, source, raw_address), ...]
        """
        with self._lock:
            con = sqlite3.connect(self.db_path)
            con.executemany(
                "INSERT OR REPLACE INTO geocode_cache "
                "(address_key, lat, lng, level, source, raw_address) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                records
            )
            con.commit()
            con.close()

    def import_json_cache(self, json_path: str):
        """
        匯入既有的 JSON 快取 (geocode_cache.json 格式)

        JSON 格式: {"address": [lat, lng], ...}
        """
        if not os.path.exists(json_path):
            logger.warning(f"JSON cache not found: {json_path}")
            return 0

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        records = []
        for addr, coords in data.items():
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lat, lng = float(coords[0]), float(coords[1])
                if 20 < lat < 27 and 118 < lng < 123:  # 台灣範圍
                    records.append((addr, lat, lng, 'exact', 'json_import', addr))

        if records:
            self.put_batch(records)
        logger.info(f"Imported {len(records)} entries from JSON cache")
        return len(records)

    @property
    def size(self) -> int:
        con = sqlite3.connect(self.db_path)
        count = con.execute("SELECT COUNT(*) FROM geocode_cache").fetchone()[0]
        con.close()
        return count

    def stats(self) -> Dict:
        """快取統計"""
        con = sqlite3.connect(self.db_path)
        total = con.execute("SELECT COUNT(*) FROM geocode_cache").fetchone()[0]
        by_level = dict(con.execute(
            "SELECT level, COUNT(*) FROM geocode_cache GROUP BY level"
        ).fetchall())
        by_source = dict(con.execute(
            "SELECT source, COUNT(*) FROM geocode_cache GROUP BY source"
        ).fetchall())
        con.close()
        return {'total': total, 'by_level': by_level, 'by_source': by_source}


# =====================================================================
# Geocoding Providers
# =====================================================================

class NominatimProvider:
    """
    OpenStreetMap Nominatim 地理編碼
    公開實例: 1 req/sec 限制
    本地實例: 無限制（需自行架設 Docker）

    用法:
        provider = NominatimProvider()
        result = provider.geocode("臺北市大安區和平東路三段1號")
        # {'lat': 25.026, 'lng': 121.543}
    """

    PUBLIC_URL = "https://nominatim.openstreetmap.org/search"

    def __init__(self, base_url: str = None, delay: float = 1.1,
                 user_agent: str = "TaiwanLandGeocoder/1.0"):
        self.base_url = base_url or self.PUBLIC_URL
        self.delay = delay  # 公開實例需 ≥1 秒間隔
        self.user_agent = user_agent
        self._last_request = 0.0

    def geocode(self, address: str) -> Optional[Dict]:
        """
        查詢地址座標

        支援兩種模式:
        1. Structured query (街道+城市+區域) — 台灣地址命中率高
        2. Free-form query — 備援

        Returns: {'lat': float, 'lng': float} or None
        """
        # 嘗試 structured query（台灣地址命中率遠高於 free-form）
        parsed = self._parse_taiwan_address(address)
        if parsed:
            result = self._structured_query(parsed)
            if result:
                return result

        # 備援: free-form query（用空白分隔增加命中率）
        spaced = self._add_spaces(address)
        result = self._free_query(spaced)
        if result:
            return result

        return None

    def _structured_query(self, parsed: Dict) -> Optional[Dict]:
        """Nominatim structured query（命中率較高）"""
        self._rate_limit()

        params = {
            'format': 'json',
            'countrycodes': 'tw',
            'limit': 1,
        }
        if parsed.get('street'):
            params['street'] = parsed['street']
        if parsed.get('city'):
            params['city'] = parsed['city']
        if parsed.get('county'):
            params['county'] = parsed['county']

        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"

        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', self.user_agent)
            req.add_header('Accept-Language', 'zh-TW,zh;q=0.9')

            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            self._last_request = time.time()

            if data and len(data) > 0:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                if 20 < lat < 27 and 118 < lng < 123:
                    return {'lat': lat, 'lng': lng}

        except Exception as e:
            logger.debug(f"Nominatim structured query error: {e}")

        return None

    def _free_query(self, address: str) -> Optional[Dict]:
        """Nominatim free-form query"""
        self._rate_limit()

        params = urllib.parse.urlencode({
            'q': address,
            'format': 'json',
            'countrycodes': 'tw',
            'limit': 1,
        })
        url = f"{self.base_url}?{params}"

        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', self.user_agent)
            req.add_header('Accept-Language', 'zh-TW,zh;q=0.9')

            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            self._last_request = time.time()

            if data and len(data) > 0:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                if 20 < lat < 27 and 118 < lng < 123:
                    return {'lat': lat, 'lng': lng}

        except Exception as e:
            logger.debug(f"Nominatim free query error: {e}")

        return None

    @staticmethod
    def _parse_taiwan_address(address: str) -> Optional[Dict]:
        """
        解析台灣地址為 structured query 欄位

        "臺北市大安區和平東路三段1號" → {city: "臺北市", county: "大安區", street: "和平東路三段"}
        """
        if not address:
            return None

        result = {}

        # 提取縣市
        m_city = re.match(r'^([\u4e00-\u9fff]{2,3}[市縣])', address)
        if m_city:
            result['city'] = m_city.group(1)
            rest = address[m_city.end():]
        else:
            rest = address

        # 提取區鎮鄉市
        m_dist = re.match(r'^([\u4e00-\u9fff]{1,4}[區鎮鄉市])', rest)
        if m_dist:
            result['county'] = m_dist.group(1)
            rest = rest[m_dist.end():]

        # 提取路段（到巷弄號之前，但含段）
        m_road = re.search(
            r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]+段)?)',
            rest
        )
        if m_road:
            result['street'] = m_road.group(1)
        elif rest.strip():
            result['street'] = rest.split('號')[0].split('巷')[0].split('弄')[0]

        return result if result.get('street') else None

    @staticmethod
    def _add_spaces(address: str) -> str:
        """在縣市/區域/路段之間加空白，提升 free-form 命中率"""
        addr = address
        addr = re.sub(r'([市縣])([\u4e00-\u9fff])', r'\1 \2', addr)
        addr = re.sub(r'([區鎮鄉])([\u4e00-\u9fff])', r'\1 \2', addr)
        return addr

    def _rate_limit(self):
        """遵守速率限制"""
        elapsed = time.time() - self._last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)


class NLSCProvider:
    """
    內政部國土測繪中心 地址轉座標 API

    API 端點: https://api.nlsc.gov.tw/other/TwoAddr/AddrTrans/{address}
    回傳 XML 格式

    比 Nominatim 更適合台灣地址，但穩定性可能較低。
    """

    BASE_URL = "https://api.nlsc.gov.tw/other/TwoAddr/AddrTrans"

    def __init__(self, delay: float = 0.3):
        self.delay = delay
        self._last_request = 0.0

    def geocode(self, address: str) -> Optional[Dict]:
        """查詢地址座標"""
        self._rate_limit()

        try:
            encoded = urllib.parse.quote(address, safe='')
            url = f"{self.BASE_URL}/{encoded}"

            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 TaiwanLandGeocoder/1.0')

            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode('utf-8')

            self._last_request = time.time()

            # 嘗試解析 XML
            try:
                root = ET.fromstring(raw)
            except ET.ParseError:
                # 可能是 JSON 或純文字
                try:
                    data = json.loads(raw)
                    if 'lat' in data and 'lng' in data:
                        return {'lat': float(data['lat']), 'lng': float(data['lng'])}
                except (json.JSONDecodeError, KeyError):
                    pass
                return None

            # 解析 XML — 嘗試多種可能的標籤名
            lat_val = None
            lng_val = None

            for tag_lat, tag_lng in [('Y', 'X'), ('lat', 'lng'),
                                      ('Latitude', 'Longitude')]:
                el_lat = root.find(f'.//{tag_lat}')
                el_lng = root.find(f'.//{tag_lng}')
                if el_lat is not None and el_lng is not None:
                    try:
                        lat_val = float(el_lat.text)
                        lng_val = float(el_lng.text)
                        break
                    except (ValueError, TypeError):
                        continue

            if lat_val is not None and lng_val is not None:
                # NLSC 可能回傳 TWD97 座標（大數值）或 WGS84
                if lat_val > 100:
                    # TWD97 (EPSG:3826) → WGS84 近似轉換
                    lat_val, lng_val = self._twd97_to_wgs84(lat_val, lng_val)

                if 20 < lat_val < 27 and 118 < lng_val < 123:
                    return {'lat': lat_val, 'lng': lng_val}

            return None

        except Exception as e:
            logger.warning(f"NLSC error for '{address}': {e}")
            return None

    @staticmethod
    def _twd97_to_wgs84(x: float, y: float) -> Tuple[float, float]:
        """
        TWD97 (TM2, EPSG:3826) → WGS84 近似轉換

        精度約 ±1m，足夠地理編碼用途。
        """
        import math

        # TWD97 參數
        a = 6378137.0
        f = 1 / 298.257222101
        b = a * (1 - f)
        e2 = (a**2 - b**2) / a**2

        x0 = 250000.0  # False Easting
        k0 = 0.9999
        lon0 = math.radians(121.0)  # 中央經線

        x -= x0
        M = y / k0

        mu = M / (a * (1 - e2/4 - 3*e2**2/64 - 5*e2**3/256))
        e1 = (1 - math.sqrt(1-e2)) / (1 + math.sqrt(1-e2))

        J1 = 3*e1/2 - 27*e1**3/32
        J2 = 21*e1**2/16 - 55*e1**4/32
        J3 = 151*e1**3/96
        J4 = 1097*e1**4/512

        fp = mu + J1*math.sin(2*mu) + J2*math.sin(4*mu) + \
             J3*math.sin(6*mu) + J4*math.sin(8*mu)

        C1 = e2 * math.cos(fp)**2 / (1-e2)
        T1 = math.tan(fp)**2
        R1 = a*(1-e2) / (1-e2*math.sin(fp)**2)**1.5
        N1 = a / math.sqrt(1-e2*math.sin(fp)**2)
        D = x / (N1*k0)

        lat = fp - (N1*math.tan(fp)/R1) * (
            D**2/2 - (5+3*T1+10*C1-4*C1**2-9*e2)*D**4/24 +
            (61+90*T1+298*C1+45*T1**2-252*e2-3*C1**2)*D**6/720
        )

        lng = lon0 + (
            D - (1+2*T1+C1)*D**3/6 +
            (5-2*C1+28*T1-3*C1**2+8*e2+24*T1**2)*D**5/120
        ) / math.cos(fp)

        return math.degrees(lat), math.degrees(lng)

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)


# =====================================================================
# ArcGISProvider - ArcGIS World Geocoder（線上備援，免費 1000 筆/天）
# =====================================================================

class ArcGISProvider:
    """
    ArcGIS World Geocoder 地理編碼（備援用途）

    特性:
    - 無需 API Token（免費 1000 筆/天）
    - 台灣門牌精確度高（score ≥ 90 視為可靠）
    - 速度慢（~0.5-1秒/筆），僅作 OSM 索引找不到時的備援

    API: https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates
    參數: SingleLine（完整地址字串），outSR=4326（WGS84）
    """

    BASE_URL = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
    MIN_SCORE = 85.0     # 最低可接受精確度分數（0-100）
    TIMEOUT = 8          # 請求逾時秒數
    DELAY = 0.5          # 請求間隔（防止超過免費限額）

    def __init__(self, min_score: float = None, delay: float = None):
        if min_score is not None:
            self.MIN_SCORE = min_score
        if delay is not None:
            self.DELAY = delay
        self._last_request = 0.0

    def geocode(self, address: str) -> Optional[Dict]:
        """
        查詢地址座標

        Args:
            address: 完整地址（建議含縣市，如「臺北市大安區和平東路三段1號」）

        Returns:
            {'lat': float, 'lng': float, 'score': float, 'source': 'arcgis', 'level': 'exact'}
            or None
        """
        self._rate_limit()

        params = {
            'SingleLine': address,
            'f': 'json',
            'maxLocations': 1,
            'outSR': '{"wkid":4326}',
            'outFields': 'Addr_type,Match_addr,City',
        }
        full_url = self.BASE_URL + '?' + urllib.parse.urlencode(params)

        try:
            req = urllib.request.Request(full_url)
            req.add_header('User-Agent', 'Mozilla/5.0 TaiwanLandGeocoder/1.0')
            with urllib.request.urlopen(req, timeout=self.TIMEOUT) as resp:
                data = json.loads(resp.read().decode())

            self._last_request = time.time()

            candidates = data.get('candidates', [])
            if not candidates:
                return None

            best = candidates[0]
            score = best.get('score', 0)
            lat = best.get('location', {}).get('y')
            lng = best.get('location', {}).get('x')

            if lat is None or lng is None:
                return None

            # 確認在台灣範圍內（過濾誤判）
            if not (20 < lat < 27 and 118 < lng < 123):
                logger.debug(f"ArcGIS 座標超出台灣範圍: {address} → ({lat}, {lng})")
                return None

            if score < self.MIN_SCORE:
                logger.debug(f"ArcGIS 精確度不足 {score:.0f}: {address}")
                return None

            return {
                'lat': lat,
                'lng': lng,
                'score': score,
                'source': 'arcgis',
                'level': 'exact',
            }

        except Exception as e:
            logger.warning(f"ArcGIS error for '{address}': {e}")
            return None

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.DELAY:
            time.sleep(self.DELAY - elapsed)


# =====================================================================
# OSMIndexProvider - 本地 OSM 門牌座標索引（精確查詢）
# =====================================================================

class OSMIndexProvider:
    """
    本地 OSM 門牌座標索引（毫秒級精確查詢）

    使用 build_osm_index.py 建立的 osm_addresses.db，
    對台灣約 900 萬筆門牌節點做快速 SQLite 查詢。

    精度等同 Nominatim place_rank=30（±10-50m，門牌級）

    用法:
        provider = OSMIndexProvider()
        if provider.is_available():
            result = provider.geocode("臺北市大安區和平東路三段168號")
            # {'lat': 25.023..., 'lng': 121.553..., 'level': 'exact', 'source': 'osm_index'}
    """

    _FW2HW = str.maketrans('０１２３４５６７８９', '0123456789')

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '..', '..', 'db', 'osm_addresses.db'
            )
        self.db_path = db_path
        self._available = os.path.exists(db_path)
        if self._available:
            # 確認索引存在
            try:
                con = sqlite3.connect(db_path)
                count = con.execute("SELECT COUNT(*) FROM osm_addresses").fetchone()[0]
                con.close()
                self._available = count > 0
                logger.info(f"OSMIndexProvider: {count:,} 個門牌節點")
            except Exception:
                self._available = False

    def is_available(self) -> bool:
        return self._available

    def geocode(self, address: str) -> Optional[Dict]:
        """
        精確查詢門牌座標

        Returns: {'lat': float, 'lng': float} or None
        """
        if not self._available:
            return None

        parsed = self._parse_address(address)
        if not parsed or not parsed.get('street') or not parsed.get('housenumber'):
            return None

        result = self._query(
            parsed['street'],
            parsed['housenumber'],
            parsed.get('district', '')
        )
        if result:
            return {'lat': result[0], 'lng': result[1], 'source': 'osm_index', 'level': 'exact'}
        return None

    def batch_geocode(self, addresses: List[str]) -> Dict[str, Optional[Dict]]:
        """
        批次精確查詢門牌座標

        Args:
            addresses: 正規化地址列表

        Returns:
            {address: {'lat', 'lng'}} or {address: None}
        """
        if not self._available:
            return {}

        results = {}
        con = sqlite3.connect(self.db_path)

        for address in addresses:
            parsed = self._parse_address(address)
            if not parsed or not parsed.get('street') or not parsed.get('housenumber'):
                continue

            street = parsed['street']
            num = parsed['housenumber']
            district = parsed.get('district', '')

            # 先含 district 查詢
            if district:
                row = con.execute(
                    "SELECT lat, lng FROM osm_addresses "
                    "WHERE district=? AND street=? AND housenumber=? LIMIT 1",
                    (district, street, num)
                ).fetchone()
            else:
                row = None

            # 無 district 的通用查詢
            if not row:
                row = con.execute(
                    "SELECT lat, lng FROM osm_addresses "
                    "WHERE street=? AND housenumber=? LIMIT 1",
                    (street, num)
                ).fetchone()

            if row:
                results[address] = {'lat': row[0], 'lng': row[1]}

        con.close()
        return results

    def _query(self, street: str, housenumber: str, district: str = '') -> Optional[Tuple]:
        con = sqlite3.connect(self.db_path)
        if district:
            row = con.execute(
                "SELECT lat, lng FROM osm_addresses "
                "WHERE district=? AND street=? AND housenumber=? LIMIT 1",
                (district, street, housenumber)
            ).fetchone()
            if row:
                con.close()
                return row
        row = con.execute(
            "SELECT lat, lng FROM osm_addresses "
            "WHERE street=? AND housenumber=? LIMIT 1",
            (street, housenumber)
        ).fetchone()
        con.close()
        return row

    @classmethod
    def _parse_address(cls, address: str) -> Optional[Dict]:
        """
        解析地址為 (district, street, housenumber) 三元組

        "臺北市大安區和平東路三段168號" → {district:"大安區", street:"和平東路三段", housenumber:"168"}
        """
        if not address:
            return None

        # 先做完整正規化（含舊制縣市轉換、全形轉半形、重複前綴去除）
        normalized = AddressNormalizer.normalize(address)
        addr = normalized if normalized else address.translate(cls._FW2HW)
        result = {}

        # 提取縣市（丟棄，支援 2-4 字縣市名）
        rest = re.sub(r'^[\u4e00-\u9fff]{2,4}[市縣]', '', addr)

        # 提取區鎮鄉市
        # 提取區/鎮/鄉/市（含縣轄市，如竹北市、彰化市）
        m_dist = re.match(r'^([\u4e00-\u9fff]{1,4}[區鎮鄉市])', rest)
        if m_dist:
            result['district'] = m_dist.group(1)
            rest = rest[m_dist.end():]

        # 提取路段（路/街/大道，含段）
        m_road = re.search(
            r'([\u4e00-\u9fff]+(?:路|街|大道)(?:[一二三四五六七八九十]+段)?)',
            rest
        )
        if not m_road:
            return None

        result['street'] = m_road.group(1)
        after_road = rest[m_road.end():]

        # 提取巷（如有）→ 加入 street
        m_lane = re.match(r'(\d+巷)', after_road)
        if m_lane:
            result['street'] += m_lane.group(1)
            after_road = after_road[m_lane.end():]

        # 提取弄（如有）→ 加入 street
        m_alley = re.match(r'(\d+弄)', after_road)
        if m_alley:
            result['street'] += m_alley.group(1)
            after_road = after_road[m_alley.end():]

        # 提取門牌號（X號、X之Y號、X-Y號，不含「號」）
        m_num = re.match(r'^(\d+(?:之\d+)?)', after_road)
        if m_num:
            num = m_num.group(1)
            # 統一 126-5 → 126之5（OSM 資料格式）
            num = re.sub(r'^(\d+)-(\d+)$', r'\1之\2', num)
            result['housenumber'] = num
        else:
            return None  # 沒有門牌號就無法做精確查詢

        return result

    @property
    def node_count(self) -> int:
        """資料庫中的節點數"""
        if not self._available:
            return 0
        con = sqlite3.connect(self.db_path)
        count = con.execute("SELECT COUNT(*) FROM osm_addresses").fetchone()[0]
        con.close()
        return count


# =====================================================================
# TaiwanGeocoder - 主要地理編碼引擎
# =====================================================================
class TaiwanGeocoder:
    """
    台灣地址地理編碼器

    特色:
    - 多層快取策略（精確→路段→區域）
    - 自動地址正規化
    - 多 Provider 備援
    - 高效批次處理

    用法:
        gc = TaiwanGeocoder(cache_dir="/path/to/cache")

        # 單一查詢
        result = gc.geocode("臺北市大安區和平東路三段1號")

        # 批次查詢
        addresses = [("大安區", "和平東路三段1號"), ...]
        results = gc.batch_geocode(addresses, strategy='road')
    """

    def __init__(self, cache_dir: str = None, provider: str = 'nominatim',
                 nominatim_url: str = None, concurrency: int = 1,
                 osm_index_db: str = None):
        """
        Args:
            cache_dir: 快取目錄（預設: 同目錄下的 cache/）
            provider: 主要 Provider ('nominatim' / 'nlsc')
            nominatim_url: 自訂 Nominatim URL（本地實例可大幅加速）
            concurrency: 並行數量（公開 Nominatim 請設 1）
            osm_index_db: OSM 門牌索引資料庫路徑（build_osm_index.py 建立）
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'db')
        os.makedirs(cache_dir, exist_ok=True)

        self.cache = GeoCache(os.path.join(cache_dir, 'geocode_cache.db'))
        self.normalizer = AddressNormalizer()
        self.concurrency = concurrency

        # OSM 本地門牌索引（精確查詢，最高優先）
        self.osm_index = OSMIndexProvider(osm_index_db)
        if self.osm_index.is_available():
            logger.info(f"OSM 門牌索引已載入：{self.osm_index.node_count:,} 個節點（精確門牌查詢已啟用）")
        else:
            logger.info("OSM 門牌索引未找到，使用 Nominatim 路段級查詢")
            logger.info("  → 執行 build_osm_index.py 建立索引可提升精度至門牌級")

        # 設定 Provider
        if provider == 'nominatim':
            delay = 0.1 if nominatim_url else 1.1  # 本地無限制
            self.primary_provider = NominatimProvider(
                base_url=nominatim_url, delay=delay
            )
        elif provider == 'nlsc':
            self.primary_provider = NLSCProvider()
        else:
            self.primary_provider = NominatimProvider()

        # 備援 Provider
        self.fallback_provider = NLSCProvider() if provider == 'nominatim' else NominatimProvider()

        # ArcGIS 最終備援（OSM + Nominatim/NLSC 都找不到時才呼叫）
        self.arcgis_provider = ArcGISProvider()

        # 路段快取: road_key → (lat, lng)
        self._road_cache: Dict[str, Tuple[float, float]] = {}

        logger.info(f"TaiwanGeocoder initialized | cache: {self.cache.size} entries | provider: {provider}")

    def geocode(self, address: str, district: str = '') -> Optional[Dict]:
        """
        地理編碼單一地址

        Args:
            address: 地址字串
            district: 區域名（輔助補全縣市）

        Returns:
            {
                'lat': float,
                'lng': float,
                'level': 'exact'|'road'|'district',
                'source': str,
                'normalized': str
            }
            or None
        """
        # Step 1: 正規化
        full_addr = self.normalizer.build_full_address(address, district)
        if not full_addr:
            return None

        base_addr = self.normalizer.extract_base_address(full_addr)
        if not base_addr:
            base_addr = full_addr

        # Step 2: OSM 本地索引（精確門牌，最快）
        if self.osm_index.is_available():
            osm_result = self.osm_index.geocode(base_addr)
            if osm_result:
                self.cache.put(base_addr, osm_result['lat'], osm_result['lng'],
                              'exact', 'osm_index', address)
                return {
                    'lat': osm_result['lat'],
                    'lng': osm_result['lng'],
                    'level': 'exact',
                    'source': 'osm_index',
                    'normalized': base_addr
                }

        # Step 3: 查快取（精確）
        cached = self.cache.get(base_addr)
        if cached:
            cached['normalized'] = base_addr
            return cached

        # Step 4: API 查詢（精確地址）
        result = self._api_geocode(base_addr)
        if result:
            self.cache.put(base_addr, result['lat'], result['lng'],
                          'exact', result.get('source', 'api'), address)
            result['level'] = 'exact'
            result['normalized'] = base_addr
            return result

        # Step 5: 路段級查詢
        road = self.normalizer.extract_road(full_addr)
        if road:
            road_key = f"{district}{road}" if district else road
            cached_road = self.cache.get(road_key)
            if cached_road and cached_road['level'] == 'road':
                cached_road['normalized'] = base_addr
                return cached_road

            # API 查詢路段
            road_addr = self.normalizer.build_full_address(road + "1號", district)
            if road_addr:
                result = self._api_geocode(road_addr)
                if result:
                    self.cache.put(road_key, result['lat'], result['lng'],
                                  'road', result.get('source', 'api'), road)
                    result['level'] = 'road'
                    result['normalized'] = base_addr
                    return result

        return None

    def _api_geocode(self, address: str) -> Optional[Dict]:
        """嘗試用 API 查詢，含備援"""
        result = self.primary_provider.geocode(address)
        if result:
            result['source'] = type(self.primary_provider).__name__.lower().replace('provider', '')
            return result

        result = self.fallback_provider.geocode(address)
        if result:
            result['source'] = type(self.fallback_provider).__name__.lower().replace('provider', '')
            return result

        # 最終備援：ArcGIS World Geocoder
        result = self.arcgis_provider.geocode(address)
        if result:
            # source 已由 ArcGISProvider.geocode() 設為 'arcgis'
            return result

        return None

    def batch_geocode(self, address_list: List[Tuple[str, str]],
                      strategy: str = 'smart',
                      progress: bool = True) -> Dict[str, Dict]:
        """
        批次地理編碼

        Args:
            address_list: [(district, address), ...] 的列表
            strategy: 'smart'（先路段後精確）/ 'exact'（每筆都精確查）/ 'road'（僅路段級）
            progress: 是否顯示進度

        Returns:
            {original_address: {lat, lng, level, source, normalized}, ...}
        """
        total = len(address_list)
        results = {}
        uncached = []
        cache_hits = 0

        # ── Step 1: 正規化 + 快取查詢 ──
        normalized_map = {}  # base_addr → (district, original_address)
        unique_bases = set()

        if progress:
            print(f"📋 正規化地址... ({total:,} 筆)")

        for district, address in address_list:
            full_addr = self.normalizer.build_full_address(address, district)
            if not full_addr:
                continue
            base_addr = self.normalizer.extract_base_address(full_addr)
            if not base_addr:
                base_addr = full_addr

            normalized_map[address] = (district, base_addr, full_addr)
            unique_bases.add(base_addr)

        if progress:
            print(f"   ✅ 不同基本地址: {len(unique_bases):,} / {total:,}")

        # 批次查詢快取
        unique_list = list(unique_bases)
        cached_results = self.cache.get_batch(unique_list)
        cache_hits = len(cached_results)

        if progress:
            print(f"   ✅ 快取命中: {cache_hits:,} / {len(unique_bases):,}")

        # 找出未快取的
        uncached_bases = [a for a in unique_list if a not in cached_results]

        # ── OSM 本地索引批次查詢（精確門牌，毫秒級）──
        osm_results = {}
        if self.osm_index.is_available() and uncached_bases:
            if progress:
                print(f"\n🏠 OSM 本地門牌索引查詢...")
            osm_results = self.osm_index.batch_geocode(uncached_bases)
            if osm_results:
                # 寫入 geocode_cache
                cache_records = [
                    (addr, r['lat'], r['lng'], 'exact', 'osm_index', addr)
                    for addr, r in osm_results.items()
                ]
                self.cache.put_batch(cache_records)
                # 從 uncached_bases 中移除已找到的
                uncached_bases = [a for a in uncached_bases if a not in osm_results]
            if progress:
                pct = len(osm_results) / max(len(unique_list), 1) * 100
                print(f"   ✅ OSM 精確命中: {len(osm_results):,} ({pct:.1f}%)")
                print(f"   剩餘需 API: {len(uncached_bases):,}")

        if strategy == 'smart' or strategy == 'road':
            # ── Step 2: 路段級批次處理 ──
            return self._batch_road_strategy(
                address_list, normalized_map, cached_results,
                osm_results, uncached_bases, strategy, progress
            )
        else:
            # ── exact 策略: 逐一查詢 ──
            return self._batch_exact_strategy(
                address_list, normalized_map, cached_results,
                osm_results, uncached_bases, progress
            )

    def _batch_road_strategy(self, address_list, normalized_map,
                              cached_results, osm_results,
                              uncached_bases, strategy, progress):
        """路段級批次策略：先對路段 geocode，再分配給所有地址"""

        # 收集需要處理的路段
        road_groups = defaultdict(list)  # road_key → [base_addr, ...]
        no_road = []  # 無法提取路段的地址

        for base_addr in uncached_bases:
            road = self.normalizer.extract_road(base_addr)
            if road:
                # 嘗試取得 district
                city_prefix = re.match(r'^(.*?[市縣].*?[區鎮鄉市])', base_addr)
                prefix = city_prefix.group(1) if city_prefix else ''
                road_key = f"{prefix}{road}"
                road_groups[road_key].append(base_addr)
            else:
                no_road.append(base_addr)

        unique_roads = list(road_groups.keys())

        if progress:
            print(f"\n🛣️  路段級處理:")
            print(f"   不同路段: {len(unique_roads):,}")
            print(f"   無路段地址: {len(no_road):,}")

        # 批次查詢路段快取
        road_cached = self.cache.get_batch(unique_roads)
        roads_to_geocode = [r for r in unique_roads if r not in road_cached]

        if progress:
            print(f"   路段快取命中: {len(unique_roads) - len(roads_to_geocode):,}")
            print(f"   需要 API 查詢: {len(roads_to_geocode):,}")

        # API 查詢路段
        if roads_to_geocode:
            road_results = self._batch_api_geocode_roads(
                roads_to_geocode, progress
            )

            # 儲存路段結果到快取
            cache_records = []
            for road_key, coords in road_results.items():
                cache_records.append((
                    road_key, coords['lat'], coords['lng'],
                    'road', coords.get('source', 'api'), road_key
                ))
            if cache_records:
                self.cache.put_batch(cache_records)

            road_cached.update(road_results)

        # ── 組合最終結果 ──
        results = {}

        for original_addr, (district, base_addr, full_addr) in normalized_map.items():
            # 1. 精確快取
            if base_addr in cached_results:
                r = cached_results[base_addr].copy()
                r['normalized'] = base_addr
                results[original_addr] = r
                continue

            # 2. OSM 精確索引結果
            if base_addr in osm_results:
                r = osm_results[base_addr].copy()
                r['level'] = 'exact'
                r['source'] = 'osm_index'
                r['normalized'] = base_addr
                results[original_addr] = r
                continue

            # 3. 路段快取
            road = self.normalizer.extract_road(base_addr)
            if road:
                city_prefix = re.match(r'^(.*?[市縣].*?[區鎮鄉市])', base_addr)
                prefix = city_prefix.group(1) if city_prefix else ''
                road_key = f"{prefix}{road}"
                if road_key in road_cached:
                    r = road_cached[road_key].copy()
                    r['normalized'] = base_addr
                    results[original_addr] = r

        if progress:
            exact_count = sum(1 for r in results.values() if r.get('level') == 'exact')
            road_count = sum(1 for r in results.values() if r.get('level') == 'road')
            print(f"\n📊 批次結果:")
            print(f"   成功: {len(results):,} / {len(address_list):,}")
            print(f"   精確門牌: {exact_count:,} | 路段級: {road_count:,}")
            print(f"   成功率: {len(results)/max(len(address_list),1)*100:.1f}%")

        return results

    def _batch_exact_strategy(self, address_list, normalized_map,
                               cached_results, osm_results,
                               uncached_bases, progress):
        """精確策略：逐一 API 查詢"""
        results = {}
        api_results = {}

        if uncached_bases and progress:
            print(f"\n🔍 精確查詢 {len(uncached_bases):,} 筆地址...")

        # 用 ThreadPoolExecutor 並行查詢
        if uncached_bases:
            api_results = self._batch_api_geocode(uncached_bases, progress)

            # 寫入快取
            cache_records = []
            for addr, coords in api_results.items():
                cache_records.append((
                    addr, coords['lat'], coords['lng'],
                    'exact', coords.get('source', 'api'), addr
                ))
            if cache_records:
                self.cache.put_batch(cache_records)

        # 組合結果
        all_results = {**cached_results, **osm_results, **api_results}

        for original_addr, (district, base_addr, full_addr) in normalized_map.items():
            if base_addr in all_results:
                r = all_results[base_addr].copy()
                r.setdefault('level', 'exact')
                r.setdefault('source', 'osm_index' if base_addr in osm_results else 'api')
                r['normalized'] = base_addr
                results[original_addr] = r

        if progress:
            print(f"\n📊 批次結果:")
            print(f"   成功: {len(results):,} / {len(address_list):,}")

        return results

    def _batch_api_geocode_roads(self, roads: List[str],
                                  progress: bool = True) -> Dict[str, Dict]:
        """批次 API 查詢路段座標"""
        results = {}
        total = len(roads)

        # 載入 tqdm（如果可用）
        pbar = None
        try:
            from tqdm import tqdm
            if progress:
                pbar = tqdm(total=total, desc="🌐 路段 API 查詢", unit="road")
        except ImportError:
            pass

        failed = 0
        for i, road_key in enumerate(roads):
            # 構造查詢地址：路段 + 1號
            query_addr = road_key
            if not query_addr.endswith('號'):
                query_addr = query_addr + "1號"

            result = self._api_geocode(query_addr)
            if result:
                result['level'] = 'road'
                results[road_key] = result
            else:
                failed += 1

            if pbar:
                pbar.update(1)
                pbar.set_postfix(ok=len(results), fail=failed)
            elif progress and (i+1) % 100 == 0:
                print(f"   進度: {i+1:,}/{total:,} | 成功: {len(results):,} | 失敗: {failed:,}")

        if pbar:
            pbar.close()

        if progress:
            print(f"   路段查詢完成: {len(results):,}/{total:,} 成功")

        return results

    def _batch_api_geocode(self, addresses: List[str],
                           progress: bool = True) -> Dict[str, Dict]:
        """批次 API 查詢地址座標"""
        results = {}
        total = len(addresses)

        pbar = None
        try:
            from tqdm import tqdm
            if progress:
                pbar = tqdm(total=total, desc="🌐 地址 API 查詢", unit="addr")
        except ImportError:
            pass

        failed = 0
        for i, addr in enumerate(addresses):
            result = self._api_geocode(addr)
            if result:
                result['level'] = 'exact'
                results[addr] = result
            else:
                failed += 1

            if pbar:
                pbar.update(1)
                pbar.set_postfix(ok=len(results), fail=failed)
            elif progress and (i+1) % 100 == 0:
                print(f"   進度: {i+1:,}/{total:,} | 成功: {len(results):,} | 失敗: {failed:,}")

        if pbar:
            pbar.close()

        return results

    def stats(self) -> Dict:
        """統計資訊"""
        return self.cache.stats()


# =====================================================================
# 便利函式
# =====================================================================

def quick_geocode(address: str, district: str = '') -> Optional[Tuple[float, float]]:
    """
    快速地理編碼（便利函式）

    Returns: (lat, lng) or None
    """
    gc = TaiwanGeocoder()
    result = gc.geocode(address, district)
    if result:
        return (result['lat'], result['lng'])
    return None


# =====================================================================
# CLI
# =====================================================================

def main():
    """命令列介面"""
    import argparse

    parser = argparse.ArgumentParser(
        description='台灣地址地理編碼器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s "臺北市大安區和平東路三段1號"
  %(prog)s "三民路29巷5號" --district 松山區
  %(prog)s --import-cache /path/to/geocode_cache.json
  %(prog)s --stats
        """
    )
    parser.add_argument('address', nargs='?', help='要查詢的地址')
    parser.add_argument('--district', '-d', default='', help='區域名稱（輔助補全）')
    parser.add_argument('--provider', choices=['nominatim', 'nlsc'],
                        default='nominatim', help='API provider (預設: nominatim)')
    parser.add_argument('--nominatim-url', help='自訂 Nominatim URL（本地實例）')
    parser.add_argument('--import-cache', metavar='JSON',
                        help='匯入 JSON 格式快取檔案')
    parser.add_argument('--stats', action='store_true', help='顯示快取統計')
    parser.add_argument('--json', '-j', action='store_true', help='JSON 輸出')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細輸出')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    gc = TaiwanGeocoder(
        provider=args.provider,
        nominatim_url=args.nominatim_url
    )

    # 匯入快取
    if args.import_cache:
        count = gc.cache.import_json_cache(args.import_cache)
        print(f"✅ 匯入 {count} 筆快取")
        return

    # 統計
    if args.stats:
        stats = gc.stats()
        print(f"📊 快取統計")
        print(f"   總數: {stats['total']:,}")
        print(f"   精度分布: {stats['by_level']}")
        print(f"   來源分布: {stats['by_source']}")
        return

    # 地址查詢
    if args.address:
        result = gc.geocode(args.address, args.district)
        if result:
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print(f"📍 {result['normalized']}")
                print(f"   緯度: {result['lat']:.7f}")
                print(f"   經度: {result['lng']:.7f}")
                print(f"   精度: {result['level']}")
                print(f"   來源: {result['source']}")
        else:
            print(f"❌ 無法解析地址: {args.address}")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
