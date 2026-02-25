#!/usr/bin/env python3
"""
地址解析單元測試
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from convert import normalize_address_numbers, parse_address, chinese_numeral_to_int


def test_chinese_numeral():
    """測試中文數字轉換"""
    cases = [
        ('一', 1), ('二', 2), ('三', 3), ('十', 10),
        ('十一', 11), ('二十', 20), ('二十三', 23),
        ('一百', 100), ('一百二十三', 123),
    ]
    for text, expected in cases:
        result = chinese_numeral_to_int(text)
        assert result == expected, f'chinese_numeral_to_int("{text}") = {result}, expected {expected}'
    print('✅ 中文數字轉換 OK')


def test_normalize():
    """測試正規化"""
    cases = [
        ('５２號', '52號'),
        ('二樓', '2樓'),
        ('２９巷', '29巷'),
        ('三樓之３', '3樓之3'),
        ('十三樓', '13樓'),
        ('臺北市', '台北市'),
    ]
    for text, expected in cases:
        result = normalize_address_numbers(text)
        assert result == expected, f'normalize("{text}") = "{result}", expected "{expected}"'
    print('✅ 正規化 OK')


def test_parse_address():
    """測試地址解析"""
    cases = [
        {
            'raw': '新竹縣竹北市日興一街５２號二樓',
            'district_col': '竹北市',
            'expected': {
                'county_city': '新竹縣', 'district': '竹北市',
                'street': '日興一街', 'number': '52', 'floor': '2',
            }
        },
        {
            'raw': '臺北市松山區三民路２９巷１號三樓之３',
            'district_col': '松山區',
            'expected': {
                'county_city': '台北市', 'district': '松山區',
                'street': '三民路', 'lane': '29', 'number': '1',
                'floor': '3', 'sub_number': '3',
            }
        },
        {
            'raw': '新北市新店區三民路２９巷２弄３號五樓',
            'district_col': '新店區',
            'expected': {
                'county_city': '新北市', 'district': '新店區',
                'street': '三民路', 'lane': '29', 'alley': '2',
                'number': '3', 'floor': '5',
            }
        },
        {
            'raw': '台南市永康區王行里育樂街１４３巷１２之１號',
            'district_col': '永康區',
            'expected': {
                'county_city': '台南市', 'district': '永康區',
                'street': '育樂街', 'lane': '143', 'number': '12',
                'sub_number': '1',
            }
        },
        {
            'raw': '新北市中和區員山路４２３巷１４弄９號三樓',
            'district_col': '中和區',
            'expected': {
                'county_city': '新北市', 'district': '中和區',
                'street': '員山路', 'lane': '423', 'alley': '14',
                'number': '9', 'floor': '3',
            }
        },
        {
            'raw': '台北市中正區忠孝東路二段１３０號九樓之１',
            'district_col': '中正區',
            'expected': {
                'county_city': '台北市', 'district': '中正區',
                'street': '忠孝東路二段', 'number': '130',
                'floor': '9', 'sub_number': '1',
            }
        },
        {
            'raw': '平鎮段827地號',
            'district_col': '平鎮區',
            'expected': {
                'county_city': '', 'district': '', 'street': '',
            }
        },
        {
            'raw': '新竹縣竹北市竹北市十興里日興一街31巷1號',
            'district_col': '竹北市',
            'expected': {
                'county_city': '新竹縣', 'district': '竹北市',
                'street': '日興一街', 'lane': '31', 'number': '1',
            }
        },
        {
            'raw': '高雄市大寮區進學路167巷86號',
            'district_col': '大寮區',
            'expected': {
                'county_city': '高雄市', 'district': '大寮區',
                'street': '進學路', 'lane': '167', 'number': '86',
            }
        },
        {
            'raw': '新北市汐止區湖前街１１０巷９７弄６之５號１４樓',
            'district_col': '汐止區',
            'expected': {
                'county_city': '新北市', 'district': '汐止區',
                'street': '湖前街', 'lane': '110', 'alley': '97',
                'number': '6', 'sub_number': '5', 'floor': '14',
            }
        },
        # === 新增: 之 解析測試 ===
        {
            'raw': '臺北市大安區仁愛路三段５３之３號二十一樓',
            'district_col': '大安區',
            'expected': {
                'county_city': '台北市', 'district': '大安區',
                'street': '仁愛路三段', 'number': '53',
                'floor': '21', 'sub_number': '3',
            }
        },
        {
            'raw': '臺北市大安區仁愛路三段５３之８號十二樓',
            'district_col': '大安區',
            'expected': {
                'county_city': '台北市', 'district': '大安區',
                'street': '仁愛路三段', 'number': '53',
                'floor': '12', 'sub_number': '8',
            }
        },
        {
            'raw': '台北市內湖區民權東路六段150之3號9樓之1',
            'district_col': '內湖區',
            'expected': {
                'county_city': '台北市', 'district': '內湖區',
                'street': '民權東路六段', 'number': '150',
                'floor': '9', 'sub_number': '3',
            }
        },
        {
            'raw': '基隆市中正區新豐街486號之5      2樓',
            'district_col': '中正區',
            'expected': {
                'county_city': '基隆市', 'district': '中正區',
                'street': '新豐街', 'number': '486',
                'floor': '2', 'sub_number': '5',
            }
        },
    ]

    for i, case in enumerate(cases):
        result = parse_address(case['raw'], case['district_col'])
        for key, val in case['expected'].items():
            actual = result.get(key, '')
            assert actual == val, (
                f'Case {i} ("{case["raw"]}"): '
                f'{key} = "{actual}", expected "{val}"'
            )
    print(f'✅ 地址解析 OK ({len(cases)} 個測試)')


def test_ambiguous_districts():
    """測試歧義區名消歧"""
    from address_utils import parse_address as pa

    # 中山區: 無 hint → fallback 台北市 (最大量)
    r = pa('中山區松江路25巷5號2樓', '中山區')
    assert r['county_city'] == '台北市', f"中山區 no hint: {r['county_city']}"
    assert r['district'] == '中山區'
    assert r['street'] == '松江路'

    # 中山區: 有 city_hint=基隆市 → 基隆市
    r = pa('中山區中和路153號', '中山區', city_hint='基隆市')
    assert r['county_city'] == '基隆市', f"中山區 hint基隆: {r['county_city']}"

    # 中正區: hint=台北市 → 台北市
    r = pa('中正區忠孝東路一段10號', '中正區', city_hint='台北市')
    assert r['county_city'] == '台北市'

    # 中正區: hint=基隆市 → 基隆市
    r = pa('中正區新豐街486號', '中正區', city_hint='基隆市')
    assert r['county_city'] == '基隆市'

    # 東區: hint=新竹市
    r = pa('東區光復路一段89號', '東區', city_hint='新竹市')
    assert r['county_city'] == '新竹市'

    # 中西區: 不歧義 → 直接台南市
    r = pa('中西區民權路100號', '中西區')
    assert r['county_city'] == '台南市', f"中西區: {r['county_city']}"

    # 中區: 不歧義 → 台中市
    r = pa('中區三民路100號', '中區')
    assert r['county_city'] == '台中市', f"中區: {r['county_city']}"

    # 安平區: 不歧義 → 台南市
    r = pa('安平區安平路100號', '安平區')
    assert r['county_city'] == '台南市', f"安平區: {r['county_city']}"

    # 臺北市 (繁體臺) 應正規化為台北市
    r = pa('臺北市大安區忠孝東路100號', '大安區')
    assert r['county_city'] == '台北市', f"臺→台: {r['county_city']}"

    print('✅ 歧義區名消歧 OK')


if __name__ == '__main__':
    test_chinese_numeral()
    test_normalize()
    test_parse_address()
    test_ambiguous_districts()
    print('\n🎉 所有測試通過!')
