import sys
sys.path.insert(0, '/home/cyclone/land/address_match')
from address_match import parse_query, CITY_PATTERN, normalize_query
import re

print("Starting tests")
addr = normalize_query('市民大道三段180號')
print('Normalized:', addr)

# 縣市
m = CITY_PATTERN.match(addr)
print('City:', m)

# 鄉鎮市區
m = re.match(r'^(.{1,4}?(?:區|鄉|鎮|市))(?=.)', addr)
print('District:', m)

# 里
m = re.match(r'^(.{1,5}?里)(?=[^\d]*(?:路|街|大道|\d))', addr)
print('Village:', m)

# 鄰
m = re.match(r'^(\d+鄰)', addr)
print('Neighbor:', m)

# 街路名 (含段)
print('Testing street before...')
m = re.match(r'^(.+?(?:路|街|大道))(\d+段)?', addr)
print('Street:', m)

