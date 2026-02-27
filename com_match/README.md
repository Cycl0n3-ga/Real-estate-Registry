# com_match — 建案名稱模糊搜尋引擎

建案名稱模糊搜尋模組，從 `land_data.db` 中搜尋最可能匹配的建案。

## 功能

- 建案名稱模糊搜尋（精確 / 包含 / 子序列 / 編輯距離 / 字元相似）
- 回傳匹配建案的交易統計（筆數、均價等）
- 支援 CLI 互動模式與模組呼叫

## 使用方式

### 模組呼叫

```python
from com_match import CommunityMatcher

matcher = CommunityMatcher()
results = matcher.search("遠雄幸福", top_n=10)
for r in results:
    print(f"{r['name']} [{r['match_type']}] {r['tx_count']}筆")
```

### 快速呼叫

```python
from com_match import fuzzy_search
results = fuzzy_search("遠雄")
```

### CLI

```bash
python3 -m com_match.com_match "遠雄"        # JSON 輸出
python3 -m com_match.com_match               # 互動模式
```

## 匹配策略

| 優先序 | 類型   | 說明                          | 分數範圍 |
|--------|--------|-------------------------------|----------|
| 1      | 精確   | 完全相同                      | 1000+    |
| 2      | 包含   | query 是 name 的子字串        | 400-700  |
| 3      | 子序列 | query 字元依序出現在 name 中  | 200-400  |
| 4      | 模糊   | 編輯距離 ≤ len/3              | 60-100   |
| 5      | 相似   | 共同字元比率 ≥ 60%            | 50-130   |
