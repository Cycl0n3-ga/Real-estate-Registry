"""
內政部不動產交易實價查詢 API 客戶端
=====================================
逆向工程自 https://lvr.land.moi.gov.tw/

已實作功能：
  - 縣市 / 鄉鎮清單 (公開 API)
  - 社區/建案名稱搜尋 (SaleBuild，需 Session)
  - Open Data CSV 直連下載 (plvr.land.moi.gov.tw)
  - AES 加密主查詢 (模擬前端 CryptoJS 加密)

加密金鑰：window.location.host = "lvr.land.moi.gov.tw"
加密演算法：CryptoJS AES (OpenSSL EVP_BytesToKey 衍生 key+iv)
"""

from __future__ import annotations
import base64
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any

# ─────────────────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────────────────
BASE_URL   = "https://lvr.land.moi.gov.tw"
PLVR_URL   = "https://plvr.land.moi.gov.tw"
AES_PASSPHRASE = b"lvr.land.moi.gov.tw"  # 來自 window.location.host

# 縣市代碼對照表 (來自 /SERVICE/CITY API)
CITY_CODES = {
    "A": "臺北市", "B": "臺中市", "C": "基隆市", "D": "臺南市",
    "E": "高雄市", "F": "新北市", "G": "宜蘭縣", "H": "桃園市",
    "I": "嘉義市", "J": "新竹縣", "K": "苗栗縣", "M": "南投縣",
    "N": "彰化縣", "O": "新竹市", "P": "雲林縣", "Q": "嘉義縣",
    "T": "屏東縣", "U": "花蓮縣", "V": "臺東縣", "W": "金門縣",
    "X": "澎湖縣", "Z": "連江縣",
}

# 交易類別代碼
TRADE_TYPES = {
    "A": "不動產買賣(成屋)",
    "B": "預售屋買賣",
    "C": "不動產租賃",
}


# ─────────────────────────────────────────────────────────
# AES 加密 (模擬 CryptoJS AES)
# ─────────────────────────────────────────────────────────
def _evp_bytes_to_key(password: bytes, salt: bytes, key_len: int, iv_len: int
                      ) -> tuple[bytes, bytes]:
    """
    OpenSSL EVP_BytesToKey — CryptoJS 預設的 Key/IV 衍生方式。
    MD5 迭代直到取得足夠的位元組。
    """
    d, d_i = b"", b""
    while len(d) < key_len + iv_len:
        d_i = hashlib.md5(d_i + password + salt).digest()
        d += d_i
    return d[:key_len], d[key_len: key_len + iv_len]


def cryptojs_aes_encrypt(plaintext: str) -> str:
    """
    模擬 CryptoJS.AES.encrypt(plaintext, passphrase).toString()

    輸出格式：Base64 of "Salted__" + salt(8B) + AES-CBC ciphertext
    等同於 OpenSSL: echo -n "..." | openssl enc -aes-256-cbc -pbkdf1 -pass pass:...
    """
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives import padding
    except ImportError:
        raise ImportError(
            "需要安裝 cryptography：pip install cryptography"
        )

    salt = os.urandom(8)
    key, iv = _evp_bytes_to_key(AES_PASSPHRASE, salt, 32, 16)

    # PKCS7 Padding
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext.encode("utf-8")) + padder.finalize()

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    enc = cipher.encryptor()
    ciphertext = enc.update(padded) + enc.finalize()

    salted_blob = b"Salted__" + salt + ciphertext
    return base64.b64encode(salted_blob).decode("ascii")


def _md5_hash(text: str) -> str:
    """計算 URL path 中的 MD5 hash（模擬前端 f() 函式）"""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────────────
# HTTP 工具
# ─────────────────────────────────────────────────────────
def _make_headers(session_id: str | None = None,
                  referer: str | None = None) -> dict[str, str]:
    h = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    }
    if session_id:
        h["Cookie"] = f"JSESSIONID={session_id}"
    if referer:
        h["Referer"] = referer
    return h


def _get(url: str, headers: dict | None = None, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _post(url: str, data: bytes = b"",
          headers: dict | None = None, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, data=data,
                                  headers=headers or {}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


# ─────────────────────────────────────────────────────────
# 主客戶端
# ─────────────────────────────────────────────────────────
class MoiClient:
    """
    內政部不動產實價登錄 API 客戶端

    用法：
        client = MoiClient()
        client.login()                           # 取得 Session
        results = client.search_building("A02", "信義")
        cities  = client.get_cities()
        towns   = client.get_towns("A")          # 臺北市各區
        data    = client.query_price(            # 主查詢 (需加密)
            city="A", town="A02",
            ptype="1,2,3,4,5",
            starty="113", startm="1",
            endy="114",   endm="12",
        )
    """

    def __init__(self):
        self._session_id: str | None = None
        self._referer = f"{BASE_URL}/jsp/index.jsp"

    # ── Session ──────────────────────────────────────────
    def login(self) -> str:
        """訪問首頁取得 JSESSIONID（不需帳號密碼）"""
        try:
            req = urllib.request.Request(
                f"{BASE_URL}/jsp/index.jsp",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw_cookie = resp.headers.get("Set-Cookie", "")
                for part in raw_cookie.split(";"):
                    if "JSESSIONID" in part:
                        self._session_id = part.split("=", 1)[1].strip()
                        break
        except urllib.error.URLError as e:
            raise ConnectionError(f"無法連線到 {BASE_URL}: {e}")

        if not self._session_id:
            raise RuntimeError("取得 Session 失敗")
        return self._session_id

    def _ensure_session(self):
        if not self._session_id:
            self.login()

    # ── 縣市 / 鄉鎮 ──────────────────────────────────────
    def get_cities(self) -> list[dict]:
        """
        取得縣市清單（公開 API，不需 Session）

        回傳：[{"code": "A", "title": "臺北市", "use": True}, ...]
        """
        raw = _get(
            f"{BASE_URL}/SERVICE/CITY",
            headers={"User-Agent": "Mozilla/5.0",
                     "Accept": "application/json"},
        )
        return json.loads(raw.decode("utf-8"))

    def get_towns(self, city_code: str) -> list[dict]:
        """
        取得指定縣市的鄉鎮市區清單（公開 API）

        city_code: 縣市代碼，如 "A" = 臺北市
        回傳：[{"code": "A02", "title": "大安區", ...}, ...]
        """
        raw = _get(
            f"{BASE_URL}/SERVICE/CITY/{city_code.upper()}/",
            headers={"User-Agent": "Mozilla/5.0",
                     "Accept": "application/json"},
        )
        return json.loads(raw.decode("utf-8"))

    # ── 社區/建案名稱搜尋 ────────────────────────────────
    def search_building(self, town_code: str, keyword: str) -> list[str]:
        """
        搜尋社區/建案名稱（需 Session）

        town_code: 鄉鎮代碼，如 "A02" = 大安區
                   也可用縣市代碼 "A" 但結果較寬泛
        keyword:   搜尋關鍵字（至少 2 字）

        回傳：建案/社區名稱清單

        原理：對應 /SERVICE/QueryPrice/SaleBuild/{town}/{keyword}
        """
        self._ensure_session()
        url = (
            f"{BASE_URL}/SERVICE/QueryPrice/SaleBuild"
            f"/{town_code}/{urllib.parse.quote(keyword)}"
        )
        raw = _get(url, headers=_make_headers(
            self._session_id, self._referer
        ))
        data = json.loads(raw.decode("utf-8"))
        if data.get("CHK") == "Y":
            return [item["buildname"] for item in data.get("LIST", [])]
        return []

    def search_building_all_cities(self, keyword: str,
                                   delay: float = 0.3) -> dict[str, list[str]]:
        """
        在所有縣市搜尋社區/建案名稱

        回傳：{city_code: [buildname, ...], ...}
        """
        self._ensure_session()
        results: dict[str, list[str]] = {}
        for code in CITY_CODES:
            found = self.search_building(code, keyword)
            if found:
                results[code] = found
            time.sleep(delay)
        return results

    # ── 主查詢（AES 加密）───────────────────────────────
    def query_price(self,
                    city: str = "0",
                    town: str = "0",
                    ptype: str = "1,2,3,4,5",
                    starty: str = "113",
                    startm: str = "1",
                    endy: str = "114",
                    endm: str = "12",
                    sq: str = "",
                    **extra) -> Any:
        """
        主查詢 API（需 Session + AES 加密）

        city:   縣市代碼，如 "A" = 臺北市，"0" = 全國
        town:   鄉鎮代碼，如 "A02" = 大安區，"0" = 全部
        ptype:  交易標的類型，"1,2,3,4,5"（1=土地,2=建物,3=車位,4=農地,5=工業）
        starty: 開始年份（民國）
        startm: 開始月份
        endy:   結束年份（民國）
        endm:   結束月份
        sq:     社區/大樓名稱（從 search_building() 取得）

        回傳：後端 JSON 資料（list of transaction records）
        """
        self._ensure_session()

        params = {
            "ptype": ptype,
            "starty": starty,
            "startm": startm,
            "endy": endy,
            "endm": endm,
        }
        if city != "0":
            params["city"] = city
        if town != "0":
            params["town"] = town
        if sq:
            params["sq"] = sq
        params.update(extra)

        # 移除 'done' callback（前端用，後端不需要）
        params.pop("done", None)

        encrypted = cryptojs_aes_encrypt(json.dumps(params))
        # URL path hash = MD5 of original JSON params
        path_hash = _md5_hash(json.dumps(params))

        url = (
            f"{BASE_URL}/SERVICE/QueryPrice"
            f"/{path_hash}?q={urllib.parse.quote(encrypted)}"
        )
        try:
            raw = _get(url, headers=_make_headers(
                self._session_id, self._referer
            ))
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"查詢失敗 HTTP {e.code}: {url}\n"
                f"注意：加密格式可能有版本差異，建議改用 Open Data 下載"
            )

    def query_community_price(self,
                               sq: str,
                               city: str = "0",
                               unit: str = "坪") -> Any:
        """
        社區歷史成交查詢（需先用 search_building 取得社區名稱）

        sq:   社區/大樓名稱
        city: 縣市代碼
        unit: 坪 or 平方公尺

        對應：/SERVICE/QueryPrice/community/{hash}/{encrypted}
        """
        self._ensure_session()

        params = {"sq": sq, "unit": unit}
        if city != "0":
            params["city"] = city

        encrypted = cryptojs_aes_encrypt(json.dumps(params))
        path_hash = _md5_hash(json.dumps(params))

        url = (
            f"{BASE_URL}/SERVICE/QueryPrice/community"
            f"/{path_hash}/{urllib.parse.quote(encrypted)}"
        )
        try:
            raw = _get(url, headers=_make_headers(
                self._session_id, self._referer
            ))
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"查詢失敗 HTTP {e.code}: {url}")


# ─────────────────────────────────────────────────────────
# Open Data 直接下載（不需帳號）
# ─────────────────────────────────────────────────────────
def download_csv(city_code: str, trade_type: str,
                 dest_dir: str = ".",
                 verbose: bool = True) -> str:
    """
    從 plvr.land.moi.gov.tw 直接下載 Open Data CSV

    city_code:  縣市代碼，如 "A" = 臺北市
    trade_type: "A"=買賣, "B"=預售屋, "C"=租賃
    dest_dir:   儲存目錄
    回傳：      儲存的檔案路徑

    URL 格式：/Download?fileName={city_lower}_lvr_land_{type_lower}.csv
    """
    filename = f"{city_code.lower()}_lvr_land_{trade_type.lower()}.csv"
    url = f"{PLVR_URL}/Download?fileName={filename}"
    dest_path = os.path.join(dest_dir, filename)

    os.makedirs(dest_dir, exist_ok=True)

    if verbose:
        city_name = CITY_CODES.get(city_code.upper(), city_code)
        type_name = TRADE_TYPES.get(trade_type.upper(), trade_type)
        print(f"下載 {city_name} {type_name} → {dest_path}")

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python/3",
        "Accept": "*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            if verbose:
                print(f"  ⚠ 無資料 (HTTP 404)：此縣市可能無此類型交易")
            return None
        raise

    with open(dest_path, "wb") as f:
        f.write(data)

    if verbose:
        print(f"  完成：{len(data):,} bytes")

    return dest_path


def download_all_csv(dest_dir: str = ".", delay: float = 0.5) -> list[str]:
    """下載全國所有縣市所有類型的 CSV（可能需要數分鐘）"""
    paths = []
    for city in CITY_CODES:
        for trade_type in TRADE_TYPES:
            try:
                p = download_csv(city, trade_type, dest_dir)
                paths.append(p)
                time.sleep(delay)
            except Exception as e:
                print(f"  跳過 {city}_{trade_type}: {e}")
    return paths


def download_national_zip(dest_dir: str = ".",
                           file_format: str = "csv") -> str:
    """
    下載全國資料 ZIP 壓縮檔（最大約 500MB）

    file_format: csv / xls / xml / txt
    """
    filename = f"lvr_land{file_format}.zip"
    url = f"{PLVR_URL}/Download?type=zip&fileName={filename}"
    dest_path = os.path.join(dest_dir, filename)

    os.makedirs(dest_dir, exist_ok=True)
    print(f"下載全國 {file_format.upper()} ZIP → {dest_path}")

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=300) as resp:
        data = resp.read()

    with open(dest_path, "wb") as f:
        f.write(data)

    print(f"完成：{len(data):,} bytes")
    return dest_path


# ─────────────────────────────────────────────────────────
# 指令列介面
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    def demo():
        print("=" * 60)
        print("內政部不動產實價登錄 API Demo")
        print("=" * 60)

        client = MoiClient()

        # 1. 取得縣市清單（不需登入）
        print("\n[1] 縣市清單（公開API）")
        cities = client.get_cities()
        for c in cities[:5]:
            print(f"  {c['code']} = {c['title']}")
        print(f"  ... 共 {len(cities)} 個縣市")

        # 2. 取得台北市鄉鎮
        print("\n[2] 台北市各區")
        towns = client.get_towns("A")
        for t in towns[:5]:
            print(f"  {t['code']} = {t['title']}")

        # 3. 搜尋社區名稱（需 Session）
        print("\n[3] 搜尋社區名稱：台北市大安區 '信義'")
        session = client.login()
        print(f"  Session: {session[:20]}...")
        buildings = client.search_building("A02", "信義")
        print(f"  找到 {len(buildings)} 個建案：")
        for b in buildings[:5]:
            print(f"    - {b}")

        # 4. 下載一個小型 CSV 樣本
        print("\n[4] 下載台北市預售屋 CSV 樣本 (Type B 含建案名稱)")
        try:
            path = download_csv("A", "B", "/tmp/moi_test")
            import csv
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                headers_row = next(reader)
                next(reader)  # skip English row
                row = next(reader)
                print(f"  欄位：{', '.join(headers_row[:6])}...")
                print(f"  第一筆：{row[0]}, {row[1]}, {row[27] if len(row) > 27 else 'N/A'}")
        except Exception as e:
            print(f"  下載失敗：{e}")

        print("\n完成！")

    demo()
