import base64
import hashlib
import http.client
import json
import logging
import os
import ssl
import time
import urllib.parse

logger = logging.getLogger(__name__)

# 台灣建案名稱常見字元掃描集（約 200 個字，涵蓋絕大多數建案名稱）
SWEEP_CHARS = list(
    # 數字
    '一二三四五六七八九十百千萬'
    # 大小/方位
    '大中小上下內外東西南北'
    # 自然
    '天地山水海湖河川林森竹松梅蘭菊'
    # 日月光
    '日月星光明輝煌亮晴昇'
    # 四季/景
    '春夏秋冬景色韻'
    # 形容詞/質感
    '美好富贏金玉珠翠璞碧悅雅麗靚華彩'
    # 吉祥/福氣
    '福祿壽喜慶吉安康樂平順昌盛'
    # 德/信
    '仁義禮智信德誠勤永遠久長'
    # 建案常用字
    '城市都廈邸院苑園庭府第宅居所'
    # 高度/頂
    '高峰峻頂巔崇'
    # 台灣地名常用字
    '台臺灣北中南高基宜花東屏'
    # 敬稱/豪華
    '國華帝皇御豪奢尊品格致'
    # 建設業常用字
    '建設興創業家合和開發'
    # 電梯/複合字
    '邦宏偉'
    # 知名建商相關字
    '遠雄富泰信義勤美三輝國真'
    # 其他常見字
    '新潮銀白綠紫藍紅'
)
# 去除重複並轉為 list
SWEEP_CHARS = list(dict.fromkeys(SWEEP_CHARS))


def _make_ssl_ctx() -> ssl.SSLContext:
    """建立允許舊 TLS cipher 的 SSL Context（台灣政府網站需要）"""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers("DEFAULT@SECLEVEL=1")
    return ctx


# ---------------------------------------------------------------------------
# CryptoJS AES 加密（複製瀏覽器端的 getEncodeStr 邏輯）
# ---------------------------------------------------------------------------

def _evp_bytes_to_key(password: bytes, salt: bytes,
                      key_len: int = 32, iv_len: int = 16) -> tuple:
    """OpenSSL EVP_BytesToKey（MD5）- 對應 CryptoJS 預設 key derivation"""
    d = b""
    d_i = b""
    while len(d) < key_len + iv_len:
        d_i = hashlib.md5(d_i + password + salt).digest()
        d += d_i
    return d[:key_len], d[key_len:key_len + iv_len]


def _pkcs7_pad(data: bytes, block: int = 16) -> bytes:
    n = block - (len(data) % block)
    return data + bytes([n] * n)


def _cryptojs_aes_encrypt(plaintext: str, passphrase: str) -> str:
    """
    CryptoJS.AES.encrypt(plaintext, passphrase).toString()
    輸出格式：base64("Salted__" + salt[8] + ciphertext)
    """
    from Crypto.Cipher import AES as _AES
    salt = os.urandom(8)
    key, iv = _evp_bytes_to_key(passphrase.encode("utf-8"), salt)
    cipher = _AES.new(key, _AES.MODE_CBC, iv)
    ciphertext = cipher.encrypt(_pkcs7_pad(plaintext.encode("utf-8")))
    return base64.b64encode(b"Salted__" + salt + ciphertext).decode("utf-8")


def get_encode_str(params: dict) -> str:
    """
    複製 window.common.getEncodeStr(params)：
      1. JSON.stringify(params)
      2. CryptoJS AES 加密（passphrase = 'lvr.land.moi.gov.tw'）
      3. btoa()（再次 base64）
    """
    json_str = json.dumps(params, separators=(",", ":"))
    cryptojs_b64 = _cryptojs_aes_encrypt(json_str, "lvr.land.moi.gov.tw")
    return base64.b64encode(cryptojs_b64.encode("utf-8")).decode("utf-8")


def get_path_hash(params: dict) -> str:
    """
    複製 getMd5(JSON.stringify(params))：URL 路徑用的 MD5 hex
    """
    json_str = json.dumps(params, separators=(",", ":"))
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


class LvrApiClient:
    HOST = "lvr.land.moi.gov.tw"
    BASE_URL = f"https://{HOST}"
    TIMEOUT = 30

    def __init__(self):
        self.session_id = None
        self._ssl_ctx = _make_ssl_ctx()
        self._base_headers = {
            "Host": self.HOST,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE_URL}/jsp/list.jsp",
        }

    def _get_headers(self) -> dict:
        h = dict(self._base_headers)
        if self.session_id:
            h["Cookie"] = f"JSESSIONID={self.session_id}"
        return h

    def _conn(self) -> http.client.HTTPSConnection:
        return http.client.HTTPSConnection(self.HOST, context=self._ssl_ctx, timeout=self.TIMEOUT)

    def _get(self, path: str) -> bytes | None:
        """執行 GET 請求，自動處理 401 重新登入"""
        for attempt in range(2):
            try:
                conn = self._conn()
                conn.request("GET", path, headers=self._get_headers())
                resp = conn.getresponse()
                if resp.status == 401 and attempt == 0:
                    logger.warning("Session 過期，重新登入...")
                    self.session_id = None
                    self.login()
                    continue
                if resp.status not in (200, 201):
                    logger.debug(f"HTTP {resp.status} for {path}")
                    return None
                return resp.read()
            except Exception as e:
                logger.warning(f"_get({path}) failed: {e}")
                return None
        return None

    def login(self) -> bool:
        """
        完整登入流程：
          1. GET /  → 取得 JSESSIONID
          2. GET /jsp/list.jsp → 初始化 server-side session state
        """
        try:
            # Step 1: 取 JSESSIONID
            conn = self._conn()
            conn.request("GET", "/", headers=self._base_headers)
            resp = conn.getresponse()
            resp.read()
            for part in (resp.headers.get("Set-Cookie", "") or "").split(";"):
                if "JSESSIONID" in part:
                    self.session_id = part.split("=", 1)[1].strip()
            if not self.session_id:
                logger.error("Login failed: no JSESSIONID from /")
                return False

            # Step 2: 訪問 list.jsp 初始化 session（必須，否則 QueryPrice 會 500）
            conn2 = self._conn()
            conn2.request("GET", "/jsp/list.jsp", headers=self._get_headers())
            resp2 = conn2.getresponse()
            resp2.read()
            # 可能更新 JSESSIONID
            for part in (resp2.headers.get("Set-Cookie", "") or "").split(";"):
                if "JSESSIONID" in part:
                    self.session_id = part.split("=", 1)[1].strip()

            logger.info(f"Login OK (session: {self.session_id[:16]}...)")
            return True
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def get_token(self) -> str:
        """
        呼叫 /jsp/setToken.jsp 取得查詢用 token。
        每次查詢前需重新取得（token 為一次性）。
        """
        if not self.session_id:
            if not self.login():
                return ""
        try:
            conn = self._conn()
            headers = self._get_headers()
            headers["Referer"] = f"{self.BASE_URL}/jsp/list.jsp"
            conn.request("GET", "/jsp/setToken.jsp", headers=headers)
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", "ignore").strip()
            data = json.loads(body)
            token = data.get("token", "")
            if token == "401":
                logger.warning("setToken returned 401, re-logging in...")
                self.session_id = None
                if self.login():
                    return self.get_token()
                return ""
            logger.debug(f"token: {token}")
            return token
        except Exception as e:
            logger.warning(f"get_token failed: {e}")
            return ""

    def search_communities_raw(self, town_code: str, keyword: str) -> list:
        """
        搜尋指定行政區中建案名稱「包含」keyword 的建案列表。
        API 為自動完成端點，需中文字元；英數字無效（CHK=N）。
        """
        if not self.session_id:
            self.login()
        encoded_kw = urllib.parse.quote(keyword, safe="")
        path = f"/SERVICE/QueryPrice/SaleBuild/{town_code}/{encoded_kw}"
        body = self._get(path)
        if body is None:
            return []
        try:
            data = json.loads(body.decode("utf-8"))
            if data.get("CHK") == "Y":
                return data.get("LIST", [])
        except Exception as e:
            logger.warning(f"JSON parse error ({path}): {e}")
        return []

    def get_cities(self) -> list:
        if not self.session_id:
            self.login()
        body = self._get("/SERVICE/CITY")
        if body is None:
            return []
        try:
            return json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"get_cities JSON error: {e}")
            return []

    def get_towns(self, city_code: str) -> list:
        if not self.session_id:
            self.login()
        path = f"/SERVICE/CITY/{city_code.upper()}/"
        body = self._get(path)
        if body is None:
            return []
        try:
            return json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"get_towns JSON error ({city_code}): {e}")
            return []

    def query_price(self, city: str, town: str,
                    starty: int, startm: int,
                    endy: int, endm: int,
                    ptype: str = "1,2",
                    community: str = "",
                    **extra) -> list:
        """
        查詢不動產買賣交易清單（主要 API）。

        對應瀏覽器的 loadQueryPrice2() AJAX 呼叫：
          GET /SERVICE/QueryPrice/{md5}?q={encrypted_q}

        正確流程：
          1. login() → JSESSIONID + list.jsp 初始化
          2. get_token() → 一次性 token
          3. 帶 token 加密成 q 參數送出

        參數：
          city       城市代碼，如 'C'（基隆市）
          town       鄉鎮市區代碼，如 'C04'（仁愛區）
          starty / startm  起始年（民國）/ 月
          endy   / endm    結束年（民國）/ 月
          ptype      交易類型，'1,2'=土地+建物（預設），可依需調整
          community  社區/建案名稱篩選（空字串=不限）
          **extra    其他 params 覆寫

        回傳：list，每筆為一個 dict（交易記錄）；錯誤時回傳 []。
        """
        if not self.session_id:
            if not self.login():
                return []

        token = self.get_token()
        if not token:
            logger.error("query_price: 無法取得 token")
            return []

        params = {
            "ptype": ptype,
            "starty": str(starty),
            "startm": str(startm),
            "endy": str(endy),
            "endm": str(endm),
            "qryType": "biz",
            "city": city.upper(),
            "town": town.upper(),
            "p_build": community,
            "ftype": "",
            "price_s": "", "price_e": "",
            "unit_price_s": "", "unit_price_e": "",
            "area_s": "", "area_e": "",
            "build_s": "", "build_e": "",
            "buildyear_s": "", "buildyear_e": "",
            "doorno": "", "pattern": "",
            "community": community,
            "floor": "",
            "rent_type": "", "rent_order": "",
            "urban": "", "urbantext": "", "nurban": "",
            "aa12": "", "p_purpose": "", "p_unusualcode": "",
            "tmoney_unit": "1", "pmoney_unit": "1",
            "unit": "2",
            "token": token,
        }
        params.update(extra)

        q = get_encode_str(params)
        path_hash = get_path_hash(params)
        path = f"/SERVICE/QueryPrice/{path_hash}?q={urllib.parse.quote(q, safe='')}"
        logger.debug(f"query_price path: /SERVICE/QueryPrice/{path_hash}?q=...")

        body = self._get(path)
        if body is None:
            return []
        try:
            data = json.loads(body.decode("utf-8"))
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # 有時回傳 {token:'401'} 表示 session 過期
                if data.get("token") == "401":
                    logger.warning("query_price: token 401，重新登入")
                    self.session_id = None
                    return []
                return data.get("LIST", [])
            return []
        except Exception as e:
            logger.warning(f"query_price JSON error: {e}")
            return []

    # --- Legacy aliases (保留向後相容) ---
    def query_sale_list(self, city, town, starty, startm, endy, endm, sq="0") -> list:
        """已棄用：請改用 query_price()"""
        return self.query_price(city, town, starty, startm, endy, endm)

    def query_sale_detail(self, city, town, starty, startm, endy, endm, sq="0") -> list:
        """已棄用：請改用 query_price()"""
        return self.query_price(city, town, starty, startm, endy, endm)
