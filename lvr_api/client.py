import urllib.request
import urllib.parse
import json
import logging

logger = logging.getLogger(__name__)

class LvrApiClient:
    BASE_URL = "https://lvr.land.moi.gov.tw"
    
    def __init__(self):
        self.session_id = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE_URL}/jsp/index.jsp"
        }
        
    def login(self):
        try:
            req = urllib.request.Request(f"{self.BASE_URL}/jsp/index.jsp", headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw_cookie = resp.headers.get("Set-Cookie", "")
                for part in raw_cookie.split(";"):
                    if "JSESSIONID" in part:
                        self.session_id = part.split("=", 1)[1].strip()
                        self.headers["Cookie"] = f"JSESSIONID={self.session_id}"
                        return True
            return False
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def search_communities_raw(self, town_code: str, keyword: str, params: dict = None) -> list:
        if not self.session_id:
            self.login()
        
        path = f"/SERVICE/QueryPrice/SaleBuild/{town_code}/{urllib.parse.quote(keyword)}"
        url = f"{self.BASE_URL}{path}"
        
        if params:
            query_string = urllib.parse.urlencode(params)
            url = f"{url}?{query_string}"
            
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("CHK") == "Y":
                    return data.get("LIST", [])
        except Exception as e:
            logger.warning(f"Error searching {town_code} {keyword}: {e}")
        return []

    def get_cities(self) -> list:
        url = f"{self.BASE_URL}/SERVICE/CITY"
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.error(f"get_cities failed: {e}")
            return []

    def get_towns(self, city_code: str) -> list:
        url = f"{self.BASE_URL}/SERVICE/CITY/{city_code.upper()}/"
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.error(f"get_towns failed ({city_code}): {e}")
            return []
