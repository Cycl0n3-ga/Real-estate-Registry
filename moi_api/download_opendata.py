#!/usr/bin/env python3
"""
內政部實價登錄 Open Data 批次下載工具
======================================
資料來源：https://plvr.land.moi.gov.tw/DownloadOpenData

用法：
  python3 download_opendata.py                     # 顯示說明
  python3 download_opendata.py --city A --type A   # 台北市買賣
  python3 download_opendata.py --city all --type B # 全國預售屋
  python3 download_opendata.py --city A,B,F        # 北市+中市+新北(全類型)
  python3 download_opendata.py --all-zip           # 下載全國ZIP(最快)
  python3 download_opendata.py --schema            # 僅下載欄位定義
  python3 download_opendata.py --list              # 列出所有可用檔案
"""

import argparse
import csv
import io
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# ─────────────────────────────────────────────────────────
PLVR_URL = "https://plvr.land.moi.gov.tw"

CITY_CODES = {
    "A": "臺北市", "B": "臺中市", "C": "基隆市", "D": "臺南市",
    "E": "高雄市", "F": "新北市", "G": "宜蘭縣", "H": "桃園市",
    "I": "嘉義市", "J": "新竹縣", "K": "苗栗縣", "M": "南投縣",
    "N": "彰化縣", "O": "新竹市", "P": "雲林縣", "Q": "嘉義縣",
    "T": "屏東縣", "U": "花蓮縣", "V": "臺東縣", "W": "金門縣",
    "X": "澎湖縣", "Z": "連江縣",
}

TRADE_TYPES = {
    "A": "不動產買賣(成屋)",
    "B": "預售屋買賣  ★含建案名稱",
    "C": "不動產租賃",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Python/3",
    "Accept": "*/*",
    "Referer": "https://plvr.land.moi.gov.tw/DownloadOpenData",
}


# ─────────────────────────────────────────────────────────
# 核心下載函式
# ─────────────────────────────────────────────────────────
def _http_get_with_progress(url: str, timeout: int = 120) -> bytes:
    """下載並顯示進度"""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            total = resp.headers.get("Content-Length")
            total = int(total) if total else None
            chunks = []
            downloaded = 0
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
                    print(f"\r    [{bar}] {pct:.0f}% "
                          f"({downloaded:,}/{total:,} bytes)", end="", flush=True)
                else:
                    print(f"\r    已下載 {downloaded:,} bytes", end="", flush=True)
            print()
            return b"".join(chunks)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return b""   # 此組合無資料（例如小縣市無預售屋）
        raise


def download_file(city_code: str, trade_type: str,
                  dest_dir: str, dry_run: bool = False) -> str | None:
    """
    下載單一城市+類型的 CSV

    回傳儲存路徑，若無資料回傳 None
    """
    filename = f"{city_code.lower()}_lvr_land_{trade_type.lower()}.csv"
    url = f"{PLVR_URL}/Download?fileName={filename}"
    city_name = CITY_CODES.get(city_code.upper(), city_code)
    type_name = TRADE_TYPES.get(trade_type.upper(), trade_type)

    dest_path = os.path.join(dest_dir, filename)
    print(f"  [{city_code}{trade_type}] {city_name} {type_name}")

    if dry_run:
        print(f"       URL: {url}")
        return dest_path

    data = _http_get_with_progress(url)
    if not data:
        print(f"       ⚠ 無資料（此縣市可能無此類型交易）")
        return None

    os.makedirs(dest_dir, exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(data)

    # 計算筆數（跳過 2 行標頭）
    try:
        text = data.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        row_count = sum(1 for _ in reader) - 2  # 扣除中英文標頭
        print(f"       ✓ 儲存：{dest_path}（約 {max(0, row_count):,} 筆）")
    except Exception:
        print(f"       ✓ 儲存：{dest_path}（{len(data):,} bytes）")

    return dest_path


def download_national_zip(dest_dir: str, file_format: str = "csv") -> str:
    """下載全國 ZIP 壓縮檔（最快方式）"""
    filename = f"lvr_land{file_format}.zip"
    url = f"{PLVR_URL}/Download?type=zip&fileName={filename}"
    dest_path = os.path.join(dest_dir, filename)

    print(f"  下載全國 {file_format.upper()} ZIP：{url}")
    data = _http_get_with_progress(url, timeout=600)

    if not data:
        raise RuntimeError("下載失敗")

    os.makedirs(dest_dir, exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(data)

    print(f"  ✓ 儲存：{dest_path}（{len(data):,} bytes）")
    return dest_path


def download_schema(dest_dir: str) -> None:
    """下載欄位定義（schema）CSV"""
    schemas = ["schema-land.csv", "schema-main.csv", "schema-park.csv"]
    for schema in schemas:
        url = f"{PLVR_URL}/Download?fileName={schema}&q=aa&type=csv"
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read()
            path = os.path.join(dest_dir, schema)
            os.makedirs(dest_dir, exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            print(f"  ✓ {schema} → {path}")
        except Exception as e:
            print(f"  ✗ {schema}: {e}")


def check_available_files() -> list[dict]:
    """
    檢查哪些縣市/類型有資料

    透過嘗試 HEAD 或少量下載來確認
    回傳可用檔案清單
    """
    available = []
    print("檢查可用檔案（HEAD 請求）...")
    for city in CITY_CODES:
        for trade in TRADE_TYPES:
            filename = f"{city.lower()}_lvr_land_{trade.lower()}.csv"
            url = f"{PLVR_URL}/Download?fileName={filename}"
            req = urllib.request.Request(url, headers=HEADERS, method="HEAD")
            try:
                with urllib.request.urlopen(req, timeout=8) as resp:
                    size = resp.headers.get("Content-Length", "?")
                    available.append({
                        "city": city,
                        "type": trade,
                        "city_name": CITY_CODES[city],
                        "type_name": TRADE_TYPES[trade],
                        "filename": filename,
                        "size": size,
                    })
                    print(f"  ✓ {city}{trade} {CITY_CODES[city]} "
                          f"{TRADE_TYPES[trade]} ({size} bytes)")
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    print(f"  ? {city}{trade}: HTTP {e.code}")
            except Exception as e:
                print(f"  ✗ {city}{trade}: {e}")
            time.sleep(0.1)
    return available


# ─────────────────────────────────────────────────────────
# CSV 工具函式
# ─────────────────────────────────────────────────────────
def read_csv_info(path: str) -> dict:
    """讀取 CSV 基本資訊"""
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        try:
            headers_row = next(reader)
            en_row = next(reader)
            rows = list(reader)
            return {
                "path": path,
                "columns": headers_row,
                "column_count": len(headers_row),
                "row_count": len(rows),
                "sample": rows[:3] if rows else [],
            }
        except StopIteration:
            return {"path": path, "error": "空檔案或格式錯誤"}


def print_csv_info(path: str):
    info = read_csv_info(path)
    if "error" in info:
        print(f"  錯誤：{info['error']}")
        return
    print(f"  欄位數：{info['column_count']}")
    print(f"  筆數：  {info['row_count']:,}")
    print(f"  欄位：  {', '.join(info['columns'])}")
    if info["sample"]:
        print(f"  樣本：  {info['sample'][0][:4]}")


# ─────────────────────────────────────────────────────────
# 指令列解析
# ─────────────────────────────────────────────────────────
def parse_city_arg(city_arg: str) -> list[str]:
    """解析 --city 參數"""
    if city_arg.lower() == "all":
        return list(CITY_CODES.keys())
    return [c.strip().upper() for c in city_arg.split(",")]


def parse_type_arg(type_arg: str) -> list[str]:
    """解析 --type 參數"""
    if type_arg.lower() == "all":
        return list(TRADE_TYPES.keys())
    return [t.strip().upper() for t in type_arg.split(",")]


def main():
    parser = argparse.ArgumentParser(
        description="內政部實價登錄 Open Data 批次下載工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  %(prog)s --city A --type A              下載台北市買賣CSV
  %(prog)s --city A,B,F --type A,B        下載三縣市買賣+預售屋
  %(prog)s --city all --type B            全國預售屋（含建案名稱）
  %(prog)s --all-zip                      下載全國ZIP（最快，約500MB）
  %(prog)s --list                         列出可用檔案
  %(prog)s --schema --dest ./schema       下載欄位定義
  %(prog)s --city A --type A --info       下載後顯示CSV資訊
        """,
    )
    parser.add_argument("--city", default="A",
                        help="縣市代碼，如 A 或 A,B,F 或 all (預設: A)")
    parser.add_argument("--type", default="A",
                        help="交易類型：A=買賣, B=預售屋, C=租賃 或 all (預設: A)")
    parser.add_argument("--dest", default="./opendata",
                        help="儲存目錄 (預設: ./opendata)")
    parser.add_argument("--all-zip", action="store_true",
                        help="下載全國ZIP壓縮檔（最快）")
    parser.add_argument("--format", default="csv",
                        choices=["csv", "xls", "xml", "txt"],
                        help="ZIP格式 (配合 --all-zip)")
    parser.add_argument("--schema", action="store_true",
                        help="下載欄位定義CSV")
    parser.add_argument("--list", action="store_true",
                        help="列出所有可用檔案（不下載）")
    parser.add_argument("--dry-run", action="store_true",
                        help="僅顯示URL，不實際下載")
    parser.add_argument("--info", action="store_true",
                        help="下載後顯示CSV資訊")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="各檔案間隔秒數 (預設: 0.3)")

    args = parser.parse_args()

    print("=" * 60)
    print("內政部實價登錄 Open Data 下載工具")
    print(f"資料來源：{PLVR_URL}")
    print("=" * 60)

    dest = args.dest

    # 列出可用檔案
    if args.list:
        check_available_files()
        return

    # 下載欄位定義
    if args.schema:
        print(f"\n下載欄位定義 → {dest}")
        download_schema(dest)
        return

    # 下載全國 ZIP
    if args.all_zip:
        print(f"\n下載全國 {args.format.upper()} ZIP → {dest}")
        download_national_zip(dest, args.format)
        return

    # 下載指定縣市+類型
    cities = parse_city_arg(args.city)
    types = parse_type_arg(args.type)

    unknown_cities = [c for c in cities if c not in CITY_CODES]
    unknown_types = [t for t in types if t not in TRADE_TYPES]
    if unknown_cities:
        print(f"錯誤：未知的縣市代碼：{unknown_cities}")
        print(f"可用代碼：{list(CITY_CODES.keys())}")
        sys.exit(1)
    if unknown_types:
        print(f"錯誤：未知的交易類型：{unknown_types}")
        sys.exit(1)

    total = len(cities) * len(types)
    print(f"\n準備下載 {total} 個檔案 → {dest}/")
    print(f"  縣市：{', '.join(f'{c}({CITY_CODES[c]})' for c in cities)}")
    print(f"  類型：{', '.join(f'{t}({TRADE_TYPES[t]})' for t in types)}")
    print()

    downloaded_paths = []
    for city in cities:
        for trade_type in types:
            path = download_file(city, trade_type, dest, dry_run=args.dry_run)
            if path:
                downloaded_paths.append(path)
            if total > 1:
                time.sleep(args.delay)

    print(f"\n完成！下載了 {len(downloaded_paths)} 個檔案")

    if args.info and not args.dry_run:
        print("\n=== CSV 資訊 ===")
        for path in downloaded_paths:
            if os.path.exists(path):
                print(f"\n{os.path.basename(path)}:")
                print_csv_info(path)


if __name__ == "__main__":
    main()
