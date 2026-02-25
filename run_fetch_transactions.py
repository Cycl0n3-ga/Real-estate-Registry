"""
執行 fetch_transactions，繞過資料夾名稱含空格的問題。
"""
import sys
import os
import importlib.util
import types

ROOT = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(ROOT, "fetch com from lvr")

# 手動將 "fetch com from lvr" 以 "fetch_com_from_lvr" 名稱載入為 package
PKG_NAME = "fetch_com_from_lvr"

def _load_module(name, filepath, pkg_name=None):
    spec = importlib.util.spec_from_file_location(
        name,
        filepath,
        submodule_search_locations=[PKG_DIR] if pkg_name is None else None,
    )
    mod = importlib.util.module_from_spec(spec)
    if pkg_name:
        mod.__package__ = pkg_name
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

# 1. 載入 package __init__
pkg = _load_module(PKG_NAME, os.path.join(PKG_DIR, "__init__.py"))
pkg.__path__ = [PKG_DIR]
pkg.__package__ = PKG_NAME

# 2. 載入 client
client_mod = _load_module(f"{PKG_NAME}.client", os.path.join(PKG_DIR, "client.py"), PKG_NAME)

# 3. 載入 fetch_transactions
ft_mod = _load_module(f"{PKG_NAME}.fetch_transactions", os.path.join(PKG_DIR, "fetch_transactions.py"), PKG_NAME)

# 4. 執行 CLI main
if __name__ == "__main__":
    ft_mod.download_all(
        starty=int(os.environ.get("STARTY", 101)),
        startm=int(os.environ.get("STARTM", 1)),
        endy=int(os.environ.get("ENDY", 115)),
        endm=int(os.environ.get("ENDM", 2)),
        db_path=os.environ.get("DB_PATH", os.path.join(ROOT, "db", "transactions_all_original.db")),
        delay=float(os.environ.get("DELAY", 0.5)),
        ptype=os.environ.get("PTYPE", "1,2,3,4,5"),
    )
