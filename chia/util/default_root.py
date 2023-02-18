from __future__ import annotations

import os
from pathlib import Path
import sys
import json

DEFAULT_ROOT_PATH = Path(os.path.expanduser(os.getenv("CHIA_ROOT", "~/.chia/mainnet"))).resolve()

DEFAULT_KEYS_ROOT_PATH = Path(os.path.expanduser(os.getenv("CHIA_KEYS_ROOT", "~/.chia_keys"))).resolve()

bin_path = Path(sys.path[0])
print(f"获取到当前启动路径： {bin_path}")
config_file = bin_path/"config_root.json"
if config_file.exists():
    _json = json.loads(config_file.read_bytes())
    root = _json["DEFAULT_ROOT_PATH"]
    if root and len(root) > 1:
        DEFAULT_ROOT_PATH = Path(root).expanduser().resolve()
    keys_root = _json["DEFAULT_KEYS_ROOT_PATH"]
    if keys_root and len(keys_root) > 1:
        DEFAULT_KEYS_ROOT_PATH = Path(keys_root).expanduser().resolve()