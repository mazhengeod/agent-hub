"""Configuration loading with mtime-based hot reload."""
from __future__ import annotations

import time
import yaml
from pathlib import Path
from typing import Optional, Any

CONFIG_DIR = Path.home() / ".config" / "agent-hub"
CONFIG_FILE = CONFIG_DIR / "config.yaml"

_config_cache: Optional[dict] = None
_config_mtime: float = 0.0


def load_config() -> dict:
    """Load config from YAML, cached with mtime check for hot reload."""
    global _config_cache, _config_mtime
    try:
        mtime = CONFIG_FILE.stat().st_mtime if CONFIG_FILE.exists() else 0.0
    except OSError:
        mtime = 0.0

    if _config_cache is not None and mtime == _config_mtime:
        return _config_cache

    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding="utf-8") as f:
            _config_cache = yaml.safe_load(f) or {}
    else:
        _config_cache = {}
    _config_mtime = mtime
    return _config_cache


def get_config(key: str, default: Any = None) -> Any:
    cfg = load_config()
    return cfg.get(key, default)
