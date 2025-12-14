"""
Persistent configuration manager.

Stores configuration as JSON and provides runtime updates. Defaults:
 - max_retries: 3
 - backoff_base: 2
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict


class ConfigManager:
    DEFAULTS = {"max_retries": 3, "backoff_base": 2}

    def __init__(self, path: str = None):
        if path:
            self.path = os.path.abspath(path)
        else:
            self.path = os.path.abspath(os.path.join(os.getcwd(), "config.json"))

        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.DEFAULTS, f)

    def _read(self) -> Dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return dict(self.DEFAULTS)

    def _write_atomic(self, data: Dict[str, Any]) -> None:
        dirpath = os.path.dirname(self.path) or "."
        fd, tmp_path = tempfile.mkstemp(dir=dirpath)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                json.dump(data, tmp, indent=2)
                tmp.flush()
                try:
                    os.fsync(tmp.fileno())
                except Exception:
                    pass
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def get_all(self) -> Dict[str, Any]:
        cfg = dict(self.DEFAULTS)
        cfg.update(self._read())
        return cfg

    def get(self, key: str, default: Any = None) -> Any:
        data = self.get_all()
        return data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        data = self.get_all()
        data[key] = value
        self._write_atomic(data)
