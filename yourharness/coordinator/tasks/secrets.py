"""Secrets the coordinator keeps for itself, such as the GitHub token.

They live in ``secrets.json`` in the coordinator's data folder, readable only by
its user (mode 0600). Nothing in here is sent to a device, written to a log,
returned by the API or put in a Git remote URL.
"""

from __future__ import annotations

import json
import os
from pathlib import Path


class SecretStore:
    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "secrets.json"

    def _load(self) -> dict:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _save(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    def get(self, key: str, default=None):
        return self._load().get(key, default)

    def set(self, key: str, value) -> None:
        data = self._load()
        data[key] = value
        self._save(data)

    def delete(self, *keys: str) -> None:
        data = self._load()
        for k in keys:
            data.pop(k, None)
        self._save(data)

    def redact(self, text: str) -> str:
        """Text with any stored secret replaced by ***, for error messages that came from elsewhere."""
        out = str(text)
        for k, v in self._load().items():
            if ("token" in k or "secret" in k) and isinstance(v, str) and len(v) >= 8:
                out = out.replace(v, "***")
        return out
