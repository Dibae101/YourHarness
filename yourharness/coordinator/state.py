"""State that survives a restart: the join link and every device's sign-in.

Device tokens look like ``<device id>.<secret>``. Only a SHA-256 of the secret is
stored, so reading this file doesn't let anyone sign in as a device.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from pathlib import Path


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


class Store:
    def __init__(self, data_dir: Path):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "coordinator.json"
        self.data: dict = {"join_token": None, "join_code": None, "devices": {}}
        if self.path.exists():
            self.data.update(json.loads(self.path.read_text(encoding="utf-8")))
        if not self.data.get("join_token") or not self.data.get("join_code"):
            self.data["join_token"] = self.data.get("join_token") or secrets.token_urlsafe(18)
            self.data["join_code"] = self.data.get("join_code") or self._new_code()
            self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    # --- join link
    @property
    def join_token(self) -> str:
        return self.data["join_token"]

    @property
    def join_code(self) -> str:
        return self.data["join_code"]

    @staticmethod
    def _new_code() -> str:
        return f"{secrets.randbelow(10000):04d}"

    def rotate_join_token(self) -> str:
        self.data["join_token"] = secrets.token_urlsafe(18)
        self.data["join_code"] = self._new_code()
        self.save()
        return self.join_token

    def rotate_join_code(self) -> str:
        self.data["join_code"] = self._new_code()
        self.save()
        return self.join_code

    def join_ok(self, token) -> bool:
        """True for the long link token or the current 4-digit code."""
        if not isinstance(token, str):
            return False
        value = token.strip().encode()
        return secrets.compare_digest(value, self.join_token.encode()) | secrets.compare_digest(value, self.join_code.encode())

    # --- devices
    def add_device(self, kind: str, name: str) -> tuple[dict, str]:
        device_id = secrets.token_hex(6)
        secret = secrets.token_urlsafe(32)
        rec = {"id": device_id, "kind": kind, "name": name, "secret": _digest(secret), "created": time.time(), "color": None}
        self.data["devices"][device_id] = rec
        self.save()
        return rec, f"{device_id}.{secret}"

    def device_for_token(self, token) -> dict | None:
        if not isinstance(token, str) or "." not in token:
            return None
        device_id, _, secret = token.partition(".")
        rec = self.data["devices"].get(device_id)
        if not rec or not secrets.compare_digest(rec["secret"], _digest(secret)):
            return None
        return rec

    def get(self, device_id: str) -> dict | None:
        return self.data["devices"].get(device_id)

    def update_device(self, device_id: str, **fields) -> None:
        rec = self.data["devices"].get(device_id)
        if rec is None:
            return
        changed = {k: v for k, v in fields.items() if rec.get(k) != v}
        if changed:
            rec.update(changed)
            self.save()

    def remove_device(self, device_id: str) -> bool:
        if self.data["devices"].pop(device_id, None) is None:
            return False
        self.save()
        return True
