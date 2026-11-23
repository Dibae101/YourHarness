"""State that survives a restart: the join links and every device's sign-in.

Device tokens look like ``<device id>.<secret>``. Only a SHA-256 of the secret is
stored, so reading this file doesn't let anyone sign in as a device.

There are two join links, and which one a device used decides what it may do:

* the **owner link** (and, on a local network, the 4-digit code) gives a device
  everything: submitting jobs, opening shells, browsing other computers' files.
* the **lend link** gives a device nothing but the right to take part. It
  contributes its processor and computes pieces, and it cannot submit a job,
  open a shell, or read a file on anyone else's computer.

The lend link is what you give a friend over the internet. Lending resources
should never mean handing over the machine, and the owner link does hand over
the machine, because any device holding it can run commands on every computer
in the pool.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import time
from pathlib import Path

# What a device is allowed to do. A device's set is fixed when it joins.
CAP_SUBMIT = "submit"  # create and cancel runs
CAP_SHELL = "shell"  # open the YourHarness OS shell, or a direct shell on a computer
CAP_FILES = "files"  # browse and change files on other computers, in /pool and in the shared folder
CAP_ADMIN = "admin"  # remove other devices from the pool

OWNER_CAPS: tuple[str, ...] = (CAP_SUBMIT, CAP_SHELL, CAP_FILES, CAP_ADMIN)
LENDER_CAPS: tuple[str, ...] = ()  # takes part in runs, controls nothing

CAP_REFUSALS = {
    CAP_SUBMIT: "This device joined with a lend link, so it can take part in runs but can't start one.",
    CAP_SHELL: "This device joined with a lend link, so it can't open a terminal on the pool's computers.",
    CAP_FILES: "This device joined with a lend link, so it can't read or change files on the pool's computers.",
    CAP_ADMIN: "This device joined with a lend link, so it can't change the pool's devices.",
}


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


# Friend invite codes: 16 characters (80 bits) from an alphabet without look-alikes
# (no 0/O, 1/I), shown as ABCD-EFGH-JKLM-NPQR. Each is good for one computer and
# expires, and joining with one gives the same powers as the lend link: none.
INVITE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
INVITE_RE = re.compile(f"[{INVITE_ALPHABET}]{{16}}")
AUDIT_KEEP = 2000  # entries in the audit trail (joins, removals, friend codes, replaced links)


def normalize_invite(text) -> str | None:
    if not isinstance(text, str):
        return None
    s = re.sub(r"[\s-]", "", text).upper()
    return s if INVITE_RE.fullmatch(s) else None


def caps_of(rec: dict | None) -> tuple[str, ...]:
    """A device's capabilities. Sign-ins saved before lending existed get everything,
    which is what they already had, so an upgrade never silently takes powers away."""
    if not rec:
        return ()
    saved = rec.get("caps")
    if saved is None:
        return OWNER_CAPS
    return tuple(c for c in OWNER_CAPS if c in set(saved))


class Store:
    def __init__(self, data_dir: Path):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "coordinator.json"
        self.data: dict = {"join_token": None, "lend_token": None, "join_code": None, "devices": {}}
        self.code_locked = False  # too many wrong codes; set by the coordinator, cleared by a new code or a restart
        if self.path.exists():
            self.data.update(json.loads(self.path.read_text(encoding="utf-8")))
        if not self.data.get("join_token") or not self.data.get("join_code") or not self.data.get("lend_token"):
            self.data["join_token"] = self.data.get("join_token") or secrets.token_urlsafe(18)
            self.data["lend_token"] = self.data.get("lend_token") or secrets.token_urlsafe(18)
            self.data["join_code"] = self.data.get("join_code") or self._new_code()
            self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    # --- join links
    @property
    def join_token(self) -> str:
        return self.data["join_token"]

    @property
    def lend_token(self) -> str:
        """The link that lets another person's computer contribute without gaining any control."""
        return self.data["lend_token"]

    @property
    def join_code(self) -> str:
        return self.data["join_code"]

    @staticmethod
    def _new_code() -> str:
        return f"{secrets.randbelow(10000):04d}"

    def rotate_join_token(self) -> str:
        self.data["join_token"] = secrets.token_urlsafe(18)
        self.data["join_code"] = self._new_code()
        self.audit("join link replaced")  # saves
        return self.join_token

    def rotate_lend_token(self) -> str:
        self.data["lend_token"] = secrets.token_urlsafe(18)
        self.audit("lend link replaced")  # saves
        return self.lend_token

    def rotate_join_code(self) -> str:
        self.data["join_code"] = self._new_code()
        self.save()
        return self.join_code

    def caps_for_join(self, token, *, allow_code: bool = True) -> tuple[str, ...] | None:
        """What a device joining with this token may do, or None if the token is wrong.

        ``allow_code=False`` turns off the 4-digit code. A code has only 10,000
        possibilities, which is fine on a network you own and far too few once the
        coordinator can be reached from the internet, so online pools accept links only.
        """
        if not isinstance(token, str):
            return None
        value = token.strip().encode()
        # every branch is compared, so a wrong token takes the same time whichever it missed
        owner = secrets.compare_digest(value, self.join_token.encode())
        lender = secrets.compare_digest(value, self.lend_token.encode())
        code = secrets.compare_digest(value, self.join_code.encode()) and allow_code and not self.code_locked
        if owner or code:
            return OWNER_CAPS
        if lender or self.invite_for(token) is not None:
            return LENDER_CAPS
        return None

    # --- friend invites
    def create_invite(self, label: str, hours: float = 24, uses: int = 1) -> tuple[dict, str]:
        """A one-time code for a friend's computer. Only its hash is kept, so the code is shown once."""
        code = "".join(secrets.choice(INVITE_ALPHABET) for _ in range(16))
        now = time.time()
        rec = {"id": secrets.token_hex(4), "label": label, "created": now, "expires": now + hours * 3600, "uses": uses,
               "hash": _digest(code)}
        self.data.setdefault("invites", []).append(rec)
        self.audit("friend code made", invite=rec["id"], label=label, hours=hours, uses=uses)  # saves
        return self._invite_view(rec), "-".join(code[i:i + 4] for i in range(0, 16, 4))

    def invite_for(self, token) -> dict | None:
        norm = normalize_invite(token)
        if norm is None:
            return None
        h, now = _digest(norm), time.time()
        for rec in self.data.get("invites", []):
            if secrets.compare_digest(rec["hash"], h) and rec["uses"] > 0 and rec["expires"] > now:
                return rec
        return None

    def use_invite(self, token) -> None:
        rec = self.invite_for(token)
        if rec is not None:
            rec["uses"] -= 1
            rec["used"] = time.time()
            self.audit("friend code used", invite=rec["id"], label=rec.get("label"))  # saves

    def invites(self) -> list[dict]:
        """Invites still usable, and ones used or expired in the last day, newest first."""
        now = time.time()
        keep = [r for r in self.data.get("invites", []) if (r["uses"] > 0 and r["expires"] > now) or now - max(r["expires"], r.get("used", 0)) < 86400]
        if len(keep) != len(self.data.get("invites", [])):
            self.data["invites"] = keep
            self.save()
        return [self._invite_view(r) for r in sorted(keep, key=lambda r: -r["created"])]

    def revoke_invite(self, invite_id: str) -> bool:
        before = len(self.data.get("invites", []))
        self.data["invites"] = [r for r in self.data.get("invites", []) if r["id"] != invite_id]
        if len(self.data["invites"]) == before:
            return False
        self.audit("friend code revoked", invite=invite_id)  # saves
        return True

    @staticmethod
    def _invite_view(rec: dict) -> dict:
        now = time.time()
        state = "used" if rec["uses"] <= 0 else "expired" if rec["expires"] <= now else "open"
        return {"id": rec["id"], "label": rec["label"], "created": rec["created"], "expires": rec["expires"], "state": state}

    def join_ok(self, token, *, allow_code: bool = True) -> bool:
        """True for any current join link, or the 4-digit code when it is allowed."""
        return self.caps_for_join(token, allow_code=allow_code) is not None

    # --- devices
    def add_device(self, kind: str, name: str, caps: tuple[str, ...] = OWNER_CAPS) -> tuple[dict, str]:
        device_id = secrets.token_hex(6)
        secret = secrets.token_urlsafe(32)
        rec = {"id": device_id, "kind": kind, "name": name, "secret": _digest(secret), "created": time.time(),
               "color": None, "caps": list(caps)}
        self.data["devices"][device_id] = rec
        self.audit("joined", device=device_id, name=name, kind=kind, friend=tuple(caps) != tuple(OWNER_CAPS))  # saves
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

    def remove_device(self, device_id: str, by: str | None = None) -> bool:
        rec = self.data["devices"].pop(device_id, None)
        if rec is None:
            return False
        self.audit("removed", device=device_id, name=rec.get("name"), by=by)  # saves
        return True

    # --- audit: devices joining and being removed, friend codes made, used and revoked, links replaced
    def audit(self, action: str, **fields) -> None:
        trail = self.data.setdefault("audit", [])
        trail.append({"at": round(time.time(), 3), "action": action, **{k: v for k, v in fields.items() if v is not None}})
        del trail[:-AUDIT_KEEP]
        self.save()

    def audit_log(self, device_id: str | None = None, limit: int = 200) -> list[dict]:
        """Newest first; only one device's entries when device_id is given."""
        rows = [r for r in self.data.get("audit", []) if device_id is None or r.get("device") == device_id]
        return rows[::-1][:limit]
