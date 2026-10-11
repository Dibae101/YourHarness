"""Sign-in checks and the limit on wrong guesses.

Browsers carry their device token in an HttpOnly, SameSite=Strict cookie, so
plain links (downloads) and worker fetches work without putting secrets in URLs.
Agents and the workspace CLI send it as ``Authorization: Bearer``.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from urllib.parse import urlsplit

COOKIE = "yh_session"


class Limiter:
    """Blocks an address after too many failed sign-ins in a window."""

    def __init__(self, max_fails: int = 10, window_s: float = 600.0):
        self.max_fails = max_fails
        self.window_s = window_s
        self.fails: dict[str, deque] = defaultdict(deque)

    def _prune(self, ip: str, now: float) -> deque:
        q = self.fails[ip]
        while q and now - q[0] > self.window_s:
            q.popleft()
        if not q:
            self.fails.pop(ip, None)
        return q

    def retry_after(self, ip: str) -> float:
        """Seconds until this address may try again, or 0 if it isn't blocked."""
        now = time.monotonic()
        q = self._prune(ip, now)
        if len(q) < self.max_fails:
            return 0.0
        return max(1.0, self.window_s - (now - q[0]))

    def fail(self, ip: str) -> None:
        self.fails[ip].append(time.monotonic())


def client_ip(request) -> str:
    # The coordinator usually talks to devices directly. Behind a proxy that terminates TLS
    # (tailscale serve, Caddy), set HARNESS_TRUST_PROXY=1 so each device keeps its own address
    # for the sign-in limit and for telling computers apart.
    # The proxy appends the address it saw, so only the LAST entry is trustworthy; earlier
    # ones are whatever the client chose to send.
    if os.environ.get("HARNESS_TRUST_PROXY") == "1":
        forwarded = request.headers.get("X-Forwarded-For", "").split(",")[-1].strip()
        if forwarded:
            return forwarded
    return request.remote or "unknown"


def token_from(request) -> tuple[str | None, str | None]:
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[7:].strip(), "bearer"
    cookie = request.cookies.get(COOKIE)
    if cookie:
        return cookie, "cookie"
    return None, None


def same_origin(request) -> bool:
    """False when a browser on another site sent this request."""
    origin = request.headers.get("Origin")
    if not origin:
        return True
    try:
        return urlsplit(origin).netloc.lower() == request.host.lower()
    except ValueError:
        return False
