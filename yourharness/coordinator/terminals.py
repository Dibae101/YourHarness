"""Relays workspace terminals between a browser and the agent that hosts the workspace.

The coordinator only forwards bytes. The shell itself runs on the agent's computer
(on the Windows PC, inside WSL2), and with tmux every device attaches to the same
session, so you can start something on the Mac and pick it up on the iPad.
"""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING

from .jobkinds import clamp_int

if TYPE_CHECKING:
    from .scheduler import Device, Scheduler

MAX_INPUT = 64 * 1024


class Terminals:
    def __init__(self, scheduler: "Scheduler"):
        self.s = scheduler
        self.sessions: dict[str, tuple[str, str]] = {}  # sid -> (browser device id, agent device id)

    def hosts(self) -> list["Device"]:
        hosts = [d for d in self.s.devices.values() if d.kind == "agent" and d.workspace and not d.closed]
        # Ubuntu (Linux, or WSL on Windows) is the workspace; other computers are the fallback
        hosts.sort(key=lambda d: 0 if any(w in d.workspace["system"] for w in ("Ubuntu", "Linux", "Debian", "Fedora")) else 1)
        return hosts

    def from_browser(self, browser: "Device", msg: dict) -> None:
        kind = msg.get("type")
        if kind == "term.open":
            self._open(browser, msg)
            return
        sid = msg.get("sid")
        session = self.sessions.get(sid) if isinstance(sid, str) else None
        if not session or session[0] != browser.id:
            return
        host = self.s.devices.get(session[1])
        if kind == "term.in":
            data = msg.get("data")
            if host and isinstance(data, str) and len(data) <= MAX_INPUT:
                host.send({"type": "term.in", "sid": sid, "data": data})
        elif kind == "term.resize":
            if host:
                host.send({"type": "term.resize", "sid": sid, "cols": clamp_int(msg.get("cols"), 2, 500, 80), "rows": clamp_int(msg.get("rows"), 2, 300, 24)})
        elif kind == "term.close":
            self.sessions.pop(sid, None)
            if host:
                host.send({"type": "term.close", "sid": sid})

    def _open(self, browser: "Device", msg: dict) -> None:
        hosts = self.hosts()
        wanted = msg.get("host")
        host = next((h for h in hosts if h.id == wanted), hosts[0] if hosts else None)
        ref = msg.get("ref") if isinstance(msg.get("ref"), str) else None
        if host is None:
            browser.send({"type": "term.error", "ref": ref, "message": "No workspace is running. Start an agent with --workspace on a computer, for example the Windows PC."})
            return
        sid = secrets.token_hex(8)
        self.sessions[sid] = (browser.id, host.id)
        host.send({
            "type": "term.open", "sid": sid, "user": browser.name,
            "cols": clamp_int(msg.get("cols"), 2, 500, 80), "rows": clamp_int(msg.get("rows"), 2, 300, 24),
        })
        browser.send({"type": "term.opened", "sid": sid, "ref": ref, "host": host.name, "hostId": host.id, "workspace": host.workspace,
                      "hostInfo": host.info.get("platform")})

    def from_agent(self, agent: "Device", msg: dict) -> None:
        sid = msg.get("sid")
        session = self.sessions.get(sid) if isinstance(sid, str) else None
        if not session or session[1] != agent.id:
            return
        browser = self.s.devices.get(session[0])
        kind = msg.get("type")
        if kind == "term.out" and isinstance(msg.get("data"), str):
            if browser:
                browser.send({"type": "term.out", "sid": sid, "data": msg["data"]})
        elif kind == "term.exit":
            self.sessions.pop(sid, None)
            if browser:
                browser.send({"type": "term.exit", "sid": sid, "code": msg.get("code"), "message": "The shell ended."})

    def device_gone(self, d: "Device") -> None:
        for sid, (browser_id, agent_id) in list(self.sessions.items()):
            if browser_id == d.id:
                self.sessions.pop(sid, None)
                host = self.s.devices.get(agent_id)
                if host:
                    host.send({"type": "term.close", "sid": sid})
            elif agent_id == d.id:
                self.sessions.pop(sid, None)
                browser = self.s.devices.get(browser_id)
                if browser:
                    browser.send({"type": "term.exit", "sid": sid, "code": None, "message": f"{d.name} disconnected, so its workspace closed."})
