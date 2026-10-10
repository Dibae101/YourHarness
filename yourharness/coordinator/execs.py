"""Where each command of the pool shell runs.

A command goes to the least-busy computer that can run it. Computers running Linux
(Ubuntu, or Ubuntu under WSL on Windows) are preferred, because the shell is a Linux
shell; Macs are used only when no Linux computer is connected, or when you ask for
one with `on`. Each computer runs one command at a time; others wait their turn.
"""

from __future__ import annotations

import base64
import logging
import secrets
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger("yourharness")
RECENT = 30


@dataclass
class Proc:
    id: str
    cmd: str
    cwd: str
    env: dict
    by: str
    target: str | None = None  # a computer's name, "linux" or "mac"; None lets the pool choose
    on_out: Callable[[bytes], None] | None = None
    on_done: Callable[["Proc"], None] | None = None
    on_start: Callable[["Proc"], None] | None = None
    status: str = "queued"
    runner_id: str | None = None
    runner_name: str | None = None
    runner_os: str | None = None
    queued_at: float = field(default_factory=time.time)
    started_at: float | None = None
    ended_at: float | None = None
    code: int | None = None
    error: str | None = None
    new_cwd: str | None = None
    sync: dict = field(default_factory=dict)

    def view(self) -> dict:
        return {
            "id": self.id, "cmd": self.cmd[:200], "by": self.by, "status": self.status, "runner": self.runner_name,
            "os": self.runner_os, "queuedAt": int(self.queued_at * 1000), "startedAt": int((self.started_at or 0) * 1000) or None,
            "endedAt": int((self.ended_at or 0) * 1000) or None, "code": self.code, "error": self.error, "cwd": self.cwd,
        }


class Execs:
    def __init__(self, sched):
        self.s = sched
        self.procs: dict[str, Proc] = {}
        self.queue: deque[Proc] = deque()
        self.recent: deque[Proc] = deque(maxlen=RECENT)

    # ------------------------------------------------------------------ choosing a computer
    def runners(self) -> list:
        return [d for d in self.s.devices.values() if d.kind == "agent" and d.exec and not d.closed and not d.stale and d.sharing]

    def busy_ids(self) -> set[str]:
        return {p.runner_id for p in self.procs.values() if p.status == "running" and p.runner_id}

    @staticmethod
    def _matches(d, target: str) -> bool:
        t = target.lower()
        if t == "linux":
            return bool(d.exec.get("linux"))
        if t in ("mac", "macos"):
            return "macos" in d.exec.get("os", "").lower()
        if t in ("windows", "wsl"):
            return "windows" in d.exec.get("os", "").lower()
        return t == d.name.lower() or t in d.name.lower()

    def eligible(self, target: str | None) -> list:
        runners = self.runners()
        if target:
            return [d for d in runners if self._matches(d, target)]
        linux = [d for d in runners if d.exec.get("linux")]
        return linux or runners

    @staticmethod
    def _score(d) -> tuple:
        cpu = (d.load or {}).get("cpu")
        return (cpu if cpu is not None else 0.0, -d.info.get("cores", 1), d.name)

    def describe_runners(self) -> list[dict]:
        busy = self.busy_ids()
        return [{"name": d.name, "os": d.exec.get("os"), "linux": bool(d.exec.get("linux")), "busy": d.id in busy, "cpu": (d.load or {}).get("cpu")}
                for d in sorted(self.runners(), key=self._score)]

    # ------------------------------------------------------------------ lifecycle
    def submit(self, cmd: str, cwd: str, env: dict, by: str, target: str | None = None, **callbacks) -> Proc:
        p = Proc(secrets.token_hex(6), cmd, cwd, env, by, target, **callbacks)
        self.procs[p.id] = p
        if not self.eligible(target):
            where = f" matching “{target}”" if target else ""
            self._finish(p, "failed", error=f"No computer{where} can run commands right now. Open the YourHarness app on a computer, or add a Linux computer.")
            return p
        self.queue.append(p)
        self.pump()
        self.changed()
        return p

    def pump(self) -> None:
        busy = self.busy_ids()
        for p in list(self.queue):
            candidates = self.eligible(p.target)
            if not candidates:
                self.queue.remove(p)
                self._finish(p, "failed", error="The computers that could run this command left.")
                continue
            free = sorted((d for d in candidates if d.id not in busy), key=self._score)
            if not free:
                continue
            d = free[0]
            busy.add(d.id)
            self.queue.remove(p)
            p.status, p.runner_id, p.runner_name, p.runner_os = "running", d.id, d.name, d.exec.get("os")
            p.started_at = time.time()
            d.send({"type": "exec.start", "id": p.id, "cmd": p.cmd, "cwd": p.cwd, "env": p.env})
            if p.on_start:
                p.on_start(p)
            self.changed()

    def cancel(self, proc_id: str) -> None:
        p = self.procs.get(proc_id)
        if p is None or p.status not in ("queued", "running"):
            return
        if p.status == "queued":
            self.queue.remove(p)
            self._finish(p, "cancelled", error="cancelled")
            return
        d = self.s.devices.get(p.runner_id or "")
        if d:
            d.send({"type": "exec.cancel", "id": p.id})
        p.error = "cancelled"

    def _finish(self, p: Proc, status: str, code: int | None = None, error: str | None = None) -> None:
        p.status = status
        p.ended_at = time.time()
        p.code = code if code is not None else p.code
        p.error = error or p.error
        self.procs.pop(p.id, None)
        self.recent.append(p)
        if p.on_done:
            try:
                p.on_done(p)
            except Exception:
                log.exception("a command's result handler failed")
        self.changed()
        self.pump()

    # ------------------------------------------------------------------ messages from computers
    def handle(self, d, msg: dict) -> None:
        p = self.procs.get(str(msg.get("id")))
        if p is None or p.runner_id != d.id:
            return
        if msg.get("type") == "exec.out":
            if p.on_out and isinstance(msg.get("data"), str):
                try:
                    p.on_out(base64.b64decode(msg["data"]))
                except ValueError:
                    pass
        elif msg.get("type") == "exec.done":
            p.new_cwd = msg.get("cwd") if isinstance(msg.get("cwd"), str) else p.cwd
            p.sync = msg.get("sync") if isinstance(msg.get("sync"), dict) else {}
            err = msg.get("error")
            code = msg.get("code") if isinstance(msg.get("code"), int) else None
            self._finish(p, "cancelled" if err == "cancelled" else "done" if not err else "failed", code=code, error=str(err)[:300] if err else None)

    def device_gone(self, d) -> None:
        for p in [x for x in self.procs.values() if x.runner_id == d.id and x.status == "running"]:
            self._finish(p, "failed", error=f"{d.name} disconnected while this was running")
        self.pump()

    def listing(self) -> list[dict]:
        live = [p.view() for p in self.procs.values()]
        done = [p.view() for p in reversed(self.recent)]
        return live + done

    def changed(self) -> None:
        self.s.broadcast({"type": "procs", "procs": self.listing()})
