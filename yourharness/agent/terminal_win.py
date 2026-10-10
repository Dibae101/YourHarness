"""Workspace terminals on Windows, through the ConPTY console API (pywinpty).

If Ubuntu (or another distribution) is installed under WSL, the terminal opens
it, so a Windows PC gives the pool a Linux shell with no extra setup. Otherwise
it opens PowerShell.
"""

from __future__ import annotations

import asyncio
import base64
import os
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Callable

from winpty import PtyProcess  # pywinpty; ImportError tells the agent to skip hosting a terminal

from .wsl import to_wsl_path, wsl_distro

PACKAGE_PARENT = Path(__file__).resolve().parent.parent.parent

# Sourced by bash inside WSL when a terminal opens: the user's own ~/.bashrc first, then
# the `harness` command and `from yourharness import pool`, both talking to the pool.
WSL_RC = """# YourHarness: makes the pool usable from this Ubuntu shell
[ -f ~/.bashrc ] && . ~/.bashrc
export PYTHONPATH="__PKG__${PYTHONPATH:+:$PYTHONPATH}"
# The pool's address as Windows sees it; inside WSL, Windows' own 127.0.0.1 is the Windows host
case "$HARNESS_URL" in
  http://127.0.0.1:*|http://localhost:*)
    winhost=$(ip route show default 2>/dev/null | awk '{print $3; exit}')
    [ -n "$winhost" ] && export HARNESS_URL="http://$winhost:${HARNESS_URL##*:}" ;;
esac
harness() { python3 -m yourharness "$@"; }
"""


class WinTerminalHost:
    def __init__(self, send: Callable[[dict], None], home: Path, env_extra: dict[str, str]):
        self.send = send
        self.env_extra = env_extra
        self.sessions: dict[str, PtyProcess] = {}
        self.distro = wsl_distro()
        self.rc = home / "wsl-rc.sh"
        if self.distro:
            try:  # LF line endings: bash reads this file
                self.rc.parent.mkdir(parents=True, exist_ok=True)
                self.rc.write_bytes(WSL_RC.replace("__PKG__", to_wsl_path(PACKAGE_PARENT)).encode("utf-8"))
            except OSError:
                self.rc = None

    def describe(self) -> dict:
        if self.distro:
            return {"shell": "bash", "shared": False, "system": f"{self.distro} on Windows (WSL)", "harness": bool(self.rc)}
        return {"shell": "PowerShell", "shared": False, "system": "Windows", "harness": False}

    def _argv(self) -> str:
        if self.distro and self.rc:
            return f'wsl.exe -d "{self.distro}" --cd ~ -e bash --rcfile "{to_wsl_path(self.rc)}" -i'
        if self.distro:
            return f'wsl.exe -d "{self.distro}" --cd ~'
        return "powershell.exe -NoLogo"

    def open(self, sid: str, cols: int, rows: int, user: str) -> None:
        if sid in self.sessions:
            return
        env = dict(os.environ)
        env.update(self.env_extra)
        env["HARNESS_USER"] = user
        env["WSLENV"] = ":".join(filter(None, [env.get("WSLENV", ""), "HARNESS_URL/u", "HARNESS_TOKEN/u", "HARNESS_USER/u"]))
        proc = PtyProcess.spawn(self._argv(), dimensions=(max(2, rows), max(2, cols)), env=env, cwd=os.path.expanduser("~"))
        self.sessions[sid] = proc
        loop = asyncio.get_running_loop()
        threading.Thread(target=self._pump, args=(sid, proc, loop), daemon=True).start()

    def _pump(self, sid: str, proc: PtyProcess, loop: asyncio.AbstractEventLoop) -> None:
        while True:
            try:
                data = proc.read(4096)
            except EOFError:
                break
            except Exception:
                break
            if data:
                payload = base64.b64encode(data.encode("utf-8", "replace")).decode("ascii")
                loop.call_soon_threadsafe(self.send, {"type": "term.out", "sid": sid, "data": payload})
        loop.call_soon_threadsafe(self._ended, sid, proc)

    def _ended(self, sid: str, proc: PtyProcess) -> None:
        if self.sessions.get(sid) is proc:
            del self.sessions[sid]
            self.send({"type": "term.exit", "sid": sid, "code": proc.exitstatus})

    def write(self, sid: str, text: str) -> None:
        proc = self.sessions.get(sid)
        if proc is not None:
            try:
                proc.write(text)
            except Exception:
                pass

    def resize(self, sid: str, cols: int, rows: int) -> None:
        proc = self.sessions.get(sid)
        if proc is not None:
            try:
                proc.setwinsize(max(2, rows), max(2, cols))
            except Exception:
                pass

    def close(self, sid: str) -> None:
        proc = self.sessions.pop(sid, None)
        if proc is not None:
            try:
                proc.terminate(force=True)
            except Exception:
                pass

    def close_all(self) -> None:
        for sid in list(self.sessions):
            self.close(sid)
