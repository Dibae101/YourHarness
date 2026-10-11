"""Workspace terminals on the computer that hosts the workspace (Linux, WSL2 or macOS).

With tmux installed, every terminal attaches to one shared session on a private
tmux server, so all devices see the same shell and it survives disconnects.
Without tmux, each terminal gets its own login shell.
"""

from __future__ import annotations

import asyncio
import base64
import fcntl
import os
import pty
import shutil
import signal
import struct
import sys
import termios
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from ..sysinfo import os_name

PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)

TMUX_CONF = """\
set -g mouse on
set -g history-limit 50000
set -g window-size latest
setw -g aggressive-resize on
set -g status-style "bg=#17212B,fg=#E6EBF0"
set -g status-left " YourHarness "
set -g status-left-length 20
set -g status-right " #{session_attached} device(s) attached "
"""


@dataclass
class Session:
    pid: int
    fd: int
    pending: bytearray = field(default_factory=bytearray)
    writing: bool = False


class TerminalHost:
    def __init__(self, send: Callable[[dict], None], home: Path, env_extra: dict[str, str]):
        self.send = send
        self.home = home
        self.env_extra = env_extra
        self.sessions: dict[str, Session] = {}
        self.tmux = shutil.which("tmux")
        self.shell = os.environ.get("SHELL") or shutil.which("bash") or "/bin/sh"
        self.bin_dir = home / "workspace-bin"
        self._write_helpers()

    def describe(self) -> dict:
        return {"shell": "tmux" if self.tmux else os.path.basename(self.shell), "shared": bool(self.tmux), "system": os_name(), "harness": True}

    def _write_helpers(self) -> None:
        """A `harness` command for the workspace shell, pointing at this agent's Python."""
        self.bin_dir.mkdir(parents=True, exist_ok=True)
        launcher = self.bin_dir / "harness"
        if getattr(sys, "frozen", False):  # a packaged build is the executable itself
            body = f'exec "{sys.executable}" "$@"\n'
        else:
            body = f'PYTHONPATH="{PACKAGE_PARENT}${{PYTHONPATH:+:$PYTHONPATH}}" exec "{sys.executable}" -m yourharness "$@"\n'
        launcher.write_text("#!/bin/sh\n" + body)
        launcher.chmod(0o755)
        (self.home / "tmux.conf").write_text(TMUX_CONF)

    def _command(self) -> list[str]:
        if self.tmux:
            return [self.tmux, "-L", "yourharness", "-f", str(self.home / "tmux.conf"), "new-session", "-A", "-s", "harness"]
        return [self.shell, "-l"]

    def _env(self, user: str) -> dict[str, str]:
        env = dict(os.environ)
        env.update(self.env_extra)
        env["TERM"] = "xterm-256color"
        env["COLORTERM"] = "truecolor"
        env.setdefault("LANG", "C.UTF-8")
        env["HARNESS_USER"] = user
        env["PATH"] = f"{self.bin_dir}{os.pathsep}{env.get('PATH', '')}"
        env["PYTHONPATH"] = PACKAGE_PARENT + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        env.pop("TMUX", None)
        return env

    # ------------------------------------------------------------------ sessions
    def open(self, sid: str, cols: int, rows: int, user: str) -> None:
        if sid in self.sessions:
            return
        cmd = self._command()
        env = self._env(user)
        cwd = os.path.expanduser("~")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # forking a threaded process; the child execs at once
            pid, fd = pty.fork()
        if pid == 0:  # child
            try:
                os.chdir(cwd)
                os.execvpe(cmd[0], cmd, env)
            finally:
                os._exit(127)
        self._set_size(fd, cols, rows)
        os.set_blocking(fd, False)
        self.sessions[sid] = Session(pid, fd)
        asyncio.get_running_loop().add_reader(fd, self._readable, sid)

    def _readable(self, sid: str) -> None:
        s = self.sessions.get(sid)
        if s is None:
            return
        try:
            data = os.read(s.fd, 65536)
        except BlockingIOError:
            return
        except OSError:  # EIO: the shell exited (Linux)
            data = b""
        if not data:
            self._ended(sid)
            return
        self.send({"type": "term.out", "sid": sid, "data": base64.b64encode(data).decode("ascii")})

    def write(self, sid: str, text: str) -> None:
        s = self.sessions.get(sid)
        if s is None:
            return
        s.pending += text.encode("utf-8", "replace")
        self._flush(sid)

    def _flush(self, sid: str) -> None:
        s = self.sessions.get(sid)
        if s is None:
            return
        loop = asyncio.get_running_loop()
        try:
            while s.pending:
                n = os.write(s.fd, s.pending)
                del s.pending[:n]
        except BlockingIOError:
            pass
        except OSError:
            s.pending.clear()
        if s.pending and not s.writing:
            loop.add_writer(s.fd, self._flush, sid)
            s.writing = True
        elif not s.pending and s.writing:
            loop.remove_writer(s.fd)
            s.writing = False

    def resize(self, sid: str, cols: int, rows: int) -> None:
        s = self.sessions.get(sid)
        if s is not None:
            self._set_size(s.fd, cols, rows)

    @staticmethod
    def _set_size(fd: int, cols: int, rows: int) -> None:
        try:
            fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", max(2, rows), max(2, cols), 0, 0))
        except OSError:
            pass

    def close(self, sid: str) -> None:
        s = self._detach(sid)
        if s is None:
            return
        try:
            os.kill(s.pid, signal.SIGHUP)
        except ProcessLookupError:
            pass
        loop = asyncio.get_running_loop()
        loop.call_later(1.0, self._reap, s.pid, False)
        loop.call_later(4.0, self._reap, s.pid, True)

    def close_all(self) -> None:
        for sid in list(self.sessions):
            self.close(sid)

    def _ended(self, sid: str) -> None:
        s = self._detach(sid)
        if s is None:
            return
        code = self._reap(s.pid, False)
        self.send({"type": "term.exit", "sid": sid, "code": code})
        if code is None:
            asyncio.get_running_loop().call_later(2.0, self._reap, s.pid, True)

    def _detach(self, sid: str) -> Session | None:
        s = self.sessions.pop(sid, None)
        if s is None:
            return None
        loop = asyncio.get_running_loop()
        loop.remove_reader(s.fd)
        if s.writing:
            loop.remove_writer(s.fd)
        try:
            os.close(s.fd)
        except OSError:
            pass
        return s

    @staticmethod
    def _reap(pid: int, force: bool) -> int | None:
        try:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                return os.waitstatus_to_exitcode(status)
            if force:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
        except ChildProcessError:
            pass
        except ProcessLookupError:
            pass
        return None
