"""Runs one command of the pool shell on this computer.

Every computer that allows commands keeps a local copy of the OS disk (/pool) in
`osdisk/` next to the agent's settings. Before a command: files that changed
elsewhere are downloaded. After it: files this command created, changed or
deleted are sent back, so the next command, on any computer, sees them.

Only one command runs at a time on a computer, so what a command changed can be
told apart from what another one did. Commands get no keyboard input (stdin is
closed); output is streamed back as it appears.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import shlex
import shutil
import signal
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Callable

import aiohttp

from .wsl import to_wsl_path, wsl_distro

CHUNK = 4 * 1024 * 1024
MAX_OUTPUT = 40 * 1024 * 1024
PACKAGE_PARENT = Path(__file__).resolve().parent.parent.parent

# Inside WSL, Windows' own 127.0.0.1 is the Windows host, so the pool is reached there.
WSL_PRELUDE = r'''
case "$HARNESS_URL" in
  http://127.0.0.1:*|http://localhost:*)
    __yh_host=$(ip route show default 2>/dev/null | awk '{print $3; exit}')
    [ -n "$__yh_host" ] && export HARNESS_URL="http://$__yh_host:${HARNESS_URL##*:}" ;;
esac
'''

SCRIPT = r'''
cd -- "$YH_DIR" 2>/dev/null || { echo "yharness: /pool/$YH_REL doesn't exist on this computer" >&2; exit 1; }
eval "$YH_CMD"
__yh_rc=$?
pwd -P > "$YH_CWDFILE" 2>/dev/null
exit $__yh_rc
'''


async def _error_of(res) -> str:
    try:
        body = await res.json(content_type=None)
        return str(body.get("error") or res.status)
    except Exception:
        return f"status {res.status}"


def file_digest(path: Path) -> str:
    """The same whole-file fingerprint the coordinator makes from chunk fingerprints."""
    chunks = []
    with open(path, "rb") as f:
        while True:
            data = f.read(CHUNK)
            if not data:
                break
            chunks.append(hashlib.sha256(data).hexdigest())
    return hashlib.sha256("".join(chunks).encode("ascii")).hexdigest()


class Runner:
    def __init__(self, server: str, token: str, home: Path, send: Callable[[dict], None], env_extra: dict[str, str]):
        self.server, self.token = server.rstrip("/"), token
        self.home = home
        self.root = home / "osdisk"
        self.run_dir = home / "run"
        self.state_path = home / "osdisk-state.json"
        self.send = send
        self.env_extra = env_extra
        self.lock = asyncio.Lock()
        self.procs: dict[str, asyncio.subprocess.Process] = {}
        self.cancelled: set[str] = set()
        self.distro = wsl_distro() if os.name == "nt" else None
        # what the pool's files are called here; shown as /pool in output so paths mean the same on every computer
        names = {str(self.root), str(self.root.resolve())}
        if os.name == "nt":
            names.add(to_wsl_path(self.root))
        self.root_names = sorted((n.encode("utf-8") for n in names), key=len, reverse=True)
        self.bin_dir = home / "workspace-bin"
        self._write_launcher()
        self._folds_case: bool | None = None
        self.remote_dirs: set[str] = set()  # folders made on purpose in /pool, as of the last download
        self.notes: list[str] = []  # told to the user before the command's output

    def folds_case(self) -> bool:
        """True when this disk treats README and readme as one file (Windows, usually macOS)."""
        if self._folds_case is None:
            self.root.mkdir(parents=True, exist_ok=True)
            probe = self.root / ".yh-case-probe"
            try:
                probe.write_bytes(b"")
                self._folds_case = (self.root / ".YH-CASE-PROBE").exists()
            except OSError:
                self._folds_case = os.name == "nt" or sys.platform == "darwin"
            finally:
                probe.unlink(missing_ok=True)
        return self._folds_case

    # ------------------------------------------------------------------ what this computer offers
    @classmethod
    def available(cls) -> bool:
        if os.name == "nt":
            return wsl_distro() is not None
        return shutil.which("bash") is not None

    def describe(self) -> dict:
        if os.name == "nt":
            return {"os": f"{self.distro} on Windows (WSL)", "linux": True}
        import platform

        from ..sysinfo import os_name

        return {"os": os_name(), "linux": platform.system() == "Linux"}

    def _write_launcher(self) -> None:
        """A `harness` command for programs run here, so scripts can use the pool too."""
        self.bin_dir.mkdir(parents=True, exist_ok=True)
        launcher = self.bin_dir / "harness"
        if os.name == "nt":  # run by bash inside WSL, with WSL's own Python
            pkg = shlex.quote(to_wsl_path(PACKAGE_PARENT))
            launcher.write_bytes(f'#!/bin/sh\nPYTHONPATH={pkg}"${{PYTHONPATH:+:$PYTHONPATH}}" exec python3 -m yourharness "$@"\n'.encode("utf-8"))
            return
        if getattr(sys, "frozen", False):
            body = f'exec "{sys.executable}" "$@"\n'
        else:
            body = f'PYTHONPATH="{PACKAGE_PARENT}${{PYTHONPATH:+:$PYTHONPATH}}" exec "{sys.executable}" -m yourharness "$@"\n'
        launcher.write_text("#!/bin/sh\n" + body)
        launcher.chmod(0o755)

    # ------------------------------------------------------------------ local copy bookkeeping
    def _load_state(self) -> dict:
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def _save_state(self, state: dict) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, self.state_path)

    def _local(self, rel: str) -> Path:
        p = (self.root / rel).resolve()
        if p != self.root and self.root not in p.parents:
            raise ValueError(f"unsafe path from the pool: {rel}")
        return p

    @staticmethod
    def _sig(p: Path) -> tuple[int, int]:
        st = p.stat()
        return st.st_size, st.st_mtime_ns

    # ------------------------------------------------------------------ syncing
    def _http(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(headers={"Authorization": f"Bearer {self.token}"}, timeout=aiohttp.ClientTimeout(total=None, sock_connect=15))

    async def sync_down(self, http: aiohttp.ClientSession) -> dict:
        self.root.mkdir(parents=True, exist_ok=True)
        async with http.get(f"{self.server}/api/os/manifest") as res:
            if res.status != 200:
                raise RuntimeError(f"couldn't read the pool's file list (status {res.status})")
            remote = await res.json()
        state = self._load_state()
        files, loop = 0, asyncio.get_running_loop()
        nbytes = 0
        self.remote_dirs = set(remote["dirs"])
        skipped: list[str] = []
        for rel in remote["dirs"]:
            try:
                self._local(rel).mkdir(parents=True, exist_ok=True)
            except OSError:
                skipped.append(rel)
        # On a disk that folds case, README and readme would land on one file and the
        # upload afterwards would overwrite one with the other. Only the first is copied
        # here; commands that need the other must run on a computer that keeps both.
        taken: dict[str, str] = {}
        for rel, meta in remote["files"].items():
            if self.folds_case():
                key = rel.casefold()
                if key in taken:
                    skipped.append(rel)
                    state.pop(rel, None)
                    continue
                taken[key] = rel
            dest = self._local(rel)
            known = state.get(rel)
            if known and known["digest"] == meta["digest"] and dest.is_file() and self._sig(dest) == (known["size"], known["mtime_ns"]):
                continue
            tmp = dest.with_name(f".{dest.name}.yhpart")
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.is_dir():
                    shutil.rmtree(dest)
                out = open(tmp, "wb")
            except OSError:  # a name this disk can't hold, such as "a:b" or "aux" on Windows
                skipped.append(rel)
                state.pop(rel, None)
                continue
            with out:
                async with http.get(f"{self.server}/api/os/file", params={"path": rel}) as res:
                    if res.status != 200:
                        raise RuntimeError(f"couldn't download /pool/{rel} ({(await res.json()).get('error', res.status)})")
                    async for part in res.content.iter_chunked(1024 * 1024):
                        await loop.run_in_executor(None, out.write, part)
            os.replace(tmp, dest)
            size, mtime_ns = self._sig(dest)
            state[rel] = {"digest": meta["digest"], "size": size, "mtime_ns": mtime_ns}
            files += 1
            nbytes += size
        for rel in [r for r in state if r not in remote["files"]]:  # deleted elsewhere
            if state[rel].get("rejected"):  # never reached the pool; it's this computer's own file
                continue
            local = self._local(rel)
            if local.is_file():
                local.unlink()
            del state[rel]
        self._save_state(state)
        if skipped:
            shown = ", ".join(f"/pool/{r}" for r in skipped[:5]) + (f" and {len(skipped) - 5} more" if len(skipped) > 5 else "")
            self.notes.append(f"Not copied to this computer, whose disk can't hold these names: {shown}")
        return {"files": files, "bytes": nbytes, "skipped": len(skipped)}

    async def sync_up(self, http: aiohttp.ClientSession) -> dict:
        state = self._load_state()
        loop = asyncio.get_running_loop()
        seen: set[str] = set()
        up = deleted = nbytes = 0
        empty_dirs: list[str] = []
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = [d for d in dirnames if not d.endswith(".yhpart")]
            rel_dir = Path(dirpath).relative_to(self.root).as_posix()
            if rel_dir != "." and not dirnames and not filenames:
                empty_dirs.append(rel_dir)
            for name in filenames:
                if name.endswith(".yhpart"):
                    continue
                p = Path(dirpath) / name
                rel = p.relative_to(self.root).as_posix()
                seen.add(rel)
                try:
                    size, mtime_ns = self._sig(p)
                except OSError:
                    continue
                known = state.get(rel)
                if known and (known["size"], known["mtime_ns"]) == (size, mtime_ns):
                    continue
                digest = await loop.run_in_executor(None, file_digest, p)
                if known and known["digest"] == digest:  # touched, not changed
                    known.update(size=size, mtime_ns=mtime_ns)
                    continue
                with open(p, "rb") as f:
                    async with http.put(f"{self.server}/api/os/file", params={"path": rel}, data=f) as res:
                        if 400 <= res.status < 500 and res.status not in (401, 403):
                            # The pool refused this file (too big, a bad name). Remember that, so
                            # it isn't sent again until it changes and other files still go up.
                            reason = await _error_of(res)
                            self.notes.append(f"/pool/{rel} stayed on this computer only: {reason}")
                            state[rel] = {"digest": digest, "size": size, "mtime_ns": mtime_ns, "rejected": True}
                            continue
                        if res.status >= 300:
                            raise RuntimeError(f"couldn't save /pool/{rel} ({await _error_of(res)})")
                state[rel] = {"digest": digest, "size": size, "mtime_ns": mtime_ns}
                up += 1
                nbytes += size
        for rel in [r for r in state if r not in seen]:  # deleted by the command
            if not state[rel].get("rejected"):
                async with http.delete(f"{self.server}/api/os/file", params={"path": rel}) as res:
                    if res.status not in (200, 404):
                        raise RuntimeError(f"couldn't delete /pool/{rel}")
                deleted += 1
            del state[rel]
        # Folders the command removed. Deepest first and never recursive: a folder that
        # another computer has put files in since is not empty, so it stays.
        for rel in sorted(self.remote_dirs, key=lambda r: -r.count("/")):
            try:
                gone = not self._local(rel).is_dir()
            except ValueError:
                continue
            if gone:
                async with http.delete(f"{self.server}/api/os/file", params={"path": rel}) as res:
                    await res.read()
        for rel in empty_dirs:
            async with http.post(f"{self.server}/api/os/mkdir", json={"path": rel}) as res:
                await res.read()
        self._save_state(state)
        return {"files": up, "bytes": nbytes, "deleted": deleted}

    def _as_pool_paths(self, data: bytes, carry: bytes) -> tuple[bytes, bytes]:
        """Show this computer's copy of the disk as /pool. Holds back a few bytes that might be half a path."""
        data = carry + data
        for name in self.root_names:
            data = data.replace(name, b"/pool")
        hold = 0
        for name in self.root_names:
            for k in range(min(len(name) - 1, len(data)), 0, -1):
                if data.endswith(name[:k]):
                    hold = max(hold, k)
                    break
        return (data[:-hold], data[-hold:]) if hold else (data, b"")

    # ------------------------------------------------------------------ running a command
    def _write_script(self, run_id: str, cmd: str, rel: str, env: dict[str, str]) -> tuple[Path, Path]:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        script = self.run_dir / f"{run_id}.sh"
        cwdfile = self.run_dir / f"{run_id}.cwd"
        shell_path = (lambda p: to_wsl_path(p)) if os.name == "nt" else (lambda p: str(p))
        exports = {
            "YH_DIR": shell_path(self._local(rel)), "YH_REL": rel, "YH_CMD": cmd, "YH_CWDFILE": shell_path(cwdfile),
            "TERM": "xterm-256color", "FORCE_COLOR": "1", "CLICOLOR_FORCE": "1", "PYTHONUNBUFFERED": "1",
            "HARNESS_URL": self.env_extra.get("HARNESS_URL", self.server), "HARNESS_TOKEN": self.token,
            **{k: v for k, v in env.items() if k.isidentifier()},
        }
        lines = [f"export {k}={shlex.quote(str(v))}" for k, v in exports.items()]
        lines.append(f'export PATH={shlex.quote(shell_path(self.bin_dir))}:"$PATH"')
        prelude = WSL_PRELUDE if os.name == "nt" else ""
        script.write_bytes(("\n".join(lines) + "\n" + prelude + SCRIPT).encode("utf-8"))
        return script, cwdfile

    async def run(self, msg: dict) -> None:
        run_id = str(msg["id"])
        rel = str(msg.get("cwd") or "").strip("/")
        t0 = time.monotonic()
        async with self.lock:
            sync = {}
            code, error = None, None
            new_cwd = rel
            try:
                async with self._http() as http:
                    self.notes = []
                    sync["down"] = await self.sync_down(http)
                    self._tell_notes(run_id)
                    if run_id in self.cancelled:
                        raise asyncio.CancelledError
                    script, cwdfile = self._write_script(run_id, str(msg.get("cmd", "")), rel, msg.get("env") or {})
                    argv = ["bash", to_wsl_path(script)] if os.name == "nt" else ["bash", str(script)]
                    if os.name == "nt":
                        argv = ["wsl.exe", "-d", self.distro, "-e", *argv]
                    proc = await asyncio.create_subprocess_exec(
                        *argv, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                        start_new_session=(os.name != "nt"), cwd=str(self.root),
                    )
                    self.procs[run_id] = proc
                    sent = 0
                    carry = b""
                    assert proc.stdout is not None
                    while True:
                        data = await proc.stdout.read(16384)
                        if not data:
                            if carry:
                                self.send({"type": "exec.out", "id": run_id, "data": base64.b64encode(carry).decode("ascii")})
                            break
                        data, carry = self._as_pool_paths(data, carry)
                        if not data:
                            continue
                        sent += len(data)
                        if sent > MAX_OUTPUT:
                            self.kill(run_id)
                            self.send({"type": "exec.out", "id": run_id, "data": base64.b64encode(b"\n[output stopped: more than 40 MB]\n").decode("ascii")})
                            break
                        self.send({"type": "exec.out", "id": run_id, "data": base64.b64encode(data).decode("ascii")})
                    code = await proc.wait()
                    try:
                        raw = cwdfile.read_text(encoding="utf-8").strip()
                        if os.name == "nt":  # a WSL path; Path() would turn its slashes into backslashes
                            new_cwd = self._from_wsl(raw, rel)
                        else:
                            resolved = Path(raw).resolve()
                            new_cwd = resolved.relative_to(self.root).as_posix() if self.root == resolved or self.root in resolved.parents else rel
                            new_cwd = "" if new_cwd == "." else new_cwd
                    except (OSError, ValueError):
                        new_cwd = rel
                    sync["up"] = await self.sync_up(http)
                    self._tell_notes(run_id)
                    if run_id in self.cancelled:  # stopped by the user, however the process ended
                        error = "cancelled"
            except asyncio.CancelledError:
                code, error = code if code is not None else -15, "cancelled"
            except Exception as e:  # report instead of leaving the shell waiting
                error = f"{type(e).__name__}: {e}"
                code = code if code is not None else 125
            finally:
                self.procs.pop(run_id, None)
                self.cancelled.discard(run_id)
                for suffix in (".sh", ".cwd"):
                    (self.run_dir / f"{run_id}{suffix}").unlink(missing_ok=True)
            if code is not None and code < 0 and not error:
                error = f"stopped by signal {-code}"
            self.send({"type": "exec.done", "id": run_id, "code": code, "cwd": new_cwd, "error": error, "sync": sync, "ms": round((time.monotonic() - t0) * 1000)})

    def _tell_notes(self, run_id: str) -> None:
        for note in self.notes:
            self.send({"type": "exec.out", "id": run_id, "data": base64.b64encode(f"\x1b[33m{note}\x1b[0m\r\n".encode("utf-8")).decode("ascii")})
        self.notes = []

    def _from_wsl(self, wsl_path: str, fallback: str) -> str:
        base = to_wsl_path(self.root).rstrip("/")
        if wsl_path == base:
            return ""
        return wsl_path[len(base) + 1:] if wsl_path.startswith(base + "/") else fallback

    def kill(self, run_id: str) -> None:
        proc = self.procs.get(run_id)
        self.cancelled.add(run_id)
        if proc is None or proc.returncode is not None:
            return
        try:
            if os.name == "nt":
                proc.kill()
            else:
                os.killpg(proc.pid, signal.SIGTERM)
                asyncio.get_running_loop().call_later(3.0, self._force, proc)
        except (ProcessLookupError, PermissionError):
            pass

    @staticmethod
    def _force(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass
