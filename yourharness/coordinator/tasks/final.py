"""Final checks: a run's own commands on the combined branch, on the coordinator's computer.

Each task's checks ran on the computer that did the task, on that task's changes
alone. A run's final checks run once the results are combined, on exactly the commit
that would be pushed, on the coordinator's computer (yours). So "validated" means
the combined branch passed, as checked by a computer you trust, and a pull request
says plainly when it didn't.

They run your commands on code the tasks wrote. If a task came from a friend's
computer, its code would run on yours too, so then the final checks run only when
the run allows it (final_with_friends). The commands get a trimmed environment: no
YourHarness or GitHub tokens, and no variable whose name looks like a secret.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import socket
import time
from pathlib import Path
from typing import Callable

KEEP = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "TERM", "TMPDIR", "TEMP", "TMP", "SYSTEMROOT", "COMSPEC", "PATHEXT",
        "WINDIR", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "PROGRAMFILES", "HOMEDRIVE", "HOMEPATH")
SECRETISH = re.compile(r"TOKEN|SECRET|PASSW|CREDENTIAL|API_?KEY|PRIVATE|SESSION|COOKIE|AUTH|^AWS_|^GITHUB_|^GH_|ANTHROPIC|OPENAI|^AZURE_|^GOOGLE_",
                       re.I)
LOG_PER_COMMAND = 2 * 1024 * 1024


def clean_env(base=None) -> dict:
    base = os.environ if base is None else base
    env = {k: v for k, v in base.items() if (k in KEEP or k.startswith("LC_")) and not SECRETISH.search(k)}
    env.update(CI="true", YOURHARNESS_FINAL_CHECKS="1", GIT_TERMINAL_PROMPT="0")
    return env


def shell(cmd: str) -> list[str]:
    if os.name == "nt":
        return ["cmd", "/c", cmd]
    return [shutil.which("bash") or "/bin/sh", "-c", cmd]


async def run_checks(work: Path, setup: list[str], checks: list[str], limit_s: float, log: Callable[[str], None]) -> dict:
    """Run the setup commands, then the checks, in `work`. Stops at the first setup failure or at the time limit."""
    env = clean_env()
    end = time.monotonic() + limit_s
    results: list[dict] = []
    failed = False
    for phase, cmds in (("setup", setup), ("check", checks)):
        for cmd in cmds:
            if failed and phase == "setup":
                break
            left = end - time.monotonic()
            if left <= 0:
                results.append({"phase": phase, "cmd": cmd, "code": None, "ms": 0, "error": "not run: the final checks' time limit was reached"})
                failed = True
                continue
            log(f"\n$ {cmd}\n")
            t0 = time.monotonic()
            code, error = await _one(cmd, work, env, left, log)
            ms = int((time.monotonic() - t0) * 1000)
            results.append({"phase": phase, "cmd": cmd, "code": code, "ms": ms, **({"error": error} if error else {})})
            log(f"[exit code {code if code is not None else '-'} after {ms / 1000:.1f} s{f': {error}' if error else ''}]\n")
            if code != 0:
                failed = True
        if failed and phase == "setup":
            break
    passed = not failed and all(r["code"] == 0 for r in results if r["phase"] == "check")
    return {"status": "passed" if passed else "failed", "checks": results, "on": socket.gethostname()[:80], "at": time.time()}


async def _one(cmd: str, work: Path, env: dict, timeout: float, log: Callable[[str], None]) -> tuple[int | None, str | None]:
    kw = {"start_new_session": True} if os.name != "nt" else {}
    try:
        proc = await asyncio.create_subprocess_exec(*shell(cmd), cwd=str(work), env=env, stdin=asyncio.subprocess.DEVNULL,
                                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, **kw)
    except OSError as e:
        return None, f"couldn't start it: {e}"
    written = 0

    async def pump():
        nonlocal written
        while True:
            data = await proc.stdout.read(65536)
            if not data:
                return
            if written < LOG_PER_COMMAND:
                log(data[: LOG_PER_COMMAND - written].decode("utf-8", "replace"))
                written += len(data)
                if written >= LOG_PER_COMMAND:
                    log("\n[More output than this isn't kept.]\n")

    try:
        await asyncio.wait_for(asyncio.gather(pump(), proc.wait()), timeout)
    except asyncio.TimeoutError:
        _kill(proc)
        await proc.wait()
        return None, "ran past the final checks' time limit"
    return proc.returncode, None


def _kill(proc) -> None:
    try:
        if os.name != "nt":
            os.killpg(proc.pid, signal.SIGKILL)  # the whole process group: a test runner's children too
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError):
        pass


def summary(v: dict | None) -> str:
    """One line for the web app and the pull request."""
    if not v:
        return ""
    if v.get("status") == "passed":
        return f"Validated: the final checks passed on the combined branch at {v.get('commit', '')[:12]}."
    if v.get("status") == "skipped":
        return f"Not validated: {v.get('reason', 'the final checks were skipped')}"
    failed = [c for c in v.get("checks") or [] if c.get("code") != 0]
    what = failed[0]["cmd"] if failed else "a check"
    return f"Not validated: the combined branch failed its final checks ({what})."
