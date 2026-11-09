"""Agent tasks on this computer.

The coordinator sends a task. This computer works on it in a Git worktree of its
own, at the commit the task was given, and sends the commits back as a bundle.
Every step is a small bash script: run directly on a Mac or Linux, or inside
Ubuntu (WSL) on Windows, where the workspace lives on WSL's own disk rather than
under /mnt/c.

What a step can reach:
- The attempt token (good for downloading this task's starting point and
  uploading its result, nothing else) is set only in the two steps that use it.
- The agent CLI, setup and check commands run through ``yh_clean``: an empty
  environment plus an allowlist, so neither that token nor anything else this
  program has in its environment reaches them.
"""

from __future__ import annotations

import asyncio
import atexit
import codecs
import logging
import os
import re
import shlex
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Callable

from .adapters import ADAPTERS, BINARIES, DETECT_SCRIPT, OutputReader, agent_command, parse_detect
from .prompts import build_prompt
from .wsl import to_wsl_path, wsl_distro

log = logging.getLogger("yourharness")

WINDOWS = os.name == "nt"
HEARTBEAT_S = 15.0
LOG_FLUSH_S = 0.4
DETECT_EVERY_S = 600.0
OUTPUT_TAIL = 8000
SAFE_TASK = re.compile(r"t[0-9a-f]{8}")
SAFE_REPO = re.compile(r"[A-Za-z0-9._-]{1,120}")
SAFE_SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")

# Variables an agent CLI may need: where it keeps its sign-in, keys if you use them, proxies.
PASS_ENV = (
    "USER", "LOGNAME", "SHELL", "TMPDIR", "LC_ALL", "LC_CTYPE", "TZ",
    "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL", "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX",
    "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION", "OPENAI_API_KEY", "OPENAI_BASE_URL", "CODEX_HOME",
    "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME",
    "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy", "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS",
)

DERIVED = r'''
export YH_WS="$YH_ROOT/ws/${YH_TASK:-x}-${YH_ATTEMPT:-0}"
export YH_OUT="$YH_ROOT/out/${YH_TASK:-x}-${YH_ATTEMPT:-0}"
export YH_CACHE="$YH_ROOT/cache/${YH_REPO:-none}.git"
'''

# Programs started by the desktop app get a short PATH; add the usual places CLIs are installed.
PRELUDE = r'''
for __d in "$HOME/.local/bin" "$HOME/.npm-global/bin" "$HOME/.bun/bin" "$HOME/.cargo/bin" "$HOME/.volta/bin" /opt/homebrew/bin /usr/local/bin; do
  if [ -d "$__d" ]; then case ":$PATH:" in *":$__d:"*) ;; *) PATH="$__d:$PATH" ;; esac; fi
done
if [ -d "$HOME/.nvm/versions/node" ]; then
  __n=$(ls -1d "$HOME"/.nvm/versions/node/*/bin 2>/dev/null | tail -n 1)
  if [ -n "$__n" ]; then PATH="$__n:$PATH"; fi
fi
export PATH
yh_env=("HOME=$HOME" "PATH=$PATH" "LANG=${LANG:-en_US.UTF-8}" "TERM=dumb" "NO_COLOR=1")
for __v in ''' + " ".join(PASS_ENV) + r'''; do
  if [ -n "${!__v+x}" ]; then yh_env+=("$__v=${!__v}"); fi
done
yh_clean() { env -i "${yh_env[@]}" "$@"; }
'''

# Inside WSL, the Windows host's own 127.0.0.1 isn't this machine's.
WSL_URL_FIX = r'''
case "${YH_URL:-}" in
  http://127.0.0.1:*|http://localhost:*)
    __yh_host=$(ip route show default 2>/dev/null | awk '{print $3; exit}')
    if [ -n "$__yh_host" ]; then export YH_URL="http://$__yh_host:${YH_URL##*:}"; fi ;;
esac
'''

PREPARE = r'''
set -e
mkdir -p "$YH_ROOT/cache" "$YH_ROOT/ws" "$YH_ROOT/out"
find "$YH_ROOT/ws" "$YH_ROOT/out" -mindepth 1 -maxdepth 1 -mtime +2 -exec rm -rf {} + 2>/dev/null || true
[ -d "$YH_CACHE" ] || git init --bare -q "$YH_CACHE"
git -C "$YH_CACHE" worktree prune
rm -rf "$YH_OUT"
mkdir -p "$YH_OUT"
if ! git -C "$YH_CACHE" cat-file -e "$YH_BASE^{commit}" 2>/dev/null; then
  echo "Downloading the repository from the pool..."
  curl -fsS --retry 3 -H "Authorization: Bearer $YH_TOKEN" -o "$YH_OUT/start.bundle" "$YH_URL/api/agent-tasks/$YH_TASK/start.bundle"
  git -C "$YH_CACHE" fetch -q "$YH_OUT/start.bundle" "+refs/yh/base/*:refs/yh/base/*"
  rm -f "$YH_OUT/start.bundle"
fi
if [ -e "$YH_WS" ]; then git -C "$YH_CACHE" worktree remove --force "$YH_WS" >/dev/null 2>&1 || rm -rf "$YH_WS"; fi
git -C "$YH_CACHE" worktree add -q --detach "$YH_WS" "$YH_BASE"
git -C "$YH_WS" config user.name "YourHarness agent"
git -C "$YH_WS" config user.email "agent@yourharness.invalid"
git -C "$YH_WS" config core.autocrlf false
echo "Workspace ready at commit ${YH_BASE:0:12}."
'''

COMMIT = r'''
set -e
cd "$YH_WS"
git add -A
if ! git diff --cached --quiet; then
  git -c commit.gpgsign=false commit -q --no-verify -m "$YH_TITLE" -m "YourHarness task $YH_TASK, attempt $YH_ATTEMPT, on $(hostname 2>/dev/null || echo 'a pool computer')."
fi
if [ "$(git rev-parse HEAD)" = "$(git rev-parse "$YH_BASE^{commit}")" ]; then echo "::yh-nochange::1"; exit 0; fi
__n=$(git diff --name-only "$YH_BASE" HEAD | wc -l | tr -d ' ')
if [ "$__n" -gt 5000 ]; then
  echo "The task changed $__n files, more than the 5000 allowed. Did it create build output that .gitignore doesn't cover?"
  exit 3
fi
git update-ref "refs/yh/result/$YH_TASK-$YH_ATTEMPT" HEAD
git bundle create "$YH_OUT/result.bundle" "$YH_BASE..refs/yh/result/$YH_TASK-$YH_ATTEMPT"
echo "::yh-commit::$(git rev-parse HEAD)"
echo "::yh-files::$__n"
echo "::yh-bytes::$(wc -c < "$YH_OUT/result.bundle" | tr -d ' ')"
'''

UPLOAD = r'''
set -e
curl -fsS --retry 2 -H "Authorization: Bearer $YH_TOKEN" -H "Content-Type: application/octet-stream" -T "$YH_OUT/result.bundle" "$YH_URL/api/agent-tasks/$YH_TASK/result.bundle" >/dev/null
echo "Sent the result to the pool."
'''

CLEANUP = r'''
git -C "$YH_CACHE" worktree remove --force "$YH_WS" >/dev/null 2>&1 || true
git -C "$YH_CACHE" update-ref -d "refs/yh/result/$YH_TASK-$YH_ATTEMPT" >/dev/null 2>&1 || true
rm -rf "$YH_OUT"
'''


def q(s) -> str:
    return shlex.quote(str(s))


def commands_script(cmds: list[str]) -> str:
    lines = ["set -e", 'cd "$YH_WS"']
    for c in cmds:
        lines += [f"printf '%s\\n' {q('$ ' + c)}", f"yh_clean bash -c {q(c)}"]
    return "\n".join(lines)


def _ms(since: float) -> int:
    return int((time.monotonic() - since) * 1000)


class TaskFailed(Exception):
    pass


async def _start_time(pid: int) -> str:
    """When a process started, or "" if it isn't running (or that can't be told)."""
    stat = Path(f"/proc/{pid}/stat")
    if os.path.isdir("/proc/self"):  # Linux, including Ubuntu under WSL
        try:
            return stat.read_text().rsplit(")", 1)[1].split()[19]  # field 22, starttime
        except (OSError, IndexError):
            return ""
    try:
        proc = await asyncio.create_subprocess_exec("ps", "-o", "lstart=", "-p", str(pid), stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.DEVNULL, env={**os.environ, "LC_ALL": "C"})
        out, _ = await asyncio.wait_for(proc.communicate(), 5)
        return " ".join(out.decode("utf-8", "replace").split())
    except (OSError, asyncio.TimeoutError):
        return ""


_LIVE_GROUPS: set[int] = set()  # process groups of running steps, stopped if this program exits


@atexit.register
def _stop_live_groups() -> None:
    """An agent CLI must not keep working, and spending, after the program that started it is gone."""
    for pgid in list(_LIVE_GROUPS):
        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass


class Shell:
    """Runs a bash script on this computer, or inside WSL on Windows."""

    def __init__(self, home: Path):
        self.home = Path(home)
        self.dir = self.home / "taskrun"
        self.distro = wsl_distro() if WINDOWS else None
        self.procs: dict[str, asyncio.subprocess.Process] = {}

    @property
    def available(self) -> bool:
        return self.distro is not None if WINDOWS else shutil.which("bash") is not None

    def exec_path(self, p: Path) -> str:
        """A path in this program's folder, as bash on the computer doing the work sees it."""
        return to_wsl_path(p) if WINDOWS else str(p)

    def root_expr(self) -> str:
        return '"$HOME/.yourharness-tasks"' if WINDOWS else q(str(self.home / "tasks"))

    def _write(self, path: Path, text: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)

    def write_file(self, name: str, text: str) -> str:
        path = self.dir / name
        self._write(path, text)
        return self.exec_path(path)

    def remove_file(self, name: str) -> None:
        try:
            (self.dir / name).unlink()
        except OSError:
            pass

    async def run(self, key: str, body: str, *, env: dict | None = None, on_output: Callable[[str], None] | None = None,
                  timeout: float | None = None) -> tuple[int, str, bool]:
        """Run body; returns (exit code, the last part of its output, whether it ran out of time)."""
        exports = "".join(f"export {k}={q(v)}\n" for k, v in (env or {}).items())
        header = f"set -u\nexport YH_ROOT={self.root_expr()}\n{exports}{DERIVED}{PRELUDE}" + (WSL_URL_FIX if WINDOWS else "")
        if WINDOWS:  # its own process group inside WSL, so cancelling stops everything it started
            pid = f'"$YH_ROOT/pids/{key}.pid"'
            text = (header + f'mkdir -p "$YH_ROOT/pids"\n__yh_main() {{\n{body}\n}}\nset -m\n__yh_main &\n__yh_job=$!\n'
                    f'echo "$__yh_job" > {pid}\nwait "$__yh_job"\n__yh_rc=$?\nrm -f {pid}\nexit $__yh_rc\n')
        else:
            text = header + body + "\n"
        script = self.dir / f"{key}.sh"
        self._write(script, text)
        if WINDOWS:
            argv = ["wsl.exe", "-d", self.distro or "Ubuntu", "-e", "bash", to_wsl_path(script)]
            kw = {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0)}
        else:
            argv = ["bash", str(script)]
            kw = {"start_new_session": True}
        tail = ""
        timed_out = False
        try:
            proc = await asyncio.create_subprocess_exec(*argv, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.STDOUT, **kw)
        except OSError as e:
            script.unlink(missing_ok=True)
            return 127, f"Couldn't start bash: {e}", False
        self.procs[key] = proc
        pidfile = self.dir / "pids" / f"{key}.pid"
        if not WINDOWS:
            _LIVE_GROUPS.add(proc.pid)  # start_new_session: its process group id is its pid
            started = await _start_time(proc.pid)
            if started:  # pid plus start time: a pid the system later gives to another program won't match
                try:
                    pidfile.parent.mkdir(parents=True, exist_ok=True)
                    pidfile.write_text(f"{proc.pid}\n{started}\n")
                except OSError:
                    pass
        decoder = codecs.getincrementaldecoder("utf-8")("replace")

        async def pump():
            nonlocal tail
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(16384)
                text = decoder.decode(chunk, final=not chunk)
                if text:
                    tail = (tail + text)[-OUTPUT_TAIL:]
                    if on_output:
                        on_output(text)
                if not chunk:
                    break
            await proc.wait()

        try:
            await asyncio.wait_for(pump(), timeout)
            code = proc.returncode if proc.returncode is not None else 1
        except asyncio.TimeoutError:
            timed_out = True
            await self.stop(key)
            code = 124
        except asyncio.CancelledError:
            await self.stop(key)
            raise
        finally:
            self.procs.pop(key, None)
            _LIVE_GROUPS.discard(proc.pid)
            pidfile.unlink(missing_ok=True)
            script.unlink(missing_ok=True)
        return code, tail, timed_out

    async def reap(self) -> int:
        """Stop steps an earlier run of this program left behind when it was killed outright.

        Only a process whose command line is one of this program's step scripts is
        stopped, so a process id the system has since given to something else is safe.
        """
        if WINDOWS:
            body = ('for __f in "$YH_ROOT"/pids/*.pid; do [ -e "$__f" ] || continue\n'
                    '  case "$__f" in */reap.pid) continue ;; esac\n'
                    '  __p=$(cat "$__f" 2>/dev/null)\n'
                    '  if [ -n "$__p" ] && ps -o args= -p "$__p" 2>/dev/null | grep -q taskrun; then kill -TERM -- "-$__p" 2>/dev/null && echo reaped; fi\n'
                    '  rm -f "$__f"\ndone\n')
            _, out, _ = await self.run("reap", body, timeout=30)
            return out.count("reaped")
        stopped = 0
        folder = self.dir / "pids"
        for f in folder.glob("*.pid") if folder.exists() else []:
            try:
                pid_text, started = (f.read_text().splitlines() + ["", ""])[:2]
                pid = int(pid_text)
                if started and await _start_time(pid) == started:
                    os.killpg(pid, signal.SIGTERM)
                    stopped += 1
            except (ValueError, OSError):
                pass
            f.unlink(missing_ok=True)
        return stopped

    async def stop(self, key: str) -> None:
        proc = self.procs.get(key)
        if proc is None or proc.returncode is not None:
            return
        if WINDOWS:
            kill = self.dir / f"kill-{key}.sh"
            self._write(kill, f'__p=$(cat "$HOME/.yourharness-tasks/pids/{key}.pid" 2>/dev/null) || exit 0\n'
                              'kill -TERM -- "-$__p" 2>/dev/null; sleep 3; kill -KILL -- "-$__p" 2>/dev/null; exit 0\n')
            try:
                k = await asyncio.create_subprocess_exec("wsl.exe", "-d", self.distro or "Ubuntu", "-e", "bash", to_wsl_path(kill),
                                                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                await asyncio.wait_for(k.wait(), 15)
            except (OSError, asyncio.TimeoutError):
                pass
            finally:
                kill.unlink(missing_ok=True)
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        else:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except asyncio.TimeoutError:
            pass


class Attempt:
    """One attempt at one task on this computer."""

    def __init__(self, host: "TaskHost", m: dict, spec: dict):
        self.host = host
        self.shell = host.shell
        self.task = m["task"]
        self.attempt = m["attempt"]
        self.title = re.sub(r"[\x00-\x1f\x7f]", " ", str(m.get("title") or "Task"))[:200]
        self.spec = spec
        self.adapter = spec.get("adapter")
        self.token = str(m.get("token") or "")
        self.env = {"YH_TASK": self.task, "YH_ATTEMPT": str(self.attempt), "YH_REPO": m["repo"], "YH_BASE": m["base"],
                    "YH_TITLE": self.title, "YH_URL": host.server}
        self.cancelled = False
        self.future: asyncio.Future | None = None
        self.phase = "starting"
        self.deadline = 0.0
        self.timings: dict[str, int] = {}
        self._logbuf: list[str] = []
        self._logsize = 0
        self._flush_at: asyncio.TimerHandle | None = None

    # ------------------------------------------------------------------ reporting
    def cancel(self) -> None:
        self.cancelled = True
        if self.future is not None and not self.future.done():
            self.future.cancel()

    def set_phase(self, phase: str, note: str = "") -> None:
        self.phase = phase
        self.host.send({"type": "task.update", "task": self.task, "attempt": self.attempt, "phase": phase, "note": note})

    def log(self, text: str) -> None:
        if not text:
            return
        self._logbuf.append(text)
        self._logsize += len(text)
        if self._logsize > 8192:
            self.flush_log()
        elif self._flush_at is None:
            self._flush_at = asyncio.get_running_loop().call_later(LOG_FLUSH_S, self.flush_log)

    def flush_log(self) -> None:
        if self._flush_at is not None:
            self._flush_at.cancel()
            self._flush_at = None
        if not self._logbuf:
            return
        data, self._logbuf, self._logsize = "".join(self._logbuf), [], 0
        for i in range(0, len(data), 60000):
            self.host.send({"type": "task.log", "task": self.task, "attempt": self.attempt, "data": data[i:i + 60000]})

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_S)
            self.host.send({"type": "task.update", "task": self.task, "attempt": self.attempt, "phase": self.phase, "note": ""})

    # ------------------------------------------------------------------ steps
    def remaining(self) -> float:
        return max(5.0, self.deadline - time.monotonic())

    async def step(self, name: str, body: str, *, token: bool = False, reader: OutputReader | None = None,
                   timeout: float | None = None, quiet: bool = False) -> tuple[int, str, bool]:
        env = dict(self.env)
        if token:
            env["YH_TOKEN"] = self.token

        def out(text: str) -> None:
            if not quiet:
                self.log(reader.feed(text) if reader else text)

        result = await self.shell.run(f"{self.task}-{self.attempt}-{name}", body, env=env, on_output=out, timeout=timeout)
        if reader and not quiet:
            self.log(reader.flush())
        return result

    def need(self, res: tuple[int, str, bool], what: str) -> None:
        code, tail, timed_out = res
        if timed_out:
            raise TaskFailed(f"{what}: it ran out of time (the limit is {self.spec.get('time_limit_min') or 30} min).")
        if code != 0:
            last = tail.strip()[-600:]
            raise TaskFailed(f"{what} (exit code {code})." + (f" Last output: {last}" if last else ""))

    async def run_checks(self, checks: list[str]) -> tuple[list[dict], str]:
        results, failures = [], []
        for i, cmd in enumerate(checks, 1):
            self.log(f"\n$ {cmd}\n")
            t = time.monotonic()
            code, tail, timed_out = await self.step(f"check{i}", f'cd "$YH_WS"\nyh_clean bash -c {q(cmd)}', timeout=self.remaining())
            results.append({"cmd": cmd, "code": code, "ms": _ms(t)})
            if timed_out:
                raise TaskFailed(f"The check `{cmd}` ran out of time.")
            if code != 0:
                failures.append(f"$ {cmd}\n(exit code {code})\n{tail[-3000:]}")
        return results, "\n\n".join(failures)

    async def run(self) -> None:
        self.deadline = time.monotonic() + float(self.spec.get("time_limit_min") or 30) * 60
        beat = asyncio.ensure_future(self._beat())
        ok, error, result = False, None, {}
        prompt_name = f"{self.task}-{self.attempt}.prompt.md"
        name = ADAPTERS.get(self.adapter, self.adapter)
        check_results: list[dict] = []
        try:
            t = time.monotonic()
            self.set_phase("fetch", "Getting the repository")
            self.need(await self.step("prepare", PREPARE, token=True, timeout=self.remaining()), "Couldn't set up the workspace")
            self.timings["fetch"] = _ms(t)
            if self.spec.get("setup"):
                t = time.monotonic()
                self.set_phase("setup", "Running the setup commands")
                self.need(await self.step("setup", commands_script(self.spec["setup"]), timeout=self.remaining()), "A setup command failed")
                self.timings["setup"] = _ms(t)
            rounds = 1 if self.adapter == "command" else max(1, min(5, int(self.spec.get("rounds") or 1)))
            checks = self.spec.get("checks") or []
            feedback, reader, agent_s, checks_s, rnd = None, None, 0.0, 0.0, 0
            for rnd in range(1, rounds + 1):
                t = time.monotonic()
                self.set_phase("agent", f"{name}, round {rnd} of {rounds}" if rounds > 1 else name)
                prompt_path = self.shell.write_file(prompt_name, build_prompt(self.title, self.spec, feedback))
                reader = OutputReader(self.adapter)
                body = f'cd "$YH_WS"\nyh_clean {agent_command(self.adapter, self.spec, prompt_path)}'
                self.need(await self.step(f"agent{rnd}", body, reader=reader, timeout=self.remaining()), f"{name} stopped")
                agent_s += time.monotonic() - t
                if self.adapter == "codex":
                    _, last, _ = await self.step("lastmsg", 'cat "$YH_OUT/last-message.txt" 2>/dev/null || true', timeout=30, quiet=True)
                    if last.strip():
                        reader.summary = last.strip()[:4000]
                if not checks:
                    break
                t = time.monotonic()
                self.set_phase("checks", "Running the checks")
                check_results, failed = await self.run_checks(checks)
                checks_s += time.monotonic() - t
                if not failed:
                    break
                if rnd == rounds:
                    names = ", ".join(c["cmd"] for c in check_results if c["code"] != 0)
                    raise TaskFailed(f"The checks still fail after {rounds} round{'s' if rounds > 1 else ''}: {names}")
                feedback = failed
                self.log(f"\nThe checks failed, so {name} gets another round.\n")
            self.timings["agent"] = int(agent_s * 1000)
            if checks:
                self.timings["checks"] = int(checks_s * 1000)
            t = time.monotonic()
            self.set_phase("commit", "Saving the changes")
            res = await self.step("commit", COMMIT, timeout=max(60.0, self.remaining()))
            self.need(res, "Couldn't save the changes")
            marks = dict(re.findall(r"::yh-(\w+)::(\S+)", res[1]))
            self.timings["commit"] = _ms(t)
            version = next((a.get("version", "") for a in self.host.tools.get("agents", []) if a["id"] == self.adapter), "")
            result = {"summary": reader.final_text() if reader else "", "checks": check_results, "rounds": rnd,
                      "usd": reader.usd if reader else None, "adapter_version": version}
            if "nochange" in marks:
                result["no_changes"] = True
            else:
                result.update(commit=marks.get("commit"), files=int(marks.get("files") or 0))
                t = time.monotonic()
                self.set_phase("upload", "Sending the result")
                self.need(await self.step("upload", UPLOAD, token=True, timeout=max(120.0, self.remaining())), "Couldn't send the result to the pool")
                self.timings["upload"] = _ms(t)
            result["timings_ms"] = self.timings
            ok = True
            await self.step("cleanup", CLEANUP, timeout=120, quiet=True)
        except TaskFailed as e:
            error = str(e)
            result = {"checks": check_results, "timings_ms": self.timings}
        except asyncio.CancelledError:
            self.cancelled = True
        except Exception as e:  # report it rather than leave the task hanging
            log.exception("agent task %s failed", self.task)
            error = f"{type(e).__name__}: {e}"
        finally:
            beat.cancel()
            self.flush_log()
            self.shell.remove_file(prompt_name)
        if error:
            self.log(f"\n{error}\n")
            self.flush_log()
        self.host.finish(self, ok, error, result)


def needs_commands(spec: dict) -> bool:
    """Same rule as the coordinator's rules.needs_commands: the task runs commands of the pool owner's choosing."""
    return (spec.get("adapter") == "command" or bool(spec.get("setup")) or bool(spec.get("checks"))
            or spec.get("autonomy") == "full")


class TaskHost:
    def __init__(self, server: str, home: Path, send: Callable[[dict], None], enabled: bool | None = None, slots: int = 1,
                 allow_commands: bool = False):
        self.server = server.rstrip("/")
        self.send = send
        self.enabled = enabled  # None: allowed on your own computers, not on a friend's
        # On a friend's computer: may the pool's owner run commands here (the Command agent, setup and
        # check commands, an AI agent allowed to run commands)? Off unless started with --allow-commands.
        self.allow_commands = bool(allow_commands)
        self.slots = max(1, min(16, int(slots or 1)))
        self.shell = Shell(home)
        self.tools: dict = {"git": None, "agents": []}
        self.owned: bool | None = None  # from the welcome message: did this computer join with the owner link?
        self.attempts: dict[str, Attempt] = {}
        self.unacked: dict[tuple[str, int], dict] = {}
        self._detected_at = 0.0
        self._reaped = False

    async def detect(self) -> None:
        self._detected_at = time.monotonic()
        if self.shell.available and not self._reaped:
            self._reaped = True
            try:
                n = await self.shell.reap()
                if n:
                    log.warning("Stopped %d agent task step(s) left running by an earlier run.", n)
            except Exception as e:  # never stop the agent from starting
                log.warning("Couldn't check for leftover task steps: %s", e)
        if not self.shell.available:
            self.tools = {"git": None, "agents": [], "problem": "Ubuntu (WSL) isn't set up on this PC." if WINDOWS else "bash isn't installed."}
            return
        wanted = os.environ.get("HARNESS_AGENT_CLIS")
        probe = " ".join(BINARIES.values()) if wanted is None else " ".join(
            BINARIES[a] for a in re.split(r"[\s,]+", wanted) if a in BINARIES)
        try:
            code, out, _ = await self.shell.run("detect", DETECT_SCRIPT, env={"YH_PROBE": probe}, timeout=45)
            self.tools = parse_detect(out)
            if self.tools.get("git") and not self.tools.get("curl"):
                self.tools["problem"] = "curl isn't installed" + (" inside Ubuntu (WSL)." if WINDOWS else ".")
        except Exception as e:  # tasks just stay unavailable here
            log.warning("Couldn't look for agent tools: %s", e)
            self.tools = {"git": None, "agents": []}

    async def refresh(self) -> bool:
        """Look for newly installed tools now and then. True if it looked."""
        if self.attempts or time.monotonic() - self._detected_at < DETECT_EVERY_S:
            return False
        await self.detect()
        return True

    def describe(self) -> dict:
        running = [{"task": a.task, "attempt": a.attempt} for a in self.attempts.values()]
        running += [{"task": t, "attempt": n} for (t, n) in self.unacked if t not in self.attempts]
        return {"enabled": self.enabled, "slots": self.slots, "git": None if self.tools.get("problem") else self.tools.get("git"),
                "problem": self.tools.get("problem"), "agents": self.tools.get("agents", []), "running": running,
                "commands": self.allow_commands}

    def connected(self) -> None:
        """After a reconnect: say again what finished while the connection was down."""
        for msg in list(self.unacked.values()):
            self.send(msg)

    def handle(self, m: dict) -> None:
        kind = m.get("type")
        if kind == "task.start":
            self._start(m)
        elif kind == "task.cancel":
            task, attempt = str(m.get("task") or ""), m.get("attempt")
            a = self.attempts.get(task)
            if a is not None and (attempt is None or a.attempt == attempt):
                log.info("Stopping task %s", task)
                a.cancel()
            self.unacked.pop((task, attempt), None)
        elif kind == "task.ack":
            self.unacked.pop((str(m.get("task") or ""), m.get("attempt")), None)

    def _start(self, m: dict) -> None:
        task, attempt = str(m.get("task") or ""), m.get("attempt")
        if not isinstance(attempt, int):
            return

        def refuse(reason: str) -> None:
            self.send({"type": "task.refuse", "task": task, "attempt": attempt, "reason": reason})

        if not (SAFE_TASK.fullmatch(task) and SAFE_REPO.fullmatch(str(m.get("repo") or "")) and SAFE_SHA.fullmatch(str(m.get("base") or ""))):
            return refuse("The task's details weren't in the expected form.")
        allowed = self.enabled if self.enabled is not None else bool(self.owned)
        if not allowed:
            return refuse("Agent tasks aren't allowed on this computer.")
        current = self.attempts.get(task)
        if current is not None:
            if current.attempt == attempt:
                return
            current.cancel()  # a newer attempt of the same task replaces the old one
            self.attempts.pop(task, None)
        if len(self.attempts) >= self.slots:
            return refuse("This computer is busy with another task.")
        spec = m.get("spec") if isinstance(m.get("spec"), dict) else {}
        if not self.owned and not self.allow_commands and needs_commands(spec):
            return refuse("This is a friend's computer that doesn't allow commands: only AI agents that may only edit "
                          "files, with no setup or check commands. Its owner can allow commands with --allow-commands.")
        if spec.get("adapter") not in {a["id"] for a in self.tools.get("agents", [])}:
            return refuse(f"{ADAPTERS.get(spec.get('adapter'), spec.get('adapter'))} isn't installed on this computer.")
        if not self.tools.get("git") or self.tools.get("problem"):
            return refuse(self.tools.get("problem") or "Git isn't installed on this computer.")
        a = Attempt(self, m, spec)
        self.attempts[task] = a
        log.info("Starting task %s (attempt %s): %s", task, attempt, a.title)
        a.future = asyncio.ensure_future(a.run())

    def finish(self, a: Attempt, ok: bool, error: str | None, result: dict) -> None:
        if self.attempts.get(a.task) is a:
            del self.attempts[a.task]
        if a.cancelled:
            return
        log.info("Task %s %s", a.task, "finished" if ok else f"failed: {error}")
        msg = {"type": "task.done", "task": a.task, "attempt": a.attempt, "ok": ok, "error": error, "result": result}
        self.unacked[(a.task, a.attempt)] = msg
        self.send(msg)

    def stop_all(self) -> None:
        for a in list(self.attempts.values()):
            a.cancel()
