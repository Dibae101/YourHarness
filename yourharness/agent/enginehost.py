"""Batch-job partitions on this computer.

The coordinator sends a partition; this computer runs it in a worker process of
its own (engine/worker.py), one per partition, up to the number of threads it
shares. The worker streams its input and uploads its output with the attempt's
token; this host relays its progress, checkpoints and final report, keeps the final
report until the coordinator acknowledges it (so a dropped connection loses nothing),
and stops the worker when the coordinator cancels the attempt.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import shutil
import sys
from pathlib import Path
from typing import Callable

log = logging.getLogger("yourharness")
PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)
HEARTBEAT_S = float(os.environ.get("HARNESS_ENGINE_BEAT_S", 2))  # the lease is renewed this often while a partition runs
OPS = ["batch.aggregate", "batch.groupby", "batch.transform", "python.map"]
FORMATS = ["text", "jsonl", "csv"]


class EngineHost:
    def __init__(self, server: str, home: Path, send: Callable[[dict], None], slots: int, runtimes=None):
        self.server = server.rstrip("/")
        self.home = Path(home)
        self.work = self.home / "engine"
        self.send = send
        self.slots = max(0, int(slots))
        self.runtimes = runtimes  # the managed-Python environments on this computer, if any
        self.owned: bool | None = None
        self.running: dict[tuple[str, int], asyncio.subprocess.Process | None] = {}
        self.cancelled: set[tuple[str, int]] = set()
        self.unacked: dict[tuple[str, int], dict] = {}
        self.on_busy: Callable[[bool], None] | None = None

    def describe(self) -> dict:
        runtimes = ["builtin", "native-python"]
        if self.runtimes is not None and self.runtimes.available():
            runtimes.append("managed-python")
        running = [{"partition": p, "attempt": n} for (p, n) in self.running]
        running += [{"partition": p, "attempt": n} for (p, n) in self.unacked if (p, n) not in self.running]
        out = {"slots": self.slots, "ops": OPS, "formats": FORMATS, "runtimes": runtimes, "python": platform.python_version(),
               "running": running}
        if self.runtimes is not None:
            out["envs"] = self.runtimes.ready_list()
        return out

    def connected(self) -> None:
        for msg in list(self.unacked.values()):
            self.send(msg)

    def handle(self, m: dict) -> None:
        kind = m.get("type")
        key = (str(m.get("partition") or ""), m.get("attempt"))
        if kind == "eng.start":
            self._start(m)
        elif kind == "eng.cancel":
            self.unacked.pop(key, None)
            if key in self.running:
                self.cancelled.add(key)
                proc = self.running.get(key)
                if proc is not None and proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
        elif kind == "eng.ack":
            self.unacked.pop(key, None)

    def _refuse(self, m: dict, reason: str) -> None:
        self.send({"type": "eng.refuse", "partition": m.get("partition"), "attempt": m.get("attempt"), "reason": reason})

    def _start(self, m: dict) -> None:
        key = (str(m.get("partition") or ""), m.get("attempt"))
        if key in self.running or not isinstance(key[1], int):
            return
        task = m.get("task") if isinstance(m.get("task"), dict) else {}
        kind = (task.get("runtime") or {}).get("kind", "builtin")
        if len(self.running) >= max(1, self.slots):
            return self._refuse(m, "This computer is busy with other partitions.")
        if kind != "builtin" and not self.owned:
            return self._refuse(m, "Your own code runs only on the pool owner's own computers.")
        if kind == "managed-python" and (self.runtimes is None or not self.runtimes.has(task.get("env"))):
            return self._refuse(m, "This computer hasn't prepared the job's Python environment.")
        self.running[key] = None
        self._busy()
        asyncio.ensure_future(self._run(key, m))

    def _busy(self) -> None:
        if self.on_busy:
            self.on_busy(bool(self.running))

    def _command(self, task: dict) -> tuple[list[str], dict]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("HARNESS_TOKEN")}
        env["PYTHONPATH"] = PACKAGE_PARENT + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        env["PYTHONUNBUFFERED"] = "1"
        if (task.get("runtime") or {}).get("kind") == "managed-python" and self.runtimes is not None:
            return [self.runtimes.python(task["env"]), "-m", "yourharness.engine.worker"], self.runtimes.environ(env)
        if getattr(sys, "frozen", False):
            return [sys.executable, "__engine__"], env
        return [sys.executable, "-m", "yourharness.engine.worker"], env

    async def _run(self, key: tuple[str, int], m: dict) -> None:
        pid, n = key
        out_dir = self.work / f"{pid}-{n}"
        result: dict | None = None
        try:
            argv, env = self._command(m["task"])
            proc = await asyncio.create_subprocess_exec(*argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE, env=env, cwd=str(self.home),
                                                        limit=64 * 1024 * 1024)
            self.running[key] = proc
            job = {"server": self.server, "token": m["token"], "partition": pid, "attempt": n, "task": m["task"], "outDir": str(out_dir)}
            assert proc.stdin is not None and proc.stdout is not None
            proc.stdin.write((json.dumps(job) + "\n").encode("utf-8"))
            await proc.stdin.drain()
            proc.stdin.close()
            stderr_task = asyncio.ensure_future(self._drain(proc.stderr, pid))
            seen = {"records": None}

            async def heartbeat():  # renews the lease while the worker lives, whether or not it reports progress
                while True:
                    await asyncio.sleep(HEARTBEAT_S)
                    self.send({"type": "eng.progress", "partition": pid, "attempt": n, "records": seen["records"]})

            beat = asyncio.ensure_future(heartbeat())
            try:
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    t = msg.get("type")
                    if t == "progress":
                        seen["records"] = msg.get("records")
                    elif t == "checkpoint":
                        self.send({"type": "eng.checkpoint", "partition": pid, "attempt": n, "offset": msg.get("offset"), "state": msg.get("state")})
                    elif t in ("done", "error"):
                        result = msg
            finally:
                beat.cancel()
            code = await proc.wait()
            await stderr_task
            if result is None:
                result = {"type": "error", "error": f"The worker process stopped (exit code {code}).", "errorKind": "infra"}
        except Exception as e:  # never leave the coordinator waiting
            log.exception("partition %s failed to run", pid)
            result = {"type": "error", "error": f"{type(e).__name__}: {e}", "errorKind": "infra"}
        finally:
            self.running.pop(key, None)
            self._busy()
            shutil.rmtree(out_dir, ignore_errors=True)
        if key in self.cancelled:
            self.cancelled.discard(key)
            return
        done = {"type": "eng.done", "partition": pid, "attempt": n, "ok": result.get("type") == "done"}
        if done["ok"]:
            done.update({k: v for k, v in result.items() if k != "type"})
        else:
            done.update(error=result.get("error"), errorKind=result.get("errorKind", "infra"))
        self.unacked[key] = done
        self.send(done)

    async def _drain(self, stream, pid: str) -> None:
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                return
            log.debug("partition %s: %s", pid, line.decode("utf-8", "replace").rstrip())

    def stop_all(self) -> None:
        for key, proc in list(self.running.items()):
            self.cancelled.add(key)
            if proc is not None and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
