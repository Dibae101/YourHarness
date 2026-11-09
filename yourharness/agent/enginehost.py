"""Batch-job partitions on this computer.

The coordinator sends a partition; this computer runs it in a worker process of
its own (engine/worker.py), one per partition, up to the number of threads it
shares. The worker streams its input and uploads its output with the attempt's
token; this host relays its progress, checkpoints and final report, keeps the final
report until the coordinator acknowledges it (so a dropped connection loses nothing),
and stops the worker when the coordinator cancels the attempt.

A partition with runtime isolated-oci runs in a container instead (runtime/oci.py).
The container has no network, so this host downloads the partition's input first,
runs the worker inside on those files, and uploads what it wrote once it ends. The
container never sees the attempt's token. Without a checked container this computer
refuses such a partition; it never runs it outside one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import secrets
import shutil
import sys
from pathlib import Path
from typing import Callable

log = logging.getLogger("yourharness")
PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)
HEARTBEAT_S = float(os.environ.get("HARNESS_ENGINE_BEAT_S", 2))  # the lease is renewed this often while a partition runs
OPS = ["batch.aggregate", "batch.groupby", "batch.transform", "python.map", "ml.evaluate", "batch.join"]
FORMATS = ["text", "jsonl", "csv"]


class _Cancelled(Exception):
    pass


class EngineHost:
    def __init__(self, server: str, home: Path, send: Callable[[dict], None], slots: int, runtimes=None, oci=None, ca: str | None = None):
        self.server = server.rstrip("/")
        self.home = Path(home)
        self.work = self.home / "engine"
        self.send = send
        self.slots = max(0, int(slots))
        self.runtimes = runtimes  # the managed-Python environments on this computer, if any
        self.oci = oci  # containers (runtime/oci.py), if this computer has Docker or Podman
        from ..tls import ca_file

        self.ca = ca or ca_file()  # an HTTPS pool's private authority, if it has one (yourharness/tls.py)
        self.owned: bool | None = None
        self.accelerators: list[dict] = []  # GPUs found at start (runtime/inventory.py)
        self.running: dict[tuple[str, int], asyncio.subprocess.Process | None] = {}
        self.containers: dict[tuple[str, int], str] = {}
        self.cancelled: set[tuple[str, int]] = set()
        self.unacked: dict[tuple[str, int], dict] = {}
        self.on_busy: Callable[[bool], None] | None = None
        self.on_activity: Callable[[dict], None] | None = None  # a line in this computer's own activity log

    def describe(self) -> dict:
        runtimes = ["builtin", "native-python"]
        if self.runtimes is not None and self.runtimes.available():
            runtimes.append("managed-python")
        if self.oci is not None and self.oci.available():
            runtimes.append("isolated-oci")
        running = [{"partition": p, "attempt": n} for (p, n) in self.running]
        running += [{"partition": p, "attempt": n} for (p, n) in self.unacked if (p, n) not in self.running]
        out = {"slots": self.slots, "ops": OPS, "formats": FORMATS, "runtimes": runtimes, "python": platform.python_version(),
               "running": running, "accelerators": self.accelerators}
        if self.runtimes is not None:
            out.update(self.runtimes.describe())
        if self.oci is not None:  # its checked images are listed with the Python environments, by id
            out.update(self.oci.describe())
            extra = self.oci.summary()
            out["envs"] = sorted(set(out.get("envs", [])) | set(extra["envs"]))
            out["building"] = sorted(set(out.get("building", [])) | set(extra["building"]))
            out["envErrors"] = {**out.get("envErrors", {}), **extra["envErrors"]}
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
                self._stop(key)
        elif kind == "eng.ack":
            self.unacked.pop(key, None)

    def _stop(self, key: tuple[str, int]) -> None:
        self.cancelled.add(key)
        proc = self.running.get(key)
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        name = self.containers.get(key)
        if name and self.oci is not None:  # killing the docker program doesn't stop its container
            asyncio.ensure_future(self.oci.kill(name))

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
        if kind != "builtin" and not self.owned:  # the same rule as engine/isolation.py, applied here too
            return self._refuse(m, "Your own code runs only on the pool owner's own computers.")
        if kind == "managed-python" and (self.runtimes is None or not self.runtimes.has(task.get("env"))):
            return self._refuse(m, "This computer hasn't prepared the job's Python environment.")
        if kind == "isolated-oci" and (self.oci is None or not self.oci.has(task.get("env"))):
            return self._refuse(m, "This computer hasn't checked the job's container image.")
        if kind not in ("builtin", "native-python", "managed-python", "isolated-oci"):
            return self._refuse(m, f"This computer doesn't know the {kind} runtime.")
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
            return [self.runtimes.python(task["env"]), "-m", "yourharness.engine.worker"], self.runtimes.environ(env, task["env"])
        if getattr(sys, "frozen", False):
            return [sys.executable, "__engine__"], env
        return [sys.executable, "-m", "yourharness.engine.worker"], env

    async def _isolated(self, key: tuple[str, int], m: dict, out_dir: Path):
        """Fetch the partition's input, then the container command and the worker's task: files in, files out, no network."""
        from ..engine.worker import Http, stage_in

        pid, n = key
        task = dict(m["task"], checkpoint=None)  # a container starts its partition from the beginning
        http = Http(self.server, m["token"], pid, n, self.ca)
        in_dir, box = out_dir / "in", out_dir / "box"
        (box / "work").mkdir(parents=True, exist_ok=True)
        await asyncio.get_running_loop().run_in_executor(None, stage_in, http, task, in_dir)
        if key in self.cancelled:
            raise _Cancelled()
        self.oci.open_dirs(box, box / "work")
        name = f"yh-{secrets.token_hex(6)}"
        self.containers[key] = name
        rt = task["runtime"]
        argv = self.oci.argv(name, in_dir, box, rt["image"], rt["limits"]) + ["-m", "yourharness.engine.worker"]
        job = {"staged": "/in", "upDir": "/out/up", "partition": pid, "attempt": n, "task": task, "outDir": "/out/work", "cache": "/out/cache"}
        return argv, self.oci.environ(), job, http, box

    @staticmethod
    def _send_outputs(http, up: Path, result: dict) -> None:
        """After a container ends: upload what it wrote, checking the coordinator stored the same bytes."""
        for name, info in (result.get("outputs") or {}).items():
            got = http.upload(name, up / name)
            if got.get("sha") != info.get("sha"):
                raise ConnectionError("the coordinator stored different bytes than the container wrote")

    async def _heartbeat(self, pid: str, n: int, seen: dict) -> None:
        """Renews the lease while the partition is being worked on, whether or not it reports progress."""
        while True:
            await asyncio.sleep(HEARTBEAT_S)
            self.send({"type": "eng.progress", "partition": pid, "attempt": n, "records": seen["records"]})

    async def _run(self, key: tuple[str, int], m: dict) -> None:
        pid, n = key
        out_dir = self.work / f"{pid}-{n}"
        result: dict | None = None
        iso = (m["task"].get("runtime") or {}).get("kind") == "isolated-oci"
        seen = {"records": None}
        beat = asyncio.ensure_future(self._heartbeat(pid, n, seen))
        self._activity("started", m)
        try:
            if iso:
                argv, env, job, http, box = await self._isolated(key, m, out_dir)
            else:
                argv, env = self._command(m["task"])
                job = {"server": self.server, "token": m["token"], "partition": pid, "attempt": n, "task": m["task"], "outDir": str(out_dir),
                       "cache": str(self.home / "engine-cache"), "ca": self.ca}
            proc = await asyncio.create_subprocess_exec(*argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE, env=env, cwd=str(self.home),
                                                        limit=64 * 1024 * 1024)
            self.running[key] = proc
            if key in self.cancelled:
                proc.kill()
            assert proc.stdin is not None and proc.stdout is not None
            proc.stdin.write((json.dumps(job) + "\n").encode("utf-8"))
            await proc.stdin.drain()
            proc.stdin.close()
            stderr_task = asyncio.ensure_future(self._drain(proc.stderr, pid))
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
            code = await proc.wait()
            await stderr_task
            if result is None:
                result = {"type": "error", "error": f"The worker process stopped (exit code {code}).", "errorKind": "infra"}
            if iso and result.get("type") == "done" and key not in self.cancelled:
                await asyncio.get_running_loop().run_in_executor(None, self._send_outputs, http, box / "up", result)
        except _Cancelled:
            pass
        except Exception as e:  # never leave the coordinator waiting
            log.exception("partition %s failed to run", pid)
            result = {"type": "error", "error": f"{type(e).__name__}: {e}", "errorKind": "infra"}
        finally:
            beat.cancel()
            self.running.pop(key, None)
            self.containers.pop(key, None)
            self._busy()
            shutil.rmtree(out_dir, ignore_errors=True)
        if key in self.cancelled:
            self.cancelled.discard(key)
            self._activity("cancelled", m)
            return
        done = {"type": "eng.done", "partition": pid, "attempt": n, "ok": result.get("type") == "done"}
        if done["ok"]:
            done.update({k: v for k, v in result.items() if k != "type"})
        else:
            done.update(error=result.get("error"), errorKind=result.get("errorKind", "infra"))
        self._activity("finished" if done["ok"] else "failed", m, bytes=done.get("bytes"), records=done.get("records"))
        self.unacked[key] = done
        self.send(done)

    def _activity(self, what: str, m: dict, **extra) -> None:
        if self.on_activity is None:
            return
        task = m.get("task") or {}
        self.on_activity({"kind": "partition", "what": what, "job": m.get("job"), "partition": m.get("partition"), "attempt": m.get("attempt"),
                          "workload": (task.get("workload") or {}).get("type"), "runtime": (task.get("runtime") or {}).get("kind", "builtin"),
                          **{k: v for k, v in extra.items() if v is not None}})

    async def _drain(self, stream, pid: str) -> None:
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                return
            log.debug("partition %s: %s", pid, line.decode("utf-8", "replace").rstrip())

    def stop_all(self) -> None:
        for key in list(self.running):
            self._stop(key)
