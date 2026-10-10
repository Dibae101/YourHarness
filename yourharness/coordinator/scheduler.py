"""Devices, jobs and the scheduler.

Scheduling is pull based: a device gets a new piece whenever it has a free
thread, so fast devices naturally take more and slow ones never hold up a run.

Every device sends a small "beat" message every few seconds. A device that goes
quiet (a phone whose screen locked, a frozen tab) is marked as not responding and
its pieces go to other devices. This catches frozen pages that a WebSocket ping
can't, because browsers answer pings from their network layer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from pathlib import Path

from . import files as shared_files
from .jobkinds import KINDS, PlanContext, UserError, clamp_int, clamp_num
from .pool_store import PoolStore
from .remote_fs import RemoteFS
from .terminals import Terminals

log = logging.getLogger("yourharness")

WIRE_COLORS = ["#E0A100", "#2563C9", "#D1433B", "#23935A", "#8B5CC4", "#E0742A", "#1795A8", "#C2417D"]
OUTBOX_LIMIT = 5000


def now_ms() -> int:
    return int(time.time() * 1000)


def clean_text(v, max_len: int, fallback: str) -> str:
    s = re.sub(r"[\x00-\x1f\x7f]", "", "" if v is None else str(v)).strip()[:max_len]
    return s or fallback


def _num_or_none(v, lo, hi):
    if v is None or isinstance(v, bool):
        return None
    n = clamp_num(v, lo, hi, None)
    return None if n is None else round(n, 1)


def sanitize_info(info) -> dict:
    info = info if isinstance(info, dict) else {}
    gpus = info.get("gpus") if isinstance(info.get("gpus"), list) else []
    return {
        "platform": clean_text(info.get("platform"), 30, "Device"),
        "os": clean_text(info.get("os"), 60, ""),
        "arch": clean_text(info.get("arch"), 20, ""),
        "python": clean_text(info.get("python"), 20, ""),
        "cores": clamp_int(info.get("cores"), 1, 1024, 1),
        "memoryGB": _num_or_none(info.get("memoryGB"), 0, 65536),
        "memoryFreeGB": _num_or_none(info.get("memoryFreeGB"), 0, 65536),
        "diskFreeGB": _num_or_none(info.get("diskFreeGB"), 0, 10**7),
        "storageGB": _num_or_none(info.get("storageGB"), 0, 10**7),
        "gpu": bool(info.get("gpu")) or bool(gpus),
        "gpus": [clean_text(g, 80, "GPU") for g in gpus[:4] if isinstance(g, str)],
    }


def sanitize_workspace(ws) -> dict | None:
    if not isinstance(ws, dict):
        return None
    return {"shell": clean_text(ws.get("shell"), 20, "shell"), "shared": bool(ws.get("shared")),
            "system": clean_text(ws.get("system"), 60, "Linux"), "harness": bool(ws.get("harness"))}


class Device:
    def __init__(self, rec: dict, ws, hello: dict, color: str, host: str = ""):
        self.host = host
        self.id: str = rec["id"]
        self.kind: str = rec["kind"]
        self.ws = ws
        self.color = color
        self.name = clean_text(hello.get("name"), 40, rec.get("name") or "Unnamed device")
        self.info = sanitize_info(hello.get("info"))
        self.threads = clamp_int(hello.get("threads"), 1, 256, 1)
        self.sharing = hello.get("sharing") is not False
        offered = hello.get("runtimes") if isinstance(hello.get("runtimes"), list) else []
        allowed = {"python"} if self.kind == "agent" else {"js"}
        self.runtimes = frozenset(r for r in offered if r in allowed) or frozenset(allowed)
        self.workspace = sanitize_workspace(hello.get("workspace")) if self.kind == "agent" else None
        self.coordinator = self.kind == "agent" and bool(hello.get("coordinator"))  # the computer that runs the pool
        self.files = self.kind == "agent" and isinstance(hello.get("files"), dict)  # lets the Files app browse its home folder
        self.load: dict = {}  # live CPU and memory use, reported every few seconds
        self.inflight: set[tuple[str, int]] = set()
        self.params_sent: set[str] = set()
        self.done = 0
        self.busy_ms = 0.0
        self.joined_at = now_ms()
        self.last_msg = time.monotonic()
        self.stale = False
        self.closed = False
        self._close: tuple[int, str] | None = None
        self.outbox: asyncio.Queue = asyncio.Queue()
        self.writer = asyncio.ensure_future(self._write())

    def send(self, msg: dict) -> None:
        self.send_raw(json.dumps(msg, separators=(",", ":")))

    def send_raw(self, data: str) -> None:
        if self.closed:
            return
        if self.outbox.qsize() > OUTBOX_LIMIT:
            log.warning("%s fell too far behind; disconnecting it", self.name)
            self.close(4008, "Too far behind")
            return
        self.outbox.put_nowait(data)

    def close(self, code: int, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        self._close = (code, reason)
        self.outbox.put_nowait(None)

    async def _write(self) -> None:
        try:
            while True:
                data = await self.outbox.get()
                if data is None:
                    break
                await self.ws.send_str(data)
            code, reason = self._close or (1000, "")
            await self.ws.close(code=code, message=reason.encode())
        except Exception:  # the socket went away; the reader side cleans up
            pass

    def can_take(self, job: "Job") -> bool:
        return (self.sharing and not self.stale and not self.closed
                and bool(self.runtimes & job.runtimes) and self.threads - len(self.inflight) > 0)


class Job:
    def __init__(self, job_id: str, kind: str, plan, created_by: str):
        self.id = job_id
        self.kind = kind
        self.title = plan.title
        self.params = plan.params
        self.data = plan.data
        self.total = plan.total
        self.runtimes = plan.runtimes
        self.pending: deque[int] = deque(range(plan.total))
        self.pending_set: set[int] = set(range(plan.total))
        self.inflight: dict[int, tuple[str, float]] = {}
        self.results: list = [None] * plan.total
        self.by: list[str | None] = [None] * plan.total
        self.attempts = [0] * plan.total
        self.done_count = 0
        self.per_device: dict[str, dict] = {}
        self.status = "queued"
        self.created_by = created_by
        self.created_at = now_ms()
        self.started_at: int | None = None
        self.finished_at: int | None = None
        self.result = None
        self.error: str | None = None
        self.waiters: list[asyncio.Future] = []

    @property
    def finished(self) -> bool:
        return self.status in ("done", "failed", "cancelled")

    def requeue(self, idx: int, front: bool = True) -> None:
        if self.by[idx] is None and idx not in self.pending_set:
            (self.pending.appendleft if front else self.pending.append)(idx)
            self.pending_set.add(idx)

    def take(self) -> int:
        idx = self.pending.popleft()
        self.pending_set.discard(idx)
        return idx

    def summary(self) -> dict:
        return {
            "id": self.id, "kind": self.kind, "title": self.title, "params": self.params, "total": self.total,
            "done": self.done_count, "status": self.status, "perDevice": self.per_device, "runtimes": sorted(self.runtimes),
            "createdBy": self.created_by, "createdAt": self.created_at, "startedAt": self.started_at,
            "finishedAt": self.finished_at, "result": self.result, "error": self.error,
        }


class Scheduler:
    def __init__(self, store, shared_dir: Path, *, stale_s: float = 20.0, chunk_timeout_s: float = 300.0, history: int = 20):
        self.store = store
        self.shared_dir = Path(shared_dir)
        self.stale_s = stale_s
        self.chunk_timeout_s = chunk_timeout_s
        self.history = history
        self.devices: dict[str, Device] = {}
        self.jobs: list[Job] = []
        self.seq = 0
        self.file_meta: dict[str, dict] = {}
        self.terminals = Terminals(self)
        self.pool_store = PoolStore(self, store.dir)
        self.remote_fs = RemoteFS(self)
        self.public_url = ""  # the address other devices use; set once the port is known
        self.repair_s = float(os.environ.get("HARNESS_REPAIR_S", 10))
        self._devices_pending = False

    # ------------------------------------------------------------------ messaging
    def broadcast(self, msg: dict) -> None:
        data = json.dumps(msg, separators=(",", ":"))
        for d in list(self.devices.values()):
            d.send_raw(data)

    def broadcast_devices(self) -> None:
        if self._devices_pending:
            return
        self._devices_pending = True

        def flush():
            self._devices_pending = False
            self.broadcast({"type": "devices", "devices": self.device_list()})

        asyncio.get_running_loop().call_later(0.25, flush)

    def broadcast_files(self) -> None:
        self.broadcast({"type": "files", "files": self.list_files()})

    def list_files(self) -> list[dict]:
        return shared_files.list_files(self.shared_dir, self.file_meta)

    @staticmethod
    def device_view(d: Device) -> dict:
        return {
            "id": d.id, "name": d.name, "kind": d.kind, "color": d.color, "info": d.info, "threads": d.threads,
            "sharing": d.sharing, "runtimes": sorted(d.runtimes), "workspace": d.workspace, "busy": len(d.inflight),
            "done": d.done, "busyMs": round(d.busy_ms), "joinedAt": d.joined_at, "stale": d.stale, "host": d.host, "coordinator": d.coordinator, "files": d.files, "load": d.load,
        }

    def device_list(self) -> list[dict]:
        return [self.device_view(d) for d in self.devices.values()]

    def snapshot(self, job: Job | None) -> dict | None:
        if job is None:
            return None
        return {
            "job": job.summary(),
            "colors": [job.per_device.get(b, {}).get("color") if b else None for b in job.by],
            "values": job.results if job.kind == "mandelbrot" else None,
        }

    # ------------------------------------------------------------------ devices
    def pick_color(self, rec: dict) -> str:
        in_use = {d.color for d in self.devices.values() if d.id != rec["id"]}
        if rec.get("color") and rec["color"] not in in_use:
            return rec["color"]
        return next((c for c in WIRE_COLORS if c not in in_use), WIRE_COLORS[len(self.devices) % len(WIRE_COLORS)])

    def connect(self, rec: dict, ws, hello: dict, host: str = "") -> Device:
        previous = self.devices.pop(rec["id"], None)
        if previous is not None:  # the same device connected again, from another tab or after a drop
            self.requeue_device(previous)
            self.terminals.device_gone(previous)
            self.pool_store.device_gone(previous)
            self.remote_fs.device_gone(previous)
            previous.close(4002, "This device connected again from somewhere else.")
        d = Device(rec, ws, hello, self.pick_color(rec), host)
        self.store.update_device(d.id, name=d.name, color=d.color)
        self.devices[d.id] = d
        latest = self.running_job() or (self.jobs[-1] if self.jobs else None)
        d.send({
            "type": "welcome", "id": d.id, "name": d.name, "color": d.color, "kind": d.kind,
            "devices": self.device_list(), "jobs": [j.summary() for j in self.jobs], "snapshot": self.snapshot(latest),
            "files": self.list_files(), "joinToken": self.store.join_token, "joinCode": self.store.join_code, "pool": self.pool_store.listing(), "address": self.public_url,
        })
        log.info("%s joined (%s, %s threads)", d.name, d.kind, d.threads)
        self.broadcast_devices()
        self.dispatch()
        return d

    def disconnect(self, d: Device) -> None:
        d.close(1000, "")
        if self.devices.get(d.id) is d:
            del self.devices[d.id]
            self.requeue_device(d)
            self.terminals.device_gone(d)
            self.pool_store.device_gone(d)
            self.remote_fs.device_gone(d)
            log.info("%s left", d.name)
            self.pool_store.changed()
            self.broadcast_devices()
            self.dispatch()

    def remove_device(self, device_id: str) -> bool:
        removed = self.store.remove_device(device_id)
        d = self.devices.get(device_id)
        if d is not None:
            d.send({"type": "denied", "reason": "This device was removed from the pool. Open a new join link to add it again."})
            d.close(4401, "Removed")
            self.disconnect(d)
        return removed or d is not None

    def requeue_device(self, d: Device) -> None:
        for job_id, idx in list(d.inflight):
            job = self.find_job(job_id)
            if job and job.status == "running" and job.inflight.get(idx, (None,))[0] == d.id:
                del job.inflight[idx]
                job.requeue(idx)
        d.inflight.clear()

    # ------------------------------------------------------------------ messages from devices
    def handle(self, d: Device, msg: dict) -> None:
        d.last_msg = time.monotonic()
        if d.stale:
            d.stale = False
            log.info("%s is responding again", d.name)
            self.broadcast_devices()
            self.dispatch()
        kind = msg.get("type")
        if kind == "beat":
            return
        if kind == "ping":  # the page measures how quickly the pool answers
            d.send({"type": "pong", "t": msg.get("t")})
            return
        if kind == "update":
            if "name" in msg:
                d.name = clean_text(msg["name"], 40, d.name)
                self.store.update_device(d.id, name=d.name)
            if "threads" in msg:
                d.threads = clamp_int(msg["threads"], 1, 256, d.threads)
            if "sharing" in msg:
                d.sharing = bool(msg["sharing"])
            if "info" in msg:
                d.info = sanitize_info(msg["info"])
            if isinstance(msg.get("load"), dict):
                d.load = {"cpu": _num_or_none(msg["load"].get("cpu"), 0, 100), "memUsedGB": _num_or_none(msg["load"].get("memUsedGB"), 0, 65536)}
                self.broadcast_devices()
                return  # a load report changes nothing the scheduler acts on
            self.broadcast_devices()
            self.dispatch()
        elif kind == "result":
            self.accept_result(d, msg)
        elif kind == "failed":
            self.chunk_failed(d, msg)
        elif kind == "release":
            self.release(d, msg)
        elif kind == "submit":
            spec = msg.get("job") if isinstance(msg.get("job"), dict) else {}
            try:
                self.create_job(spec.get("kind"), spec.get("params"), d.name)
            except UserError as e:
                d.send({"type": "error", "message": str(e)})
        elif kind == "cancel":
            job = self.find_job(msg.get("jobId"))
            if job:
                self.stop_job(job, "cancelled", f"Cancelled by {d.name}.")
        elif kind == "getSnapshot":
            job = self.find_job(msg.get("jobId"))
            if job:
                d.send({"type": "snapshot", "snapshot": self.snapshot(job)})
        elif kind in ("fs.reply", "fs.error"):
            if d.kind == "agent":
                self.remote_fs.handle(d, msg)
        elif isinstance(kind, str) and kind.startswith("store."):
            if d.kind == "agent":
                self.pool_store.handle(d, msg)
        elif isinstance(kind, str) and kind.startswith("term."):
            if d.kind == "agent":
                self.terminals.from_agent(d, msg)
            else:
                self.terminals.from_browser(d, msg)

    # ------------------------------------------------------------------ jobs
    def find_job(self, job_id) -> Job | None:
        return next((j for j in self.jobs if j.id == job_id), None)

    def running_job(self) -> Job | None:
        return next((j for j in self.jobs if j.status == "running"), None)

    def create_job(self, kind, params, created_by: str) -> Job:
        spec = KINDS.get(kind) if isinstance(kind, str) else None
        if spec is None:
            raise UserError("Unknown job type.")
        plan = spec.plan(params if isinstance(params, dict) else {}, PlanContext(self.shared_dir))
        self.seq += 1
        job = Job(f"j{self.seq}", kind, plan, created_by)
        self.jobs.append(job)
        while len(self.jobs) > self.history:  # keep a short history, never dropping live jobs
            old = next((i for i, j in enumerate(self.jobs) if j.finished), None)
            if old is None:
                break
            self.jobs.pop(old)
        log.info("%s started: %s", created_by, job.title)
        self.broadcast({"type": "job", "job": job.summary()})
        self.start_next()
        return job

    def start_next(self) -> None:
        if self.running_job():
            return
        job = next((j for j in self.jobs if j.status == "queued"), None)
        if job is None:
            return
        job.status = "running"
        job.started_at = now_ms()
        self.broadcast({"type": "job", "job": job.summary()})
        self.dispatch()

    def dispatch(self) -> None:
        job = self.running_job()
        if job is None:
            return
        spec = KINDS[job.kind]
        gave = True
        while gave and job.pending:
            gave = False
            for d in list(self.devices.values()):
                if not job.pending:
                    break
                if not d.can_take(job):
                    continue
                idx = job.take()
                job.inflight[idx] = (d.id, time.monotonic())
                d.inflight.add((job.id, idx))
                msg = {"type": "chunk", "jobId": job.id, "kind": job.kind, "idx": idx, "total": job.total}
                if job.id not in d.params_sent:  # send the job's params once per device, not with every piece
                    msg["params"] = job.params
                    d.params_sent.add(job.id)
                piece = spec.piece(job, idx)
                if piece is not None:
                    msg["piece"] = piece
                d.send(msg)
                gave = True
        self.broadcast_devices()

    def accept_result(self, d: Device, msg: dict) -> None:
        job_id, idx = msg.get("jobId"), msg.get("idx")
        d.inflight.discard((job_id, idx))
        job = self.find_job(job_id)
        if not job or job.status != "running" or not isinstance(idx, int) or not 0 <= idx < job.total or job.by[idx] is not None:
            self.dispatch()
            return
        if not KINDS[job.kind].check(job, idx, msg.get("value")):
            self.chunk_failed(d, {"jobId": job_id, "idx": idx, "error": "Returned a result in the wrong format."})
            return
        holder = job.inflight.pop(idx, None)
        if holder and holder[0] != d.id and holder[0] in self.devices:
            self.devices[holder[0]].inflight.discard((job_id, idx))
        if idx in job.pending_set:
            job.pending.remove(idx)
            job.pending_set.discard(idx)
        ms = clamp_num(msg.get("ms"), 0, 1e9, 0)
        job.results[idx] = msg.get("value")
        job.by[idx] = d.id
        job.done_count += 1
        pd = job.per_device.setdefault(d.id, {"name": d.name, "color": d.color, "chunks": 0, "ms": 0})
        pd["chunks"] += 1
        pd["ms"] += ms
        d.done += 1
        d.busy_ms += ms
        self.broadcast({
            "type": "progress", "jobId": job.id, "idx": idx, "deviceId": d.id, "deviceName": d.name, "color": d.color,
            "ms": ms, "done": job.done_count, "total": job.total,
            "value": msg.get("value") if job.kind == "mandelbrot" else None,
        })
        if job.done_count == job.total:
            self.finish_job(job)
        else:
            self.dispatch()

    def chunk_failed(self, d: Device, msg: dict) -> None:
        job_id, idx = msg.get("jobId"), msg.get("idx")
        d.inflight.discard((job_id, idx))
        job = self.find_job(job_id)
        if not job or job.status != "running" or not isinstance(idx, int) or not 0 <= idx < job.total or job.by[idx] is not None:
            self.dispatch()
            return
        if job.inflight.get(idx, (None,))[0] == d.id:
            del job.inflight[idx]
        job.attempts[idx] += 1
        reason = clean_text(msg.get("error"), 500, "Unknown error")
        # Code that throws will throw again on the next device, so stop right away.
        if job.kind in ("custom", "pymap") or job.attempts[idx] >= 3:
            self.stop_job(job, "failed", f"Piece {idx + 1} failed on {d.name}: {reason}")
            return
        job.requeue(idx)
        self.dispatch()

    def release(self, d: Device, msg: dict) -> None:
        """A device stopped sharing before it started this piece."""
        job_id, idx = msg.get("jobId"), msg.get("idx")
        d.inflight.discard((job_id, idx))
        job = self.find_job(job_id)
        if job and job.status == "running" and isinstance(idx, int) and 0 <= idx < job.total:
            if job.inflight.get(idx, (None,))[0] == d.id:
                del job.inflight[idx]
                job.requeue(idx)
        self.dispatch()

    def _clear_job_from_devices(self, job: Job) -> None:
        for d in self.devices.values():
            d.inflight = {k for k in d.inflight if k[0] != job.id}
            d.params_sent.discard(job.id)

    def _wake_waiters(self, job: Job) -> None:
        for fut in job.waiters:
            if not fut.done():
                fut.set_result(None)
        job.waiters.clear()

    def finish_job(self, job: Job) -> None:
        try:
            job.result = KINDS[job.kind].reduce(job)
            job.status = "done"
        except UserError as e:
            job.status, job.error = "failed", str(e)
        except Exception as e:  # a bug in combining shouldn't take the coordinator down
            log.exception("combining %s failed", job.id)
            job.status, job.error = "failed", f"Could not combine the results: {e}"
        job.finished_at = now_ms()
        job.pending.clear()
        job.pending_set.clear()
        job.inflight.clear()
        self._clear_job_from_devices(job)
        log.info("%s %s in %.1f s", job.title, job.status, (job.finished_at - (job.started_at or job.finished_at)) / 1000)
        self.broadcast({"type": "job", "job": job.summary()})
        self.broadcast_devices()
        self._wake_waiters(job)
        self.start_next()

    def stop_job(self, job: Job, status: str, error: str | None = None) -> None:
        if job.finished:
            return
        was_running = job.status == "running"
        job.status = status
        job.error = error
        job.finished_at = now_ms()
        job.pending.clear()
        job.pending_set.clear()
        job.inflight.clear()
        self._clear_job_from_devices(job)
        if was_running:
            self.broadcast({"type": "abort", "jobId": job.id})
        self.broadcast({"type": "job", "job": job.summary()})
        self.broadcast_devices()
        self._wake_waiters(job)
        self.start_next()

    async def wait_job(self, job: Job, timeout: float) -> None:
        if job.finished:
            return
        fut = asyncio.get_running_loop().create_future()
        job.waiters.append(fut)
        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            if fut in job.waiters:
                job.waiters.remove(fut)

    # ------------------------------------------------------------------ housekeeping
    def sweep(self) -> None:
        now = time.monotonic()
        changed = False
        for d in list(self.devices.values()):
            if not d.stale and now - d.last_msg > self.stale_s:
                d.stale = True
                changed = True
                log.info("%s stopped responding; its pieces go to other devices", d.name)
                self.requeue_device(d)
        job = self.running_job()
        if job:
            for idx, (device_id, t) in list(job.inflight.items()):
                if now - t > self.chunk_timeout_s:
                    del job.inflight[idx]
                    holder = self.devices.get(device_id)
                    if holder:
                        holder.inflight.discard((job.id, idx))
                    job.requeue(idx, front=False)
                    changed = True
        if changed:
            self.broadcast_devices()
            self.dispatch()

    async def run_maintenance(self, every_s: float = 1.0) -> None:
        last_repair = 0.0
        while True:
            await asyncio.sleep(every_s)
            try:
                self.sweep()
                if time.monotonic() - last_repair >= self.repair_s:
                    last_repair = time.monotonic()
                    asyncio.ensure_future(self.pool_store.repair())
            except Exception:
                log.exception("maintenance failed")
