"""Batch jobs on the coordinator: datasets, jobs, partitions, attempts, results.

A job's partitions are handed to computers as they have room (pull, so faster
computers take more). Each hand-off is an attempt with its own number and token: the
token reads only that partition's input and writes only that attempt's outputs.
While a computer works it renews a lease; when the lease runs out (the computer left
or froze) the partition goes to another computer, keeping any checkpoint it reached.

Exactly one result per partition is accepted: the first valid one. A late result
from an attempt that was given up on still counts if no other attempt finished
first, because a partition computes the same answer wherever it runs. Once every
partition is accepted, the states are combined by the job's reducer, an optional
spot check recomputes some partitions on the coordinator and compares them, and
the job is done. All of this is saved, so a restart picks up where it left off.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import secrets
import time
from pathlib import Path

from ..coordinator.state import CAP_SUBMIT
from . import spec as jobspec
from .db import EngineDB
from .execute import dump_record, make_op, run_items
from .index import Indexer
from .local import run_range
from .ops import halo_records_needed
from .partition import check_ranges, plan_items, plan_ranges
from .planner import Planner
from .records import iter_records
from .reducer import final_result, merge_states
from .store import ArtifactStore, StoreError

log = logging.getLogger("yourharness")

MAX_DATASET = int(float(os.environ.get("HARNESS_MAX_DATASET_GB", 64)) * 2**30)
MAX_OUTPUT = 4 * 2**30
INLINE_RESULT = 256 * 1024
MAX_MOVES = 12
CLASSIFICATIONS = ("owned-private", "shareable")
FORMATS = ("text", "jsonl", "csv")
FINISHED = ("done", "failed", "cancelled")
LIVE_ATTEMPT = ("running", "lost")
NAME_OK = re.compile(r"[a-z0-9][a-z0-9._-]{0,39}")


class EngineError(ValueError):
    """A problem the person can fix; shown as it is."""


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _canon(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)


def guess_format(name: str) -> str:
    low = name.lower()
    if low.endswith((".jsonl", ".ndjson", ".json")):
        return "jsonl"
    if low.endswith((".csv", ".tsv")):
        return "csv"
    return "text"


class EngineManager:
    def __init__(self, sched, data_dir: Path):
        self.s = sched
        self.dir = Path(data_dir) / "engine"
        self.db = EngineDB(self.dir / "engine.db")
        self.store = ArtifactStore(self.dir / "artifacts", self.db)
        self.planner = Planner(self)
        self.started = False
        self.index_cache: dict[str, list[int]] = {}
        self.refused: dict[tuple[str, str], float] = {}  # (job, device) -> until when
        self.reducing: set[str] = set()
        self._bg: set[asyncio.Future] = set()
        self._dirty: set[str] = set()
        self._flush_at = None
        self._last_sweep = 0.0
        self._last_gc = 0.0
        self.runtimes = None  # set by the runtime manager (managed Python), when there is one

    # ------------------------------------------------------------------ start-up
    def start(self) -> None:
        if self.started:
            return
        self.started = True
        now = time.time()
        for p in self.db.all("SELECT * FROM partitions WHERE status = 'leased'"):
            job = self.db.job(p["job_id"])
            lease = (job["spec"]["spec"]["retry"]["leaseSeconds"] if job else 45) + 30
            self.db.update("partitions", {"id": p["id"]}, {"lease_until": now + lease})  # time for its computer to reconnect
            self._reserve(job, p["device_id"], p["id"], p["attempt"])
        for j in self.db.all("SELECT id FROM jobs WHERE status = 'reducing'"):
            self._spawn(self._reduce(j["id"]))

    def _spawn(self, coro) -> None:
        fut = asyncio.ensure_future(coro)
        self._bg.add(fut)

        def done(f):
            self._bg.discard(f)
            if not f.cancelled() and f.exception() is not None:
                log.error("job engine work failed", exc_info=f.exception())

        fut.add_done_callback(done)

    # ------------------------------------------------------------------ datasets
    async def add_dataset(self, name: str, fmt: str | None, classification: str | None, chunks, size_hint: int | None,
                          by: str, source: str) -> dict:
        name = (name or "dataset").strip()[:200] or "dataset"
        fmt = fmt or guess_format(name)
        if fmt not in FORMATS:
            raise EngineError(f"The format should be one of {', '.join(FORMATS)}.")
        classification = classification or "owned-private"
        if classification not in CLASSIFICATIONS:
            raise EngineError("Mark the dataset owned-private or shareable.")
        ix = Indexer(fmt, size_hint)
        try:
            sha, size = await self.store.put_stream(chunks, "dataset", MAX_DATASET, observe=ix.feed)
            meta = ix.finish()
        except (ValueError, StoreError) as e:
            raise EngineError(str(e)) from None
        splits = meta.pop("splits")
        meta["indexSha"] = self.store.put_json(splits, "index")
        ds_id = "d" + secrets.token_hex(4)
        self.db.insert("datasets", {"id": ds_id, "name": name, "format": fmt, "sha": sha, "size": size, "meta": meta,
                                    "classification": classification, "created_by": by, "created_at": time.time(), "source": source})
        self.index_cache[ds_id] = splits
        self.db.event(None, "dataset", {"id": ds_id, "name": name, "size": size, "by": by})
        self.s.broadcast({"type": "eng.datasets"}, need=CAP_SUBMIT)
        return self.dataset_view(self.db.dataset(ds_id))

    def dataset_meta(self, ds: dict) -> dict:
        m = dict(ds["meta"])
        m.update(id=ds["id"], format=ds["format"], size=ds["size"], sha=ds["sha"], classification=ds["classification"])
        return m

    def splits(self, ds: dict) -> list[int]:
        got = self.index_cache.get(ds["id"])
        if got is None:
            got = self.index_cache[ds["id"]] = self.store.read_json(ds["meta"]["indexSha"])
        return got

    def dataset_view(self, ds: dict) -> dict:
        m = ds["meta"]
        return {"id": ds["id"], "ref": f"dataset:{ds['id']}", "name": ds["name"], "format": ds["format"], "size": ds["size"],
                "lines": m.get("lines"), "header": m.get("header"), "classification": ds["classification"],
                "createdBy": ds["created_by"], "createdAt": ds["created_at"], "source": ds["source"], "sha": ds["sha"],
                "quoted": bool(m.get("quoted"))}

    def datasets(self) -> list[dict]:
        return [self.dataset_view(d) for d in self.db.all("SELECT * FROM datasets ORDER BY created_at DESC")]

    async def preview(self, ds_id: str, n: int = 20) -> list:
        ds = self.db.dataset(ds_id)
        if ds is None:
            raise EngineError("There's no dataset with that id.")
        m = ds["meta"]
        start = m["dataStart"]
        end = min(ds["size"], start + 256 * 1024)
        data = b"".join([c async for c in self.store.stream_range(ds["sha"], start, end)])
        out = []
        for _s, _e, rec in iter_records(ds["format"], [data[: data.rfind(b"\n") + 1] if end < ds["size"] else data], start,
                                         m.get("header"), on_bad=lambda e: out.append({"_bad": e.reason})):
            out.append(rec)
            if len(out) >= n:
                break
        return out[:n]

    def set_classification(self, ds_id: str, classification: str) -> dict:
        if classification not in CLASSIFICATIONS:
            raise EngineError("Mark the dataset owned-private or shareable.")
        if self.db.dataset(ds_id) is None:
            raise EngineError("There's no dataset with that id.")
        self.db.update("datasets", {"id": ds_id}, {"classification": classification})
        return self.dataset_view(self.db.dataset(ds_id))

    def delete_dataset(self, ds_id: str) -> None:
        ds = self.db.dataset(ds_id)
        if ds is None:
            raise EngineError("There's no dataset with that id.")
        users = [j for j in self.db.all("SELECT id, spec, status FROM jobs WHERE status NOT IN ('done', 'failed', 'cancelled')")
                 if j["spec"]["spec"]["inputs"].get("dataset", {}).get("ref") == f"dataset:{ds_id}"]
        if users:
            raise EngineError("A job that's still running reads this dataset. Cancel it first.")
        self.db.conn.execute("DELETE FROM datasets WHERE id = ?", (ds_id,))
        self.index_cache.pop(ds_id, None)
        self.s.broadcast({"type": "eng.datasets"}, need=CAP_SUBMIT)

    # ------------------------------------------------------------------ jobs
    def _resolve(self, body) -> tuple[dict, dict | None]:
        try:
            job = jobspec.parse(body.get("job") if isinstance(body, dict) and "job" in body else body)
        except jobspec.SpecError as e:
            raise EngineError(str(e)) from None
        ds = None
        ref = job["spec"]["inputs"].get("dataset", {}).get("ref")
        if ref:
            ds = self.db.dataset(ref.split(":", 1)[1])
            if ds is None:
                raise EngineError(f"There's no dataset {ref}.")
            wl = job["spec"]["workload"]
            if halo_records_needed(wl) and ds["format"] == "csv" and ds["meta"].get("quoted"):
                raise EngineError("This CSV has quoted fields, which may span lines, so a rolling mean can't read across partitions. "
                                  "Convert it to JSON Lines, or remove the rolling mean.")
        return job, ds

    def devices(self) -> list:
        return [d for d in self.s.devices.values() if getattr(d, "engine_info", None) is not None or d.kind == "agent"]

    def plan(self, body) -> dict:
        job, ds = self._resolve(body)
        return {"job": job, "plan": self.planner.estimate({"spec": job["spec"]}, self.dataset_meta(ds) if ds else None, self.devices())}

    def create_job(self, body, by: str) -> dict:
        job, ds = self._resolve(body)
        meta = self.dataset_meta(ds) if ds else None
        plan = self.planner.estimate({"spec": job["spec"]}, meta, self.devices())
        spec = job["spec"]
        if "items" in spec["inputs"]:
            n = len(spec["inputs"]["items"])
            per = plan.get("itemsPerPartition") or spec["partitioning"]["itemsPerPartition"] or max(1, n // 8)
            parts = [{"kind": "items", "first": i, "count": j - i} for i, j in plan_items(n, per)]
        else:
            target = plan.get("targetBytes") or spec["partitioning"]["targetBytes"] or 8 * 2**20
            ranges = plan_ranges(self.splits(ds), meta["dataStart"], meta["size"], target, spec["partitioning"]["maxPartitions"])
            need = halo_records_needed(spec["workload"])
            avg = (meta["size"] - meta["dataStart"]) / max(1, meta.get("lines") or 1)
            parts = [{"kind": "range", "start": s, "end": e,
                      "halo": 0 if not need or s <= meta["dataStart"] else int(min(s - meta["dataStart"], need * avg * 3 + 4096))}
                     for s, e in ranges]
        job_id = "j" + secrets.token_hex(4)
        now = time.time()
        stage_id = f"{job_id}-s0"
        status = "running" if parts else "reducing"
        with self.db.tx():
            self.db.insert("jobs", {"id": job_id, "name": job["metadata"]["name"], "spec": job, "status": status,
                                    "decision": plan["decision"], "plan": plan, "note": " ".join(plan.get("reasons", [])[:2]),
                                    "created_by": by, "created_at": now, "updated_at": now, "started_at": now})
            self.db.insert("stages", {"id": stage_id, "job_id": job_id, "seq": 0, "name": "map", "kind": "map", "status": "running",
                                      "depends_on": [], "total": len(parts), "spec": {}})
            for i, inp in enumerate(parts):
                self.db.insert("partitions", {"id": f"{job_id}-p{i}", "job_id": job_id, "stage_id": stage_id, "idx": i, "input": inp,
                                              "status": "ready", "updated_at": now,
                                              "bytes": inp["end"] - inp["start"] if inp["kind"] == "range" else None})
            self.db.event(job_id, "created", {"by": by, "partitions": len(parts), "decision": plan["decision"]})
        log.info("%s started job %s (%s, %d partitions, %s)", by, job["metadata"]["name"], spec["workload"]["type"], len(parts), plan["decision"])
        if not parts:
            self._spawn(self._reduce(job_id))
        self._changed(job_id)
        self.dispatch()
        return self.job_view(self.db.job(job_id))

    def cancel_job(self, job_id: str, by: str) -> dict:
        job = self.db.job(job_id)
        if job is None:
            raise EngineError("There's no job with that id.")
        if job["status"] not in FINISHED:
            self._stop_job(job_id, "cancelled", f"Cancelled by {by}.")
        return self.job_view(self.db.job(job_id))

    def retry_job(self, job_id: str) -> dict:
        """Run a failed job's unfinished partitions again; accepted ones are kept."""
        job = self.db.job(job_id)
        if job is None:
            raise EngineError("There's no job with that id.")
        if job["status"] not in ("failed", "cancelled"):
            raise EngineError("Only a failed or cancelled job can be retried.")
        with self.db.tx():
            self.db.conn.execute("UPDATE partitions SET status = 'ready', tries = 0, moves = 0, error = NULL, device_id = NULL "
                                 "WHERE job_id = ? AND status IN ('failed', 'cancelled', 'ready', 'leased')", (job_id,))
            self.db.update("jobs", {"id": job_id}, {"status": "running", "error": None, "finished_at": None, "updated_at": time.time(),
                                                    "note": "Retrying the partitions that didn't finish."})
            self.db.event(job_id, "retried")
        if not self.db.counts(job_id).get("ready"):
            self._spawn(self._reduce(job_id))
        self._changed(job_id)
        self.dispatch()
        return self.job_view(self.db.job(job_id))

    def delete_job(self, job_id: str) -> None:
        job = self.db.job(job_id)
        if job is None:
            raise EngineError("There's no job with that id.")
        if job["status"] not in FINISHED:
            raise EngineError("Cancel the job before deleting it.")
        self.db.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.s.broadcast({"type": "eng.job", "job": {"id": job_id, "deleted": True}}, need=CAP_SUBMIT)

    def _stop_job(self, job_id: str, status: str, error: str | None) -> None:
        for p in self.db.all("SELECT * FROM partitions WHERE job_id = ? AND status = 'leased'", (job_id,)):
            self._cancel_attempt(p, p["attempt"], "cancelled")
        with self.db.tx():
            self.db.conn.execute("UPDATE partitions SET status = 'cancelled' WHERE job_id = ? AND status IN ('ready', 'leased')", (job_id,))
            self.db.update("jobs", {"id": job_id}, {"status": status, "error": error, "finished_at": time.time(), "updated_at": time.time()})
            self.db.event(job_id, "status", {"status": status, "error": error})
        self._changed(job_id)

    # ------------------------------------------------------------------ placing partitions
    def _capacity(self, d) -> int:
        info = getattr(d, "engine_info", None) or {}
        return max(0, min(int(info.get("slots") or 0), d.threads)) - len(self.s.resources.held(d.id, "engine"))

    def dispatch(self) -> None:
        if not self.started:
            return
        devices = [d for d in self.s.devices.values() if getattr(d, "engine_info", None) and not d.closed and not d.stale and d.sharing]
        if not devices:
            return
        free = {d.id: self._capacity(d) for d in devices}
        if not any(v > 0 for v in free.values()):
            return
        now = time.time()
        jobs: dict[str, tuple] = {}
        rows = self.db.all("SELECT p.* FROM partitions p JOIN jobs j ON j.id = p.job_id WHERE p.status = 'ready' AND j.status = 'running' "
                           "ORDER BY j.created_at, p.idx LIMIT 5000")
        for p in rows:
            if not any(v > 0 for v in free.values()):
                break
            if p["job_id"] not in jobs:
                job = self.db.job(p["job_id"])
                ref = job["spec"]["spec"]["inputs"].get("dataset", {}).get("ref")
                ds = self.db.dataset(ref.split(":", 1)[1]) if ref else None
                meta = self.dataset_meta(ds) if ds else None
                inflight: dict[str, int] = {}
                for r in self.db.all("SELECT device_id, COUNT(*) AS n FROM partitions WHERE job_id = ? AND status = 'leased' GROUP BY device_id", (job["id"],)):
                    inflight[r["device_id"]] = r["n"]
                jobs[p["job_id"]] = (job, meta, inflight)
            job, meta, inflight = jobs[p["job_id"]]
            spec = job["spec"]["spec"]
            best, best_score = None, None
            for d in devices:
                if free[d.id] <= 0 or self.refused.get((job["id"], d.id), 0) > now:
                    continue
                if inflight.get(d.id, 0) >= spec["partitioning"]["maxInflightPerWorker"]:
                    continue
                if self.planner.eligible(job["spec"] | {"plan": job["plan"]}, meta, d):
                    continue
                if not self.s.resources.fits(d, "engine", cpu=spec["resources"]["cpuSlots"], mem_gb=spec["resources"]["minFreeMemoryGiB"]):
                    continue
                score = (0 if d.coordinator else 1, inflight.get(d.id, 0), (d.load or {}).get("cpu") or 0.0, d.name)
                if best_score is None or score < best_score:
                    best, best_score = d, score
            if best is not None:
                self._assign(job, meta, p, best)
                free[best.id] -= 1
                inflight[best.id] = inflight.get(best.id, 0) + 1

    def _reserve(self, job, device_id, pid, n) -> None:
        if job is None or not device_id:
            return
        res = job["spec"]["spec"]["resources"]
        self.s.resources.reserve(device_id, f"eng:{pid}:{n}", "engine", cpu=res["cpuSlots"], mem_gb=res["minFreeMemoryGiB"])

    def _assign(self, job: dict, meta: dict | None, p: dict, d) -> None:
        spec = job["spec"]["spec"]
        n = (p["attempt"] or 0) + 1
        secret = secrets.token_urlsafe(24)
        now = time.time()
        with self.db.tx():
            self.db.insert("attempts", {"partition_id": p["id"], "n": n, "device_id": d.id, "device_name": d.name,
                                        "token_hash": _hash(secret), "status": "running", "started_at": now, "outputs": {}})
            self.db.update("partitions", {"id": p["id"]}, {"status": "leased", "attempt": n, "device_id": d.id, "device_name": d.name,
                                                          "lease_until": now + spec["retry"]["leaseSeconds"], "started_at": p["started_at"] or now,
                                                          "updated_at": now})
            self.db.event(job["id"], "assigned", {"attempt": n, "device": d.name}, p["id"], d.id)
        self._reserve(job, d.id, p["id"], n)
        inp = dict(p["input"])
        if inp["kind"] == "items":
            inp["items"] = spec["inputs"]["items"][inp["first"]:inp["first"] + inp["count"]]
        task = {"workload": spec["workload"], "input": inp, "onBadRecord": spec["output"]["onBadRecord"],
                "checkpoint": p["checkpoint"], "checkpointEvery": spec["retry"]["checkpointIntervalSeconds"],
                "ctx": {}, "runtime": spec["runtime"], "maxOutputBytes": MAX_OUTPUT}
        if meta is not None:
            task.update(format=meta["format"], header=meta.get("header"), dataStart=meta["dataStart"])
        if spec["runtime"]["kind"] == "managed-python" and self.runtimes is not None:
            task["env"] = self.runtimes.env_for(job, d)
        d.send({"type": "eng.start", "job": job["id"], "partition": p["id"], "attempt": n, "token": f"{p['id']}.{n}.{secret}",
                "timeout": spec["retry"]["timeoutSeconds"], "task": task})
        self._changed(job["id"])

    def _cancel_attempt(self, p: dict, n: int, status: str) -> None:
        a = self.db.attempt(p["id"], n)
        if a is None:
            return
        if a["status"] in LIVE_ATTEMPT:
            self.db.update("attempts", {"partition_id": p["id"], "n": n}, {"status": status, "ended_at": time.time()})
            d = self.s.devices.get(a["device_id"])
            if d is not None and not d.closed:
                d.send({"type": "eng.cancel", "partition": p["id"], "attempt": n})
        self.s.resources.release(f"eng:{p['id']}:{n}")

    # ------------------------------------------------------------------ computers coming and going
    def device_connected(self, d) -> None:
        info = getattr(d, "engine_info", None) or {}
        reported = {(r["partition"], r["attempt"]) for r in info.get("running", [])}
        for p in self.db.all("SELECT * FROM partitions WHERE status = 'leased' AND device_id = ?", (d.id,)):
            if (p["id"], p["attempt"]) not in reported:
                self._lose(p, f"{d.name} came back without it (it may have restarted).")
        for pid, n in reported:
            p = self.db.partition(pid)
            a = self.db.attempt(pid, n) if p else None
            if p is None or a is None or a["device_id"] != d.id or p["status"] == "done" or a["status"] not in LIVE_ATTEMPT:
                d.send({"type": "eng.cancel", "partition": pid, "attempt": n})
        self.dispatch()

    def device_gone(self, d) -> None:
        pass  # leases run out on their own; the computer may be back in a moment

    def _lose(self, p: dict, why: str) -> None:
        """The partition's computer stopped answering: hand it to another."""
        a = self.db.attempt(p["id"], p["attempt"])
        if a is not None and a["status"] == "running":
            self.db.update("attempts", {"partition_id": p["id"], "n": p["attempt"]}, {"status": "lost", "error": why, "ended_at": time.time()})
        self.s.resources.release(f"eng:{p['id']}:{p['attempt']}")
        moves = (p["moves"] or 0) + 1
        self.db.update("partitions", {"id": p["id"]}, {"status": "ready", "moves": moves, "lease_until": None, "updated_at": time.time()})
        self.db.event(p["job_id"], "lost", {"attempt": p["attempt"], "why": why}, p["id"], p["device_id"])
        if moves > MAX_MOVES:
            self._stop_job(p["job_id"], "failed", f"Partition {p['idx'] + 1} lost its computer {MAX_MOVES} times. {why}")
        self._changed(p["job_id"])

    def sweep(self) -> None:
        now = time.time()
        if not self.started or now - self._last_sweep < 1.0:
            return
        self._last_sweep = now
        for p in self.db.all("SELECT * FROM partitions WHERE status = 'leased' AND lease_until < ?", (now,)):
            self._lose(p, f"{p['device_name']} stopped reporting progress.")
        for row in self.db.all("SELECT a.*, j.spec AS jspec, p.job_id AS job_id FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                               "JOIN jobs j ON j.id = p.job_id WHERE a.status = 'running' AND p.status = 'leased' AND p.attempt = a.n"):
            limit = row["jspec"]["spec"]["retry"]["timeoutSeconds"]
            if now - (row["started_at"] or now) > limit:
                p = self.db.partition(row["partition_id"])
                self._cancel_attempt(p, row["n"], "failed")
                self._failed(p, f"It ran past the {limit} s time limit on {row['device_name']}.", retryable=True)
        for key in [k for k, until in self.refused.items() if until < now]:
            self.refused.pop(key, None)
        if now - self._last_gc > 3600:
            self._last_gc = now
            self._spawn(self.collect_garbage())
        self.dispatch()

    # ------------------------------------------------------------------ messages from computers
    def handle(self, d, msg: dict) -> None:
        kind = msg.get("type")
        pid = str(msg.get("partition") or "")
        n = msg.get("attempt")
        if kind == "eng.done":
            d.send({"type": "eng.ack", "partition": pid, "attempt": n})
        p = self.db.partition(pid)
        a = self.db.attempt(pid, n) if p is not None and isinstance(n, int) else None
        if p is None or a is None or a["device_id"] != d.id:
            if kind == "eng.progress":
                d.send({"type": "eng.cancel", "partition": pid, "attempt": n})
            return
        current = p["status"] == "leased" and p["attempt"] == n
        if kind == "eng.progress":
            if not current:
                if p["status"] == "done" or a["status"] not in LIVE_ATTEMPT:
                    d.send({"type": "eng.cancel", "partition": pid, "attempt": n})
                return
            job = self.db.job(p["job_id"])
            self.db.update("partitions", {"id": pid}, {"lease_until": time.time() + job["spec"]["spec"]["retry"]["leaseSeconds"],
                                                       "records": msg.get("records") if isinstance(msg.get("records"), int) else p["records"]})
            self._changed(p["job_id"])
        elif kind == "eng.checkpoint":
            off, st = msg.get("offset"), msg.get("state")
            inp = p["input"]
            old = (p["checkpoint"] or {}).get("offset", 0)
            if current and inp["kind"] == "range" and isinstance(off, int) and inp["start"] < off <= inp["end"] and off > old and isinstance(st, dict):
                if len(_canon(st)) <= INLINE_RESULT:
                    self.db.update("partitions", {"id": pid}, {"checkpoint": {"offset": off, "state": st}})
        elif kind == "eng.refuse":
            if current:
                self.refused[(p["job_id"], d.id)] = time.time() + 60
                self._cancel_attempt(p, n, "failed")
                self.db.update("partitions", {"id": pid}, {"status": "ready", "moves": (p["moves"] or 0) + 1})
                self.db.event(p["job_id"], "refused", {"why": str(msg.get("reason") or "")[:300]}, pid, d.id)
                self._changed(p["job_id"])
                self.dispatch()
        elif kind == "eng.done":
            self._on_done(p, a, d, msg)

    def _on_done(self, p: dict, a: dict, d, msg: dict) -> None:
        if p["status"] == "done" or a["status"] not in LIVE_ATTEMPT:
            return  # another attempt was accepted first, or this one was cancelled
        if not msg.get("ok"):
            current = p["status"] == "leased" and p["attempt"] == a["n"]
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]},
                           {"status": "failed", "ended_at": time.time(), "error": str(msg.get("error") or "")[:2000]})
            self.s.resources.release(f"eng:{p['id']}:{a['n']}")
            if not current:
                return
            kind = msg.get("errorKind")
            error = str(msg.get("error") or "The partition failed.")[:2000]
            if kind == "halo":
                inp = dict(p["input"])
                inp["halo"] = min(inp["start"], max(4096, inp["halo"] * 4))
                self.db.update("partitions", {"id": p["id"]}, {"status": "ready", "input": inp})
                self._changed(p["job_id"])
                self.dispatch()
                return
            self._failed(p, f"{error} (on {d.name})", retryable=kind != "data")
            return
        job = self.db.job(p["job_id"])
        problem = self._check_result(job, p, a, msg)
        if problem:
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]}, {"status": "failed", "ended_at": time.time(), "error": problem})
            self.s.resources.release(f"eng:{p['id']}:{a['n']}")
            if p["attempt"] == a["n"]:
                self._failed(p, f"{d.name} sent back a result that doesn't check out: {problem}", retryable=True)
            return
        self._commit(job, p, a, d, msg)

    def _check_result(self, job: dict, p: dict, a: dict, msg: dict) -> str | None:
        uploaded = a["outputs"] or {}
        outputs = msg.get("outputs") if isinstance(msg.get("outputs"), dict) else {}
        for name, o in outputs.items():
            got = uploaded.get(name)
            if not isinstance(o, dict) or got is None or got["sha"] != o.get("sha") or got["size"] != o.get("size"):
                return f"the output {name} wasn't uploaded as reported"
        state = msg.get("state")
        if state is None and msg.get("stateOutput"):
            if msg["stateOutput"] not in uploaded:
                return "its state was never uploaded"
        elif not isinstance(state, dict):
            return "it has no result state"
        wl = job["spec"]["spec"]["workload"]
        if (wl["type"] == "batch.transform" or (wl["type"] == "python.map" and wl["reducer"] == "concat")) and "part.jsonl" not in outputs:
            return "the records it wrote are missing"
        return None

    def _commit(self, job: dict, p: dict, a: dict, d, msg: dict) -> None:
        now = time.time()
        outputs = msg.get("outputs") or {}
        out = {"records": int(msg.get("records") or 0), "skipped": int(msg.get("skipped") or 0), "bad": (msg.get("bad") or [])[:20],
               "outputs": outputs}
        if msg.get("state") is not None:
            out["state"] = msg["state"]
        else:
            out["stateSha"] = (a["outputs"] or {})[msg["stateOutput"]]["sha"]
        with self.db.tx():
            cur = self.db.conn.execute("UPDATE partitions SET status = 'done', accepted_attempt = ?, output = ?, finished_at = ?, updated_at = ?, "
                                       "device_id = ?, device_name = ?, records = ? WHERE id = ? AND status != 'done'",
                                       (a["n"], json.dumps(out, separators=(",", ":"), allow_nan=False), now, now, d.id, d.name,
                                        out["records"], p["id"]))
            if cur.rowcount != 1:
                return  # accepted by someone else a moment ago
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]}, {"status": "done", "ended_at": now,
                                                                              "metrics": {"seconds": msg.get("seconds"), "bytes": msg.get("bytes")}})
            self.db.conn.execute("UPDATE stages SET done = done + 1 WHERE id = ?", (p["stage_id"],))
            wl_type = job["spec"]["spec"]["workload"]["type"]
            units = p["input"]["count"] if p["input"]["kind"] == "items" else p["input"]["end"] - p["input"]["start"]
            secs = msg.get("seconds")
            if isinstance(secs, (int, float)) and secs > 0:
                self.db.insert("perf", {"device_id": d.id, "workload": wl_type + (":items" if p["input"]["kind"] == "items" else ""),
                                        "units": units, "seconds": float(secs), "ts": now})
            self.db.event(job["id"], "done", {"attempt": a["n"], "device": d.name, "records": out["records"], "seconds": secs}, p["id"], d.id)
        self.s.resources.release(f"eng:{p['id']}:{a['n']}")
        for other in self.db.all("SELECT n FROM attempts WHERE partition_id = ? AND n != ? AND status IN ('running', 'lost')", (p["id"], a["n"])):
            self._cancel_attempt(p, other["n"], "superseded")
        self._changed(job["id"])
        if not self.db.one("SELECT id FROM partitions WHERE job_id = ? AND status != 'done' LIMIT 1", (job["id"],)):
            self._spawn(self._reduce(job["id"]))
        self.dispatch()

    def _failed(self, p: dict, error: str, retryable: bool) -> None:
        job = self.db.job(p["job_id"])
        tries = (p["tries"] or 0) + 1
        if retryable and tries < job["spec"]["spec"]["retry"]["maxAttempts"]:
            self.db.update("partitions", {"id": p["id"]}, {"status": "ready", "tries": tries, "error": error, "updated_at": time.time()})
            self.db.event(job["id"], "retry", {"error": error, "tries": tries}, p["id"])
            self._changed(job["id"])
            self.dispatch()
            return
        self.db.update("partitions", {"id": p["id"]}, {"status": "failed", "tries": tries, "error": error, "updated_at": time.time()})
        self._stop_job(job["id"], "failed", f"Partition {p['idx'] + 1} failed: {error}")

    # ------------------------------------------------------------------ combining the results
    def _state_of(self, p: dict) -> dict:
        out = p["output"]
        return out["state"] if "state" in out else self.store.read_json(out["stateSha"])

    async def _reduce(self, job_id: str) -> None:
        if job_id in self.reducing:
            return
        self.reducing.add(job_id)
        try:
            job = self.db.job(job_id)
            if job is None or job["status"] in FINISHED:
                return
            self.db.update("jobs", {"id": job_id}, {"status": "reducing", "note": "Combining the results…", "updated_at": time.time()})
            self._changed(job_id)
            parts = self.db.all("SELECT * FROM partitions WHERE job_id = ? ORDER BY idx", (job_id,))
            spec = job["spec"]["spec"]
            wl = spec["workload"]
            loop = asyncio.get_running_loop()
            try:
                result, outputs, validation = await loop.run_in_executor(None, self._combine, job, parts)
            except Exception as e:  # a bug or a corrupted state: the job fails with the reason
                log.exception("combining job %s failed", job_id)
                self._stop_job(job_id, "failed", f"The results couldn't be combined: {e}")
                return
            if validation.get("failed"):
                self._stop_job(job_id, "failed", validation["failed"])
                self.db.update("jobs", {"id": job_id}, {"validation": validation})
                return
            stored = {"value": result}
            text = _canon(result)
            if len(text) > INLINE_RESULT:
                stored = {"artifact": self.store.put_bytes(text.encode("utf-8"), "result"), "size": len(text),
                          "preview": _preview(wl, result)}
            now = time.time()
            self.db.update("jobs", {"id": job_id}, {"status": "done", "result": stored, "outputs": outputs, "validation": validation,
                                                    "finished_at": now, "updated_at": now, "note": "", "error": None})
            self.db.event(job_id, "status", {"status": "done"})
            log.info("Job %s done in %.1f s", job["name"], now - (job["started_at"] or now))
            self._changed(job_id)
        finally:
            self.reducing.discard(job_id)

    def _combine(self, job: dict, parts: list[dict]) -> tuple[dict, dict | None, dict]:
        """Runs in a thread: merge the states, check them, and say how much was checked."""
        spec = job["spec"]["spec"]
        wl = spec["workload"]
        checks = []
        if parts and parts[0]["input"]["kind"] == "range":
            ds = self.db.dataset(spec["inputs"]["dataset"]["ref"].split(":", 1)[1])
            meta = self.dataset_meta(ds)
            check_ranges([(p["input"]["start"], p["input"]["end"]) for p in parts], meta["dataStart"], meta["size"])
            checks.append(f"Every byte of the input belongs to exactly one of the {len(parts)} partitions, and each was accepted once.")
        else:
            ds, meta = None, None
            covered = sum(p["input"]["count"] for p in parts)
            if "items" in spec["inputs"] and covered != len(spec["inputs"]["items"]):
                raise ValueError("the partitions don't cover every item")
            checks.append(f"Every item belongs to exactly one of the {len(parts)} partitions, and each was accepted once.")
        states = [self._state_of(p) for p in parts]
        merged = merge_states(wl, states)
        result = final_result(wl, merged)
        skipped = sum(int(p["output"].get("skipped") or 0) for p in parts)
        if skipped:
            result["badRecordsSkipped"] = skipped
        outputs = None
        if wl["type"] == "batch.transform" or (wl["type"] == "python.map" and wl["reducer"] == "concat"):
            pieces = [{"idx": p["idx"], **p["output"]["outputs"]["part.jsonl"]} for p in parts]
            for piece in pieces:
                if not self.store.has(piece["sha"]):
                    raise ValueError(f"the records of partition {piece['idx'] + 1} are missing from storage")
            outputs = {"parts": pieces, "bytes": sum(x["size"] for x in pieces), "records": sum(int(x.get("records") or 0) for x in pieces)}
            checks.append("Every partition's records were stored under the digest its computer reported.")
        validation = {"level": "structural", "checks": checks}
        n = min(spec["output"]["spotCheck"], len(parts))
        if n:
            rng = random.Random(job["id"])
            sample = sorted(rng.sample(range(len(parts)), n))
            for i in sample:
                p = parts[i]
                ref_state, ref_out = self._recompute(job, meta, p)
                if _canon(ref_state) != _canon(states[i]):
                    return result, outputs, {**validation, "failed": f"Spot check failed: partition {i + 1}, computed on {p['device_name']}, "
                                                                     "doesn't match the coordinator's own result. Nothing was published."}
                if outputs is not None:
                    digest = hashlib.sha256("".join(dump_record(r) for r in ref_out).encode("utf-8")).hexdigest()
                    if digest != outputs["parts"][i]["sha"]:
                        return result, outputs, {**validation, "failed": f"Spot check failed: the records of partition {i + 1}, written on "
                                                                         f"{p['device_name']}, differ from the coordinator's. Nothing was published."}
            validation = {"level": "spot-checked", "checks": checks + [f"{n} of {len(parts)} partitions were computed again on the coordinator and matched exactly."]}
        return result, outputs, validation

    def _recompute(self, job: dict, meta: dict | None, p: dict) -> tuple[dict, list]:
        inp = p["input"]
        if inp["kind"] == "items":
            out: list = []
            op = make_op(job["spec"]["spec"]["workload"], {}, out.append)
            run_items(op, job["spec"]["spec"]["inputs"]["items"][inp["first"]:inp["first"] + inp["count"]], inp["first"])
            return op.state(), out
        return run_range(job["spec"], meta, self.store.path(meta["sha"]), inp["start"], inp["end"], halo_bytes=inp.get("halo") or None)

    # ------------------------------------------------------------------ files for a computer working on a partition
    def attempt_for_token(self, token: str | None, pid: str, n: int, writing: bool = False) -> tuple[dict, dict] | None:
        parts = (token or "").split(".")
        if len(parts) != 3 or parts[0] != pid or parts[1] != str(n):
            return None
        p = self.db.partition(pid)
        a = self.db.attempt(pid, n) if p else None
        if a is None or a["status"] not in LIVE_ATTEMPT or not hmac.compare_digest(a["token_hash"], _hash(parts[2])):
            return None
        if p["status"] == "done":
            return None
        return p, a

    def input_range(self, p: dict, halo: bool, from_offset: int | None) -> tuple[str, int, int]:
        job = self.db.job(p["job_id"])
        ds = self.db.dataset(job["spec"]["spec"]["inputs"]["dataset"]["ref"].split(":", 1)[1])
        inp = p["input"]
        if halo:
            return ds["sha"], max(ds["meta"]["dataStart"], inp["start"] - inp["halo"]), inp["start"]
        start = inp["start"] if from_offset is None else min(max(inp["start"], from_offset), inp["end"])
        return ds["sha"], start, inp["end"]

    async def save_output(self, p: dict, a: dict, name: str, chunks) -> dict:
        if not NAME_OK.fullmatch(name):
            raise EngineError("That output name isn't allowed.")
        sha, size = await self.store.put_stream(chunks, "output", MAX_OUTPUT)
        a = self.db.attempt(p["id"], a["n"])
        outs = dict(a["outputs"] or {})
        outs[name] = {"sha": sha, "size": size}
        self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]}, {"outputs": outs})
        return outs[name]

    # ------------------------------------------------------------------ results
    async def result_stream(self, job_id: str):
        """The job's records (transform, concat), in input order, or its result as JSON."""
        job = self.db.job(job_id)
        if job is None or job["status"] != "done":
            raise EngineError("The job hasn't finished.")
        if job["outputs"]:
            for piece in job["outputs"]["parts"]:
                async for chunk in self.store.stream_range(piece["sha"], 0, piece["size"]):
                    yield chunk
            return
        r = job["result"] or {}
        if "artifact" in r:
            async for chunk in self.store.stream_range(r["artifact"], 0, r["size"]):
                yield chunk
        else:
            yield _canon(r.get("value")).encode("utf-8")

    async def collect_garbage(self) -> int:
        """Delete expired jobs, then artifacts nothing refers to."""
        now = time.time()
        for j in self.db.all("SELECT id, spec, finished_at FROM jobs WHERE status IN ('done', 'failed', 'cancelled')"):
            days = j["spec"]["spec"]["output"]["retentionDays"]
            if days and j["finished_at"] and now - j["finished_at"] > days * 86400:
                self.db.conn.execute("DELETE FROM jobs WHERE id = ?", (j["id"],))
        keep: set[str] = set()
        for ds in self.db.all("SELECT sha, meta FROM datasets"):
            keep.add(ds["sha"])
            keep.add(ds["meta"].get("indexSha"))
        for j in self.db.all("SELECT result, outputs FROM jobs"):
            if j["result"] and j["result"].get("artifact"):
                keep.add(j["result"]["artifact"])
            for piece in (j["outputs"] or {}).get("parts", []):
                keep.add(piece["sha"])
        for p in self.db.all("SELECT output FROM partitions WHERE output IS NOT NULL"):
            if p["output"].get("stateSha"):
                keep.add(p["output"]["stateSha"])
            for o in p["output"].get("outputs", {}).values():
                keep.add(o["sha"])
        for a in self.db.all("SELECT outputs FROM attempts a JOIN partitions p ON p.id = a.partition_id WHERE p.status != 'done' AND a.outputs IS NOT NULL"):
            for o in (a["outputs"] or {}).values():
                keep.add(o["sha"])
        return self.store.collect_garbage(keep)

    # ------------------------------------------------------------------ views for the web app
    def job_view(self, job: dict, full: bool = False) -> dict:
        counts = self.db.counts(job["id"])
        spec = job["spec"]["spec"]
        ref = spec["inputs"].get("dataset", {}).get("ref")
        ds = self.db.dataset(ref.split(":", 1)[1]) if ref else None
        per_device = self.db.all("SELECT device_name AS name, COUNT(*) AS n, COALESCE(SUM(bytes), 0) AS bytes, COALESCE(SUM(records), 0) AS records "
                                 "FROM partitions WHERE job_id = ? AND status = 'done' GROUP BY device_name ORDER BY n DESC", (job["id"],))
        result = job["result"] or {}
        view = {
            "id": job["id"], "name": job["name"], "status": job["status"], "note": job["note"] or "", "error": job["error"] or "",
            "decision": job["decision"], "workload": spec["workload"]["type"], "mode": spec["mode"],
            "dataset": {"id": ds["id"], "name": ds["name"], "size": ds["size"]} if ds else None,
            "items": len(spec["inputs"]["items"]) if "items" in spec["inputs"] else None,
            "createdBy": job["created_by"], "createdAt": job["created_at"], "startedAt": job["started_at"], "finishedAt": job["finished_at"],
            "total": sum(counts.values()), "counts": counts, "perDevice": per_device,
            "result": result.get("value") if "value" in result else result.get("preview"), "resultTruncated": "artifact" in result,
            "outputs": {"records": job["outputs"]["records"], "bytes": job["outputs"]["bytes"]} if job["outputs"] else None,
            "validation": job["validation"], "plan": {k: (job["plan"] or {}).get(k) for k in ("decision", "reasons", "localSeconds", "distributedSeconds", "localDevice")},
        }
        if full:
            view["spec"] = job["spec"]
            view["partitions"] = [{"idx": p["idx"], "status": p["status"], "device": p["device_name"], "attempt": p["attempt"], "tries": p["tries"],
                                   "moves": p["moves"], "bytes": p["bytes"], "records": p["records"], "error": p["error"],
                                   "seconds": round((p["finished_at"] or time.time()) - p["started_at"], 1) if p["started_at"] else None,
                                   "resumed": bool(p["checkpoint"])}
                                  for p in self.db.all("SELECT * FROM partitions WHERE job_id = ? ORDER BY idx LIMIT 2000", (job["id"],))]
            view["events"] = [{"ts": e["ts"], "type": e["type"], "partition": e["partition_id"], "data": e["data"]} for e in self.db.events(job["id"], 200)]
            view["plan"] = job["plan"]
        return view

    def device_view(self, d) -> dict | None:
        info = getattr(d, "engine_info", None)
        if not info:
            return None
        return {"slots": info.get("slots", 0), "ops": info.get("ops", []), "formats": info.get("formats", []), "runtimes": info.get("runtimes", []),
                "busy": len(self.s.resources.held(d.id, "engine")), "python": info.get("python")}

    def overview(self) -> dict:
        jobs = [self.job_view(j) for j in self.db.all("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50")]
        return {"jobs": jobs, "datasets": self.datasets(), "storage": self.store.usage()}

    def _changed(self, job_id: str) -> None:
        self._dirty.add(job_id)
        if self._flush_at is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        def flush():
            self._flush_at = None
            dirty, self._dirty = self._dirty, set()
            for jid in dirty:
                job = self.db.job(jid)
                if job is not None:
                    self.s.broadcast({"type": "eng.job", "job": self.job_view(job)}, need=CAP_SUBMIT)

        self._flush_at = loop.call_later(0.3, flush)

    # ------------------------------------------------------------------ managed runtimes (filled in by runtime.py)
    def runtime_why_not(self, job: dict, d) -> str | None:
        if self.runtimes is None:
            return "can't prepare managed Python environments yet"
        return self.runtimes.why_not(job, d)


def _preview(wl: dict, result: dict) -> dict:
    """A small version of a large result for the web page; the whole thing is a download."""
    out = dict(result)
    if "groups" in out:
        out["groups"] = out["groups"][:100]
        out["groupsTotal"] = len(result["groups"])
    if isinstance(out.get("value"), list):
        out["valueTotal"] = len(out["value"])
        out["value"] = out["value"][:100]
    if isinstance(out.get("value"), dict) and len(out["value"]) > 100:
        out["valueTotal"] = len(out["value"])
        out["value"] = dict(list(out["value"].items())[:100])
    return out
