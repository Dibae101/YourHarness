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
spot check re-runs some partitions and compares them (on the coordinator for
built-in operators, on another of your computers for your own code: verify.py),
and the job is done. All of this is saved, so a restart picks up where it left off.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import statistics
import time
from pathlib import Path

from ..coordinator.state import CAP_SUBMIT
from . import isolation
from . import spec as jobspec
from . import stages
from .db import EngineDB
from .index import Indexer
from .ops import halo_records_needed
from .planner import Planner
from .records import iter_records
from .spec import CODE_WORKLOADS
from .store import ArtifactStore, StoreError

log = logging.getLogger("yourharness")

MAX_DATASET = int(float(os.environ.get("HARNESS_MAX_DATASET_GB", 64)) * 2**30)
MAX_MODEL = int(float(os.environ.get("HARNESS_MAX_MODEL_GB", 8)) * 2**30)
MODEL_ADAPTERS = ("python", "torchscript")
MAX_OUTPUT = 4 * 2**30
INLINE_RESULT = 256 * 1024
MAX_MOVES = 12
CLASSIFICATIONS = ("owned-private", "shareable")
FORMATS = ("text", "jsonl", "csv")
FINISHED = ("done", "failed", "cancelled")
LIVE_ATTEMPT = ("running", "lost")
STAGE_KINDS = ("map", "shuffle-map", "shuffle-reduce", "join-map", "join-reduce")
SPECULATE_AFTER_S = float(os.environ.get("HARNESS_SPECULATE_AFTER_S", 10))  # a partition is never duplicated before this
SPECULATE_FACTOR = 3.0  # ...nor before it has taken this many times its stage's median


LOCALITY_WAIT_S = float(os.environ.get("HARNESS_LOCALITY_WAIT_S", 3))  # how long a partition waits for the computer holding its input


def local_bytes(inp: dict, device_id: str) -> int:
    """How much of a partition's input a computer already has: the earlier-stage files it wrote itself."""
    if inp["kind"] == "artifact":
        return inp["size"] if inp.get("device") == device_id else 0
    if inp["kind"] == "buckets":
        return sum(x["size"] for x in inp["parts"] if x.get("device") == device_id)
    return 0


def data_holder(inp: dict) -> str | None:
    """The computer that holds most of a partition's input, when one holds at least half of it."""
    if inp["kind"] == "artifact":
        return inp.get("device")
    if inp["kind"] == "buckets":
        by: dict[str, int] = {}
        for x in inp["parts"]:
            if x.get("device"):
                by[x["device"]] = by.get(x["device"], 0) + x["size"]
        if by:
            dev, n = max(by.items(), key=lambda kv: kv[1])
            if n * 2 >= sum(x["size"] for x in inp["parts"]):
                return dev
    return None
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
        self._stage_cache: dict[str, dict] = {}  # stage id -> its row; a stage's spec never changes
        self._bg: set[asyncio.Future] = set()
        self._dirty: set[str] = set()
        self._flush_at = None
        self._last_sweep = 0.0
        self._last_gc = 0.0
        from .runtime import RuntimeManager
        from .verify import Verifier

        self.runtimes = RuntimeManager(self)  # managed Python environments
        self.verifier = Verifier(self)  # spot checks of your own code, re-run on another computer

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
        self.verifier.started(now)
        for a in self.db.all("SELECT a.*, p.job_id AS job_id FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                             "WHERE a.purpose = 'speculative' AND a.status = 'running'"):
            job = self.db.job(a["job_id"])
            self.db.update("attempts", {"partition_id": a["partition_id"], "n": a["n"]}, {"lease_until": now + 75})
            self._reserve(job, a["device_id"], a["partition_id"], a["n"])
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
                 if f"dataset:{ds_id}" in (j["spec"]["spec"]["inputs"].get("dataset", {}).get("ref"), j["spec"]["spec"]["inputs"].get("right", {}).get("ref"))]
        if users:
            raise EngineError("A job that's still running reads this dataset. Cancel it first.")
        self.db.conn.execute("DELETE FROM datasets WHERE id = ?", (ds_id,))
        self.index_cache.pop(ds_id, None)
        self.s.broadcast({"type": "eng.datasets"}, need=CAP_SUBMIT)

    # ------------------------------------------------------------------ models (ml.evaluate)
    async def add_model(self, name: str, adapter: str | None, chunks, by: str) -> dict:
        """Store a model file: model.py, a .zip with model.py at its top, or a TorchScript .pt."""
        from .models import detect_format

        name = (name or "model").strip()[:200] or "model"
        head = bytearray()

        async def watched():
            async for part in chunks:
                if len(head) < 8:
                    head.extend(part[: 8 - len(head)])
                yield part

        try:
            sha, size = await self.store.put_stream(watched(), "model", MAX_MODEL)
        except StoreError as e:
            raise EngineError(str(e)) from None
        fmt = detect_format(name, bytes(head))
        adapter = adapter or ("torchscript" if fmt == "pt" else "python")
        if adapter not in MODEL_ADAPTERS:
            raise EngineError(f"The adapter should be one of {', '.join(MODEL_ADAPTERS)}.")
        if fmt is None or (adapter == "python") != (fmt in ("py", "zip")):
            raise EngineError("A python model is model.py, or a .zip with model.py at its top; a torchscript model is a .pt file "
                              "saved with torch.jit.save.")
        meta = {"filename": name}
        if fmt == "py":
            try:
                compile(self.store.path(sha).read_bytes().decode("utf-8"), "model.py", "exec")  # checked, never run here
            except (SyntaxError, UnicodeDecodeError) as e:
                raise EngineError(f"model.py doesn't compile: {e}") from None
        elif fmt == "zip":
            import zipfile

            try:
                with zipfile.ZipFile(self.store.path(sha)) as z:
                    names = z.namelist()
            except zipfile.BadZipFile:
                raise EngineError("That .zip can't be read.") from None
            if "model.py" not in names:
                raise EngineError("The .zip needs model.py at its top level (not inside a folder).")
            meta["files"] = names[:200]
        model_id = "m" + secrets.token_hex(4)
        self.db.insert("models", {"id": model_id, "name": name, "adapter": adapter, "format": fmt, "sha": sha, "size": size, "meta": meta,
                                  "created_by": by, "created_at": time.time()})
        self.db.event(None, "model", {"id": model_id, "name": name, "sha": sha, "by": by})
        self.s.broadcast({"type": "eng.datasets"}, need=CAP_SUBMIT)
        return self.model_view(self.db.model(model_id))

    def model_view(self, m: dict) -> dict:
        return {"id": m["id"], "ref": f"model:{m['id']}", "name": m["name"], "adapter": m["adapter"], "format": m["format"], "sha": m["sha"],
                "size": m["size"], "files": (m["meta"] or {}).get("files"), "createdBy": m["created_by"], "createdAt": m["created_at"]}

    def models(self) -> list[dict]:
        return [self.model_view(m) for m in self.db.all("SELECT * FROM models ORDER BY created_at DESC")]

    def delete_model(self, model_id: str) -> None:
        if self.db.model(model_id) is None:
            raise EngineError("There's no model with that id.")
        users = [j for j in self.db.all("SELECT id, spec FROM jobs WHERE status NOT IN ('done', 'failed', 'cancelled')")
                 if j["spec"]["spec"]["inputs"].get("model", {}).get("ref") == f"model:{model_id}"]
        if users:
            raise EngineError("A job that's still running uses this model. Cancel it first.")
        self.db.conn.execute("DELETE FROM models WHERE id = ?", (model_id,))
        self.s.broadcast({"type": "eng.datasets"}, need=CAP_SUBMIT)

    def job_model(self, job: dict) -> dict | None:
        """The model a job was started with, by the SHA-256 pinned in its spec."""
        m = job["spec"]["spec"]["inputs"].get("model")
        if not m:
            return None
        row = self.db.model(m["ref"].split(":", 1)[1])
        if row is None or row["sha"] != m["sha256"]:
            return None
        return row

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
            reading = [s["workload"] for s in (job["spec"].get("stages") or [{"workload": job["spec"]["workload"], "from": "inputs"}])
                       if s["from"] == "inputs"]
            if any(halo_records_needed(wl) for wl in reading) and ds["format"] == "csv" and ds["meta"].get("quoted"):
                raise EngineError("This CSV has quoted fields, which may span lines, so a rolling mean can't read across partitions. "
                                  "Convert it to JSON Lines, or remove the rolling mean.")
        right = job["spec"]["inputs"].get("right")
        if right and self.db.dataset(right["ref"].split(":", 1)[1]) is None:
            raise EngineError(f"There's no dataset {right['ref']}.")
        m = job["spec"]["inputs"].get("model")
        if m:
            row = self.db.model(m["ref"].split(":", 1)[1])
            if row is None:
                raise EngineError(f"There's no model {m['ref']}.")
            if m["sha256"] and m["sha256"] != row["sha"]:
                raise EngineError(f"{m['ref']} isn't the version this job asks for: its SHA-256 is {row['sha'][:12]}…, "
                                  f"the job names {m['sha256'][:12]}….")
            m["sha256"] = row["sha"]  # the job is pinned to exactly this model, even if another is uploaded under its name
            if row["adapter"] == "torchscript" and not job["spec"]["workload"]["features"]:
                raise EngineError("A TorchScript model needs features: the numeric fields it reads, in order.")
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
        plan["stages"] = stages.plan_stages(job)
        job_id = "j" + secrets.token_hex(4)
        now = time.time()
        status = "needs_approval" if plan["decision"] == "NEEDS_SETUP_APPROVAL" else "running"
        with self.db.tx():
            self.db.insert("jobs", {"id": job_id, "name": job["metadata"]["name"], "spec": job, "status": status,
                                    "decision": plan["decision"], "plan": plan, "note": " ".join(plan.get("reasons", [])[:2]),
                                    "created_by": by, "created_at": now, "updated_at": now, "started_at": now,
                                    "validation": {"level": "structural", "checks": [], "stages": 0, "spotChecked": 0}})
            n = self._start_stage(job_id, 0)
            self.db.event(job_id, "created", {"by": by, "partitions": n, "decision": plan["decision"], "stages": len(plan["stages"])})
        log.info("%s started job %s (%s, %d partitions, %s)", by, job["metadata"]["name"], job["spec"]["workload"]["type"], n, plan["decision"])
        self._changed(job_id)
        self.dispatch()
        return self.job_view(self.db.job(job_id))

    # ------------------------------------------------------------------ stages
    def stage_row(self, stage_id: str) -> dict:
        got = self._stage_cache.get(stage_id)
        if got is None:
            got = self._stage_cache[stage_id] = self.db.stage(stage_id)
        return got

    def current_stage(self, job_id: str) -> dict | None:
        return self.db.one("SELECT * FROM stages WHERE job_id = ? AND status != 'done' ORDER BY seq LIMIT 1", (job_id,))

    def _start_stage(self, job_id: str, seq: int) -> int:
        """Create stage seq's partitions (inside the caller's transaction). Returns how many."""
        job = self.db.job(job_id)
        plan = job["plan"]
        st = dict(plan["stages"][seq])
        doc = job["spec"]
        if st["from"] == "inputs":
            parts = stages.first_parts(self, doc, st, plan)
        else:
            up = self.db.one("SELECT * FROM stages WHERE job_id = ? AND seq = ?", (job_id, seq - 1))
            parts = stages.later_parts(self, job_id, st, up)
        if st["kind"] in ("shuffle-map", "join-map"):
            st["buckets"] = stages.default_buckets(self, doc, st, len(parts))
        values = dict((doc["spec"].get("context") or {}).get("values") or {})
        if doc["spec"].get("stages"):  # a later stage sees every earlier stage's result, published at its barrier
            done = self.db.all("SELECT name, result FROM stages WHERE job_id = ? AND seq < ? ORDER BY seq", (job_id, seq))
            if done:
                values["stages"] = {r["name"]: self._stored_value(r["result"]) for r in done}
        if values:
            text = _canon(values)
            st["context"], st["contextSize"] = self.store.put_bytes(text.encode("utf-8"), "context"), len(text)
        else:
            st["context"], st["contextSize"] = None, 0
        stage_id = f"{job_id}-s{seq}"
        now = time.time()
        first = 0 if seq == 0 else (self.db.one("SELECT MAX(idx) AS m FROM partitions WHERE job_id = ?", (job_id,))["m"] or 0) + 1
        self.db.insert("stages", {"id": stage_id, "job_id": job_id, "seq": seq, "name": st["name"], "kind": st["kind"], "status": "running",
                                  "depends_on": [f"{job_id}-s{seq - 1}"] if seq else [], "total": len(parts), "spec": st, "started_at": now})
        for i, inp in enumerate(parts):
            self.db.insert("partitions", {"id": f"{job_id}-p{first + i}", "job_id": job_id, "stage_id": stage_id, "idx": first + i, "input": inp,
                                          "status": "ready", "updated_at": now, "bytes": stages.part_bytes(inp)})
        if seq:
            self.db.event(job_id, "stage", {"stage": st["name"], "seq": seq, "partitions": len(parts), "context": st["context"]})
        if not parts and job["status"] != "needs_approval":
            self._spawn(self._reduce(job_id))
        return len(parts)

    def _stored_value(self, stored: dict | None):
        stored = stored or {}
        if "artifact" in stored:
            return self.store.read_json(stored["artifact"])
        return stored.get("value")

    def _store_value(self, value, wl: dict) -> dict:
        text = _canon(value)
        if len(text) > INLINE_RESULT:
            return {"artifact": self.store.put_bytes(text.encode("utf-8"), "result"), "size": len(text), "preview": _preview(wl, value)}
        return {"value": value}

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
            self.db.conn.execute("UPDATE stages SET status = 'running', finished_at = NULL WHERE job_id = ? AND status IN ('failed', 'cancelled')",
                                 (job_id,))
            val = {k: v for k, v in (job["validation"] or {}).items() if k != "verify"}
            if val.get("level") == "failed":
                val["level"] = "structural"
            self.db.update("jobs", {"id": job_id}, {"status": "running", "error": None, "finished_at": None, "updated_at": time.time(),
                                                    "note": "Retrying the partitions that didn't finish.", "validation": val})
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

    def stop_job(self, job_id: str, status: str, error: str | None) -> None:
        self._stop_job(job_id, status, error)

    def _stop_job(self, job_id: str, status: str, error: str | None) -> None:
        for p in self.db.all("SELECT * FROM partitions WHERE job_id = ? AND status = 'leased'", (job_id,)):
            self._cancel_attempt(p, p["attempt"], "cancelled")
        self.verifier.cancel_job(job_id)
        for a in self.db.all("SELECT a.partition_id, a.n FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                             "WHERE p.job_id = ? AND a.purpose = 'speculative' AND a.status IN ('running', 'lost')", (job_id,)):
            self._cancel_attempt(self.db.partition(a["partition_id"]), a["n"], "cancelled")
        with self.db.tx():
            self.db.conn.execute("UPDATE stages SET status = ? WHERE job_id = ? AND status = 'running'", (status if status != "done" else "running", job_id))
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
        online = {d.id for d in devices}
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
            kind = self.stage_row(p["stage_id"])["spec"]["kind"]
            for d in devices:
                if free[d.id] <= 0 or self.refused.get((job["id"], d.id), 0) > now:
                    continue
                if inflight.get(d.id, 0) >= spec["partitioning"]["maxInflightPerWorker"]:
                    continue
                if kind not in (d.engine_info.get("stages") or STAGE_KINDS):
                    continue
                if self.planner.eligible(job["spec"] | {"plan": job["plan"]}, meta, d):
                    continue
                if not self.s.resources.fits(d, "engine", cpu=spec["resources"]["cpuSlots"], mem_gb=spec["resources"]["minFreeMemoryGiB"]):
                    continue
                # the computer that already holds the input (it wrote it) first, then the coordinator's own, then the least busy
                score = (-local_bytes(p["input"], d.id), 0 if d.coordinator else 1, inflight.get(d.id, 0), (d.load or {}).get("cpu") or 0.0, d.name)
                if best_score is None or score < best_score:
                    best, best_score = d, score
            if best is not None:
                holder = data_holder(p["input"])
                if holder and holder != best.id and holder in online and now - (p["updated_at"] or now) < LOCALITY_WAIT_S:
                    continue  # its input is on another computer that's busy right now: wait a moment for it rather than copy the data
                self._assign(job, meta, p, best)
                free[best.id] -= 1
                inflight[best.id] = inflight.get(best.id, 0) + 1
        self.verifier.dispatch(devices, free)
        if any(v > 0 for v in free.values()):
            self._speculate(devices, free, now)

    # ------------------------------------------------------------------ stragglers
    def _speculate(self, devices: list, free: dict, now: float) -> None:
        """A partition that runs far longer than its stage's others may also run on a free computer; the first result wins.

        Only for work without side effects (the engine's operators, ml.evaluate, python.map
        with sideEffects "none"), at most a tenth of a stage's partitions at once, and only
        once most of the stage is done, so a duplicate doesn't take a computer that has
        first-time work to do.
        """
        for job in self.db.all("SELECT * FROM jobs WHERE status = 'running'"):
            spec = job["spec"]["spec"]
            if not spec["retry"].get("speculate", True):
                continue
            stage = self.current_stage(job["id"])
            if stage is None:
                continue
            wl = stage["spec"]["workload"]
            if wl["type"] == "python.map" and wl.get("sideEffects") != "none":
                continue
            if self.db.one("SELECT id FROM partitions WHERE stage_id = ? AND status = 'ready' LIMIT 1", (stage["id"],)):
                continue  # first-time work is still waiting
            took = [r["t"] for r in self.db.all("SELECT a.ended_at - a.started_at AS t FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                                                "WHERE p.stage_id = ? AND p.status = 'done' AND a.n = p.accepted_attempt", (stage["id"],)) if r["t"] is not None]
            total = stage["total"] or 0
            if len(took) < max(3, total // 2):
                continue
            limit = max(SPECULATE_AFTER_S, SPECULATE_FACTOR * statistics.median(took))
            live = self.db.one("SELECT COUNT(*) AS n FROM attempts a JOIN partitions p ON p.id = a.partition_id WHERE p.stage_id = ? "
                               "AND a.purpose = 'speculative' AND a.status = 'running'", (stage["id"],))["n"]
            cap = max(1, total // 10)
            meta = None
            ref = spec["inputs"].get("dataset", {}).get("ref")
            if ref:
                ds = self.db.dataset(ref.split(":", 1)[1])
                meta = self.dataset_meta(ds) if ds else None
            for p in self.db.all("SELECT * FROM partitions WHERE stage_id = ? AND status = 'leased' ORDER BY started_at", (stage["id"],)):
                if live >= cap:
                    break
                a = self.db.attempt(p["id"], p["attempt"])
                if a is None or a["status"] != "running" or now - (a["started_at"] or now) < limit:
                    continue
                if self.db.one("SELECT n FROM attempts WHERE partition_id = ? AND purpose = 'speculative' AND status IN ('running', 'lost')", (p["id"],)):
                    continue
                best = None
                for d in devices:
                    if d.id == a["device_id"] or free.get(d.id, 0) <= 0 or self.refused.get((job["id"], d.id), 0) > now:
                        continue
                    if stage["spec"]["kind"] not in (d.engine_info.get("stages") or STAGE_KINDS):
                        continue
                    if self.planner.eligible(job["spec"] | {"plan": {}}, meta, d):
                        continue
                    if not self.s.resources.fits(d, "engine", cpu=spec["resources"]["cpuSlots"], mem_gb=spec["resources"]["minFreeMemoryGiB"]):
                        continue
                    if best is None or (0 if d.coordinator else 1, d.name) < (0 if best.coordinator else 1, best.name):
                        best = d
                if best is None:
                    continue
                self._assign_speculative(job, meta, p, best, now - (a["started_at"] or now), limit)
                free[best.id] -= 1
                live += 1

    def _assign_speculative(self, job: dict, meta: dict | None, p: dict, d, ran: float, limit: float) -> None:
        spec = job["spec"]["spec"]
        n = self.next_attempt(p["id"])
        secret = secrets.token_urlsafe(24)
        now = time.time()
        with self.db.tx():
            self.db.insert("attempts", {"partition_id": p["id"], "n": n, "device_id": d.id, "device_name": d.name, "purpose": "speculative",
                                        "token_hash": _hash(secret), "status": "running", "started_at": now, "outputs": {},
                                        "lease_until": now + spec["retry"]["leaseSeconds"]})
            self.db.event(job["id"], "speculative", {"attempt": n, "device": d.name, "slowDevice": p["device_name"], "ranSeconds": round(ran, 1),
                                                     "limitSeconds": round(limit, 1)}, p["id"], d.id)
        self._reserve(job, d.id, p["id"], n)
        d.send(self.start_message(d, job, p, n, secret, self.task_for(job, meta, p, d)))
        self._changed(job["id"])

    def _reserve(self, job, device_id, pid, n) -> None:
        if job is None or not device_id:
            return
        res = job["spec"]["spec"]["resources"]
        self.s.resources.reserve(device_id, f"eng:{pid}:{n}", "engine", cpu=res["cpuSlots"], mem_gb=res["minFreeMemoryGiB"])

    def next_attempt(self, pid: str) -> int:
        row = self.db.one("SELECT MAX(n) AS n FROM attempts WHERE partition_id = ?", (pid,))
        return int((row or {}).get("n") or 0) + 1

    def _assign(self, job: dict, meta: dict | None, p: dict, d) -> None:
        spec = job["spec"]["spec"]
        n = self.next_attempt(p["id"])
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
        d.send(self.start_message(d, job, p, n, secret, self.task_for(job, meta, p, d)))
        self._changed(job["id"])

    @staticmethod
    def start_message(d, job: dict, p: dict, n: int, secret: str, task: dict) -> dict:
        """eng.start for one attempt. A browser tab gets the task as JSON text, which it reads keeping
        Python's int and float apart (JavaScript's JSON.parse would make them all floats)."""
        msg = {"type": "eng.start", "job": job["id"], "partition": p["id"], "attempt": n, "token": f"{p['id']}.{n}.{secret}",
               "timeout": job["spec"]["spec"]["retry"]["timeoutSeconds"]}
        if d.kind == "browser":
            msg["taskJSON"] = json.dumps(task, separators=(",", ":"), allow_nan=False)
        else:
            msg["task"] = task
        return msg

    def task_for(self, job: dict, meta: dict | None, p: dict, d, resume: bool = True) -> dict:
        """What a computer needs to run partition p: everything but its input bytes, which it streams."""
        spec = job["spec"]["spec"]
        st = self.stage_row(p["stage_id"])["spec"]
        wl = st["workload"]
        runtime = stages.runtime_for(spec, wl)
        inp = dict(p["input"])
        if inp["kind"] == "items":
            inp["items"] = spec["inputs"]["items"][inp["first"]:inp["first"] + inp["count"]]
        task = {"workload": wl, "stage": st["kind"], "input": inp, "onBadRecord": spec["output"]["onBadRecord"],
                "checkpoint": p["checkpoint"] if resume else None, "checkpointEvery": spec["retry"]["checkpointIntervalSeconds"],
                "ctx": {}, "runtime": runtime, "maxOutputBytes": MAX_OUTPUT}
        if inp["kind"] == "artifact":
            task.update(format="jsonl", header=None, dataStart=0)
        elif inp["kind"] == "range":
            if inp.get("ds"):
                meta = self.dataset_meta(self.db.dataset(inp["ds"]))
            task.update(format=meta["format"], header=meta.get("header"), dataStart=meta["dataStart"])
        if st.get("buckets"):
            task["buckets"] = st["buckets"]
        if inp.get("side"):
            task["side"] = inp["side"]
        if st.get("context") and wl["type"] in CODE_WORKLOADS:  # the stage's snapshot; the engine's operators don't read it
            if st["contextSize"] <= INLINE_RESULT:
                task["ctx"] = self.store.read_json(st["context"])
            else:
                task["ctxRef"] = {"sha": st["context"], "size": st["contextSize"]}
        if runtime["kind"] in ("managed-python", "isolated-oci") and self.runtimes is not None:
            task["env"] = self.runtimes.env_for(job, d)
        model = self.job_model(job) if wl["type"] == "ml.evaluate" else None
        if model is not None:
            task["model"] = {"sha": model["sha"], "size": model["size"], "adapter": model["adapter"], "format": model["format"]}
        return task

    def stage_context(self, p: dict) -> tuple[str, int] | None:
        st = self.stage_row(p["stage_id"])["spec"]
        return (st["context"], st["contextSize"]) if st.get("context") else None

    def part_input(self, p: dict, i: int) -> tuple[str, int]:
        """The i-th file a bucket partition reads."""
        parts = p["input"].get("parts") or []
        if not 0 <= i < len(parts):
            raise EngineError("This partition has no such input.")
        return parts[i]["sha"], parts[i]["size"]

    @staticmethod
    def hash_secret(secret: str) -> str:
        return _hash(secret)

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
        for a in self.db.all("SELECT * FROM attempts WHERE purpose = 'speculative' AND status = 'running' AND device_id = ?", (d.id,)):
            if (a["partition_id"], a["n"]) not in reported:
                self.db.update("attempts", {"partition_id": a["partition_id"], "n": a["n"]},
                               {"status": "lost", "error": f"{d.name} came back without it.", "ended_at": time.time()})
                self.s.resources.release(f"eng:{a['partition_id']}:{a['n']}")
        for pid, n in reported:
            p = self.db.partition(pid)
            a = self.db.attempt(pid, n) if p else None
            if self.verifier.keeps(a, d):
                continue
            if p is None or a is None or a["device_id"] != d.id or p["status"] == "done" or a["status"] not in LIVE_ATTEMPT:
                d.send({"type": "eng.cancel", "partition": pid, "attempt": n})
        self.verifier.device_connected(d, reported)
        self.runtimes.device_connected(d)
        self.dispatch()

    def device_gone(self, d) -> None:
        pass  # leases run out on their own; the computer may be back in a moment

    def device_removed(self, device_id: str, name: str) -> None:
        """A computer removed from the pool: what it held goes to the others now, not when its leases run out."""
        for p in self.db.all("SELECT * FROM partitions WHERE status = 'leased' AND device_id = ?", (device_id,)):
            self._lose(p, f"{name} was removed from the pool.")
        for a in self.db.all("SELECT * FROM attempts WHERE device_id = ? AND status = 'running' AND purpose != 'run'", (device_id,)):
            p = self.db.partition(a["partition_id"])
            if p is not None:
                self._cancel_attempt(p, a["n"], "lost")
        for key in [k for k in self.refused if k[1] == device_id]:
            self.refused.pop(key, None)
        for key in [k for k in self.runtimes.status if k[0] == device_id]:
            self.runtimes.status.pop(key, None)
        self.dispatch()

    def device_activity(self, device_id: str, limit: int = 300) -> dict:
        """What a computer did for batch jobs: per job, the data it read, the code and model it ran, and its attempts."""
        rows = self.db.all(
            "SELECT a.partition_id, a.n, a.status, a.purpose, a.started_at, a.ended_at, a.error, p.job_id, p.idx, p.bytes, p.records, "
            "p.accepted_attempt, j.name AS job_name, j.spec AS spec FROM attempts a JOIN partitions p ON p.id = a.partition_id "
            "JOIN jobs j ON j.id = p.job_id WHERE a.device_id = ? ORDER BY a.started_at DESC LIMIT ?", (device_id, limit))
        jobs: dict[str, dict] = {}
        attempts = []
        for r in rows:
            spec = r["spec"]["spec"]
            j = jobs.get(r["job_id"])
            if j is None:
                ref = spec["inputs"].get("dataset", {}).get("ref")
                ds = self.db.dataset(ref.split(":", 1)[1]) if ref else None
                m = spec["inputs"].get("model")
                model = self.db.model(m["ref"].split(":", 1)[1]) if m else None
                j = jobs[r["job_id"]] = {
                    "job": r["job_id"], "name": r["job_name"], "workload": spec["workload"]["type"], "runtime": spec["runtime"]["kind"],
                    "dataset": {"name": ds["name"], "classification": ds["classification"]} if ds else None,
                    "items": len(spec["inputs"]["items"]) if "items" in spec["inputs"] else None,
                    "model": model["name"] if model else None, "code": bool(spec["workload"].get("code")),
                    "partitions": 0, "accepted": 0, "bytes": 0, "first": r["started_at"], "last": r["started_at"]}
            j["partitions"] += 1
            accepted = r["accepted_attempt"] == r["n"] and r["purpose"] == "run"
            j["accepted"] += 1 if accepted else 0
            j["bytes"] += int(r["bytes"] or 0) if accepted else 0
            j["first"] = min(j["first"] or r["started_at"], r["started_at"] or j["first"])
            j["last"] = max(j["last"] or 0, r["started_at"] or 0)
            if len(attempts) < 50:
                attempts.append({"job": r["job_id"], "partition": r["idx"] + 1, "attempt": r["n"], "purpose": r["purpose"], "status": r["status"],
                                 "startedAt": r["started_at"], "endedAt": r["ended_at"], "error": (r["error"] or "")[:300] or None})
        return {"jobs": sorted(jobs.values(), key=lambda j: -(j["last"] or 0)), "attempts": attempts}

    def _lose(self, p: dict, why: str) -> None:
        """The partition's computer stopped answering: hand it to another."""
        a = self.db.attempt(p["id"], p["attempt"])
        if a is not None and a["status"] == "running":
            self.db.update("attempts", {"partition_id": p["id"], "n": p["attempt"]}, {"status": "lost", "error": why, "ended_at": time.time()})
        self.s.resources.release(f"eng:{p['id']}:{p['attempt']}")
        dup = self.db.one("SELECT * FROM attempts WHERE partition_id = ? AND purpose = 'speculative' AND status = 'running'", (p["id"],))
        if dup is not None:  # the duplicate already running elsewhere carries on as the partition's attempt
            self.db.update("attempts", {"partition_id": p["id"], "n": dup["n"]}, {"purpose": "run"})
            self.db.update("partitions", {"id": p["id"]}, {"attempt": dup["n"], "device_id": dup["device_id"], "device_name": dup["device_name"],
                                                          "lease_until": dup["lease_until"] or time.time() + 45, "updated_at": time.time()})
            self.db.event(p["job_id"], "lost", {"attempt": p["attempt"], "why": why, "continuedBy": dup["device_name"]}, p["id"], p["device_id"])
            self._changed(p["job_id"])
            return
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
        for a in self.db.all("SELECT * FROM attempts WHERE purpose = 'speculative' AND status = 'running' AND lease_until < ?", (now,)):
            self.db.update("attempts", {"partition_id": a["partition_id"], "n": a["n"]},
                           {"status": "lost", "error": f"{a['device_name']} stopped reporting progress.", "ended_at": now})
            self.s.resources.release(f"eng:{a['partition_id']}:{a['n']}")
        for row in self.db.all("SELECT a.*, j.spec AS spec, p.job_id AS job_id FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                               "JOIN jobs j ON j.id = p.job_id WHERE a.status = 'running' AND p.status = 'leased' AND p.attempt = a.n"):
            limit = row["spec"]["spec"]["retry"]["timeoutSeconds"]  # "spec" is a JSON column, so it arrives decoded
            if now - (row["started_at"] or now) > limit:
                p = self.db.partition(row["partition_id"])
                self._cancel_attempt(p, row["n"], "failed")
                self._failed(p, f"It ran past the {limit} s time limit on {row['device_name']}.", retryable=True)
        self.verifier.sweep(now)
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
        if a.get("purpose") == "verify":
            self.verifier.handle(p, a, d, msg)
            return
        if a.get("purpose") == "speculative" and kind != "eng.done":
            self._speculative_message(p, a, d, msg)
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
        elif kind == "eng.release":  # a browser tab that was hidden hands it back; not a failed try
            if current:
                self.refused[(p["job_id"], d.id)] = time.time() + 30  # it said it can't work now: give the job to others for a while
                self._lose(p, f"{d.name} handed it back ({str(msg.get('reason') or 'it stopped')[:120]}).")
                self.dispatch()

    def _speculative_message(self, p: dict, a: dict, d, msg: dict) -> None:
        kind = msg.get("type")
        if p["status"] == "done" or a["status"] != "running":
            if kind == "eng.progress":
                d.send({"type": "eng.cancel", "partition": p["id"], "attempt": a["n"]})
            return
        if kind == "eng.progress":
            job = self.db.job(p["job_id"])
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]},
                           {"lease_until": time.time() + job["spec"]["spec"]["retry"]["leaseSeconds"]})
        elif kind == "eng.refuse":
            self.refused[(p["job_id"], d.id)] = time.time() + 60
            self._cancel_attempt(p, a["n"], "failed")
        elif kind == "eng.release":
            self._cancel_attempt(p, a["n"], "lost")

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
            if kind == "unsupported":  # a browser tab that can't be sure to compute it exactly like a computer: not a failed try
                self.refused[(p["job_id"], d.id)] = time.time() + 600
                self.db.update("partitions", {"id": p["id"]}, {"status": "ready", "moves": (p["moves"] or 0) + 1, "updated_at": time.time()})
                self.db.event(p["job_id"], "refused", {"why": error, "device": d.name}, p["id"], d.id)
                self._changed(p["job_id"])
                self.dispatch()
                return
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
        st = self.stage_row(p["stage_id"])["spec"]
        if stages.stage_writes(st) and "part.jsonl" not in outputs:
            return "the records it wrote are missing"
        if st["kind"] == "shuffle-map" and isinstance(state, dict):
            files = [n for n in outputs if n.startswith("g")]
            if len(files) != int(state.get("files", -1)) or any(not re.fullmatch(r"g\d{1,4}\.json", n) for n in files):
                return "its bucket files don't match what it reported"
            if any(int(n[1:-5]) >= st["buckets"] for n in files):
                return "it wrote a bucket that doesn't exist"
        if st["kind"] == "join-map" and any(not re.fullmatch(r"b\d{1,4}\.jsonl", n) or int(n[1:-6]) >= st["buckets"] for n in outputs if n != "state.json"):
            return "it wrote a bucket that doesn't exist"
        return None

    def check_result(self, job: dict, p: dict, a: dict, msg: dict) -> str | None:
        return self._check_result(job, p, a, msg)

    def state_of(self, p: dict) -> dict:
        return self._state_of(p)

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
            metrics = {"seconds": msg.get("seconds"), "bytes": msg.get("bytes")}
            extra = msg.get("metrics") if isinstance(msg.get("metrics"), dict) else {}
            for k in ("predictSeconds", "predictCalls"):
                if isinstance(extra.get(k), (int, float)) and not isinstance(extra.get(k), bool):
                    metrics[k] = extra[k]
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]}, {"status": "done", "ended_at": now, "metrics": metrics})
            self.db.conn.execute("UPDATE stages SET done = done + 1 WHERE id = ?", (p["stage_id"],))
            st = self.stage_row(p["stage_id"])["spec"]
            units = p["input"]["count"] if p["input"]["kind"] == "items" else stages.part_bytes(p["input"])
            secs = msg.get("seconds")
            if isinstance(secs, (int, float)) and secs > 0 and st["kind"] == "map" and st["from"] == "inputs":  # what the planner estimates from
                self.db.insert("perf", {"device_id": d.id, "workload": st["workload"]["type"] + (":items" if p["input"]["kind"] == "items" else ""),
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
            stage = self.current_stage(job_id)
            if stage is None:
                return
            total = len(job["plan"]["stages"])
            note = "Combining the results…" if stage["seq"] == total - 1 else f"Finishing stage {stage['seq'] + 1} of {total} ({stage['name']})…"
            self.db.update("jobs", {"id": job_id}, {"status": "reducing", "note": note, "updated_at": time.time()})
            self._changed(job_id)
            parts = self.db.all("SELECT * FROM partitions WHERE stage_id = ? ORDER BY idx", (stage["id"],))
            first = None
            if stage["seq"] > 0 and stage["kind"] in ("shuffle-reduce", "join-reduce"):
                first = self._stored_value(self.db.one("SELECT result FROM stages WHERE job_id = ? AND seq = 0", (job_id,))["result"])
            loop = asyncio.get_running_loop()
            try:
                result, outputs, validation = await loop.run_in_executor(None, stages.combine, self, job, stage, parts, first)
            except Exception as e:  # a bug or a corrupted state: the job fails with the reason
                log.exception("combining job %s failed", job_id)
                self._stop_job(job_id, "failed", f"The results couldn't be combined: {e}")
                return
            if validation.get("failed"):
                self._stop_job(job_id, "failed", validation["failed"])
                self.db.update("jobs", {"id": job_id}, {"validation": {**(job["validation"] or {}), "level": "failed",
                                                                      "checks": (job["validation"] or {}).get("checks", []) + validation["checks"]}})
                return
            sample = validation.pop("verifySample", None)
            pending = {"result": self._store_value(result, stage["spec"]["workload"]), "outputs": outputs, "validation": validation}
            self.db.update("stages", {"id": stage["id"]}, {"result": pending})
            if sample:  # your own code: checked by running partitions again on other computers before it's used
                self.verifier.begin(job_id, sample, validation)
                return
            self._stage_complete(job_id)
        finally:
            self.reducing.discard(job_id)

    def verified(self, job_id: str, line: str) -> None:
        """The verifier re-ran the stage's sampled partitions elsewhere and they matched."""
        self._stage_complete(job_id, line)

    def _stage_complete(self, job_id: str, verified: str | None = None) -> None:
        """The current stage's result is checked: start the next stage, or publish the job's result."""
        job = self.db.job(job_id)
        stage = self.current_stage(job_id)
        if job is None or stage is None or job["status"] in FINISHED:
            return
        pending = stage["result"] or {}
        val = pending.get("validation") or {"level": "structural", "checks": []}
        if verified:
            val = {"level": "spot-checked", "checks": val["checks"] + [verified]}
        jv = {k: v for k, v in (job["validation"] or {}).items() if k != "verify"}
        total = len(job["plan"]["stages"])
        prefix = f"Stage {stage['seq'] + 1} ({stage['name']}): " if total > 1 else ""
        done_stages = int(jv.get("stages", 0)) + 1
        spot = int(jv.get("spotChecked", 0)) + (val["level"] == "spot-checked")
        new_val = {"level": "spot-checked" if spot == done_stages else "structural", "checks": jv.get("checks", []) + [prefix + c for c in val["checks"]],
                   "stages": done_stages, "spotChecked": spot}
        now = time.time()
        with self.db.tx():
            self.db.update("stages", {"id": stage["id"]}, {"status": "done", "finished_at": now, "result": pending.get("result")})
            if stage["seq"] + 1 < total:
                nxt = job["plan"]["stages"][stage["seq"] + 1]
                self.db.update("jobs", {"id": job_id}, {"status": "running", "validation": new_val, "updated_at": now,
                                                        "note": f"Stage {stage['seq'] + 2} of {total}: {nxt['name']}."})
                self._start_stage(job_id, stage["seq"] + 1)
            else:
                self.db.update("jobs", {"id": job_id}, {"status": "done", "result": pending.get("result"), "outputs": pending.get("outputs"),
                                                        "validation": new_val, "finished_at": now, "updated_at": now, "note": "", "error": None})
                self.db.event(job_id, "status", {"status": "done"})
        self._stage_cache.pop(stage["id"], None)
        if stage["seq"] + 1 >= total:
            log.info("Job %s done in %.1f s", job["name"], now - (job["started_at"] or now))
        self._changed(job_id)
        self.dispatch()

    def provenance(self, job: dict) -> dict:
        """Exactly what a result came from: the job, its data, its model and code, and the Python it ran in."""
        spec = job["spec"]["spec"]
        out: dict = {"job": hashlib.sha256(_canon(job["spec"]).encode("utf-8")).hexdigest()}
        for key, name in (("dataset", "dataset"), ("right", "right")):
            ref = spec["inputs"].get(key, {}).get("ref")
            if ref:
                ds = self.db.dataset(ref.split(":", 1)[1])
                out[name] = {"ref": ref, "name": ds["name"] if ds else None, "sha256": ds["sha"] if ds else None, "size": ds["size"] if ds else None}
        m = spec["inputs"].get("model")
        if m:
            row = self.db.model(m["ref"].split(":", 1)[1])
            out["model"] = {"ref": m["ref"], "name": row["name"] if row else None, "sha256": m["sha256"], "adapter": row["adapter"] if row else None}
        codes = [st["workload"]["code"] for st in (spec.get("stages") or [{"workload": spec["workload"]}]) if st["workload"].get("code")]
        if codes:
            out["code"] = {"sha256": hashlib.sha256("\n".join(codes).encode("utf-8")).hexdigest()}
        if spec.get("context"):
            out["context"] = {"version": spec["context"]["version"]}
        ctx = [r["spec"].get("context") for r in self.db.stages(job["id"]) if (r["spec"] or {}).get("context")]
        if ctx:
            out["stageContexts"] = ctx  # the snapshot each stage read, by SHA-256
        rt = spec["runtime"]
        if rt["kind"] == "managed-python":
            out["environment"] = {"kind": rt["kind"], "id": self.runtimes.fp(job["spec"]), "python": rt["python"], "requirements": rt["requirements"]}
        elif rt["kind"] == "isolated-oci":
            out["environment"] = {"kind": rt["kind"], "id": self.runtimes.fp(job["spec"]), "image": rt["image"], "limits": rt["limits"],
                                  "network": "none"}
        else:
            out["environment"] = {"kind": rt["kind"]}
        return out

    def latency(self, parts: list[dict]) -> dict:
        secs, calls, samples = 0.0, 0, 0
        for p in parts:
            a = self.db.attempt(p["id"], p["accepted_attempt"]) if p["accepted_attempt"] else None
            m = (a or {}).get("metrics") or {}
            secs += float(m.get("predictSeconds") or 0)
            calls += int(m.get("predictCalls") or 0)
            samples += int(p["records"] or 0)
        return {"predictSeconds": round(secs, 3), "batches": calls, "msPerRecord": round(secs * 1000 / samples, 4) if samples else None,
                "note": "Measured on each computer while it predicted; timing isn't part of the checked result."}

    # ------------------------------------------------------------------ files for a computer working on a partition
    def attempt_for_token(self, token: str | None, pid: str, n: int, writing: bool = False) -> tuple[dict, dict] | None:
        parts = (token or "").split(".")
        if len(parts) != 3 or parts[0] != pid or parts[1] != str(n):
            return None
        p = self.db.partition(pid)
        a = self.db.attempt(pid, n) if p else None
        if a is None or a["status"] not in LIVE_ATTEMPT or not hmac.compare_digest(a["token_hash"], _hash(parts[2])):
            return None
        if a.get("purpose") == "verify":
            return (p, a) if a["status"] == "running" else None  # re-running an accepted partition to check it
        if p["status"] == "done":
            return None
        return p, a

    def attempt_model(self, p: dict) -> tuple[str, int]:
        job = self.db.job(p["job_id"])
        model = self.job_model(job) if job else None
        if model is None:
            raise EngineError("This partition's job has no model.")
        return model["sha"], model["size"]

    def input_range(self, p: dict, halo: bool, from_offset: int | None) -> tuple[str, int, int]:
        inp = p["input"]
        if inp["kind"] == "artifact":  # an earlier stage's records
            start = 0 if from_offset is None else min(max(0, from_offset), inp["size"])
            return inp["sha"], start, inp["size"]
        job = self.db.job(p["job_id"])
        ds = self.db.dataset(inp.get("ds") or job["spec"]["spec"]["inputs"]["dataset"]["ref"].split(":", 1)[1])
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
        for m in self.db.all("SELECT sha FROM models"):
            keep.add(m["sha"])
        for st in self.db.all("SELECT spec, result FROM stages"):
            if (st["spec"] or {}).get("context"):
                keep.add(st["spec"]["context"])
            r = st["result"] or {}
            for stored in (r, r.get("result") or {}):
                if isinstance(stored, dict) and stored.get("artifact"):
                    keep.add(stored["artifact"])
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
        done = job["status"] == "done"  # a result being checked isn't shown until it passed
        model = spec["inputs"].get("model")
        view = {
            "id": job["id"], "name": job["name"], "status": job["status"], "note": job["note"] or "", "error": job["error"] or "",
            "decision": job["decision"], "workload": spec["workload"]["type"], "mode": spec["mode"],
            "dataset": {"id": ds["id"], "name": ds["name"], "size": ds["size"]} if ds else None,
            "model": {"ref": model["ref"], "sha": model["sha256"]} if model else None,
            "items": len(spec["inputs"]["items"]) if "items" in spec["inputs"] else None,
            "createdBy": job["created_by"], "createdAt": job["created_at"], "startedAt": job["started_at"], "finishedAt": job["finished_at"],
            "total": sum(counts.values()), "counts": counts, "perDevice": per_device,
            "result": (result.get("value") if "value" in result else result.get("preview")) if done else None,
            "resultTruncated": done and "artifact" in result,
            "outputs": {"records": job["outputs"]["records"], "bytes": job["outputs"]["bytes"]} if job["outputs"] and done else None,
            "validation": {k: v for k, v in (job["validation"] or {}).items() if k != "verify"} or None,
            "verify": self.verifier.view(job),
            "plan": {k: (job["plan"] or {}).get(k) for k in ("decision", "reasons", "localSeconds", "distributedSeconds", "localDevice")},
            "runtime": spec["runtime"], "setup": self.runtimes.view(job),
            "setupPlan": (job["plan"] or {}).get("setup") if job["status"] == "needs_approval" else None,
        }
        plan_stages = (job["plan"] or {}).get("stages") or []
        if len(plan_stages) > 1:
            rows = {r["seq"]: r for r in self.db.stages(job["id"])}
            view["stages"] = [{"name": s["name"], "kind": s["kind"], "status": rows[i]["status"] if i in rows else "waiting",
                               "total": rows[i]["total"] if i in rows else None, "done": rows[i]["done"] if i in rows else 0}
                              for i, s in enumerate(plan_stages)]
        if full:
            view["spec"] = job["spec"]
            view["provenance"] = self.provenance(job)
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
                "busy": len(self.s.resources.held(d.id, "engine")), "python": info.get("python"),
                "envs": len(info.get("envs", [])), "accelerators": info.get("accelerators", []), "profile": info.get("profile"),
                "oci": info.get("oci"), "ociWhy": info.get("ociWhy") or "", "tier": isolation.tier(d)}

    def overview(self) -> dict:
        jobs = [self.job_view(j) for j in self.db.all("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50")]
        return {"jobs": jobs, "datasets": self.datasets(), "models": self.models(), "storage": self.store.usage()}

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
