"""One partition, in its own process on the computer that took it.

The agent starts this process for each attempt and writes the task as one JSON
line on stdin, token included (never on the command line). The process streams its
input from the coordinator with that token, runs the operator, uploads what it
wrote, and reports on stdout as JSON lines:

  {"type": "progress", "records": n, "offset": o}
  {"type": "checkpoint", "offset": o, "state": {...}}     resumable operators only
  {"type": "done", ...}  or  {"type": "error", "error": "...", "errorKind": "data|code|halo|infra"}

Anything else printed (your own code's print calls) goes to stderr, so it can never
be mistaken for a report. Standard library only: this also runs inside a managed
Python environment that has nothing but the job's own packages.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

INLINE_STATE = 256 * 1024
READ = 1024 * 1024


class Reporter:
    def __init__(self, stream):
        self.stream = stream

    def __call__(self, msg: dict) -> None:
        self.stream.write(json.dumps(msg, separators=(",", ":"), allow_nan=False) + "\n")
        self.stream.flush()


class Http:
    def __init__(self, server: str, token: str, pid: str, attempt: int):
        self.base = f"{server.rstrip('/')}/api/engine/attempts/{urllib.parse.quote(pid)}/{attempt}"
        self.headers = {"Authorization": f"Bearer {token}"}

    def chunks(self, path: str, counter: list | None = None):
        req = urllib.request.Request(self.base + path, headers=self.headers)
        with urllib.request.urlopen(req, timeout=120) as res:
            while True:
                data = res.read(READ)
                if not data:
                    return
                if counter is not None:
                    counter[0] += len(data)
                yield data

    def upload(self, name: str, path: Path) -> dict:
        size = path.stat().st_size
        with open(path, "rb") as f:
            req = urllib.request.Request(f"{self.base}/outputs/{urllib.parse.quote(name)}", data=f, method="PUT",
                                         headers={**self.headers, "Content-Type": "application/octet-stream", "Content-Length": str(size)})
            with urllib.request.urlopen(req, timeout=600) as res:
                return json.loads(res.read())


class RecordFile:
    """Output records written to disk as JSON lines, hashed and counted on the way."""

    def __init__(self, path: Path, limit: int):
        from .execute import dump_record

        self.dump = dump_record
        self.path = path
        self.f = open(path, "wb")
        self.h = hashlib.sha256()
        self.size = 0
        self.records = 0
        self.limit = limit

    def __call__(self, rec) -> None:
        data = self.dump(rec).encode("utf-8")
        self.size += len(data)
        if self.size > self.limit:
            from .ops import OpError

            raise OpError(f"the partition wrote more than {self.limit / 2**30:g} GB")
        self.f.write(data)
        self.h.update(data)
        self.records += 1

    def close(self) -> dict:
        self.f.close()
        return {"sha": self.h.hexdigest(), "size": self.size, "records": self.records}


class InputIntegrityError(RuntimeError):
    """An input file that arrived (or was cached here) isn't the one the partition names."""


class ArtifactCache:
    """Files this computer made or fetched, by SHA-256, so a later stage can read them without the network.

    Every read checks the digest first; a file that doesn't match is deleted and fetched again.
    """

    def __init__(self, root: Path):
        self.root = root / "artifacts"
        self.cap = float(os.environ.get("HARNESS_ARTIFACT_CACHE_GB", 2)) * 2**30

    def path(self, sha: str) -> Path:
        return self.root / sha

    def verified(self, sha: str) -> Path | None:
        p = self.path(sha)
        if len(sha) != 64 or not p.is_file():
            return None
        h = hashlib.sha256()
        with open(p, "rb") as f:
            while data := f.read(READ):
                h.update(data)
        if h.hexdigest() != sha:
            p.unlink(missing_ok=True)
            return None
        os.utime(p)
        return p

    def keep(self, src: Path, sha: str) -> None:
        """Move a file just uploaded into the cache (it is already hashed)."""
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            os.replace(src, self.path(sha))
            self._evict(sha)
        except OSError:
            pass

    def _evict(self, keep: str) -> None:
        files = sorted((f.stat().st_mtime, f.stat().st_size, f) for f in self.root.iterdir() if f.is_file())
        total = sum(s for _, s, _ in files)
        for _t, size, f in files:
            if total <= self.cap:
                return
            if f.name != keep:
                f.unlink(missing_ok=True)
                total -= size

    def chunks(self, sha: str, fetch, start: int = 0, counter: list | None = None):
        """The file's bytes from `start`: from the cache when it has a good copy, else fetched and checked."""
        local = self.verified(sha)
        if local is not None:
            with open(local, "rb") as f:
                f.seek(start)
                while data := f.read(READ):
                    yield data
            return
        h = hashlib.sha256() if start == 0 else None
        for data in fetch():
            if h is not None:
                h.update(data)
            if counter is not None:
                counter[0] += len(data)
            yield data
        if h is not None and h.hexdigest() != sha:
            raise InputIntegrityError(f"an input arrived with SHA-256 {h.hexdigest()[:12]}…, not {sha[:12]}…")


def _context(task: dict, http) -> dict:
    """The stage's context snapshot: inline when small, else fetched by its digest and checked."""
    ref = task.get("ctxRef")
    if not ref:
        return task.get("ctx") or {}
    data = b"".join(http.chunks("/context"))
    if hashlib.sha256(data).hexdigest() != ref["sha"]:
        raise InputIntegrityError("the job's context arrived changed")
    return json.loads(data)


def _upload(http, cache: ArtifactCache, name: str, path: Path, info: dict) -> dict:
    got = http.upload(name, path)
    if got.get("sha") != info["sha"]:
        raise ConnectionError("the coordinator stored different bytes than were sent")
    cache.keep(path, info["sha"])
    return info


def run(job: dict, report) -> None:
    from .execute import feed_halo, make_op, run_items, run_partition, writes_records

    task = job["task"]
    wl = task["workload"]
    kind = task.get("stage", "map")
    http = Http(job["server"], job["token"], job["partition"], job["attempt"])
    out_dir = Path(job["outDir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_root = Path(job.get("cache") or out_dir.parent / "cache")
    cache = ArtifactCache(cache_root)
    limit = int(task.get("maxOutputBytes") or 4 * 2**30)
    inp = task["input"]
    t0 = time.monotonic()
    read = [0]
    outputs: dict = {}
    if kind in ("shuffle-reduce", "join-reduce"):
        state, outputs, records = _reduce(task, http, cache, out_dir, limit, read)
        done = {"type": "done", "records": records, "skipped": 0, "bad": [], "seconds": round(time.monotonic() - t0, 3), "bytes": read[0]}
        _finish(done, state, outputs, http, out_dir, report)
        return
    files = None
    if kind == "join-map":
        from .shuffle import BucketFiles, JoinMapper

        files = BucketFiles(out_dir, "b")
        op, sink = JoinMapper(wl, task["side"], int(task["buckets"]), files), None
    else:
        sink = RecordFile(out_dir / "part.jsonl", limit) if kind == "map" and writes_records(wl) else None
        model = None
        if task.get("model"):
            from .models import fetch

            folder = fetch(lambda: http.chunks("/model"), cache_root, task["model"])
            model = {"adapter": task["model"]["adapter"], "path": str(folder)}
        op = make_op(wl, _context(task, http), sink, bad=task.get("onBadRecord", "fail"), model=model)
    if inp["kind"] == "items":
        stats = run_items(op, inp["items"], inp["first"])
    else:
        fmt, header, data_start = task["format"], task.get("header"), task["dataStart"]
        cp = task.get("checkpoint")
        start = inp["start"] if inp["kind"] == "range" else 0
        first = start
        if cp and getattr(op, "resumable", False):
            op.load(cp["state"])
            start = cp["offset"]
        elif inp.get("halo"):
            halo_start = max(data_start, inp["start"] - inp["halo"])
            feed_halo(op, wl, fmt, header, http.chunks("/input?halo=1", read), halo_start, data_start)
        query = f"/input?from={start}" if start != first else "/input"
        if inp["kind"] == "artifact":  # an earlier stage's records: read here if this computer made or fetched them
            chunks = cache.chunks(inp["sha"], lambda: http.chunks(query), start, read)
        else:
            chunks = http.chunks(query, read)

        def checkpoint(offset: int, state: dict) -> None:
            if len(json.dumps(state, separators=(",", ":"))) <= INLINE_STATE:
                report({"type": "checkpoint", "offset": offset, "state": state})

        def progress(records: int, offset: int) -> None:
            report({"type": "progress", "records": records, "offset": offset})

        stats = run_partition(op, fmt, header, chunks, start, bad=task.get("onBadRecord", "fail"),
                              checkpoint=checkpoint, every_s=float(task.get("checkpointEvery") or 10), progress=progress)
    seconds = time.monotonic() - t0
    done = {"type": "done", "records": stats["records"], "skipped": stats["skipped"], "bad": stats["bad"], "seconds": round(seconds, 3),
            "bytes": read[0]}
    if callable(getattr(op, "timings", None)):
        done["metrics"] = op.timings()
    if kind == "shuffle-map":
        from .shuffle import groupby_map_state

        state, blobs = groupby_map_state(op, int(task["buckets"]))
        for name, data in blobs.items():
            path = out_dir / name
            path.write_bytes(data)
            outputs[name] = _upload(http, cache, name, path, {"sha": hashlib.sha256(data).hexdigest(), "size": len(data)})
    elif kind == "join-map":
        state = op.state()
        for name, info in files.close().items():
            outputs[name] = _upload(http, cache, name, out_dir / name, info)
    else:
        state = op.state()
    if sink is not None:
        outputs["part.jsonl"] = _upload(http, cache, "part.jsonl", sink.path, sink.close())
    _finish(done, state, outputs, http, out_dir, report)


def _reduce(task: dict, http, cache: ArtifactCache, out_dir: Path, limit: int, read: list) -> tuple[dict, dict, int]:
    """Second stage of a shuffle: bucket b's files from every first-stage partition, combined."""
    from .shuffle import groupby_reduce, join_reduce, lines_of

    parts = task["input"]["parts"]
    sink = RecordFile(out_dir / "part.jsonl", limit)

    def part(i: int):
        return cache.chunks(parts[i]["sha"], lambda: http.chunks(f"/parts/{i}"), 0, read)

    if task["stage"] == "shuffle-reduce":
        state = groupby_reduce(task["workload"], (b"".join(part(i)) for i in range(len(parts))), sink)
    else:
        right = (ln for i, p in enumerate(parts) if p["side"] == "R" for ln in lines_of(part(i)))
        left = (ln for i, p in enumerate(parts) if p["side"] == "L" for ln in lines_of(part(i)))
        state = join_reduce(task["workload"], right, left, sink)
    outputs = {"part.jsonl": _upload(http, cache, "part.jsonl", sink.path, sink.close())}
    return state, outputs, outputs["part.jsonl"]["records"]


def _finish(done: dict, state: dict, outputs: dict, http, out_dir: Path, report) -> None:
    text = json.dumps(state, separators=(",", ":"), allow_nan=False)
    if len(text) > INLINE_STATE:
        path = out_dir / "state.json"
        path.write_text(text, encoding="utf-8")
        got = http.upload("state.json", path)
        outputs["state.json"] = {"sha": got["sha"], "size": got["size"]}
        done["stateOutput"] = "state.json"
    else:
        done["state"] = state
    done["outputs"] = outputs
    report(done)


def classify(e: BaseException) -> str:
    from .execute import HaloTooSmall
    from .ml import ModelCodeError
    from .models import ModelIntegrityError
    from .ops import OpError
    from .pyop import UserCodeError
    from .records import RecordError

    if isinstance(e, HaloTooSmall):
        return "halo"
    if isinstance(e, (RecordError, OpError)):
        return "data"
    if isinstance(e, (UserCodeError, ModelCodeError)):
        return "code"
    if isinstance(e, (ModelIntegrityError, InputIntegrityError)):
        return "integrity"
    return "infra"


def describe(e: BaseException) -> str:
    if isinstance(e, urllib.error.HTTPError):
        try:
            detail = json.loads(e.read()).get("error")
        except ValueError:
            detail = None
        return f"the coordinator answered {e.code}" + (f": {detail}" if detail else "")
    if isinstance(e, urllib.error.URLError):
        return f"couldn't reach the coordinator ({e.reason})"
    return f"{type(e).__name__}: {e}" if str(e) else type(e).__name__


def main() -> None:
    report = Reporter(os.fdopen(os.dup(1), "w", encoding="utf-8"))
    os.dup2(2, 1)  # print() in the job's code lands on stderr
    sys.stdout = sys.stderr
    line = sys.stdin.readline()
    if not line:
        return
    job = json.loads(line)
    try:
        run(job, report)
    except BaseException as e:  # noqa: BLE001 - report everything, including the job's own SystemExit
        if isinstance(e, KeyboardInterrupt):
            return
        report({"type": "error", "error": describe(e)[:2000], "errorKind": classify(e)})


if __name__ == "__main__":
    main()
