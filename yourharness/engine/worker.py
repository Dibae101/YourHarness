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


def run(job: dict, report) -> None:
    from .execute import feed_halo, make_op, run_items, run_partition, writes_records

    task = job["task"]
    wl = task["workload"]
    http = Http(job["server"], job["token"], job["partition"], job["attempt"])
    out_dir = Path(job["outDir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    sink = RecordFile(out_dir / "part.jsonl", int(task.get("maxOutputBytes") or 4 * 2**30)) if writes_records(wl) else None
    model = None
    if task.get("model"):
        from .models import fetch

        folder = fetch(lambda: http.chunks("/model"), Path(job.get("cache") or out_dir.parent / "cache"), task["model"])
        model = {"adapter": task["model"]["adapter"], "path": str(folder)}
    op = make_op(wl, task.get("ctx") or {}, sink, bad=task.get("onBadRecord", "fail"), model=model)
    inp = task["input"]
    t0 = time.monotonic()
    read = [0]
    if inp["kind"] == "items":
        stats = run_items(op, inp["items"], inp["first"])
    else:
        fmt, header, data_start = task["format"], task.get("header"), task["dataStart"]
        cp = task.get("checkpoint")
        start = inp["start"]
        if cp and getattr(op, "resumable", False):
            op.load(cp["state"])
            start = cp["offset"]
        elif inp.get("halo"):
            halo_start = max(data_start, inp["start"] - inp["halo"])
            feed_halo(op, wl, fmt, header, http.chunks("/input?halo=1", read), halo_start, data_start)
        query = f"/input?from={start}" if start != inp["start"] else "/input"

        def checkpoint(offset: int, state: dict) -> None:
            if len(json.dumps(state, separators=(",", ":"))) <= INLINE_STATE:
                report({"type": "checkpoint", "offset": offset, "state": state})

        def progress(records: int, offset: int) -> None:
            report({"type": "progress", "records": records, "offset": offset})

        stats = run_partition(op, fmt, header, http.chunks(query, read), start, bad=task.get("onBadRecord", "fail"),
                              checkpoint=checkpoint, every_s=float(task.get("checkpointEvery") or 10), progress=progress)
    seconds = time.monotonic() - t0
    state = op.state()
    outputs = {}
    done = {"type": "done", "records": stats["records"], "skipped": stats["skipped"], "bad": stats["bad"], "seconds": round(seconds, 3),
            "bytes": read[0]}
    if callable(getattr(op, "timings", None)):
        done["metrics"] = op.timings()
    if sink is not None:
        info = sink.close()
        got = http.upload("part.jsonl", sink.path)
        if got.get("sha") != info["sha"]:
            raise ConnectionError("the coordinator stored different bytes than were sent")
        outputs["part.jsonl"] = info
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
    if isinstance(e, ModelIntegrityError):
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
