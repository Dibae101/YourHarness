"""The job engine end to end: a real coordinator, real agents, real worker processes.

Each distributed result is compared with the reference executor run on the same
bytes in this process, so "done" always means "the right answer", not only "finished".
"""

from __future__ import annotations

import json
import os
import signal
import time
import urllib.parse
from pathlib import Path

import pytest

from conftest import free_port, start_coordinator
from yourharness.engine import spec as jobspec
from yourharness.engine.index import Indexer
from yourharness.engine.local import reference, reference_items


class Engine:
    def __init__(self, coord):
        self.coord = coord
        self.c = coord.client("engine tests")

    def upload(self, name: str, data: bytes, fmt: str | None = None, classification: str | None = None) -> dict:
        q = {"name": name, **({"format": fmt} if fmt else {}), **({"classification": classification} if classification else {})}
        return self.c.request("PUT", "/api/engine/datasets?" + urllib.parse.urlencode(q), data=data,
                              headers={"Content-Type": "application/octet-stream", "Content-Length": str(len(data))})["dataset"]

    def job_doc(self, workload: dict, dataset: dict | None = None, items=None, **spec) -> dict:
        inputs = {"dataset": {"ref": dataset["ref"]}} if dataset else {"items": items}
        return {"apiVersion": jobspec.API_VERSION, "kind": "Job", "metadata": {"name": "test"},
                "spec": {"workload": workload, "inputs": inputs, **spec}}

    def start(self, workload: dict, dataset: dict | None = None, items=None, **spec) -> dict:
        return self.c.request("POST", "/api/engine/jobs", {"job": self.job_doc(workload, dataset, items, **spec)})["job"]

    def get(self, job_id: str) -> dict:
        return self.c.request("GET", f"/api/engine/jobs/{job_id}")["job"]

    def wait(self, job_id: str, until=("done", "failed", "cancelled"), timeout: float = 90) -> dict:
        end = time.monotonic() + timeout
        job = None
        while time.monotonic() < end:
            job = self.get(job_id)
            if job["status"] in until:
                return job
            time.sleep(0.2)
        raise AssertionError(f"job stayed {job and job['status']}: {json.dumps(job, indent=1)[:4000]}")

    def run(self, workload: dict, dataset: dict | None = None, items=None, **spec) -> dict:
        return self.wait(self.start(workload, dataset, items, **spec)["id"])

    def result_bytes(self, job_id: str) -> bytes:
        return self.c.request("GET", f"/api/engine/jobs/{job_id}/result")

    def ready(self, names: set[str], timeout: float = 20) -> list[dict]:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            devs = [d for d in self.c.devices() if d["name"] in names and d.get("engine") and d["engine"]["slots"]]
            if {d["name"] for d in devs} == names:
                return devs
            time.sleep(0.2)
        raise AssertionError(f"not ready: {names}")


def expected(tmp_path: Path, data: bytes, fmt: str, workload: dict, **spec) -> dict:
    """The reference result of a job over these bytes, computed here."""
    path = tmp_path / f"ref-{time.monotonic_ns()}.{fmt}"
    path.write_bytes(data)
    ix = Indexer(fmt)
    ix.feed(data)
    meta = ix.finish()
    meta["format"] = fmt
    doc = jobspec.parse({"spec": {"workload": workload, "inputs": {"dataset": {"ref": "dataset:d00000000"}}, **spec}})
    return reference(doc, meta, path)


def sales(n: int = 20000) -> bytes:
    return b"".join(json.dumps({"id": i, "region": "nsew"[i % 4], "amount": ((i * 7919) % 1000) / 7, "qty": i % 13}).encode() + b"\n"
                    for i in range(n))


@pytest.fixture(scope="module")
def pool(tmp_path_factory):
    coord = start_coordinator(tmp_path_factory.mktemp("engine"))
    coord.agent("Alpha", threads=2, extra=("--no-tasks",))
    coord.agent("Beta", threads=2, extra=("--no-tasks",))
    eng = Engine(coord)
    eng.ready({"Alpha", "Beta"})
    yield eng
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


AGG = {"type": "batch.aggregate", "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "mean", "field": "amount"},
                                             {"op": "min", "field": "qty"}, {"op": "max", "field": "qty"}]}


def test_an_aggregate_spread_over_two_computers_matches_the_reference(pool, tmp_path):
    data = sales()
    ds = pool.upload("sales.jsonl", data)
    assert ds["format"] == "jsonl" and ds["size"] == len(data) and ds["lines"] == 20000
    job = pool.run(AGG, ds, mode="distributed", partitioning={"targetBytes": 32 * 1024})
    assert job["status"] == "done", job
    assert job["result"] == expected(tmp_path, data, "jsonl", AGG)["result"]
    assert job["total"] > 10 and job["counts"] == {"done": job["total"]}
    assert {d["name"] for d in job["perDevice"]} == {"Alpha", "Beta"}, "both computers took partitions"
    assert job["validation"]["level"] == "structural"
    full = pool.get(job["id"])
    assert all(p["status"] == "done" and p["attempt"] == 1 for p in full["partitions"])


def test_a_group_by_over_csv_with_quoted_line_breaks(pool, tmp_path):
    lines = ["region,amount,note"] + [f'{"ab"[i % 2]},{i % 17},"line one\nline two {i}"' for i in range(3000)]
    data = ("\n".join(lines) + "\n").encode()
    ds = pool.upload("notes.csv", data)
    assert ds["header"] == ["region", "amount", "note"] and ds["quoted"]
    wl = {"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}]}
    job = pool.run(wl, ds, mode="distributed", partitioning={"targetBytes": 8 * 1024}, output={"spotCheck": 3})
    assert job["status"] == "done", job
    assert job["result"] == expected(tmp_path, data, "csv", wl)["result"]
    assert job["validation"]["level"] == "spot-checked"


def test_a_rolling_mean_across_partitions_writes_the_same_records(pool, tmp_path):
    data = b"".join(json.dumps({"t": i, "v": (i * 31) % 97}).encode() + b"\n" for i in range(5000))
    ds = pool.upload("series.jsonl", data)
    wl = {"type": "batch.transform", "rolling": {"field": "v", "window": 25}, "select": ["t", "rolling_mean_v"]}
    job = pool.run(wl, ds, mode="distributed", partitioning={"targetBytes": 4 * 1024}, output={"spotCheck": 2})
    assert job["status"] == "done", job
    got = [json.loads(line) for line in pool.result_bytes(job["id"]).decode().splitlines()]
    assert got == expected(tmp_path, data, "jsonl", wl)["records"]
    assert job["outputs"]["records"] == 5000 and job["validation"]["level"] == "spot-checked"


def test_your_own_python_over_items_and_a_parameter_grid(pool):
    code = "def process(x):\n    return x * x\n"
    job = pool.run({"type": "python.map", "code": code}, items=list(range(200)), mode="distributed",
                   partitioning={"itemsPerPartition": 9})
    assert job["status"] == "done", job
    assert job["result"]["value"] == [i * i for i in range(200)]
    best = {"type": "python.map", "reducer": "best", "bestBy": {"key": "score", "order": "min"},
            "code": "def process(p):\n    return {**p, 'score': (p['a'] - 3) ** 2 + (p['b'] - 0.5) ** 2}\n"}
    doc = {"spec": {"workload": best, "inputs": {"grid": {"a": [1, 2, 3, 4], "b": [0.1, 0.5, 0.9]}}, "mode": "distributed"}}
    job = pool.wait(pool.c.request("POST", "/api/engine/jobs", {"job": doc})["job"]["id"])
    assert job["result"]["value"] == {"a": 3, "b": 0.5, "score": 0.0}


def test_a_small_job_stays_on_one_computer_and_the_plan_says_why(pool):
    ds = pool.upload("small.jsonl", sales(300))
    plan = pool.c.request("POST", "/api/engine/plan", {"job": pool.job_doc(AGG, ds)})["plan"]
    assert plan["decision"] == "RUN_LOCAL" and "small" in plan["reasons"][0]
    assert {d["name"] for d in plan["devices"] if d["eligible"]} >= {"Alpha", "Beta"}
    job = pool.run(AGG, ds, partitioning={"targetBytes": 2048})
    assert job["decision"] == "RUN_LOCAL" and len(job["perDevice"]) == 1


def test_bad_records_fail_the_job_with_the_offset_or_are_skipped(pool):
    data = b'{"amount": 1}\n{"amount": 2}\nnot json\n{"amount": 4}\n'
    ds = pool.upload("bad.jsonl", data)
    wl = {"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "amount"}]}
    job = pool.run(wl, ds)
    assert job["status"] == "failed" and "byte 28" in job["error"], job
    job = pool.run(wl, ds, output={"onBadRecord": "skip"})
    assert job["status"] == "done" and job["result"]["values"] == {"sum_amount": 7} and job["result"]["badRecordsSkipped"] == 1


def test_the_job_description_is_checked_before_anything_runs(pool):
    with pytest.raises(Exception, match="spec.workload.metrics"):
        pool.start({"type": "batch.aggregate", "metrics": [{"op": "median", "field": "x"}]}, {"ref": "dataset:d00000000"})
    with pytest.raises(Exception, match="no dataset"):
        pool.start(AGG, {"ref": "dataset:d00000000"})


def test_a_worker_resumes_from_a_checkpoint(tmp_path, monkeypatch):
    """A partition that restarts from a checkpoint reads only the rest and gets the same state."""
    from yourharness.engine import worker
    from yourharness.engine.local import file_chunks

    data = sales(3000)
    path = tmp_path / "d.jsonl"
    path.write_bytes(data)
    ix = Indexer("jsonl")
    ix.feed(data)
    meta = ix.finish()
    doc = jobspec.parse({"spec": {"workload": AGG, "inputs": {"dataset": {"ref": "dataset:d00000000"}}}})
    mid = meta["splits"][len(meta["splits"]) // 2]
    from yourharness.engine.local import run_range

    prefix_state, _ = run_range(doc, {**meta, "format": "jsonl"}, path, 0, mid)
    whole_state, _ = run_range(doc, {**meta, "format": "jsonl"}, path, 0, len(data))
    asked = []

    class FakeHttp:
        def __init__(self, *a):
            pass

        def chunks(self, query, counter=None):
            asked.append(query)
            start = int(query.split("from=")[1]) if "from=" in query else 0
            yield from file_chunks(path, start, len(data))

    monkeypatch.setattr(worker, "Http", FakeHttp)
    reports = []
    task = {"workload": doc["spec"]["workload"], "input": {"kind": "range", "start": 0, "end": len(data), "halo": 0}, "format": "jsonl",
            "header": None, "dataStart": 0, "checkpoint": {"offset": mid, "state": prefix_state}}
    worker.run({"server": "http://x", "token": "t", "partition": "p", "attempt": 2, "task": task, "outDir": str(tmp_path / "out")}, reports.append)
    assert asked == [f"/input?from={mid}"]
    assert reports[-1]["type"] == "done" and reports[-1]["state"] == whole_state


# ---------------------------------------------------------------------- failures
SLOW = {"type": "python.map", "code": "import time\ndef process(x):\n    time.sleep(0.15)\n    return x + 1\n"}


@pytest.fixture
def fresh_pool(tmp_path):
    port = free_port()
    coord = start_coordinator(tmp_path / "c", port=port)
    coord.agent("Alpha", threads=2, extra=("--no-tasks",))
    coord.agent("Beta", threads=2, extra=("--no-tasks",))
    eng = Engine(coord)
    eng.port = port
    eng.ready({"Alpha", "Beta"})
    yield eng
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


def _done_events(eng: Engine, job_id: str) -> dict[str, int]:
    seen: dict[str, int] = {}
    for e in eng.get(job_id)["events"]:
        if e["type"] == "done":
            seen[e["partition"]] = seen.get(e["partition"], 0) + 1
    return seen


def test_a_computer_that_disappears_mid_job_loses_nothing(fresh_pool):
    eng = fresh_pool
    job = eng.start(SLOW, items=list(range(60)), mode="distributed", partitioning={"itemsPerPartition": 3},
                    retry={"leaseSeconds": 5})
    end = time.monotonic() + 20
    while time.monotonic() < end and eng.get(job["id"])["counts"].get("done", 0) < 3:
        time.sleep(0.1)
    eng.coord.agents[0].stop()  # Alpha goes away mid-job
    job = eng.wait(job["id"], timeout=90)
    assert job["status"] == "done", job
    assert job["result"]["value"] == [i + 1 for i in range(60)]
    assert all(n == 1 for n in _done_events(eng, job["id"]).values()), "every partition was accepted exactly once"


def test_a_frozen_computer_is_replaced_and_its_late_result_counts_once(fresh_pool):
    eng = fresh_pool
    alpha = eng.coord.agents[0]
    job = eng.start(SLOW, items=list(range(40)), mode="distributed", partitioning={"itemsPerPartition": 4},
                    retry={"leaseSeconds": 5})
    end = time.monotonic() + 20
    while time.monotonic() < end and not any(p["device"] == "Alpha" and p["status"] == "leased" for p in eng.get(job["id"])["partitions"]):
        time.sleep(0.05)
    os.kill(alpha.p.pid, signal.SIGSTOP)  # frozen: no progress reports, its lease runs out
    try:
        time.sleep(7.5)
    finally:
        os.kill(alpha.p.pid, signal.SIGCONT)
    job = eng.wait(job["id"], timeout=90)
    assert job["status"] == "done", job
    assert job["result"]["value"] == [i + 1 for i in range(40)]
    parts = eng.get(job["id"])["partitions"]
    assert any(p["attempt"] > 1 or p["moves"] for p in parts), "at least one partition moved to another computer"
    assert all(n == 1 for n in _done_events(eng, job["id"]).values())


def test_the_coordinator_restarting_mid_job_loses_nothing(fresh_pool, tmp_path):
    eng = fresh_pool
    job = eng.start(SLOW, items=list(range(60)), mode="distributed", partitioning={"itemsPerPartition": 3})
    end = time.monotonic() + 20
    while time.monotonic() < end and eng.get(job["id"])["counts"].get("done", 0) < 4:
        time.sleep(0.1)
    eng.coord.proc.stop()
    again = start_coordinator(tmp_path / "c", port=eng.port)
    eng.coord.proc = again.proc
    eng.c = again.client("after restart")
    job = eng.wait(job["id"], timeout=120)
    assert job["status"] == "done", job
    assert job["result"]["value"] == [i + 1 for i in range(60)]
    assert all(n == 1 for n in _done_events(eng, job["id"]).values())


def test_a_friends_computer_gets_only_shareable_data_and_never_code(fresh_pool):
    eng = fresh_pool
    code = eng.c.request("POST", "/api/invites", {"label": "Sam"})["code"]
    eng.coord.agent("Sams PC", threads=2, link=code, extra=("--server", eng.coord.base, "--no-tasks"))
    eng.ready({"Sams PC"})
    ds = eng.upload("data.jsonl", sales(4000))
    friends = {"allowedTrust": "owned-and-friends"}
    plan = eng.c.request("POST", "/api/engine/plan", {"job": eng.job_doc(AGG, ds, security=friends)})["plan"]
    sam = next(d for d in plan["devices"] if d["name"] == "Sams PC")
    assert not sam["eligible"] and "isn't marked shareable" in sam["why"]
    eng.c.request("POST", f"/api/engine/datasets/{ds['id']}", {"classification": "shareable"})
    job = eng.run(AGG, ds, mode="distributed", security=friends, partitioning={"targetBytes": 4096, "maxInflightPerWorker": 1})
    assert job["status"] == "done" and "Sams PC" in {d["name"] for d in job["perDevice"]}
    job = eng.run(AGG, ds, mode="distributed", partitioning={"targetBytes": 4096})
    assert "Sams PC" not in {d["name"] for d in job["perDevice"]}, "owned-only jobs never reach a friend"
    with pytest.raises(Exception, match="your own computers"):
        eng.start(SLOW, items=[1, 2], security=friends)
