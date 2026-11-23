"""Jobs with more than one stage: barriers with context snapshots, shuffled group-bys, joins,
reading where the data already is, and duplicating stragglers.

Each result is compared with the same computation done plainly in this process.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time

import pytest

from conftest import start_coordinator
from test_engine_e2e import Engine
from yourharness.engine import spec as jobspec
from yourharness.engine.ops import GroupByOp
from yourharness.engine.shuffle import bucket_of, groupby_buckets, groupby_reduce, join_key, join_reduce, line


def sales(n: int = 6000) -> bytes:
    return b"".join(json.dumps({"id": i, "region": "nsew"[i % 4], "cust": i % 37, "amount": ((i * 7919) % 1000) / 8, "qty": i % 13}).encode() + b"\n"
                    for i in range(n))


def key_text(group: dict) -> str:
    """Groups sort by their key's JSON text, as the engine sorts them."""
    return json.dumps(list(group["key"].values()), ensure_ascii=False, separators=(",", ":"))


def customers() -> bytes:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(["cust", "name", "amount"])
    for c in range(30):  # customers 30..36 are missing on purpose, and the amount column clashes with the orders'
        w.writerow([c, f"customer {c}", c * 10])
    return out.getvalue().encode()


# ---------------------------------------------------------------- the spec
def test_stages_context_and_shuffle_in_the_spec():
    doc = jobspec.parse({"spec": {"stages": [
        {"name": "stats", "workload": {"type": "batch.aggregate", "metrics": [{"op": "mean", "field": "amount"}]}},
        {"name": "flag", "workload": {"type": "python.map", "reducer": "concat", "code": "def process(r, ctx):\n    return r\n"}},
        {"name": "count", "from": "flag", "workload": {"type": "batch.aggregate"}}],
        "inputs": {"dataset": {"ref": "dataset:d00000000"}}, "context": {"values": {"limit": 5}}}})
    s = doc["spec"]
    assert [x["name"] for x in s["stages"]] == ["stats", "flag", "count"] and s["workload"]["type"] == "batch.aggregate"
    assert s["runtime"]["kind"] == "native-python", "a job with any code of yours runs only on your computers"
    assert s["context"]["version"] == hashlib.sha256(b'{"limit":5}').hexdigest()
    assert s["retry"]["speculate"] is True
    bad = [
        ({"stages": [{"name": "a", "workload": {"type": "batch.aggregate"}}, {"name": "b", "from": "a", "workload": {"type": "batch.aggregate"}}]},
         "spec.stages[1].from"),  # an aggregate writes no records to read
        ({"stages": [{"name": "a", "workload": {"type": "batch.aggregate"}}, {"name": "a", "workload": {"type": "batch.aggregate"}}]},
         "spec.stages[1].name"),
        ({"stages": [{"workload": {"type": "batch.groupby", "by": "k", "shuffle": True}}, {"workload": {"type": "batch.aggregate"}}]},
         "spec.stages[0].workload"),
        ({"workload": {"type": "batch.aggregate"}, "context": {"values": {"a": 1}, "version": "0" * 64}}, "spec.context.version"),
        ({"workload": {"type": "batch.join", "on": "id"}}, "spec.inputs.right.ref"),
    ]
    for extra, where in bad:
        with pytest.raises(jobspec.SpecError) as e:
            jobspec.parse({"spec": {"inputs": {"dataset": {"ref": "dataset:d00000000"}}, **extra}})
        assert e.value.path == where, (e.value, extra)
    g = jobspec.parse({"spec": {"workload": {"type": "batch.groupby", "by": "k", "shuffle": {"buckets": 8}}, "inputs": {"dataset": {"ref": "dataset:d00000000"}}}})
    assert g["spec"]["workload"]["shuffle"] == {"buckets": 8} and g["spec"]["stages"] is None


def test_a_shuffle_gives_the_same_groups_as_combining_everything():
    wl = jobspec.parse({"spec": {"workload": {"type": "batch.groupby", "by": ["cust"], "shuffle": True,
                                              "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "max", "field": "qty"}]},
                                 "inputs": {"dataset": {"ref": "dataset:d00000000"}}}})["spec"]["workload"]
    rows = [json.loads(x) for x in sales(3000).splitlines()]
    whole = GroupByOp(wl)
    for r in rows:
        whole.feed(r)
    want = GroupByOp.final(wl, whole.state())["groups"]
    parts = []
    for i in range(0, len(rows), 700):  # five partitions
        op = GroupByOp(wl)
        for r in rows[i:i + 700]:
            op.feed(r)
        parts.append(groupby_buckets(op.state(), 6))
    got = []
    for b in range(6):
        groupby_reduce(wl, [p[f"g{b}.json"] for p in parts if f"g{b}.json" in p], got.append)
    assert sorted(got, key=key_text) == want
    assert bucket_of('[7]', 6) == bucket_of('[7]', 6) == int(hashlib.sha256(b"[7]").hexdigest()[:16], 16) % 6, "the same bucket everywhere"


def test_join_keys_match_numbers_and_text_alike_and_clashing_fields_are_kept():
    assert join_key({"id": 7}, ["id"]) == join_key({"id": "7"}, ["id"]) == join_key({"id": 7.0}, ["id"]) and join_key({}, ["id"]) is None
    wl = {"on": ["id"], "rightOn": ["cid"], "how": "left"}
    right = [line({"k": join_key({"cid": "1"}, ["cid"]), "r": {"cid": "1", "name": "a", "x": 9}})]
    left = [line({"k": join_key({"id": 1}, ["id"]), "r": {"id": 1, "x": 1}}), line({"k": None, "r": {"id": None}}),
            line({"k": join_key({"id": 2}, ["id"]), "r": {"id": 2, "x": 2}})]
    out: list = []
    st = join_reduce(wl, right, left, out.append)
    assert out == [{"id": 1, "x": 1, "name": "a", "right_x": 9}, {"id": None}, {"id": 2, "x": 2}]
    assert st == {"left": 3, "right": 1, "matched": 1, "out": 3}
    out = []
    join_reduce({**wl, "how": "inner"}, right, left, out.append)
    assert out == [{"id": 1, "x": 1, "name": "a", "right_x": 9}]


# ---------------------------------------------------------------- across computers
@pytest.fixture(scope="module")
def pool(tmp_path_factory):
    coord = start_coordinator(tmp_path_factory.mktemp("stages"))
    coord.agent("Alpha", threads=2, extra=("--no-tasks",))
    coord.agent("Beta", threads=2, extra=("--no-tasks",))
    eng = Engine(coord)
    eng.ready({"Alpha", "Beta"})
    yield eng
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


def submit(eng, spec: dict, timeout: float = 120) -> dict:
    job = eng.c.request("POST", "/api/engine/jobs", {"job": {"apiVersion": jobspec.API_VERSION, "kind": "Job", "metadata": {"name": "t"}, "spec": spec}})["job"]
    return eng.wait(job["id"], timeout=timeout)


FLAG = """\
def process(r, ctx):
    mean = ctx["stages"]["stats"]["values"]["mean_amount"]
    if r["amount"] > mean * ctx["factor"]:
        return {"id": r["id"], "over": round(r["amount"] - mean, 6)}
"""


def test_a_later_stage_reads_the_earlier_ones_result_and_records(pool):
    data = sales()
    ds = pool.upload("stages.jsonl", data)
    job = submit(pool, {
        "stages": [{"name": "stats", "workload": {"type": "batch.aggregate", "metrics": [{"op": "mean", "field": "amount"}]}},
                   {"name": "flag", "workload": {"type": "python.map", "reducer": "concat", "code": FLAG}},
                   {"name": "total", "from": "flag", "workload": {"type": "batch.aggregate", "metrics": [{"op": "count"}, {"op": "max", "field": "over"}]}}],
        "inputs": {"dataset": {"ref": ds["ref"]}}, "context": {"values": {"factor": 1.5}}, "mode": "distributed",
        "partitioning": {"targetBytes": 16 * 1024}, "output": {"spotCheck": 1}})
    assert job["status"] == "done", job
    rows = [json.loads(x) for x in data.splitlines()]
    mean = sum(r["amount"] for r in rows) / len(rows)
    flagged = [r for r in rows if r["amount"] > mean * 1.5]
    assert job["result"]["values"]["count"] == len(flagged)
    assert job["result"]["values"]["max_over"] == max(round(r["amount"] - mean, 6) for r in flagged)
    assert [s["status"] for s in job["stages"]] == ["done", "done", "done"]
    assert job["validation"]["level"] == "spot-checked" and any(c.startswith("Stage 2 (flag)") and "run again on" in c for c in job["validation"]["checks"])
    full = pool.get(job["id"])
    assert full["provenance"]["context"]["version"] == hashlib.sha256(b'{"factor":1.5}').hexdigest()
    assert len(full["provenance"]["stageContexts"]) == 3, "every stage read a snapshot (the user's values, then earlier results too)"
    contexts = [e["data"]["context"] for e in full["events"] if e["type"] == "stage"]
    assert len(contexts) == 2 and len(set(contexts)) == 2, "each barrier published a new context version"


def test_reading_where_the_data_is(pool):
    """A stage that reads an earlier stage's records goes to the computer that wrote them, which reads its own copy."""
    data = sales(4000)
    ds = pool.upload("local.jsonl", data)
    job = submit(pool, {
        "stages": [{"name": "keep", "workload": {"type": "batch.transform", "where": [{"field": "qty", "op": ">", "value": 2}]}},
                   {"name": "sum", "from": "keep", "workload": {"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "amount"}]}}],
        "inputs": {"dataset": {"ref": ds["ref"]}}, "mode": "distributed", "partitioning": {"targetBytes": 16 * 1024}})
    assert job["status"] == "done", job
    rows = [json.loads(x) for x in data.splitlines()]
    assert job["result"]["values"]["sum_amount"] == sum(r["amount"] for r in rows if r["qty"] > 2)
    eng_db = pool.coord.tmp / "data" / "engine" / "engine.db"
    import sqlite3

    con = sqlite3.connect(str(eng_db))
    con.row_factory = sqlite3.Row
    second = con.execute("SELECT p.input, a.device_id, a.metrics FROM partitions p JOIN stages s ON s.id = p.stage_id "
                         "JOIN attempts a ON a.partition_id = p.id AND a.n = p.accepted_attempt WHERE s.job_id = ? AND s.seq = 1", (job["id"],)).fetchall()
    con.close()
    local = [r for r in second if json.loads(r["input"])["device"] == r["device_id"]]
    assert len(local) >= len(second) / 2, "most second-stage partitions ran where their input was written"
    assert all(json.loads(r["metrics"])["bytes"] == 0 for r in local), "and read it from that computer's own cache"


def test_a_shuffled_group_by_matches_the_plain_one(pool):
    data = sales()
    ds = pool.upload("shuffle.jsonl", data)
    wl = {"type": "batch.groupby", "by": ["cust"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "min", "field": "qty"}]}
    plain = pool.run(wl, ds, partitioning={"targetBytes": 16 * 1024})
    assert plain["status"] == "done", plain
    job = pool.run({**wl, "shuffle": {"buckets": 5}}, ds, mode="distributed", partitioning={"targetBytes": 16 * 1024}, output={"spotCheck": 2})
    assert job["status"] == "done", job
    groups = [json.loads(x) for x in pool.result_bytes(job["id"]).decode().splitlines()]
    assert sorted(groups, key=key_text) == plain["result"]["groups"]
    assert job["result"]["groupsTotal"] == 37 and job["result"]["records"] == 6000 and job["outputs"]["records"] == 37
    assert [s["kind"] for s in job["stages"]] == ["shuffle-map", "shuffle-reduce"] and job["stages"][1]["total"] <= 5
    assert job["validation"]["level"] == "spot-checked"
    assert any("bucket file of the first stage was read by exactly one" in c for c in job["validation"]["checks"])


def test_a_join_of_two_datasets(pool):
    orders, people = sales(3000), customers()
    left = pool.upload("orders.jsonl", orders)
    right = pool.upload("customers.csv", people)
    rows = [json.loads(x) for x in orders.splitlines()]
    names = {int(r["cust"]): r for r in csv.DictReader(io.StringIO(people.decode()))}
    for how in ("inner", "left"):
        job = pool.c.request("POST", "/api/engine/jobs", {"job": pool.job_doc({"type": "batch.join", "on": ["cust"], "how": how, "buckets": 4}, left,
                                                                              inputs={"dataset": {"ref": left["ref"]}, "right": {"ref": right["ref"]}},
                                                                              mode="distributed", partitioning={"targetBytes": 8 * 1024},
                                                                              output={"spotCheck": 2})})["job"]
        job = pool.wait(job["id"], timeout=120)
        assert job["status"] == "done", job
        got = sorted((json.loads(x) for x in pool.result_bytes(job["id"]).decode().splitlines()), key=lambda r: r["id"])
        want = []
        for r in rows:
            c = names.get(r["cust"])
            if c is not None:
                want.append({**r, "name": c["name"], "right_amount": c["amount"]})
            elif how == "left":
                want.append(r)
        assert got == want
        assert job["result"]["leftRecords"] == 3000 and job["result"]["rightRecords"] == 30
        assert job["result"]["unmatchedLeft"] == sum(1 for r in rows if r["cust"] >= 30)
        assert job["validation"]["level"] == "spot-checked", job["validation"]


def test_a_straggler_is_run_again_on_a_free_computer(tmp_path):
    coord = start_coordinator(tmp_path / "c", extra_env={"HARNESS_SPECULATE_AFTER_S": "1"})
    try:
        coord.agent("Fast", threads=2, extra=("--no-tasks",))
        os.environ["YH_SLOW"] = "1"
        try:
            coord.agent("Slow", threads=1, extra=("--no-tasks",))
        finally:
            del os.environ["YH_SLOW"]
        eng = Engine(coord)
        eng.ready({"Fast", "Slow"})
        code = "import os, time\ndef process(x):\n    if os.environ.get('YH_SLOW'):\n        time.sleep(60)\n    return x * 10\n"
        t0 = time.monotonic()
        job = eng.run({"type": "python.map", "code": code}, items=list(range(12)), mode="distributed", partitioning={"itemsPerPartition": 1})
        took = time.monotonic() - t0
        assert job["status"] == "done" and job["result"]["value"] == [x * 10 for x in range(12)], job
        assert took < 40, f"the slow computer's partition was duplicated instead of waited for ({took:.0f} s)"
        full = eng.get(job["id"])
        spec_events = [e for e in full["events"] if e["type"] == "speculative"]
        assert spec_events and all(e["data"]["slowDevice"] == "Slow" and e["data"]["device"] == "Fast" for e in spec_events)
        assert {p["device"] for p in full["partitions"]} == {"Fast"}, "every accepted result came from the fast computer"
        # the same job with duplicates turned off waits for the slow one (cancelled here instead of waiting a minute)
        slow = eng.start({"type": "python.map", "code": code}, items=list(range(12)), mode="distributed", partitioning={"itemsPerPartition": 1},
                         retry={"speculate": False})
        time.sleep(5)
        assert eng.get(slow["id"])["status"] == "running"
        assert not [e for e in eng.get(slow["id"])["events"] if e["type"] == "speculative"]
        eng.c.request("POST", f"/api/engine/jobs/{slow['id']}/cancel", {})
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


def test_a_partition_past_its_time_limit_is_stopped(pool):
    t0 = time.monotonic()
    job = pool.run({"type": "python.map", "code": "import time\ndef process(x):\n    time.sleep(60)\n"}, items=[1],
                   retry={"timeoutSeconds": 5, "maxAttempts": 1, "speculate": False})
    assert job["status"] == "failed" and "5 s time limit" in job["error"], job
    assert time.monotonic() - t0 < 30


def test_a_context_snapshot_reaches_every_partition(pool):
    job = pool.run({"type": "python.map", "code": "def process(x, ctx):\n    return x * ctx['factor'] + len(ctx['names'])\n"},
                   items=list(range(30)), mode="distributed", partitioning={"itemsPerPartition": 5},
                   context={"values": {"factor": 3, "names": ["a", "b"]}})
    assert job["status"] == "done" and job["result"]["value"] == [x * 3 + 2 for x in range(30)], job
    big = {"factor": 1, "names": ["x" * 1000] * 400}  # over the inline limit: fetched by digest and checked
    job = pool.run({"type": "python.map", "code": "def process(x, ctx):\n    return x * ctx['factor'] + len(ctx['names'])\n"},
                   items=list(range(10)), mode="distributed", partitioning={"itemsPerPartition": 5}, context={"values": big})
    assert job["status"] == "done" and job["result"]["value"] == [x + 400 for x in range(10)], job


def scaled(x, ctx):
    return x * ctx["factor"]


def test_joins_shuffles_and_context_from_the_python_sdk(pool, monkeypatch):
    from yourharness import jobs

    monkeypatch.setenv("HARNESS_URL", pool.coord.base)
    monkeypatch.setenv("HARNESS_TOKEN", pool.c.token)
    jobs._client = None
    left = pool.upload("sdk-orders.jsonl", sales(500))["ref"]
    right = pool.upload("sdk-customers.csv", customers())["ref"]
    joined = jobs.join(left, right, on="cust", how="inner")
    assert len(joined) == sum(1 for i in range(500) if i % 37 < 30) and all("name" in r for r in joined)
    groups = jobs.group_by(left, by="region", sum="amount", shuffle=3)
    assert sorted(g["key"]["region"] for g in groups) == ["e", "n", "s", "w"]
    assert jobs.map_items(scaled, range(10), context={"factor": 4}) == [x * 4 for x in range(10)]
