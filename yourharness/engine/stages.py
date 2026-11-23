"""A job's stages: what each one reads, how it is cut into partitions, and how its results combine.

Most jobs are one stage. Three kinds of job have more:

  stages      spec.stages lists them; each starts when the one before it finished
              (a barrier), with every earlier stage's result in its context snapshot
              under ctx["stages"][name], and may read an earlier stage's records
              ("from": name) instead of the job's inputs.
  shuffle     a group-by with "shuffle": every first-stage partition splits its group
              totals into buckets by key; then one partition per bucket combines that
              bucket's totals from every partition and writes its groups.
  join        batch.join: first-stage partitions over both datasets send each record
              to its key's bucket; then one partition per bucket joins that bucket's
              left and right records.

Partition inputs: "range" (bytes of a dataset), "items" (part of the job's items),
"artifact" (the records one earlier partition wrote), "buckets" (one bucket's files
from every first-stage partition). Each stage's context is an immutable artifact named
by its SHA-256, fixed for the whole stage (a snapshot).
"""

from __future__ import annotations

import hashlib
import json
import math

from .execute import dump_record, make_op, run_items, run_partition
from .local import file_chunks, run_range
from .ops import GroupByOp, halo_records_needed
from .partition import check_ranges, plan_items, plan_ranges
from .reducer import final_result, merge_states
from .shuffle import BucketFiles, JoinMapper, groupby_map_state, groupby_reduce, join_reduce, lines_of, merge_counts
from .spec import CODE_WORKLOADS, writes_records

PREVIEW_GROUPS = 100
JOIN_BUCKET_BYTES = 32 * 2**20


def plan_stages(job: dict) -> list[dict]:
    """The stages a job runs, in order: [{"name", "kind", "workload", "from"}]."""
    spec = job["spec"]
    wl = spec["workload"]
    if spec.get("stages"):
        return [{"name": s["name"], "kind": "map", "workload": s["workload"], "from": s["from"]} for s in spec["stages"]]
    if wl["type"] == "batch.groupby" and wl.get("shuffle") is not None:
        return [{"name": "group", "kind": "shuffle-map", "workload": wl, "from": "inputs"},
                {"name": "combine", "kind": "shuffle-reduce", "workload": wl, "from": "group"}]
    if wl["type"] == "batch.join":
        return [{"name": "sort", "kind": "join-map", "workload": wl, "from": "inputs"},
                {"name": "join", "kind": "join-reduce", "workload": wl, "from": "sort"}]
    return [{"name": "main", "kind": "map", "workload": wl, "from": "inputs"}]


def runtime_for(job_spec: dict, wl: dict) -> dict:
    """Your own code runs with the job's runtime; the engine's operators always with its own."""
    return job_spec["runtime"] if wl["type"] in CODE_WORKLOADS else {"kind": "builtin"}


def stage_writes(stage_spec: dict) -> bool:
    kind = stage_spec["kind"]
    if kind == "map":
        return writes_records(stage_spec["workload"])
    return kind in ("shuffle-reduce", "join-reduce")


# ------------------------------------------------------------------ cutting a stage into partitions
def range_parts(mgr, spec: dict, wl: dict, ds: dict, meta: dict, target: int, extra: dict | None = None) -> list[dict]:
    ranges = plan_ranges(mgr.splits(ds), meta["dataStart"], meta["size"], target, spec["partitioning"]["maxPartitions"])
    need = halo_records_needed(wl)
    avg = (meta["size"] - meta["dataStart"]) / max(1, meta.get("lines") or 1)
    return [{"kind": "range", "start": s, "end": e, **(extra or {}),
             "halo": 0 if not need or s <= meta["dataStart"] else int(min(s - meta["dataStart"], need * avg * 3 + 4096))}
            for s, e in ranges]


def first_parts(mgr, job_doc: dict, stage: dict, plan: dict) -> list[dict]:
    """The partitions of a stage that reads the job's own inputs."""
    spec = job_doc["spec"]
    if "items" in spec["inputs"]:
        n = len(spec["inputs"]["items"])
        per = plan.get("itemsPerPartition") or spec["partitioning"]["itemsPerPartition"] or max(1, n // 8)
        return [{"kind": "items", "first": i, "count": j - i} for i, j in plan_items(n, per)]
    ds = mgr.db.dataset(spec["inputs"]["dataset"]["ref"].split(":", 1)[1])
    meta = mgr.dataset_meta(ds)
    target = plan.get("targetBytes") or spec["partitioning"]["targetBytes"] or 8 * 2**20
    if stage["kind"] != "join-map":
        return range_parts(mgr, spec, stage["workload"], ds, meta, target)
    right = mgr.db.dataset(spec["inputs"]["right"]["ref"].split(":", 1)[1])
    return (range_parts(mgr, spec, stage["workload"], ds, meta, target, {"ds": ds["id"], "side": "L"})
            + range_parts(mgr, spec, stage["workload"], right, mgr.dataset_meta(right), target, {"ds": right["id"], "side": "R"}))


def default_buckets(mgr, job_doc: dict, stage: dict, first_count: int) -> int:
    wl = stage["workload"]
    if stage["kind"] == "shuffle-map":
        return (wl["shuffle"] or {}).get("buckets") or max(2, min(64, first_count))
    right = mgr.db.dataset(job_doc["spec"]["inputs"]["right"]["ref"].split(":", 1)[1])
    return wl.get("buckets") or max(4, min(256, math.ceil(right["size"] / JOIN_BUCKET_BYTES)))


def later_parts(mgr, job_id: str, stage: dict, upstream: dict) -> list[dict]:
    """The partitions of a stage that reads what an earlier stage wrote."""
    ups = mgr.db.all("SELECT * FROM partitions WHERE stage_id = ? ORDER BY idx", (upstream["id"],))
    if stage["kind"] == "map":  # one partition per earlier partition: its records
        out = []
        for p in ups:
            o = p["output"]["outputs"]["part.jsonl"]
            out.append({"kind": "artifact", "sha": o["sha"], "size": o["size"], "device": p["device_id"], "from": p["id"]})
        return out
    by_bucket: dict[int, list] = {}
    for p in ups:
        side = p["input"].get("side")
        for name, o in sorted((p["output"].get("outputs") or {}).items()):
            if name == "state.json":
                continue
            b = int(o.get("bucket") if o.get("bucket") is not None else name[1:].split(".")[0])
            by_bucket.setdefault(b, []).append({"sha": o["sha"], "size": o["size"], "device": p["device_id"], "from": p["id"],
                                                **({"side": side} if side else {})})
    out = []
    for b in sorted(by_bucket):
        parts = by_bucket[b]
        if stage["kind"] == "join-reduce":
            parts = sorted(parts, key=lambda x: (x["side"] != "R", _idx(x["from"])))  # right first, then left, each in input order
            if not any(x["side"] == "L" for x in parts):
                continue  # nothing on the left: an inner or left join writes nothing for this bucket
        else:
            parts = sorted(parts, key=lambda x: _idx(x["from"]))
        out.append({"kind": "buckets", "bucket": b, "parts": parts})
    return out


def _idx(pid: str) -> int:
    return int(pid.rsplit("-p", 1)[1])


def part_bytes(inp: dict) -> int | None:
    if inp["kind"] == "range":
        return inp["end"] - inp["start"]
    if inp["kind"] == "artifact":
        return inp["size"]
    if inp["kind"] == "buckets":
        return sum(x["size"] for x in inp["parts"])
    return None


# ------------------------------------------------------------------ combining a stage's partitions
def combine(mgr, job: dict, stage: dict, parts: list[dict], first_stage_result: dict | None) -> tuple[dict, dict | None, dict]:
    """Runs in a thread: (the stage's result, its output records or None, what was checked)."""
    st = stage["spec"]
    kind, wl = st["kind"], st["workload"]
    spec = job["spec"]["spec"]
    checks = [coverage(mgr, job, stage, parts)]
    states = [mgr.state_of(p) for p in parts]
    outputs = None
    if kind == "map":
        result = final_result(wl, merge_states(wl, states))
        skipped = sum(int(p["output"].get("skipped") or 0) for p in parts)
        if skipped:
            result["badRecordsSkipped"] = skipped
    elif kind in ("shuffle-map", "join-map"):
        result = merge_counts(states)
        if kind == "join-map":
            for side in ("L", "R"):
                result[f"{'left' if side == 'L' else 'right'}Records"] = sum(int(s["records"]) for p, s in zip(parts, states) if p["input"]["side"] == side)
    elif kind == "shuffle-reduce":
        first = first_stage_result or {}
        result = {"groupsTotal": sum(int(s["groups"]) for s in states), "records": first.get("seen", 0), "matched": first.get("matched", 0)}
    else:
        totals = merge_counts(states)
        first = first_stage_result or {}
        result = {"records": totals.get("out", 0), "leftRecords": first.get("leftRecords", 0), "rightRecords": first.get("rightRecords", 0),
                  "matchedLeft": totals.get("matched", 0), "unmatchedLeft": first.get("leftRecords", 0) - totals.get("matched", 0)}
    if stage_writes(st):
        pieces = [{"idx": p["idx"], **p["output"]["outputs"]["part.jsonl"]} for p in parts]
        for piece in pieces:
            if not mgr.store.has(piece["sha"]):
                raise ValueError(f"the records of partition {piece['idx'] + 1} are missing from storage")
        outputs = {"parts": pieces, "bytes": sum(x["size"] for x in pieces), "records": sum(int(x.get("records") or 0) for x in pieces)}
        checks.append("Every partition's records were stored under the digest its computer reported.")
        if kind == "shuffle-reduce":
            result["groups"] = preview_groups(mgr, pieces)
    elif kind in ("shuffle-map", "join-map"):
        for p in parts:
            for name, o in (p["output"].get("outputs") or {}).items():
                if name != "state.json" and not mgr.store.has(o["sha"]):
                    raise ValueError(f"a bucket file of partition {p['idx'] + 1} is missing from storage")
        checks.append("Every bucket file was stored under the digest its computer reported.")
    if wl["type"] == "ml.evaluate":
        result["provenance"] = mgr.provenance(job)
        result["latency"] = mgr.latency(parts)
    if wl["type"] == "tests.pytest":  # timings and failure messages, beside the checked outcomes
        from .testing import report

        result = report(mgr.accepted_metrics(parts), result)
        result["provenance"] = mgr.provenance(job)
    validation = {"level": "structural", "checks": checks}
    n = min(spec["output"]["spotCheck"], len(parts))
    if not n:
        return result, outputs, validation
    import random

    sample = sorted(random.Random(f"{job['id']}:{stage['seq']}").sample(range(len(parts)), n))
    if runtime_for(spec, wl)["kind"] != "builtin":  # your own code never runs in the coordinator: verify.py re-runs it elsewhere
        return result, outputs, {**validation, "verifySample": [parts[i]["id"] for i in sample]}
    for i in sample:
        p = parts[i]
        ref_state, ref_files = recompute(mgr, job, stage, p)
        accepted = {k: v["sha"] for k, v in (p["output"].get("outputs") or {}).items() if k != "state.json"}
        if _canon(ref_state) != _canon(states[i]) or ref_files != accepted:
            return result, outputs, {**validation, "failed": f"Spot check failed: partition {i + 1}, computed on {p['device_name']}, "
                                                             "doesn't match the coordinator's own result. Nothing was published."}
    return result, outputs, {"level": "spot-checked",
                             "checks": checks + [f"{n} of {len(parts)} partitions were computed again on the coordinator and matched exactly."]}


def coverage(mgr, job: dict, stage: dict, parts: list[dict]) -> str:
    """Raises unless the stage's partitions read every piece of its input exactly once; says what was checked."""
    spec = job["spec"]["spec"]
    st = stage["spec"]
    if st["from"] == "inputs":
        if "items" in spec["inputs"]:
            if sum(p["input"]["count"] for p in parts) != len(spec["inputs"]["items"]):
                raise ValueError("the partitions don't cover every item")
            return f"Every item belongs to exactly one of the {len(parts)} partitions, and each was accepted once."
        sides = {}
        for p in parts:
            sides.setdefault(p["input"].get("ds") or spec["inputs"]["dataset"]["ref"].split(":", 1)[1], []).append(p)
        for ds_id, ps in sides.items():
            meta = mgr.dataset_meta(mgr.db.dataset(ds_id))
            check_ranges([(p["input"]["start"], p["input"]["end"]) for p in ps], meta["dataStart"], meta["size"])
        if not parts:
            return "The input was empty."
        what = "both datasets" if len(sides) > 1 else "the input"
        return f"Every byte of {what} belongs to exactly one of the {len(parts)} partitions, and each was accepted once."
    upstream = mgr.db.one("SELECT * FROM stages WHERE job_id = ? AND seq = ?", (job["id"], stage["seq"] - 1))
    ups = mgr.db.all("SELECT * FROM partitions WHERE stage_id = ?", (upstream["id"],))
    if st["kind"] == "map":
        made = sorted(p["output"]["outputs"]["part.jsonl"]["sha"] for p in ups)
        read = sorted(p["input"]["sha"] for p in parts)
        if made != read:
            raise ValueError("the partitions don't read every record the stage before wrote")
        return f"Each of the {len(parts)} partitions read the records of one partition of stage {upstream['name']}, and each was accepted once."
    made = {(p["id"], o["sha"]) for p in ups for name, o in (p["output"].get("outputs") or {}).items() if name != "state.json"}
    read = [(x["from"], x["sha"]) for p in parts for x in p["input"]["parts"]]
    if len(read) != len(set(read)) or not set(read) <= made:
        raise ValueError("a bucket file was read twice, or one that wasn't made was read")
    if st["kind"] == "shuffle-reduce" and set(read) != made:
        raise ValueError("a bucket file of the first stage wasn't read")
    if st["kind"] == "join-reduce":  # buckets with no left records write nothing, so only they may go unread
        left_buckets = {x["sha"] for p in parts for x in p["input"]["parts"] if x.get("side") == "L"}
        unread = made - set(read)
        if any(sha in left_buckets for _pid, sha in unread):
            raise ValueError("a bucket file of the first stage wasn't read")
    return f"Every bucket file of the first stage was read by exactly one of the {len(parts)} partitions."


def preview_groups(mgr, pieces: list[dict]) -> list[dict]:
    rows = []
    for piece in pieces:
        with open(mgr.store.path(piece["sha"]), "rb") as f:
            for raw in f:
                rows.append(json.loads(raw))
                if len(rows) >= PREVIEW_GROUPS:
                    return rows
    return rows


def _canon(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)


# ------------------------------------------------------------------ the coordinator's own run of a partition
def recompute(mgr, job: dict, stage: dict, p: dict) -> tuple[dict, dict]:
    """(state, {output file: sha}) of a partition computed here with the engine's own operators."""
    st = stage["spec"]
    kind, wl = st["kind"], st["workload"]
    inp = p["input"]
    doc = {"spec": {**job["spec"]["spec"], "workload": wl}}
    if kind in ("shuffle-reduce", "join-reduce"):
        out: list = []
        read = lambda x: file_chunks(mgr.store.path(x["sha"]), 0, x["size"])  # noqa: E731
        if kind == "shuffle-reduce":
            state = groupby_reduce(wl, (b"".join(read(x)) for x in inp["parts"]), out.append)
        else:
            right = (ln for x in inp["parts"] if x["side"] == "R" for ln in lines_of(read(x)))
            left = (ln for x in inp["parts"] if x["side"] == "L" for ln in lines_of(read(x)))
            state = join_reduce(wl, right, left, out.append)
        return state, {"part.jsonl": _digest(out)}
    if inp["kind"] == "items":
        out = []
        op = make_op(wl, {}, out.append)
        run_items(op, job["spec"]["spec"]["inputs"]["items"][inp["first"]:inp["first"] + inp["count"]], inp["first"])
        return op.state(), ({"part.jsonl": _digest(out)} if stage_writes(st) else {})
    if inp["kind"] == "artifact":
        meta, path, start, end, halo = {"format": "jsonl", "header": None, "dataStart": 0, "size": inp["size"]}, mgr.store.path(inp["sha"]), 0, inp["size"], None
    else:
        ds = mgr.db.dataset(inp.get("ds") or job["spec"]["spec"]["inputs"]["dataset"]["ref"].split(":", 1)[1])
        meta, path, start, end, halo = mgr.dataset_meta(ds), mgr.store.path(ds["sha"]), inp["start"], inp["end"], inp.get("halo") or None
    bad = job["spec"]["spec"]["output"]["onBadRecord"]
    if kind == "join-map":
        files = BucketFiles(None, "b")
        op = JoinMapper(wl, inp["side"], int(st["buckets"]), files)
        run_partition(op, meta["format"], meta.get("header"), file_chunks(path, start, end), start, bad=bad)
        return op.state(), {name: o["sha"] for name, o in files.close().items()}
    if kind == "shuffle-map":
        op = GroupByOp(wl)
        run_partition(op, meta["format"], meta.get("header"), file_chunks(path, start, end), start, bad=bad)
        state, blobs = groupby_map_state(op, int(st["buckets"]))
        return state, {name: hashlib.sha256(data).hexdigest() for name, data in blobs.items()}
    state, out = run_range(doc, meta, path, start, end, halo_bytes=halo)
    return state, ({"part.jsonl": _digest(out)} if stage_writes(st) else {})


def _digest(records: list) -> str:
    return hashlib.sha256("".join(dump_record(r) for r in records).encode("utf-8")).hexdigest()
