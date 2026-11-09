"""The job engine's contracts, with no network and no other processes.

The central property: a job's result doesn't depend on how its input was split or
in which order the partial results were combined. The randomized tests use fixed
seeds, so a failure always reproduces.
"""

from __future__ import annotations

import json
import math
import random

import pytest

from yourharness.engine import spec as jobspec
from yourharness.engine.exact import ExactSum
from yourharness.engine.index import Indexer
from yourharness.engine.local import reference, reference_items
from yourharness.engine.partition import PartitionError, check_ranges, plan_items, plan_ranges
from yourharness.engine.records import RecordError, iter_records


def index_bytes(fmt: str, data: bytes, step: int, chunk: int | None = None) -> dict:
    ix = Indexer(fmt, step=step)
    if chunk is None:
        ix.feed(data)
    else:
        for i in range(0, len(data), chunk):
            ix.feed(data[i:i + chunk])
    return ix.finish()


def dataset_file(tmp_path, fmt: str, data: bytes, step: int = 64) -> tuple[dict, object]:
    path = tmp_path / f"data.{fmt}"
    path.write_bytes(data)
    meta = index_bytes(fmt, data, step)
    meta["format"] = fmt
    return meta, path


def job(workload: dict, **spec) -> dict:
    return jobspec.parse({"spec": {"workload": workload, "inputs": {"dataset": {"ref": "dataset:d00000000"}}, **spec}})


# ---------------------------------------------------------------------- exact sums
def test_exact_sum_is_the_same_for_any_split_and_order():
    rng = random.Random(7)
    values = [rng.uniform(-1, 1) * 10 ** rng.randint(-20, 20) for _ in range(3000)]
    expected = math.fsum(values)
    for trial in range(30):
        rng.shuffle(values)
        cuts = sorted(rng.sample(range(1, len(values)), rng.randint(1, 40)))
        parts, prev = [], 0
        for c in cuts + [len(values)]:
            s = ExactSum()
            for v in values[prev:c]:
                s.add(v)
            parts.append(s.to_state())
            prev = c
        rng.shuffle(parts)
        total = ExactSum()
        for st in parts:
            total.merge(ExactSum.from_state(json.loads(json.dumps(st))))
        assert total.value() == expected, trial


def test_exact_sum_keeps_integers_exact_and_counts_nan_and_infinity():
    s = ExactSum()
    for v in (10 ** 30, 1, -(10 ** 30), 2):
        s.add(v)
    assert s.value() == 3 and isinstance(s.value(), int)
    s.add(0.5)
    assert s.value() == 3.5
    big = ExactSum()
    big.add(2 ** 80)
    big.add(1.0)
    assert big.value() == float(2 ** 80)  # correctly rounded
    inf = ExactSum()
    inf.add(math.inf)
    inf.add(5)
    assert inf.value() == "Infinity"
    inf.add(-math.inf)
    assert inf.value() == "NaN"
    json.dumps(inf.to_state(), allow_nan=False)  # states are always valid JSON


# ---------------------------------------------------------------------- record boundaries
def _records_at(fmt: str, data: bytes, header=None) -> list[int]:
    return [s for s, _e, _r in iter_records(fmt, [data], 0, header)]


def test_split_points_are_record_boundaries_however_the_bytes_arrive():
    rng = random.Random(3)
    rows = ["id,note,amount"]
    for i in range(400):
        note = rng.choice(['plain', '"has, comma"', '"two\nlines"', '"quote "" inside"', '"a\r\nb"'])
        rows.append(f"{i},{note},{rng.randint(-50, 50)}")
    data = ("\n".join(rows) + "\n").encode()
    whole = index_bytes("csv", data, step=256)
    assert whole["header"] == ["id", "note", "amount"] and whole["quoted"]
    for chunk in (1, 7, 64, 1000):
        assert index_bytes("csv", data, step=256, chunk=chunk) == whole, chunk
    starts = set(_records_at("csv", data[whole["dataStart"]:], whole["header"]))
    for s in whole["splits"]:
        assert (s - whole["dataStart"]) in starts, f"split {s} cuts a record"
    assert len(whole["splits"]) > 10


def test_text_and_jsonl_boundaries_and_the_byte_order_mark():
    data = b"\xef\xbb\xbf" + b"".join(json.dumps({"i": i}).encode() + b"\n" for i in range(200))
    ix = index_bytes("jsonl", data, step=100)
    assert ix["dataStart"] == 3 and ix["lines"] == 200
    assert all(data[s - 1:s] == b"\n" for s in ix["splits"])
    empty = index_bytes("text", b"", step=100)
    assert empty == {"splits": [], "dataStart": 0, "size": 0, "lines": 0, "step": 100}


def test_partitions_cover_the_data_exactly_once():
    rng = random.Random(11)
    for _ in range(200):
        size = rng.randint(1, 50_000)
        start = rng.randint(0, min(500, size - 1))
        splits = sorted(rng.sample(range(start + 1, size + 1), min(size - start, rng.randint(0, 300))))
        target = rng.randint(1, 5000)
        ranges = plan_ranges(splits, start, size, target)
        check_ranges(ranges, start, size)
        assert all(e - s <= target or not [p for p in splits if s < p < e] for s, e in ranges), "only one long record may exceed the target"
        assert ranges == plan_ranges(splits, start, size, target), "the same input always gives the same partitions"
    assert plan_ranges([], 10, 10, 100) == []
    with pytest.raises(PartitionError):
        check_ranges([(0, 5), (6, 10)], 0, 10)
    assert plan_items(5, 2) == [(0, 2), (2, 4), (4, 5)]


# ---------------------------------------------------------------------- the central property
def _random_rows(rng, n):
    rows = []
    for i in range(n):
        r = {"id": i, "region": rng.choice(["north", "south", "east", None]), "amount": rng.choice([rng.uniform(-1e6, 1e6), rng.randint(-10, 10), None, "12.5", "n/a"])}
        if rng.random() < 0.05:
            r["amount"] = rng.choice([1e308, -1e308, 1e-300])
        if rng.random() < 0.05:
            del r["region"]
        rows.append(r)
    return rows


AGG = {"type": "batch.aggregate", "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "mean", "field": "amount"},
                                             {"op": "min", "field": "amount"}, {"op": "max", "field": "amount"}, {"op": "count", "field": "region"}]}
GROUP = {"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "mean", "field": "amount"}]}


@pytest.mark.parametrize("workload", [AGG, GROUP], ids=["aggregate", "groupby"])
def test_any_split_and_any_merge_order_give_the_same_result(tmp_path, workload):
    rng = random.Random(42)
    rows = _random_rows(rng, 700)
    data = b"".join(json.dumps(r).encode() + b"\n" for r in rows)
    meta, path = dataset_file(tmp_path, "jsonl", data, step=97)
    j = job(workload)
    expected = reference(j, meta, path)["result"]
    assert expected["records"] == 700
    for trial in range(25):
        ranges = plan_ranges(meta["splits"], meta["dataStart"], meta["size"], rng.randint(50, 20_000))
        order = list(range(len(ranges)))
        rng.shuffle(order)
        got = reference(j, meta, path, ranges=ranges, merge_order=order)["result"]
        assert got == expected, f"trial {trial} with {len(ranges)} partitions"


def test_csv_with_quoted_line_breaks_gives_the_same_result_in_pieces(tmp_path):
    rng = random.Random(5)
    lines = ["region,amount,note"]
    for i in range(500):
        lines.append(f'{rng.choice(["a", "b"])},{rng.randint(1, 9)},"line one\nline two, {i}"')
    meta, path = dataset_file(tmp_path, "csv", ("\n".join(lines) + "\n").encode(), step=128)
    j = job({"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}]})
    expected = reference(j, meta, path)["result"]
    assert sum(g["count"] for g in expected["groups"]) == 500
    for target in (200, 1000, 5000):
        ranges = plan_ranges(meta["splits"], meta["dataStart"], meta["size"], target)
        assert len(ranges) > 1
        assert reference(j, meta, path, ranges=ranges)["result"] == expected


def test_a_rolling_mean_reads_across_partitions_with_a_halo(tmp_path):
    data = b"".join(json.dumps({"t": i, "v": (i * 37) % 11}).encode() + b"\n" for i in range(300))
    meta, path = dataset_file(tmp_path, "jsonl", data, step=50)
    j = job({"type": "batch.transform", "rolling": {"field": "v", "window": 5}, "select": ["t", "rolling_mean_v"]})
    whole = reference(j, meta, path)["records"]
    assert len(whole) == 300 and whole[4]["rolling_mean_v"] == sum((i * 37) % 11 for i in range(5)) / 5
    ranges = plan_ranges(meta["splits"], meta["dataStart"], meta["size"], 400)
    assert len(ranges) > 5
    assert reference(j, meta, path, ranges=ranges)["records"] == whole


def test_filters_select_and_rename(tmp_path):
    rows = [{"name": "a", "n": 1, "tags": "x,y"}, {"name": "b", "n": 5}, {"name": "c", "n": "7"}, {"name": "d"}]
    meta, path = dataset_file(tmp_path, "jsonl", b"".join(json.dumps(r).encode() + b"\n" for r in rows))
    j = job({"type": "batch.transform", "where": [{"field": "n", "op": ">=", "value": 5}], "select": ["name", "n"], "rename": {"n": "number"}})
    assert reference(j, meta, path)["records"] == [{"name": "b", "number": 5}, {"name": "c", "number": "7"}]
    j = job({"type": "batch.aggregate", "where": [{"field": "tags", "op": "exists"}], "metrics": [{"op": "count"}]})
    assert reference(j, meta, path)["result"]["values"] == {"count": 1}
    j = job({"type": "batch.aggregate", "where": [{"field": "name", "op": "regex", "value": "^[ab]$"}], "metrics": [{"op": "count"}]})
    assert reference(j, meta, path)["result"]["values"] == {"count": 2}


def test_empty_and_single_record_inputs(tmp_path):
    meta, path = dataset_file(tmp_path, "jsonl", b"")
    assert reference(job(AGG), meta, path)["result"]["values"]["count"] == 0
    assert reference(job(AGG), meta, path)["result"]["values"]["mean_amount"] is None
    meta, path = dataset_file(tmp_path, "jsonl", b'{"amount": 2.5}')  # no newline at the end
    assert reference(job(AGG), meta, path)["result"]["values"]["sum_amount"] == 2.5


# ---------------------------------------------------------------------- bad records
def test_bad_records_fail_the_job_or_are_skipped_and_counted(tmp_path):
    data = b'{"a": 1}\n{not json}\n\xff\xfe\n{"a": 2}\n'
    meta, path = dataset_file(tmp_path, "jsonl", data)
    with pytest.raises(RecordError) as e:
        reference(job(AGG), meta, path)
    assert e.value.offset == 9
    skip = job({"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "a"}]}, output={"onBadRecord": "skip"})
    assert reference(skip, meta, path)["result"]["values"] == {"sum_a": 3}
    csv_meta, csv_path = dataset_file(tmp_path, "csv", b"a,b\n1,2\n3\n4,5\n")
    with pytest.raises(RecordError, match="has 1 fields"):
        reference(job({"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "a"}]}), csv_meta, csv_path)


# ---------------------------------------------------------------------- your own code
def test_python_map_reducers_and_order(tmp_path):
    code = "def process(item):\n    return item * item\n"
    collect = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": code}, "inputs": {"items": list(range(50))}}})
    assert reference_items(collect, per=7)["result"]["value"] == [i * i for i in range(50)]
    total = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": "def process(x):\n    return x / 3\n", "reducer": "sum"},
                                    "inputs": {"items": list(range(1, 1001))}}})
    assert reference_items(total, per=13)["result"]["value"] == reference_items(total, per=1000)["result"]["value"]
    best = jobspec.parse({"spec": {"workload": {"type": "python.map", "reducer": "best", "bestBy": {"key": "score"},
                                                "code": "def process(p):\n    return {'lr': p['lr'], 'score': -abs(p['lr'] - 0.1)}\n"},
                                   "inputs": {"grid": {"lr": [0.001, 0.01, 0.1, 0.3]}}}})
    assert reference_items(best, per=1)["result"]["value"] == {"lr": 0.1, "score": -0.0}
    words = jobspec.parse({"spec": {"workload": {"type": "python.map", "reducer": "merge_counts",
                                                 "code": "def process(r):\n    return {w: 1 for w in r['line'].split()}\n"},
                                    "inputs": {"dataset": {"ref": "dataset:d00000000"}}}})
    meta, path = dataset_file(tmp_path, "text", b"a b\nb c\nc c\n")
    assert reference(words, meta, path)["result"]["value"] == {"b": 2, "c": 2, "a": 1}


def test_errors_in_your_code_name_the_line():
    from yourharness.engine.pyop import UserCodeError

    j = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": "def process(x):\n    return 1 / x\n"}, "inputs": {"items": [1, 0]}}})
    with pytest.raises(UserCodeError, match="line 2"):
        reference_items(j)


# ---------------------------------------------------------------------- the job description
def test_a_job_is_filled_in_with_defaults():
    j = job({"type": "batch.aggregate", "metrics": [{"op": "mean", "field": "price.usd"}]})
    s = j["spec"]
    assert j["apiVersion"] == jobspec.API_VERSION and s["mode"] == "automatic"
    assert s["workload"]["metrics"] == [{"op": "mean", "field": "price.usd", "as": "mean_price_usd"}]
    assert s["security"] == {"allowedTrust": "owned-only"}
    assert s["retry"]["maxAttempts"] == 3 and s["output"]["onBadRecord"] == "fail"
    assert jobspec.parse(json.dumps(j)) == j, "a normalized job parses to itself"


@pytest.mark.parametrize("bad, path", [
    ({"spec": {"workload": {"type": "spark"}}}, "spec.workload.type"),
    ({"spec": {"workload": {"type": "batch.aggregate", "metrics": [{"op": "median", "field": "x"}]}}}, "spec.workload.metrics[0].op"),
    ({"spec": {"workload": {"type": "batch.aggregate"}, "inputs": {"dataset": {"ref": "x"}}}}, "spec.inputs.dataset.ref"),
    ({"spec": {"workload": {"type": "batch.aggregate", "where": [{"field": "x", "op": "regex", "value": "("}]},
               "inputs": {"dataset": {"ref": "dataset:d00000000"}}}}, "spec.workload.where[0].value"),
    ({"spec": {"workload": {"type": "python.map", "code": "def process(x): return x"}, "inputs": {"items": [1]},
               "security": {"allowedTrust": "owned-and-friends"}}}, "spec.security.allowedTrust"),
    ({"spec": {"workload": {"type": "batch.aggregate"}, "inputs": {"dataset": {"ref": "dataset:d00000000"}},
               "security": {"allowUntrustedNativeFallback": True}}}, "spec.security.allowUntrustedNativeFallback"),
    ({"apiVersion": "v2", "spec": {}}, "apiVersion"),
])
def test_a_wrong_job_names_the_field(bad, path):
    with pytest.raises(jobspec.SpecError) as e:
        jobspec.parse(bad)
    assert e.value.path == path


def test_a_parameter_grid_expands_to_every_combination():
    j = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": "def process(p):\n    return p\n"},
                                "inputs": {"grid": {"a": [1, 2], "b": ["x", "y", "z"]}}}})
    assert len(j["spec"]["inputs"]["items"]) == 6 and {"a": 2, "b": "z"} in j["spec"]["inputs"]["items"]
