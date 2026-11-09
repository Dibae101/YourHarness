"""The browser's batch operators (web/engine-ops.js) give exactly the Python operators' states.

States from browser tabs and from computers are combined together and spot-checked
byte for byte, so for the same bytes the JavaScript port must report the same
partial state, the same record counts and the same bad-record offsets, or refuse
(Unsupported) and leave the partition to a computer. These cases run both on
awkward data: big integers next to floats, -0.0, NaN, numeric text Python's
int() and float() read, CSV quoting across lines, bad UTF-8, blank lines, and
chunks cut in the middle of characters.
"""

from __future__ import annotations

import base64
import json
import random
import shutil
import subprocess
from pathlib import Path

import pytest

from yourharness.engine import spec as jobspec
from yourharness.engine.execute import make_op, run_partition
from yourharness.engine.ops import OpError
from yourharness.engine.records import RecordError

ROOT = Path(__file__).resolve().parent.parent
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="needs node")

JSONL = "\n".join([
    '{"id": 1, "a": 1, "b": "x", "c": 1.0, "n": {"k": 2}, "l": [10, 20]}',
    '{"id": 2, "a": 2.5, "b": "y", "c": -0.0, "n": {"k": "3"}, "l": [30]}',
    '{"id": 3, "a": 123456789012345678901234567890, "b": "x", "c": 1e300, "n": {}, "l": []}',
    '{"id": 4, "a": "12", "b": "\\u00e9t\\u00e9", "c": NaN, "n": {"k": null}}',
    '{"id": 5, "a": " 3.5 ", "b": "x", "c": Infinity, "flag": true}',
    '',
    '   ',
    '{"id": 6, "a": "1_000", "b": "z", "c": -Infinity, "flag": false}',
    '{"id": 7, "a": "+7", "b": "", "c": 0.1, "flag": 1}',
    '{"id": 8, "a": "-0", "b": "x", "c": 5e-324}',
    '{"id": 9, "a": "1e3", "b": "line\\nbreak", "c": 2.0}',
    '{"id": 10, "a": "inf", "b": "q\\"uote", "c": 3}',
    '{"id": 11, "a": "nan", "b": "x"}',
    '{"id": 12, "a": "abc", "b": "\\ud83d\\ude00"}',
    '{"id": 13, "a": "", "b": null}',
    '{"id": 14, "a": true, "b": "x"}',
    '{"id": 15, "a": null}',
    '{"id": 16, "a": [1, 2], "b": "y"}',
    '{"id": 17, "a": 9007199254740993, "c": 9007199254740992.0}',
    '[1, 2, 3]',
    '"just text"',
    '42',
    '{"id": 18, "a": 1.5e16, "c": 1e16, "b": "\\t\\u0001"}',
    '{"id": 19, "a": "\\u00a0 8 \\u2003", "b": "x"}',
    '{"id": 20, "a": "0.000001", "c": 0.00001, "b": "y"}',
]).encode() + b"\r\n" + b'{"id": 21, "a": 4, "b": "crlf"}\r\n' + b'{"id": 22, "a": 5, "b": "last, no newline"}'

BAD_JSONL = b'{"a": 1}\n{"a": oops}\n\xff\xfe{"a": 2}\n{"a": 3,}\n{"a": 4}\n'

CSV_HEADER = ["id", "a", "b", "c"]
CSV = ("id,a,b,c\n"
       '1,1,x,"1.5"\n'
       '2,"2,5",y,""\n'
       '3,"line one\nline two",x,-0.0\n'
       '4,"say ""hi""",z,1e3\n'
       "5, 7 ,x,abc\n"
       "\n"
       '6,8,"multi\r\nline",9\r\n'
       "7,9,x\n"
       '8,"bad"x,y,1\n'
       "9,10,x,2\n"
       "10,é,ü,3").encode()

TEXT = "first line\nsecond line\n\nthird é\n".encode() + b"\xc3\x28 broken\nlast"

AGG_METRICS = [{"op": "count"}, {"op": "count", "field": "a"}, {"op": "sum", "field": "a"}, {"op": "mean", "field": "a"},
               {"op": "min", "field": "a"}, {"op": "max", "field": "a"}, {"op": "sum", "field": "c"}, {"op": "min", "field": "c"},
               {"op": "max", "field": "c"}, {"op": "sum", "field": "n.k"}, {"op": "count", "field": "l.1"}]
WHERES = [
    [],
    [{"field": "a", "op": ">", "value": 2}],
    [{"field": "a", "op": "<=", "value": 2.5}],
    [{"field": "c", "op": ">=", "value": 1}],
    [{"field": "b", "op": "==", "value": "x"}],
    [{"field": "b", "op": "!=", "value": "x"}],
    [{"field": "a", "op": "==", "value": 12}],
    [{"field": "a", "op": "==", "value": "1"}],
    [{"field": "flag", "op": "==", "value": True}],
    [{"field": "b", "op": "==", "value": None}],
    [{"field": "id", "op": "in", "value": [1, 3, "5", 7.0]}],
    [{"field": "b", "op": "contains", "value": "u"}],
    [{"field": "flag", "op": "exists"}],
    [{"field": "a", "op": "missing"}],
    [{"field": "c", "op": "==", "value": "1.0"}],
]


def wl_text(wl: dict) -> str:
    return json.dumps(jobspec._workload(wl, "w"))


def python_side(case: dict) -> dict:
    wl = json.loads(case["wl"])
    data = base64.b64decode(case["data"])
    cuts = [*case["cuts"], len(data)]
    chunks, at = [], 0
    for c in cuts:
        if c > at:
            chunks.append(data[at:min(c, len(data))])
        at = max(at, c)
    op = make_op(wl, {}, None)
    try:
        stats = run_partition(op, case["format"], case["header"], chunks, case["start"], bad=case["bad"])
    except RecordError as e:
        return {"ok": False, "kind": "data", "offset": e.offset}
    except OpError:
        return {"ok": False, "kind": "op"}
    return {"ok": True, "state": op.state(), "records": stats["records"], "skipped": stats["skipped"],
            "badOffsets": [b["offset"] for b in stats["bad"]]}


def cuts_for(data: bytes, rng: random.Random) -> list[list[int]]:
    whole = [[]]
    bytewise = [list(range(1, len(data)))] if len(data) < 3000 else []
    random_cuts = [sorted(rng.sample(range(1, len(data)), min(len(data) - 1, 7)))]
    return whole + bytewise + random_cuts


def cases() -> list[dict]:
    rng = random.Random(1234)
    out = []
    datasets = [("jsonl", None, JSONL), ("jsonl", None, BAD_JSONL), ("csv", CSV_HEADER, CSV.split(b"\n", 1)[1]), ("text", None, TEXT)]
    for fmt, header, data in datasets:
        start = 1000 if fmt == "csv" else 0
        for where in WHERES:
            metrics = AGG_METRICS if fmt != "text" else [{"op": "count"}, {"op": "count", "field": "line"}, {"op": "max", "field": "line"}]
            if fmt == "text" and where:
                where = [{"field": "line", "op": "contains", "value": "line"}]
            wls = [{"type": "batch.aggregate", "metrics": metrics, "where": where}]
            by = [["b"], ["a"], ["flag", "b"], ["n.k"], ["l"]] if fmt == "jsonl" else [["b"], ["a", "c"]] if fmt == "csv" else [["line"]]
            for b in by:
                wls.append({"type": "batch.groupby", "by": b, "metrics": [{"op": "count"}, {"op": "sum", "field": "a"}, {"op": "max", "field": "c"}],
                            "where": where})
            for wl in wls:
                for bad in ("fail", "skip"):
                    for cuts in cuts_for(data, rng):
                        out.append({"wl": wl_text(wl), "format": fmt, "header": header, "data": base64.b64encode(data).decode(),
                                    "cuts": cuts, "start": start, "bad": bad})
    # many random floats and ints: exact sums must agree to the last unit
    nums = []
    for _ in range(3000):
        k = rng.random()
        if k < 0.3:
            nums.append(repr(rng.uniform(-1e6, 1e6)))
        elif k < 0.5:
            nums.append(repr(rng.random() * 10 ** rng.randint(-320, 300)))
        elif k < 0.7:
            nums.append(str(rng.randint(-10**30, 10**30)))
        elif k < 0.8:
            nums.append(json.dumps(repr(rng.uniform(-100, 100))))  # numbers as text
        else:
            nums.append(str(rng.randint(-1000, 1000)))
    data = "".join(f'{{"g": {i % 7}, "x": {v}}}\n' for i, v in enumerate(nums)).encode()
    for wl in ({"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "x"}, {"op": "mean", "field": "x"}, {"op": "min", "field": "x"},
                                                       {"op": "max", "field": "x"}]},
               {"type": "batch.groupby", "by": ["g"], "metrics": [{"op": "sum", "field": "x"}, {"op": "count"}]}):
        out.append({"wl": wl_text(wl), "format": "jsonl", "header": None, "data": base64.b64encode(data).decode(),
                    "cuts": sorted(rng.sample(range(1, len(data)), 50)), "start": 0, "bad": "fail"})
    return out


def run_js(all_cases: list[dict]) -> list[dict]:
    proc = subprocess.run([NODE, str(ROOT / "tests" / "web" / "engine_ops_run.mjs")], input="\n".join(json.dumps(c) for c in all_cases),
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stderr[-3000:]
    return [json.loads(line) for line in proc.stdout.splitlines() if line]


def canon(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)


def test_the_browser_operators_give_the_python_operators_states():
    all_cases = cases()
    js = run_js(all_cases)
    assert len(js) == len(all_cases)
    unsupported, same_state, same_error = 0, 0, 0
    for case, got in zip(all_cases, js):
        want = python_side(case)
        where = f"{case['format']} {json.loads(case['wl'])['type']} bad={case['bad']} {case['wl'][:200]}"
        assert got.get("kind") != "crash", f"{where}: {got.get('error')}"
        if got.get("kind") == "unsupported":
            unsupported += 1
            continue
        assert got["ok"] == want["ok"], f"{where}: python {want} js {got}"
        if not want["ok"]:  # the same bad record stops both, at the same offset
            assert got["kind"] == want["kind"] and got.get("offset") == want.get("offset"), f"{where}: python {want} js {got}"
            same_error += 1
            continue
        assert canon(got["state"]) == canon(want["state"]), f"{where}:\npython {canon(want['state'])[:1500]}\njs     {canon(got['state'])[:1500]}"
        assert (got["records"], got["skipped"], got["badOffsets"]) == (want["records"], want["skipped"], want["badOffsets"]), where
        same_state += 1
    assert unsupported < 0.05 * len(all_cases), f"{unsupported} of {len(all_cases)} refused"
    assert same_state > 0.6 * len(all_cases) and same_error > 0, (same_state, same_error, unsupported)


def test_what_the_browser_refuses_instead_of_guessing():
    refuse = [
        ({"type": "batch.aggregate", "metrics": [{"op": "count"}], "where": [{"field": "b", "op": "regex", "value": "^x"}]}, b'{"b": "x"}\n'),
        ({"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "a"}]}, '{"a": "١٢"}\n'.encode()),  # Arabic-Indic digits
        ({"type": "batch.groupby", "by": ["o"], "metrics": [{"op": "count"}]}, b'{"o": {"b": 1, "1": 2}}\n'),
        ({"type": "batch.aggregate", "metrics": [{"op": "count"}], "where": [{"field": "o", "op": "==", "value": "{}"}]}, b'{"o": {}}\n'),
    ]
    all_cases = [{"wl": wl_text(wl), "format": "jsonl", "header": None, "data": base64.b64encode(d).decode(), "cuts": [], "start": 0, "bad": "fail"}
                 for wl, d in refuse]
    assert [r["kind"] for r in run_js(all_cases)] == ["unsupported"] * len(refuse)
