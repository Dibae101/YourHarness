"""Benchmarks: the same work done three ways, measured, so speed claims rest on numbers.

    python bench/run.py --mb 256 --repeat 3

For each suite it measures
  baseline     the reference executor in this process, one core, no network
  one          the job engine, kept on one computer ("mode": "local")
  spread       the job engine over every computer that can run it ("distributed")
and checks that all three give exactly the same result. It writes
bench/results/<date>-<host>.json and prints a table. Speed-up is baseline time
divided by the engine's time for the same work; below 1 means slower, and those
rows are kept too.

It needs a running pool: inside the workspace, or on a computer that joined with
`harness agent`, or with HARNESS_URL and HARNESS_TOKEN set.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from yourharness.client import Client  # noqa: E402
from yourharness.engine import spec as jobspec  # noqa: E402
from yourharness.engine.index import Indexer  # noqa: E402
from yourharness.engine.local import reference, reference_items  # noqa: E402

PY_WORK = """def process(n):
    import math
    total = 0.0
    for i in range(1, 200_000):
        total += math.sqrt(i * n % 9973)
    return total
"""


def make_data(path: Path, mb: int) -> Path:
    if path.exists() and path.stat().st_size >= mb * 2**20:
        return path
    regions = ["north", "south", "east", "west", "central"]
    with open(path, "w", encoding="utf-8") as f:
        i = 0
        while f.tell() < mb * 2**20:
            f.write(json.dumps({"id": i, "region": regions[i % 5], "amount": ((i * 7919) % 100_000) / 100, "qty": i % 17,
                                "note": "x" * (i % 40)}) + "\n")
            i += 1
    return path


def doc(workload: dict, inputs: dict, mode: str) -> dict:
    return {"apiVersion": jobspec.API_VERSION, "kind": "Job", "metadata": {"name": f"bench {workload['type']} {mode}"},
            "spec": {"workload": workload, "inputs": inputs, "mode": mode}}


SUITES = {
    "aggregate": {"type": "batch.aggregate", "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "mean", "field": "amount"}]},
    "groupby": {"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}]},
    "python": {"type": "python.map", "code": PY_WORK, "reducer": "sum"},
}


def run_engine(c: Client, job: dict) -> dict:
    t0 = time.monotonic()
    started = c.start_job(job)
    done = c.wait_engine_job(started["id"], quiet=True, poll=0.2)
    wall = time.monotonic() - t0
    if done["status"] != "done":
        raise SystemExit(f"{job['metadata']['name']} {done['status']}: {done.get('error')}")
    return {"seconds": round(wall, 3), "partitions": done["total"], "decision": done["decision"],
            "perDevice": done["perDevice"], "result": done["result"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mb", type=int, default=64, help="size of the generated dataset in MB")
    ap.add_argument("--items", type=int, default=96, help="calls of the Python suite")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--suite", default="aggregate,groupby,python")
    ap.add_argument("--out", default=str(ROOT / "bench" / "results"))
    ap.add_argument("--label", default="", help="a note saved with the results, e.g. 'Mac + Windows PC over Wi-Fi'")
    a = ap.parse_args()
    c = Client()
    devices = [d for d in c.devices() if (d.get("engine") or {}).get("slots")]
    print(f"Pool at {c.server}: {', '.join(d['name'] for d in devices) or 'no computers with the job engine'}", file=sys.stderr)
    work = Path(tempfile.gettempdir()) / "yourharness-bench"
    work.mkdir(exist_ok=True)
    data = make_data(work / f"sales-{a.mb}mb.jsonl", a.mb)
    ix = Indexer("jsonl")
    with open(data, "rb") as f:
        while chunk := f.read(4 * 2**20):
            ix.feed(chunk)
    meta = {**ix.finish(), "format": "jsonl"}
    ref = None
    rows = []
    for name in [s.strip() for s in a.suite.split(",") if s.strip()]:
        wl = SUITES[name]
        items = name == "python"
        if items:
            inputs = {"items": list(range(1, a.items + 1))}
        else:
            if ref is None:
                print(f"Uploading {data.name} ({data.stat().st_size / 2**20:.0f} MB)…", file=sys.stderr)
                ref = c.add_dataset(data)["ref"]
            inputs = {"dataset": {"ref": ref}}
        parsed = jobspec.parse(doc(wl, inputs if items else {"dataset": {"ref": "dataset:d00000000"}}, "local"))
        times = {"baseline": [], "one": [], "spread": []}
        answers = set()
        extra = {}
        for _ in range(a.repeat):
            t0 = time.monotonic()
            base = reference_items(parsed) if items else reference(parsed, meta, data)
            times["baseline"].append(time.monotonic() - t0)
            answers.add(json.dumps(base["result"], sort_keys=True))
            for mode, key in (("local", "one"), ("distributed", "spread")):
                got = run_engine(c, doc(wl, inputs, mode))
                times[key].append(got["seconds"])
                answers.add(json.dumps(got["result"], sort_keys=True))
                extra[key] = {k: got[k] for k in ("partitions", "decision", "perDevice")}
        if len(answers) != 1:
            raise SystemExit(f"{name}: the three ways gave different results: {answers}")
        med = {k: statistics.median(v) for k, v in times.items()}
        rows.append({"suite": name, "units": a.items if items else data.stat().st_size, "unit": "calls" if items else "bytes",
                     "seconds": {k: [round(x, 3) for x in v] for k, v in times.items()}, "median": {k: round(v, 3) for k, v in med.items()},
                     "speedup": {k: round(med["baseline"] / med[k], 2) for k in ("one", "spread")}, "engine": extra})
        print(f"{name}: baseline {med['baseline']:.2f} s, one computer {med['one']:.2f} s, spread {med['spread']:.2f} s", file=sys.stderr)
    out = {"date": time.strftime("%Y-%m-%d %H:%M"), "label": a.label, "host": platform.node(), "python": platform.python_version(),
           "devices": [{"name": d["name"], "cores": d["info"].get("cores"), "memoryGB": d["info"].get("memoryGB"),
                        "slots": d["engine"]["slots"], "os": d["info"].get("os")} for d in devices], "repeat": a.repeat, "results": rows}
    dest = Path(a.out)
    dest.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M")
    path = dest / f"{stamp}-{''.join(ch if ch.isalnum() else '-' for ch in platform.node())[:40]}.json"
    path.write_text(json.dumps(out, indent=2))
    print("\n| Suite | Work | Baseline | One computer | Spread | Speed-up (spread) |\n|---|---|---|---|---|---|")
    for r in rows:
        work_s = f"{r['units'] / 2**20:.0f} MB" if r["unit"] == "bytes" else f"{r['units']} calls"
        m = r["median"]
        print(f"| {r['suite']} | {work_s} | {m['baseline']:.2f} s | {m['one']:.2f} s | {m['spread']:.2f} s | {r['speedup']['spread']}× |")
    print(f"\nSaved {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
