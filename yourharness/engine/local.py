"""The reference executor: a whole job in this process, one partition after another.

It runs the same operators and reducers as the workers do. Tests compare a
distributed result with it, and a job's spot check re-runs some partitions with it
on the coordinator to compare them with what a worker sent back.
"""

from __future__ import annotations

from pathlib import Path

from .execute import HaloTooSmall, feed_halo, make_op, run_items, run_partition
from .ops import halo_records_needed
from .partition import plan_items
from .reducer import final_result, merge_states

CHUNK = 1024 * 1024


def file_chunks(path: Path, start: int, end: int, size: int = CHUNK):
    with open(path, "rb") as f:
        f.seek(start)
        left = end - start
        while left > 0:
            data = f.read(min(size, left))
            if not data:
                return
            left -= len(data)
            yield data


def run_range(job: dict, dataset: dict, path: Path, start: int, end: int, ctx: dict | None = None,
              halo_bytes: int | None = None, model: dict | None = None) -> tuple[dict, list]:
    """One byte-range partition: (its state, the records it wrote).

    halo_bytes limits how much of the data before the partition is read for operators
    that need earlier records; by default all of it, which is always enough. model is
    {"adapter", "path"} for ml.evaluate.
    """
    wl = job["spec"]["workload"]
    bad = job["spec"]["output"]["onBadRecord"]
    out: list = []
    op = make_op(wl, ctx, out.append, bad=bad, model=model)
    if halo_records_needed(wl) and start > dataset["dataStart"]:
        h0 = dataset["dataStart"] if halo_bytes is None else max(dataset["dataStart"], start - halo_bytes)
        try:
            feed_halo(op, wl, dataset["format"], dataset.get("header"), file_chunks(path, h0, start), h0, dataset["dataStart"])
        except HaloTooSmall:
            op = make_op(wl, ctx, out.append, bad=bad, model=model)
            feed_halo(op, wl, dataset["format"], dataset.get("header"), file_chunks(path, dataset["dataStart"], start),
                      dataset["dataStart"], dataset["dataStart"])
    run_partition(op, dataset["format"], dataset.get("header"), file_chunks(path, start, end), start,
                  bad=job["spec"]["output"]["onBadRecord"])
    return op.state(), out


def reference(job: dict, dataset: dict, path: Path, ranges: list[tuple[int, int]] | None = None,
              merge_order: list[int] | None = None, ctx: dict | None = None, model: dict | None = None) -> dict:
    """The job's result computed here. ranges defaults to one partition over all the data."""
    wl = job["spec"]["workload"]
    if ranges is None:
        ranges = [(dataset["dataStart"], dataset["size"])] if dataset["size"] > dataset["dataStart"] else []
    states, records = [], []
    for start, end in ranges:
        st, out = run_range(job, dataset, path, start, end, ctx, model=model)
        states.append(st)
        records.extend(out)
    if merge_order is not None:  # combine in another order, to show the order doesn't matter
        states = [states[i] for i in merge_order]
    merged = merge_states(wl, states)
    return {"result": final_result(wl, merged), "records": records}


def reference_items(job: dict, per: int | None = None, ctx: dict | None = None) -> dict:
    wl = job["spec"]["workload"]
    items = job["spec"]["inputs"]["items"]
    states, records = [], []
    for i, j in plan_items(len(items), per or len(items)):
        out: list = []
        op = make_op(wl, ctx, out.append)
        run_items(op, items[i:j], i)
        states.append(op.state())
        records.extend(out)
    return {"result": final_result(wl, merge_states(wl, states)), "records": records}
