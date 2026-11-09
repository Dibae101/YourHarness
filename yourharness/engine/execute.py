"""Running one partition: records in, partial state (and output records) out.

Used by the worker process on each computer (worker.py) and by the reference
executor (local.py), so a partition computes the same thing wherever it runs.
"""

from __future__ import annotations

import json
import time
from typing import Callable, Iterable

from .ops import OPS, OpError, halo_records_needed
from .records import RecordError, iter_records


def dump_record(rec) -> str:
    """One output record as a line of JSON. Workers and the coordinator write records the same way,
    so a recomputed partition can be compared with what a worker sent back by digest alone."""
    return json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n"


class HaloTooSmall(Exception):
    """The bytes sent before the partition held fewer records than the operator needs. Retried with more."""


def writes_records(wl: dict) -> bool:
    """Whether the workload's job writes output records (part.jsonl) besides its result."""
    from .spec import writes_records as writes

    return writes(wl)


def make_op(wl: dict, ctx: dict | None, write: Callable[[dict], None] | None, *, bad: str = "fail", model: dict | None = None):
    """The operator for a workload. model: {"adapter", "path"} of a fetched model, for ml.evaluate."""
    t = wl["type"]
    if t == "python.map":
        from .pyop import PythonOp

        return PythonOp(wl, ctx, write)
    if t == "ml.evaluate":
        from .ml import EvaluateOp, load_predictor

        if model is None:
            raise OpError("ml.evaluate needs its model")
        return EvaluateOp(wl, ctx, write, load_predictor(model, wl), bad)
    cls = OPS.get(t)
    if cls is None:
        raise OpError(f"this computer can't run {t}")
    return cls(wl, ctx, write) if t == "batch.transform" else cls(wl, ctx)


def feed_halo(op, wl: dict, fmt: str, header, halo_chunks: Iterable[bytes], halo_start: int, data_start: int) -> None:
    """Give the operator the records just before the partition, as context it doesn't own."""
    need = halo_records_needed(wl)
    if not need:
        return
    recs = []
    first = halo_start > data_start  # the first line of the halo is probably cut in two
    for i, (_s, _e, rec) in enumerate(iter_records(fmt, halo_chunks, halo_start, header, on_bad=lambda e: None)):
        if i == 0 and first:
            continue
        recs.append(rec)
        if len(recs) > need:
            recs.pop(0)
    if len(recs) < need and halo_start > data_start:
        raise HaloTooSmall(f"needed {need} records before the partition and got {len(recs)}")
    for rec in recs:
        op.prelude(rec)


def run_partition(op, fmt: str, header, chunks: Iterable[bytes], start: int, *, bad: str = "fail",
                  checkpoint: Callable[[int, dict], None] | None = None, every_s: float = 10.0,
                  progress: Callable[[int, int], None] | None = None) -> dict:
    """Feed every record to op. Returns {"records", "skipped", "bad"}.

    checkpoint(offset, state) is called about every `every_s` seconds for resumable
    operators, with the offset just after the last record taken.
    """
    skipped = []

    def on_bad(e: RecordError) -> None:
        skipped.append({"offset": e.offset, "reason": e.reason})

    n = 0
    last = time.monotonic()
    resumable = checkpoint is not None and getattr(op, "resumable", False)
    feed_at = getattr(op, "feed_at", None)  # operators that need each record's offset (a sample's id)
    for s, end, rec in iter_records(fmt, chunks, start, header, on_bad=on_bad if bad == "skip" else None):
        if feed_at is not None:
            feed_at(s, rec)
        else:
            op.feed(rec)
        n += 1
        if (resumable or progress) and n % 256 == 0 and time.monotonic() - last >= every_s:
            last = time.monotonic()
            if resumable:
                checkpoint(end, op.state())
            if progress:
                progress(n, end)
    if hasattr(op, "flush"):
        op.flush()
    return {"records": n, "skipped": len(skipped), "bad": skipped[:20]}


def run_items(op, items: list, start_index: int) -> dict:
    for it in items:
        op.feed(it)
    if hasattr(op, "flush"):
        op.flush()
    return {"records": len(items), "skipped": 0, "bad": []}
