"""Combining the accepted partial results of a job into its one result.

merge_states() takes the partition states in partition order. For everything but
"collect" and record outputs, the order doesn't change the answer; for those two it
is the input's own order, so the result reads like one computer did all the work.
"""

from __future__ import annotations

from .exact import ExactSum
from .ops import AggregateOp, GroupByOp


class ReduceError(ValueError):
    pass


def merge_states(wl: dict, states: list[dict]) -> dict:
    t = wl["type"]
    if t == "batch.aggregate":
        return AggregateOp.merge(wl, states)
    if t == "batch.groupby":
        return GroupByOp.merge(wl, states)
    if t == "batch.transform":
        return {"seen": sum(int(s["seen"]) for s in states), "out": sum(int(s["out"]) for s in states)}
    if t == "python.map":
        return _merge_python(wl, states)
    raise ReduceError(f"no reducer for {t}")


def final_result(wl: dict, st: dict) -> dict:
    t = wl["type"]
    if t == "batch.aggregate":
        return AggregateOp.final(wl, st)
    if t == "batch.groupby":
        return GroupByOp.final(wl, st)
    if t == "batch.transform":
        return {"records": st["seen"], "written": st["out"]}
    if t == "python.map":
        return _final_python(wl, st)
    raise ReduceError(f"no reducer for {t}")


def _merge_python(wl: dict, states: list[dict]) -> dict:
    r = wl["reducer"]
    calls = sum(int(s.get("calls", 0)) for s in states)
    if r == "sum":
        total = ExactSum()
        for s in states:
            total.merge(ExactSum.from_state(s["sum"]))
        return {"calls": calls, "sum": total.to_state()}
    if r == "count":
        return {"calls": calls, "n": sum(int(s["n"]) for s in states)}
    if r == "merge_counts":
        counts: dict[str, int] = {}
        for s in states:
            for k, v in s["counts"].items():
                counts[k] = counts.get(k, 0) + int(v)
        return {"calls": calls, "counts": counts}
    if r == "collect":
        values: list = []
        for s in states:
            values.extend(s["values"])
        return {"calls": calls, "values": values}
    if r == "best":
        key, want_max = wl["bestBy"]["key"], wl["bestBy"]["order"] == "max"
        best, best_at = None, None
        for i, s in enumerate(states):
            b = s.get("best")
            if b is None:
                continue
            v = b.get(key)
            # ties go to the earliest item, so the answer doesn't depend on which computer was faster
            if best is None or (v > best[key] if want_max else v < best[key]):
                best, best_at = b, (i, s.get("bestAt", 0))
        return {"calls": calls, "best": best, "bestAt": best_at}
    if r == "concat":
        return {"calls": calls, "out": sum(int(s.get("out", 0)) for s in states)}
    raise ReduceError(f"unknown reducer {r}")


def _final_python(wl: dict, st: dict) -> dict:
    r = wl["reducer"]
    if r == "sum":
        return {"value": ExactSum.from_state(st["sum"]).value(), "calls": st["calls"]}
    if r == "count":
        return {"value": st["n"], "calls": st["calls"]}
    if r == "merge_counts":
        return {"value": dict(sorted(st["counts"].items(), key=lambda kv: (-kv[1], kv[0]))), "calls": st["calls"]}
    if r == "collect":
        return {"value": st["values"], "calls": st["calls"]}
    if r == "best":
        return {"value": st["best"], "calls": st["calls"]}
    return {"written": st["out"], "calls": st["calls"]}
