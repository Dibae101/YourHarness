"""Built-in operators: what runs over the records of one partition, and how the
partial results of all partitions combine.

Every operator keeps a partial state that is plain JSON. merge() combines states
from any number of partitions, in any order, into the state one partition holding
all the records would have had; final() turns a state into the result. Sums are
exact (exact.py), so splitting the input never changes the answer.

Operators (the "workload" part of a job, see spec.py):
  batch.aggregate  metrics over all matching records: count, sum, mean, min, max
  batch.groupby    the same metrics per distinct value of one or more fields
  batch.transform  filter, select, rename and a rolling mean; writes records out
"""

from __future__ import annotations

import json
import re

from .exact import ExactSum, number_in, number_out
from .records import MISSING, get_field, to_number

MAX_GROUPS = 200_000  # distinct keys one partition may hold; more needs a shuffle
MAX_REGEX = 500


class OpError(ValueError):
    """A problem with the job's operator settings or its data that retrying won't fix."""


# ------------------------------------------------------------------ filters
def _compile_predicate(p: dict):
    field, op, value = p["field"], p["op"], p.get("value")
    if op == "exists":
        return lambda r: get_field(r, field) not in (MISSING, None)
    if op == "missing":
        return lambda r: get_field(r, field) in (MISSING, None)
    if op in ("<", "<=", ">", ">="):
        target = to_number(value)
        cmp = {"<": lambda a: a < target, "<=": lambda a: a <= target, ">": lambda a: a > target, ">=": lambda a: a >= target}[op]

        def numeric(r):
            v = to_number(get_field(r, field))
            return v is not None and v == v and cmp(v)
        return numeric
    if op in ("==", "!="):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            def eq(r):
                v = to_number(get_field(r, field))
                return v is not None and v == value
        else:
            def eq(r):
                v = get_field(r, field)
                if v is MISSING:
                    return value is None
                if isinstance(value, str) and not isinstance(v, str):
                    v = json.dumps(v) if isinstance(v, (dict, list)) else ("true" if v is True else "false" if v is False else str(v))
                return v == value
        return eq if op == "==" else (lambda r: not eq(r))
    if op == "in":
        options = {str(x) for x in value}
        return lambda r: (lambda v: v is not MISSING and str(v) in options)(get_field(r, field))
    if op == "contains":
        needle = str(value)
        return lambda r: (lambda v: isinstance(v, str) and needle in v)(get_field(r, field))
    if op == "regex":
        rx = re.compile(str(value))
        return lambda r: (lambda v: isinstance(v, str) and rx.search(v) is not None)(get_field(r, field))
    raise OpError(f"unknown filter {op}")


def compile_where(where: list | None):
    preds = [_compile_predicate(p) for p in where or []]
    if not preds:
        return lambda r: True
    return lambda r: all(p(r) for p in preds)


# ------------------------------------------------------------------ metrics
class Count:
    def __init__(self, m: dict):
        self.field = m.get("field")
        self.n = 0

    def feed(self, rec) -> None:
        if self.field is None or get_field(rec, self.field) not in (MISSING, None):
            self.n += 1

    def state(self) -> dict:
        return {"n": self.n}

    def load(self, st: dict) -> None:
        self.n = int(st["n"])

    @staticmethod
    def merge(states: list[dict]) -> dict:
        return {"n": sum(int(s["n"]) for s in states)}

    @staticmethod
    def final(st: dict, m: dict):
        return int(st["n"])


class Sum:
    """sum and mean: the exact total of the field's numeric values; others are counted as skipped."""

    def __init__(self, m: dict):
        self.field = m["field"]
        self.s = ExactSum()
        self.skip = 0

    def feed(self, rec) -> None:
        v = to_number(get_field(rec, self.field))
        if v is None:
            self.skip += 1
        else:
            self.s.add(v)

    def state(self) -> dict:
        return {"s": self.s.to_state(), "skip": self.skip}

    def load(self, st: dict) -> None:
        self.s = ExactSum.from_state(st["s"])
        self.skip = int(st["skip"])

    @staticmethod
    def merge(states: list[dict]) -> dict:
        total = ExactSum()
        for st in states:
            total.merge(ExactSum.from_state(st["s"]))
        return {"s": total.to_state(), "skip": sum(int(st["skip"]) for st in states)}

    @staticmethod
    def final(st: dict, m: dict):
        s = ExactSum.from_state(st["s"])
        return s.value() if m["op"] == "sum" else s.mean()


class MinMax:
    """min and max over the field's numeric values. NaN is counted, never compared."""

    def __init__(self, m: dict):
        self.field = m["field"]
        self.want_max = m["op"] == "max"
        self.v = None
        self.skip = 0
        self.nan = 0

    def feed(self, rec) -> None:
        v = to_number(get_field(rec, self.field))
        if v is None:
            self.skip += 1
        elif v != v:
            self.nan += 1
        elif self.v is None or (v > self.v if self.want_max else v < self.v):
            self.v = v

    def state(self) -> dict:
        return {"v": number_out(self.v), "skip": self.skip, "nan": self.nan}

    def load(self, st: dict) -> None:
        self.v = number_in(st["v"])
        self.skip, self.nan = int(st["skip"]), int(st["nan"])

    @staticmethod
    def merge(states: list[dict], want_max: bool = False) -> dict:
        best = None
        for st in states:
            v = number_in(st["v"])
            if v is not None and (best is None or (v > best if want_max else v < best)):
                best = v
        return {"v": number_out(best), "skip": sum(int(s["skip"]) for s in states), "nan": sum(int(s["nan"]) for s in states)}

    @staticmethod
    def final(st: dict, m: dict):
        return st["v"]


METRICS = {"count": Count, "sum": Sum, "mean": Sum, "min": MinMax, "max": MinMax}


def new_metric(m: dict):
    return METRICS[m["op"]](m)


def merge_metric(m: dict, states: list[dict]) -> dict:
    if m["op"] in ("min", "max"):
        return MinMax.merge(states, want_max=m["op"] == "max")
    return METRICS[m["op"]].merge(states)


def final_metric(m: dict, st: dict):
    return METRICS[m["op"]].final(st, m)


# ------------------------------------------------------------------ operators (one partition)
class AggregateOp:
    """batch.aggregate. Resumable: a checkpoint is the state after the records read so far."""

    resumable = True

    def __init__(self, wl: dict, ctx: dict | None = None):
        self.metrics_spec = wl["metrics"]
        self.where = compile_where(wl.get("where"))
        self.metrics = [new_metric(m) for m in self.metrics_spec]
        self.seen = self.matched = 0

    def feed(self, rec) -> None:
        self.seen += 1
        if self.where(rec):
            self.matched += 1
            for m in self.metrics:
                m.feed(rec)

    def state(self) -> dict:
        return {"seen": self.seen, "matched": self.matched, "metrics": [m.state() for m in self.metrics]}

    def load(self, st: dict) -> None:
        self.seen, self.matched = int(st["seen"]), int(st["matched"])
        for m, ms in zip(self.metrics, st["metrics"]):
            m.load(ms)

    @staticmethod
    def merge(wl: dict, states: list[dict]) -> dict:
        return {
            "seen": sum(int(s["seen"]) for s in states), "matched": sum(int(s["matched"]) for s in states),
            "metrics": [merge_metric(m, [s["metrics"][i] for s in states]) for i, m in enumerate(wl["metrics"])],
        }

    @staticmethod
    def final(wl: dict, st: dict) -> dict:
        values = {m["as"]: final_metric(m, st["metrics"][i]) for i, m in enumerate(wl["metrics"])}
        skipped = {m["as"]: st["metrics"][i]["skip"] for i, m in enumerate(wl["metrics"]) if st["metrics"][i].get("skip")}
        return {"values": values, "records": st["seen"], "matched": st["matched"], "skipped": skipped}


def group_key(rec, fields: list[str]) -> str:
    vals = []
    for f in fields:
        v = get_field(rec, f)
        vals.append(None if v is MISSING else v)
    return json.dumps(vals, ensure_ascii=False, separators=(",", ":"), default=str)


class GroupByOp:
    """batch.groupby with every partition combining its own groups first (map-side combine)."""

    resumable = True

    def __init__(self, wl: dict, ctx: dict | None = None):
        self.by = wl["by"]
        self.metrics_spec = wl["metrics"]
        self.where = compile_where(wl.get("where"))
        self.groups: dict[str, list] = {}
        self.seen = self.matched = 0

    def feed(self, rec) -> None:
        self.seen += 1
        if not self.where(rec):
            return
        self.matched += 1
        key = group_key(rec, self.by)
        ms = self.groups.get(key)
        if ms is None:
            if len(self.groups) >= MAX_GROUPS:
                raise OpError(f"a partition has more than {MAX_GROUPS:,} distinct groups; for this many, add \"shuffle\": true to the "
                              "group-by and use smaller partitions")
            ms = self.groups[key] = [new_metric(m) for m in self.metrics_spec]
        for m in ms:
            m.feed(rec)

    def state(self) -> dict:
        return {"seen": self.seen, "matched": self.matched, "groups": {k: [m.state() for m in ms] for k, ms in self.groups.items()}}

    def load(self, st: dict) -> None:
        self.seen, self.matched = int(st["seen"]), int(st["matched"])
        self.groups = {}
        for k, mss in st["groups"].items():
            ms = [new_metric(m) for m in self.metrics_spec]
            for m, s in zip(ms, mss):
                m.load(s)
            self.groups[k] = ms

    @staticmethod
    def merge(wl: dict, states: list[dict]) -> dict:
        by_key: dict[str, list[list[dict]]] = {}
        for st in states:
            for k, mss in st["groups"].items():
                by_key.setdefault(k, []).append(mss)
        groups = {k: [merge_metric(m, [mss[i] for mss in lists]) for i, m in enumerate(wl["metrics"])] for k, lists in by_key.items()}
        return {"seen": sum(int(s["seen"]) for s in states), "matched": sum(int(s["matched"]) for s in states), "groups": groups}

    @staticmethod
    def final(wl: dict, st: dict) -> dict:
        rows = []
        for k in sorted(st["groups"]):
            mss = st["groups"][k]
            row = {"key": dict(zip(wl["by"], json.loads(k)))}
            for i, m in enumerate(wl["metrics"]):
                row[m["as"]] = final_metric(m, mss[i])
            rows.append(row)
        return {"groups": rows, "records": st["seen"], "matched": st["matched"]}


class TransformOp:
    """batch.transform: keeps matching records, optionally adds a rolling mean, selects and renames fields.

    The rolling mean over the previous `window` records needs the records just before
    the partition, which arrive first as read-only context (the halo).
    """

    resumable = False

    def __init__(self, wl: dict, ctx: dict | None = None, write=None):
        self.where = compile_where(wl.get("where"))
        self.select = wl.get("select") or None
        self.rename = wl.get("rename") or {}
        self.rolling = wl.get("rolling")
        self.write = write
        self.window: list = []
        self.wsum = ExactSum()
        self.seen = self.out = 0

    def prelude(self, rec) -> None:
        """A record before the partition: only feeds the rolling window."""
        if self.rolling and self.where(rec):
            self._roll(rec)

    def _roll(self, rec):
        v = to_number(get_field(rec, self.rolling["field"]))
        if v is None or v != v or v in (float("inf"), float("-inf")):
            return None
        self.window.append(v)
        self.wsum.add(v)
        if len(self.window) > self.rolling["window"]:
            old = self.window.pop(0)
            self.wsum.add(-old)  # exact: the window's sum never drifts
        self.wsum.n = len(self.window)
        return number_in(self.wsum.mean())

    def feed(self, rec) -> None:
        self.seen += 1
        if not self.where(rec):
            return
        out = dict(rec) if isinstance(rec, dict) else {"value": rec}
        if self.rolling:
            out[self.rolling["as"]] = number_out(self._roll(rec))
        if self.select:
            out = {f: (None if (v := get_field(out, f)) is MISSING else v) for f in self.select}
        if self.rename:
            out = {self.rename.get(k, k): v for k, v in out.items()}
        self.out += 1
        self.write(out)

    def state(self) -> dict:
        return {"seen": self.seen, "out": self.out}


OPS = {"batch.aggregate": AggregateOp, "batch.groupby": GroupByOp, "batch.transform": TransformOp}


def halo_records_needed(wl: dict) -> int:
    """Records before the partition an operator must see; 0 for most."""
    if wl.get("type") == "batch.transform" and wl.get("rolling"):
        return int(wl["rolling"]["window"]) - 1
    return 0
