"""python.map: your own function, called for each record or item of a partition.

The function's return value is handled by the job's reducer:
  collect       any JSON value; the job's result is the list of them, in input order
  concat        a record (dict), a list of records, or None; written out as records
  sum           a number; summed exactly
  count         True/False (counts the Trues), or an int (added)
  merge_counts  a dict of name -> count; counts are added up
  best          a dict whose bestBy.key is a number; the best one is kept (ties: the first)

With "batch": N, the function gets a list of up to N records at a time; for
collect it must return a list of the same length.
"""

from __future__ import annotations

import inspect
import json
import traceback

from .exact import ExactSum
from .ops import OpError
from .records import to_number


def describe(e: BaseException) -> str:
    line = None
    for frame in traceback.extract_tb(e.__traceback__):
        if frame.filename == "<job code>":
            line = frame.lineno
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    return f"{text} (line {line} of the job's code)" if line else text


class UserCodeError(RuntimeError):
    """The job's own code raised. It may be a passing problem, so it can be retried elsewhere."""


class PythonOp:
    resumable = False

    def __init__(self, wl: dict, ctx: dict | None = None, write=None):
        ns: dict = {"__name__": "__job__"}
        try:
            exec(compile(wl["code"], "<job code>", "exec"), ns)  # noqa: S102 - running the job's code is the point
        except SyntaxError as e:
            raise OpError(f"the job's code has a syntax error: {e.msg} (line {e.lineno})") from None
        except Exception as e:
            raise UserCodeError(describe(e)) from None
        fn = ns.get(wl["function"])
        if not callable(fn):
            raise OpError(f"the job's code doesn't define a function named {wl['function']}")
        try:
            takes = len([p for p in inspect.signature(fn).parameters.values() if p.default is p.empty and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)])
        except (TypeError, ValueError):
            takes = 1
        self.fn, self.with_ctx = fn, takes >= 2
        self.ctx = ctx or {}
        self.reducer = wl["reducer"]
        self.batch = int(wl.get("batch") or 1)
        self.best_key = (wl.get("bestBy") or {}).get("key")
        self.want_max = (wl.get("bestBy") or {}).get("order", "max") == "max"
        self.write = write
        self.buf: list = []
        self.calls = 0
        self.sum = ExactSum()
        self.n = 0
        self.counts: dict[str, int] = {}
        self.values: list = []
        self.best = None
        self.best_at = 0
        self.out = 0
        self.index = 0  # position of the next record within the partition

    def feed(self, rec) -> None:
        if self.batch == 1:
            self._take(self._call(rec), [rec])
        else:
            self.buf.append(rec)
            if len(self.buf) >= self.batch:
                self.flush()

    def flush(self) -> None:
        if self.buf:
            recs, self.buf = self.buf, []
            self._take(self._call(recs), recs)

    def _call(self, arg):
        self.calls += 1
        try:
            return self.fn(arg, self.ctx) if self.with_ctx else self.fn(arg)
        except Exception as e:  # noqa: BLE001 - the job's code may raise anything
            raise UserCodeError(describe(e)) from None

    def _take(self, value, recs: list) -> None:
        r = self.reducer
        first = self.index
        self.index += len(recs)
        if r == "collect":
            if self.batch > 1:
                if not isinstance(value, list) or len(value) != len(recs):
                    raise OpError(f"with batches, the function must return a list with one value per record ({len(recs)})")
                self.values.extend(_json(v) for v in value)
            else:
                self.values.append(_json(value))
        elif r == "concat":
            for v in (value if isinstance(value, list) else [] if value is None else [value]):
                self.out += 1
                self.write(_json(v) if isinstance(v, dict) else {"value": _json(v)})
        elif r == "sum":
            if value is None:
                return
            num = to_number(value) if not isinstance(value, str) else None
            if num is None:
                raise OpError(f"the sum reducer needs numbers, but the function returned {type(value).__name__}")
            self.sum.add(num)
        elif r == "count":
            if value is True or (value and not isinstance(value, int)):
                self.n += 1
            elif isinstance(value, int) and not isinstance(value, bool):
                self.n += value
        elif r == "merge_counts":
            if value is None:
                return
            if not isinstance(value, dict):
                raise OpError("the merge_counts reducer needs a dict of name -> count")
            for k, v in value.items():
                if isinstance(v, bool) or not isinstance(v, int):
                    raise OpError(f"the count for {k!r} isn't a whole number")
                self.counts[str(k)] = self.counts.get(str(k), 0) + v
        elif r == "best":
            if value is None:
                return
            if not isinstance(value, dict) or to_number(value.get(self.best_key)) is None:
                raise OpError(f"the best reducer needs a dict with a number under {self.best_key!r}")
            value = _json(value)
            v = to_number(value[self.best_key])
            if self.best is None or (v > to_number(self.best[self.best_key]) if self.want_max else v < to_number(self.best[self.best_key])):
                self.best, self.best_at = value, first

    def state(self) -> dict:
        self.flush()
        st = {"calls": self.calls}
        r = self.reducer
        if r == "sum":
            st["sum"] = self.sum.to_state()
        elif r == "count":
            st["n"] = self.n
        elif r == "merge_counts":
            st["counts"] = self.counts
        elif r == "collect":
            st["values"] = self.values
        elif r == "best":
            st["best"], st["bestAt"] = self.best, self.best_at
        else:
            st["out"] = self.out
        return st


def _json(v):
    try:
        json.dumps(v, allow_nan=False)
    except (TypeError, ValueError):
        raise OpError(f"the function returned something JSON can't hold ({type(v).__name__}); return numbers, text, lists or dicts") from None
    return v
