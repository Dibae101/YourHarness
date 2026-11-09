"""Batch jobs from Python: one job over a dataset or a list of items, spread over the pool.

    from yourharness import jobs

    sales = jobs.dataset("sales.jsonl")                       # upload once; returns its reference
    jobs.aggregate(sales, sum="amount", mean="amount")        # {"count": ..., "sum_amount": ..., ...}
    jobs.group_by(sales, by="region", sum="amount")           # one row per region
    jobs.where("amount", ">", 100)                            # a filter, for any of these

    def score(p):
        return {**p, "score": train_and_evaluate(**p)}
    jobs.map_items(score, grid={"lr": [0.1, 0.01], "depth": [4, 8]}, reducer="best", best_by="score")

Unlike pool.map(), a batch job is saved on the coordinator: partitions that fail or
lose their computer are retried elsewhere, a coordinator restart doesn't lose it,
and the result is checked before it's reported. Everything here builds a JobSpec
(see docs/job-spec.md), so run(job) takes one you wrote by hand too.
"""

from __future__ import annotations

import inspect
import textwrap

from .client import Client, PoolError

__all__ = ["dataset", "aggregate", "group_by", "transform", "map_items", "map_records", "where", "run", "plan", "PoolError"]
API_VERSION = "yourharness.dev/v1alpha1"

_client: Client | None = None


def client() -> Client:
    global _client
    if _client is None:
        _client = Client()
    return _client


def dataset(path, name: str | None = None, format: str | None = None, shareable: bool = False) -> str:  # noqa: A002 - mirrors the field
    """Upload a file as a dataset. Returns its reference, like "dataset:d1a2b3c4"."""
    ds = client().add_dataset(path, name=name, fmt=format, classification="shareable" if shareable else None)
    return ds["ref"]


def where(field: str, op: str, value=None) -> dict:
    """A filter: where("amount", ">", 100), where("note", "contains", "late"), where("email", "exists")."""
    f = {"field": field, "op": op}
    if op not in ("exists", "missing"):
        f["value"] = value
    return f


def _metrics(count: bool, **fields) -> list[dict]:
    out = [{"op": "count"}] if count else []
    for op in ("sum", "mean", "min", "max"):
        names = fields.get(op)
        for f in ([names] if isinstance(names, str) else names or []):
            out.append({"op": op, "field": f})
    return out


def _job(workload: dict, inputs: dict, name: str | None, **spec) -> dict:
    return {"apiVersion": API_VERSION, "kind": "Job", "metadata": {"name": name or workload["type"]},
            "spec": {"workload": workload, "inputs": inputs, **{k: v for k, v in spec.items() if v is not None}}}


def _ref(ds) -> dict:
    return {"dataset": {"ref": ds if str(ds).startswith("dataset:") else f"dataset:{ds}"}}


def run(job: dict, wait: bool = True, quiet: bool = False):
    """Start a JobSpec and return its result (or, with wait=False, the started job)."""
    c = client()
    started = c.start_job(job)
    if not wait:
        return started
    done = c.wait_engine_job(started["id"], quiet=quiet)
    if done["status"] != "done":
        raise PoolError(done.get("error") or f"The job was {done['status']}.")
    if done.get("resultTruncated") or done.get("outputs"):
        return c.engine_result(done["id"])
    return done["result"]


def plan(job: dict) -> dict:
    """Where the job would run and why, without starting it."""
    return client().plan_job(job)


def aggregate(ds, *, count: bool = True, sum=None, mean=None, min=None, max=None, where=None, name=None, **spec):  # noqa: A002
    wl = {"type": "batch.aggregate", "metrics": _metrics(count, sum=sum, mean=mean, min=min, max=max), "where": _wheres(where)}
    return run(_job(wl, _ref(ds), name, **spec))["values"]


def group_by(ds, by, *, count: bool = True, sum=None, mean=None, min=None, max=None, where=None, name=None, **spec):  # noqa: A002
    wl = {"type": "batch.groupby", "by": [by] if isinstance(by, str) else list(by),
          "metrics": _metrics(count, sum=sum, mean=mean, min=min, max=max), "where": _wheres(where)}
    return run(_job(wl, _ref(ds), name, **spec))["groups"]


def transform(ds, *, where=None, select=None, rename=None, rolling=None, name=None, **spec) -> list[dict]:
    """Filter and reshape records; returns the records. rolling=("price", 20) adds a rolling mean."""
    wl = {"type": "batch.transform", "where": _wheres(where)}
    if select:
        wl["select"] = list(select)
    if rename:
        wl["rename"] = dict(rename)
    if rolling:
        wl["rolling"] = {"field": rolling[0], "window": int(rolling[1])}
    raw = run(_job(wl, _ref(ds), name, **spec))
    import json

    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]


def _wheres(w):
    if w is None:
        return []
    return [w] if isinstance(w, dict) else list(w)


def _source(fn) -> str:
    if not callable(fn) or getattr(fn, "__name__", "<lambda>") == "<lambda>":
        raise PoolError("Use a function defined with def, so its source can be sent to other computers.")
    try:
        lines = textwrap.dedent(inspect.getsource(fn)).splitlines()
    except (OSError, TypeError):
        raise PoolError("Can't read this function's source. Define it in a .py file or a notebook cell.") from None
    while lines and lines[0].lstrip().startswith("@"):
        lines.pop(0)
    return "\n".join(lines) + "\n"


def _python(fn, reducer: str, best_by: str | None, best: str, batch: int) -> dict:
    wl = {"type": "python.map", "code": _source(fn), "function": fn.__name__, "reducer": reducer, "batch": batch}
    if reducer == "best":
        if not best_by:
            raise PoolError('reducer="best" needs best_by, the key to compare.')
        wl["bestBy"] = {"key": best_by, "order": best}
    return wl


def map_items(fn, items=None, *, grid: dict | None = None, reducer: str = "collect", best_by: str | None = None, best: str = "max",
              batch: int = 1, name: str | None = None, **spec):
    """Call fn on every item (or every combination in grid) across your computers."""
    if (items is None) == (grid is None):
        raise PoolError("Pass items or grid, not both.")
    inputs = {"grid": grid} if grid is not None else {"items": list(items)}
    out = run(_job(_python(fn, reducer, best_by, best, batch), inputs, name or fn.__name__, **spec))
    return out.get("value", out) if isinstance(out, dict) else out


def map_records(fn, ds, *, reducer: str = "collect", best_by: str | None = None, best: str = "max", batch: int = 1,
                name: str | None = None, **spec):
    """Call fn on every record of a dataset across your computers."""
    out = run(_job(_python(fn, reducer, best_by, best, batch), _ref(ds), name or fn.__name__, **spec))
    return out.get("value", out) if isinstance(out, dict) else out
