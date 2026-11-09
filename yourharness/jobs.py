"""Batch jobs from Python: one job over a dataset or a list of items, spread over the pool.

    from yourharness import jobs

    sales = jobs.dataset("sales.jsonl")                       # upload once; returns its reference
    jobs.aggregate(sales, sum="amount", mean="amount")        # {"count": ..., "sum_amount": ..., ...}
    jobs.group_by(sales, by="region", sum="amount")           # one row per region
    jobs.where("amount", ">", 100)                            # a filter, for any of these
    jobs.join(sales, jobs.dataset("customers.csv"), on="customer_id", right_on="id")

    def score(p):
        return {**p, "score": train_and_evaluate(**p)}
    jobs.map_items(score, grid={"lr": [0.1, 0.01], "depth": [4, 8]}, reducer="best", best_by="score")

    def scale(x, ctx):                                        # ctx: a context snapshot, the same for every call
        return x * ctx["factor"]
    jobs.map_items(scale, range(100), context={"factor": 3})

    clf = jobs.model("model.py")                              # predict(model, batch), optionally load(folder)
    jobs.evaluate(clf, jobs.dataset("labeled.jsonl"), label="label", features=["x", "y"])

Unlike pool.map(), a batch job is saved on the coordinator: partitions that fail or
lose their computer are retried elsewhere, a coordinator restart doesn't lose it,
and the result is checked before it's reported. Everything here builds a JobSpec
(see docs/job-spec.md), so run(job) takes one you wrote by hand too.
"""

from __future__ import annotations

import inspect
import textwrap

from .client import Client, PoolError, setup_text

__all__ = ["dataset", "model", "aggregate", "group_by", "join", "transform", "map_items", "map_records", "evaluate", "where", "run", "plan",
           "PoolError", "SetupNeeded"]
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


def model(path, name: str | None = None, adapter: str | None = None) -> str:
    """Upload a model for evaluate(): model.py (defining predict(model, batch) and optionally load(folder)),
    a .zip with model.py at its top and the files it reads, or a TorchScript .pt (adapter="torchscript").
    Returns its reference, like "model:m1a2b3c4"."""
    return client().add_model(path, name=name, adapter=adapter)["ref"]


def evaluate(model_ref: str, ds, *, label: str = "label", features=None, id: str | None = None, batch: int = 64,  # noqa: A002
             predictions: bool = False, labels=None, device: str = "cpu", python: str | None = None, requirements=None,
             name: str | None = None, **spec) -> dict:
    """Score a model on a labeled dataset across your computers.

    Returns accuracy, per-class precision/recall/F1, the confusion matrix and where it
    came from (the model's and the data's SHA-256). With predictions=True the result
    also has one {"id", "label", "prediction", "correct"} record per sample (see
    run(..., wait=False) and the job's result download for large ones).
    """
    wl = {"type": "ml.evaluate", "label": label, "batch": batch, "predictions": predictions, "device": device}
    if features is not None:
        wl["features"] = [features] if isinstance(features, str) else list(features)
    if id is not None:
        wl["id"] = id
    if labels is not None:
        wl["labels"] = list(labels)
    ref = model_ref if str(model_ref).startswith("model:") else f"model:{model_ref}"
    spec, run_opts = _setup_args(_runtime(spec, python, requirements))
    quiet = bool(spec.pop("quiet", False))
    done = _wait(_start(_job(wl, {**_ref(ds), "model": {"ref": ref}}, name or "evaluate", **spec), **run_opts), quiet)
    result = dict(done["result"] or {})
    if predictions:  # the records are the job's download; the scores are its result
        import json

        raw = client().engine_result(done["id"])
        result["predictions"] = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    return result


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


class SetupNeeded(PoolError):
    """The job needs a Python environment prepared first; .job lists what each computer would run."""

    def __init__(self, job: dict):
        super().__init__(setup_text(job) + "\nOr pass approve_setup=True (and downloads=True) to run().")
        self.job = job


def run(job: dict, wait: bool = True, quiet: bool = False, approve_setup: bool = False, downloads: bool = False):
    """Start a JobSpec and return its result (or, with wait=False, the started job).

    A job with its own Python environment (runtime "managed-python") that no computer
    has yet stops for your approval: approve_setup=True gives it here, downloads=True
    also lets the computers download uv, that Python and the packages. Otherwise this
    raises SetupNeeded, which says exactly what would be installed where.
    """
    c = client()
    started = _start(job, approve_setup, downloads)
    if not wait:
        return started
    done = _wait(started, quiet)
    if done.get("resultTruncated") or done.get("outputs"):
        return c.engine_result(done["id"])
    return done["result"]


def _start(job: dict, approve_setup: bool = False, downloads: bool = False) -> dict:
    c = client()
    started = c.start_job(job)
    if started["status"] == "needs_approval":
        if not approve_setup:
            raise SetupNeeded(started)
        started = c.approve_setup(started["id"], downloads=downloads)
    return started


def _wait(started: dict, quiet: bool = False) -> dict:
    done = client().wait_engine_job(started["id"], quiet=quiet)
    if done["status"] == "needs_approval":
        raise SetupNeeded(done)
    if done["status"] != "done":
        raise PoolError(done.get("error") or f"The job was {done['status']}.")
    return done


def plan(job: dict) -> dict:
    """Where the job would run and why, without starting it."""
    return client().plan_job(job)


def aggregate(ds, *, count: bool = True, sum=None, mean=None, min=None, max=None, where=None, name=None, **spec):  # noqa: A002
    wl = {"type": "batch.aggregate", "metrics": _metrics(count, sum=sum, mean=mean, min=min, max=max), "where": _wheres(where)}
    return run(_job(wl, _ref(ds), name, **spec))["values"]


def group_by(ds, by, *, count: bool = True, sum=None, mean=None, min=None, max=None, where=None, name=None, shuffle=None, **spec):  # noqa: A002
    """One row per distinct value of `by`. shuffle=True (or a number of buckets) spreads the groups over your
    computers for very many of them (over about 200,000); the rows then come bucket by bucket."""
    wl = {"type": "batch.groupby", "by": [by] if isinstance(by, str) else list(by),
          "metrics": _metrics(count, sum=sum, mean=mean, min=min, max=max), "where": _wheres(where)}
    if shuffle:
        wl["shuffle"] = {} if shuffle is True else {"buckets": int(shuffle)}
        import json

        raw = run(_job(wl, _ref(ds), name, **spec))
        return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    return run(_job(wl, _ref(ds), name, **spec))["groups"]


def join(left, right, on, *, right_on=None, how: str = "inner", buckets: int | None = None, name=None, **spec) -> list[dict]:
    """Every record of `left` with each record of `right` whose key fields match (how="left" keeps unmatched left records).

    Keys that read as numbers match as numbers ("7" joins 7). A right field whose name the
    left record already has comes out as right_<name>. Both datasets are cut into
    partitions, sorted into buckets by key, and each bucket is joined on one computer.
    """
    wl = {"type": "batch.join", "on": [on] if isinstance(on, str) else list(on), "how": how}
    if right_on is not None:
        wl["rightOn"] = [right_on] if isinstance(right_on, str) else list(right_on)
    if buckets:
        wl["buckets"] = int(buckets)
    inputs = {**_ref(left), "right": _ref(right)["dataset"]}
    raw = run(_job(wl, inputs, name or "join", **spec))
    import json

    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]


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


def _runtime(spec: dict, python: str | None, requirements) -> dict:
    """python="3.12" and/or requirements=[...] run the function in an environment of its own (prepared with uv)."""
    if python is None and requirements is None:
        return spec
    if "runtime" in spec:
        raise PoolError("Pass python/requirements or runtime, not both.")
    import platform

    return {**spec, "runtime": {"kind": "managed-python", "python": python or ".".join(platform.python_version_tuple()[:2]),
                                "requirements": list(requirements or [])}}


def _setup_args(spec: dict) -> tuple[dict, dict]:
    run_opts = {k: spec.pop(k) for k in ("approve_setup", "downloads") if k in spec}
    if "context" in spec and isinstance(spec["context"], dict) and "values" not in spec["context"]:
        spec["context"] = {"values": spec["context"]}  # context={...}: the values every call gets as ctx
    return spec, run_opts


def map_items(fn, items=None, *, grid: dict | None = None, reducer: str = "collect", best_by: str | None = None, best: str = "max",
              batch: int = 1, name: str | None = None, python: str | None = None, requirements=None, **spec):
    """Call fn on every item (or every combination in grid) across your computers.

    python="3.12", requirements=["scikit-learn==1.5.2"] run it in an environment of its
    own on each computer; approve_setup=True (and downloads=True) approve preparing it.
    """
    if (items is None) == (grid is None):
        raise PoolError("Pass items or grid, not both.")
    inputs = {"grid": grid} if grid is not None else {"items": list(items)}
    spec, run_opts = _setup_args(_runtime(spec, python, requirements))
    out = run(_job(_python(fn, reducer, best_by, best, batch), inputs, name or fn.__name__, **spec), **run_opts)
    return out.get("value", out) if isinstance(out, dict) else out


def map_records(fn, ds, *, reducer: str = "collect", best_by: str | None = None, best: str = "max", batch: int = 1,
                name: str | None = None, python: str | None = None, requirements=None, **spec):
    """Call fn on every record of a dataset across your computers (python/requirements as in map_items)."""
    spec, run_opts = _setup_args(_runtime(spec, python, requirements))
    out = run(_job(_python(fn, reducer, best_by, best, batch), _ref(ds), name or fn.__name__, **spec), **run_opts)
    return out.get("value", out) if isinstance(out, dict) else out
