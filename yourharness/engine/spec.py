"""The job description: JobSpec, version yourharness.dev/v1alpha1.

A job says what to compute (the workload), over which input, under which policy,
and how the partial results combine. parse() checks a submitted job and returns it
normalized, with every default filled in, or raises SpecError naming the field
that's wrong. Nothing here touches the network or the disk.

    {
      "apiVersion": "yourharness.dev/v1alpha1",
      "kind": "Job",
      "metadata": {"name": "Sales totals"},
      "spec": {
        "mode": "automatic",
        "workload": {"type": "batch.aggregate", "metrics": [{"op": "sum", "field": "amount"}]},
        "inputs": {"dataset": {"ref": "dataset:d1a2b3c4"}}
      }
    }

A job always runs through the same engine. mode only decides where: "local" keeps
every partition on one computer, "distributed" spreads them, "automatic" picks
whichever is expected to finish sooner.
"""

from __future__ import annotations

import json
import math
import re

API_VERSION = "yourharness.dev/v1alpha1"
MODES = ("automatic", "local", "distributed")
TRUST = ("owned-only", "owned-and-friends")
METRIC_OPS = ("count", "sum", "mean", "min", "max")
FILTER_OPS = ("==", "!=", "<", "<=", ">", ">=", "contains", "regex", "in", "exists", "missing")
PY_REDUCERS = ("concat", "sum", "count", "merge_counts", "collect", "best")
MAX_CODE = 200_000
MAX_ITEMS = 100_000
MAX_SPEC = 2 * 1024 * 1024
# A requirement starts with a package name, so a line can't be a pip option (--index-url, -r, -e).
REQUIREMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-\[\],<>=!~ ;:'\"*+/@]{0,199}")
# A container image named by its digest, so every computer runs exactly the same one (never a moving tag like latest).
OCI_IMAGE = re.compile(r"[a-z0-9][a-z0-9._\-/:]{0,250}@sha256:[0-9a-f]{64}")
FIELD_RE = re.compile(r"[^\x00-\x1f]{1,200}")
NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,99}")

# Workload types that exist, and what each needs.
WORKLOADS = {
    "batch.aggregate": {"input": "dataset", "runtimes": ("builtin",)},
    "batch.groupby": {"input": "dataset", "runtimes": ("builtin",)},
    "batch.transform": {"input": "dataset", "runtimes": ("builtin",)},
    "python.map": {"input": "dataset|items", "runtimes": ("native-python", "managed-python", "isolated-oci")},
    "ml.evaluate": {"input": "dataset", "runtimes": ("native-python", "managed-python", "isolated-oci")},
    "batch.join": {"input": "dataset", "runtimes": ("builtin",)},
}
CODE_WORKLOADS = ("python.map", "ml.evaluate")  # they run your own code: only on your own computers
MAX_LABELS = 10_000
MAX_STAGES = 8
MAX_BUCKETS = 1024
MAX_CONTEXT = 1024 * 1024
STAGE_NAME = re.compile(r"[a-z][a-z0-9-]{0,39}")


def context_version(values: dict) -> str:
    """The digest that names a context snapshot: the same values always give the same version."""
    import hashlib

    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def writes_records(wl: dict) -> bool:
    t = wl["type"]
    return t in ("batch.transform", "batch.join") or (t == "python.map" and wl.get("reducer") == "concat") \
        or (t == "ml.evaluate" and bool(wl.get("predictions"))) or (t == "batch.groupby" and wl.get("shuffle") is not None)


def _buckets(v, path: str):
    if v is None:
        return None
    return _num(v, path, 1, MAX_BUCKETS, None, integer=True)


class SpecError(ValueError):
    def __init__(self, path: str, message: str):
        super().__init__(f"{path}: {message}")
        self.path = path


def _obj(v, path: str) -> dict:
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise SpecError(path, "should be an object")
    return v


def _num(v, path: str, lo: float, hi: float, default, integer: bool = False):
    if v is None:
        return default
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise SpecError(path, "should be a number")
    if v < lo or v > hi:
        raise SpecError(path, f"should be between {lo:g} and {hi:g}")
    return int(v) if integer else v


def _choice(v, path: str, options, default):
    if v is None:
        return default
    if v not in options:
        raise SpecError(path, "should be one of " + ", ".join(f'"{o}"' for o in options))
    return v


def _field(v, path: str) -> str:
    if not isinstance(v, str) or not FIELD_RE.fullmatch(v) or v != v.strip():
        raise SpecError(path, "should be a field name (dots reach into nested objects)")
    return v


def _fields(v, path: str, at_least: int = 1) -> list[str]:
    if isinstance(v, str):
        v = [v]
    if not isinstance(v, list) or len(v) < at_least or len(v) > 50:
        raise SpecError(path, f"should be a list of {at_least} to 50 field names")
    return [_field(x, f"{path}[{i}]") for i, x in enumerate(v)]


def _where(v, path: str) -> list[dict]:
    if v is None:
        return []
    if isinstance(v, dict):
        v = [v]
    if not isinstance(v, list) or len(v) > 50:
        raise SpecError(path, "should be a list of filters")
    out = []
    for i, p in enumerate(v):
        pp = f"{path}[{i}]"
        p = _obj(p, pp)
        op = _choice(p.get("op"), f"{pp}.op", FILTER_OPS, None)
        if op is None:
            raise SpecError(f"{pp}.op", "is required")
        f = {"field": _field(p.get("field"), f"{pp}.field"), "op": op}
        if op not in ("exists", "missing"):
            if "value" not in p:
                raise SpecError(f"{pp}.value", "is required")
            val = p["value"]
            if op in ("<", "<=", ">", ">=") and (isinstance(val, bool) or not isinstance(val, (int, float))):
                raise SpecError(f"{pp}.value", "should be a number")
            if op == "in" and (not isinstance(val, list) or len(val) > 10_000):
                raise SpecError(f"{pp}.value", "should be a list")
            if op == "regex":
                if not isinstance(val, str) or len(val) > 500:
                    raise SpecError(f"{pp}.value", "should be a regular expression of at most 500 characters")
                try:
                    re.compile(val)
                except re.error as e:
                    raise SpecError(f"{pp}.value", f"isn't a valid regular expression ({e})") from None
            if op == "contains" and not isinstance(val, str):
                raise SpecError(f"{pp}.value", "should be text")
            f["value"] = val
        out.append(f)
    return out


def _metrics(v, path: str) -> list[dict]:
    if not isinstance(v, list) or not v or len(v) > 50:
        raise SpecError(path, "should be a list of 1 to 50 metrics, like {\"op\": \"sum\", \"field\": \"amount\"}")
    out, names = [], set()
    for i, m in enumerate(v):
        mp = f"{path}[{i}]"
        m = _obj(m, mp)
        op = _choice(m.get("op"), f"{mp}.op", METRIC_OPS, None)
        if op is None:
            raise SpecError(f"{mp}.op", "is required")
        field = m.get("field")
        if op != "count" or field is not None:
            field = _field(field, f"{mp}.field")
        name = m.get("as") or (op if field is None else f"{op}_{field.replace('.', '_')}")
        if not isinstance(name, str) or not FIELD_RE.fullmatch(name):
            raise SpecError(f"{mp}.as", "should be a name for the result")
        if name in names:
            raise SpecError(f"{mp}.as", f"two metrics are both called {name}; give one another name with \"as\"")
        names.add(name)
        out.append({"op": op, "field": field, "as": name})
    return out


def _workload(w, path: str) -> dict:
    w = _obj(w, path)
    t = w.get("type")
    if t not in WORKLOADS:
        raise SpecError(f"{path}.type", "should be one of " + ", ".join(f'"{k}"' for k in WORKLOADS))
    if t == "batch.aggregate":
        return {"type": t, "metrics": _metrics(w.get("metrics", [{"op": "count"}]), f"{path}.metrics"),
                "where": _where(w.get("where"), f"{path}.where")}
    if t == "batch.groupby":
        out = {"type": t, "by": _fields(w.get("by"), f"{path}.by"),
               "metrics": _metrics(w.get("metrics", [{"op": "count"}]), f"{path}.metrics"),
               "where": _where(w.get("where"), f"{path}.where")}
        sh = w.get("shuffle")
        if sh is True:
            sh = {}
        if sh is not None and sh is not False:  # keys spread over buckets, each combined by one computer: for many groups
            sh = _obj(sh, f"{path}.shuffle")
            out["shuffle"] = {"buckets": _buckets(sh.get("buckets"), f"{path}.shuffle.buckets")}
        return out
    if t == "batch.join":
        on = _fields(w.get("on"), f"{path}.on")
        right_on = _fields(w["rightOn"], f"{path}.rightOn") if w.get("rightOn") is not None else on
        if len(right_on) != len(on):
            raise SpecError(f"{path}.rightOn", "should name as many fields as on")
        return {"type": t, "on": on, "rightOn": right_on, "how": _choice(w.get("how"), f"{path}.how", ("inner", "left"), "inner"),
                "buckets": _buckets(w.get("buckets"), f"{path}.buckets")}
    if t == "batch.transform":
        out = {"type": t, "where": _where(w.get("where"), f"{path}.where"), "select": None, "rename": {}, "rolling": None}
        if w.get("select") is not None:
            out["select"] = _fields(w["select"], f"{path}.select")
        if w.get("rename") is not None:
            ren = _obj(w["rename"], f"{path}.rename")
            out["rename"] = {_field(k, f"{path}.rename"): _field(v, f"{path}.rename.{k}") for k, v in ren.items()}
        if w.get("rolling") is not None:
            r = _obj(w["rolling"], f"{path}.rolling")
            field = _field(r.get("field"), f"{path}.rolling.field")
            out["rolling"] = {"field": field, "window": _num(r.get("window"), f"{path}.rolling.window", 1, 10_000, 10, integer=True),
                              "as": _field(r.get("as") or f"rolling_mean_{field.replace('.', '_')}", f"{path}.rolling.as")}
        return out
    if t == "python.map":
        code = w.get("code")
        if not isinstance(code, str) or not code.strip():
            raise SpecError(f"{path}.code", "should be Python source that defines the function")
        if len(code) > MAX_CODE:
            raise SpecError(f"{path}.code", f"is over {MAX_CODE // 1000} KB; put large inputs in a dataset instead")
        fn = w.get("function", "process")
        if not isinstance(fn, str) or not NAME_RE.fullmatch(fn):
            raise SpecError(f"{path}.function", "should be the name of a function defined with def")
        reducer = _choice(w.get("reducer"), f"{path}.reducer", PY_REDUCERS, "collect")
        out = {"type": t, "code": code, "function": fn, "reducer": reducer,
               "batch": _num(w.get("batch"), f"{path}.batch", 1, 100_000, 1, integer=True),
               "sideEffects": _choice(w.get("sideEffects"), f"{path}.sideEffects", ("none", "external"), "none")}
        if reducer == "best":
            b = _obj(w.get("bestBy"), f"{path}.bestBy")
            out["bestBy"] = {"key": _field(b.get("key"), f"{path}.bestBy.key"),
                             "order": _choice(b.get("order"), f"{path}.bestBy.order", ("max", "min"), "max")}
        return out
    if t == "ml.evaluate":
        predictions = w.get("predictions", False)
        if not isinstance(predictions, bool):
            raise SpecError(f"{path}.predictions", "should be true or false")
        labels = w.get("labels")
        if labels is not None and (not isinstance(labels, list) or not labels or len(labels) > MAX_LABELS
                                   or any(isinstance(x, (dict, list)) or x is None for x in labels)):
            raise SpecError(f"{path}.labels", f"should be a list of 1 to {MAX_LABELS:,} labels (text or numbers), in the model's output order")
        return {"type": t, "label": _field(w.get("label", "label"), f"{path}.label"),
                "features": _fields(w["features"], f"{path}.features") if w.get("features") is not None else None,
                "id": _field(w["id"], f"{path}.id") if w.get("id") is not None else None,
                "batch": _num(w.get("batch"), f"{path}.batch", 1, 100_000, 64, integer=True),
                "predictions": predictions, "labels": labels,
                "device": _choice(w.get("device"), f"{path}.device", ("cpu", "auto"), "cpu")}
    raise SpecError(f"{path}.type", "isn't built yet")  # pragma: no cover


def _inputs(v, path: str, wl: dict, wants_model: bool = False) -> dict:
    v = _obj(v, path)
    need = WORKLOADS[wl["type"]]["input"]
    model = None
    if wants_model or wl["type"] == "ml.evaluate":
        m = _obj(v.get("model"), f"{path}.model")
        ref = m.get("ref")
        if not isinstance(ref, str) or not re.fullmatch(r"model:m[0-9a-f]{8}", ref):
            raise SpecError(f"{path}.model.ref", 'should be an uploaded model, like "model:m1a2b3c4"')
        sha = m.get("sha256")
        if sha is not None and (not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha)):
            raise SpecError(f"{path}.model.sha256", "should be the model file's SHA-256, 64 hex digits")
        model = {"ref": ref, "sha256": sha}
    elif "model" in v:
        raise SpecError(f"{path}.model", "only ml.evaluate takes a model")
    right = None
    if wl["type"] == "batch.join":
        r = _obj(v.get("right"), f"{path}.right")
        ref = r.get("ref")
        if not isinstance(ref, str) or not re.fullmatch(r"dataset:d[0-9a-f]{8}", ref):
            raise SpecError(f"{path}.right.ref", 'should be the dataset to join with, like "dataset:d1a2b3c4"')
        right = {"ref": ref}
    elif "right" in v:
        raise SpecError(f"{path}.right", "only batch.join takes a second dataset")
    if "dataset" in v:
        d = _obj(v["dataset"], f"{path}.dataset")
        ref = d.get("ref")
        if not isinstance(ref, str) or not re.fullmatch(r"dataset:d[0-9a-f]{8}", ref):
            raise SpecError(f"{path}.dataset.ref", 'should be a registered dataset, like "dataset:d1a2b3c4"')
        return {"dataset": {"ref": ref}, **({"model": model} if model else {}), **({"right": right} if right else {})}
    if "dataset" not in need:  # pragma: no cover - every workload takes a dataset today
        raise SpecError(path, "needs a dataset")
    if "items" in need and ("items" in v or "grid" in v):
        if "items" in v:
            items = v["items"]
            if not isinstance(items, list) or not items or len(items) > MAX_ITEMS:
                raise SpecError(f"{path}.items", f"should be a list of 1 to {MAX_ITEMS:,} items")
        else:
            grid = _obj(v["grid"], f"{path}.grid")
            if not grid or any(not isinstance(vals, list) or not vals for vals in grid.values()):
                raise SpecError(f"{path}.grid", "should map each parameter name to a list of values")
            items = [{}]
            for name, vals in grid.items():
                items = [{**it, name: val} for it in items for val in vals]
                if len(items) > MAX_ITEMS:
                    raise SpecError(f"{path}.grid", f"makes more than {MAX_ITEMS:,} combinations")
        try:
            json.dumps(items, allow_nan=False)
        except (TypeError, ValueError):
            raise SpecError(f"{path}.items", "should hold only JSON values (numbers, text, lists, objects)") from None
        return {"items": items}
    raise SpecError(path, "needs a dataset" + (", or items or a grid of parameters" if "items" in need else ""))


def parse(doc) -> dict:
    """Check a job and return it with every default filled in."""
    if isinstance(doc, (str, bytes)):
        if len(doc) > MAX_SPEC:
            raise SpecError("job", "is over 2 MB")
        doc = loads(doc)
    doc = _obj(doc, "job")
    if doc.get("apiVersion", API_VERSION) != API_VERSION:
        raise SpecError("apiVersion", f'should be "{API_VERSION}"')
    if doc.get("kind", "Job") != "Job":
        raise SpecError("kind", 'should be "Job"')
    meta = _obj(doc.get("metadata"), "metadata")
    name = meta.get("name") or "Job"
    if not isinstance(name, str) or len(name) > 120:
        raise SpecError("metadata.name", "should be text of at most 120 characters")
    labels = _obj(meta.get("labels"), "metadata.labels")
    s = _obj(doc.get("spec"), "spec")
    stages = _stages(s)
    wl = stages[-1]["workload"]  # the last stage's workload is the job's result
    reading = [st["workload"] for st in stages if st["from"] == "inputs"]
    inputs = _inputs(s.get("inputs"), "spec.inputs", reading[0], any(st["workload"]["type"] == "ml.evaluate" for st in stages))
    for i, st in enumerate(stages):
        if st["from"] == "inputs" and "items" in inputs and "items" not in WORKLOADS[st["workload"]["type"]]["input"]:
            raise SpecError(f"spec.stages[{i}].workload", f"{st['workload']['type']} reads a dataset, and this job's inputs are items")
    if any(st["workload"]["type"] == "ml.evaluate" for st in stages) and "model" not in inputs:
        raise SpecError("spec.inputs.model.ref", 'should be an uploaded model, like "model:m1a2b3c4"')
    code = any(st["workload"]["type"] in CODE_WORKLOADS for st in stages)
    rt = _obj(s.get("runtime"), "spec.runtime")
    allowed_rt = ("native-python", "managed-python", "isolated-oci") if code else ("builtin",)
    runtime = {"kind": _choice(rt.get("kind"), "spec.runtime.kind", allowed_rt, allowed_rt[0])}
    iso = rt.get("isolation")
    if iso is not None and iso not in ("owned-trusted", "isolated"):
        raise SpecError("spec.runtime.isolation", 'should be "owned-trusted" or "isolated"')
    if iso == "isolated" and runtime["kind"] != "isolated-oci":
        raise SpecError("spec.runtime.isolation", f"can't be isolated with {runtime['kind']}: native and managed Python aren't sandboxes, "
                                                  "use the isolated-oci runtime (a container)")
    if runtime["kind"] == "isolated-oci":
        image = rt.get("image")
        if not isinstance(image, str) or not OCI_IMAGE.fullmatch(image):
            raise SpecError("spec.runtime.image", 'should be a container image pinned by its digest, like "python:3.12-slim@sha256:<64 hex digits>" '
                                                  "(a tag alone, such as latest, can change under you)")
        if rt.get("network", "none") != "none":
            raise SpecError("spec.runtime.network", 'can only be "none": the container gets no network; the computer fetches its input '
                                                    "and sends its output")
        lim = _obj(rt.get("limits"), "spec.runtime.limits")
        runtime.update(image=image, network="none", limits={
            "cpus": float(_num(lim.get("cpus"), "spec.runtime.limits.cpus", 0.25, 64, 1)),
            "memoryMB": _num(lim.get("memoryMB"), "spec.runtime.limits.memoryMB", 128, 262_144, 2048, integer=True),
            "pids": _num(lim.get("pids"), "spec.runtime.limits.pids", 32, 4096, 256, integer=True),
        })
    if runtime["kind"] == "managed-python":
        runtime["python"] = rt.get("python") or "3.12"
        if not isinstance(runtime["python"], str) or not re.fullmatch(r"3\.\d{1,2}", runtime["python"]):
            raise SpecError("spec.runtime.python", 'should be a Python version like "3.12"')
        reqs = rt.get("requirements") or []
        if not isinstance(reqs, list) or len(reqs) > 200 or any(not isinstance(r, str) or not REQUIREMENT.fullmatch(r.strip()) for r in reqs):
            raise SpecError("spec.runtime.requirements", "should be a list of package requirements, like \"numpy==2.1.3\" "
                            "(a package name first: no pip options such as --index-url)")
        runtime["requirements"] = sorted(r.strip() for r in reqs)
    part = _obj(s.get("partitioning"), "spec.partitioning")
    partitioning = {
        "targetBytes": _num(part.get("targetBytes"), "spec.partitioning.targetBytes", 1024, 2 * 2**30, None, integer=True),
        "targetDurationSeconds": _num(part.get("targetDurationSeconds"), "spec.partitioning.targetDurationSeconds", 1, 3600, 30),
        "maxPartitions": _num(part.get("maxPartitions"), "spec.partitioning.maxPartitions", 1, 100_000, 5000, integer=True),
        "maxInflightPerWorker": _num(part.get("maxInflightPerWorker"), "spec.partitioning.maxInflightPerWorker", 1, 64, 4, integer=True),
        "itemsPerPartition": _num(part.get("itemsPerPartition"), "spec.partitioning.itemsPerPartition", 1, MAX_ITEMS, None, integer=True),
    }
    res = _obj(s.get("resources"), "spec.resources")
    resources = {
        "cpuSlots": _num(res.get("cpuSlots"), "spec.resources.cpuSlots", 1, 64, 1, integer=True),
        "minFreeMemoryGiB": _num(res.get("minFreeMemoryGiB"), "spec.resources.minFreeMemoryGiB", 0, 4096, 0.25),
    }
    sec = _obj(s.get("security"), "spec.security")
    if sec.get("allowUntrustedNativeFallback"):
        raise SpecError("spec.security.allowUntrustedNativeFallback", "can't be turned on: code never falls back to running unisolated on someone else's computer")
    security = {"allowedTrust": _choice(sec.get("allowedTrust"), "spec.security.allowedTrust", TRUST, "owned-only")}
    if security["allowedTrust"] != "owned-only" and code:
        raise SpecError("spec.security.allowedTrust", "your own code runs only on your own computers: a friend's computer runs only the "
                                                      "built-in operators, even when the job uses a container")
    retry = _obj(s.get("retry"), "spec.retry")
    speculate = retry.get("speculate", True)
    if not isinstance(speculate, bool):
        raise SpecError("spec.retry.speculate", "should be true or false")
    retry = {
        "maxAttempts": _num(retry.get("maxAttempts"), "spec.retry.maxAttempts", 1, 20, 3, integer=True),
        "timeoutSeconds": _num(retry.get("timeoutSeconds"), "spec.retry.timeoutSeconds", 5, 86_400, 900, integer=True),
        "checkpointIntervalSeconds": _num(retry.get("checkpointIntervalSeconds"), "spec.retry.checkpointIntervalSeconds", 1, 3600, 10),
        "leaseSeconds": _num(retry.get("leaseSeconds"), "spec.retry.leaseSeconds", 5, 3600, 45),
        "speculate": speculate,  # a slow partition of a side-effect-free job may also run on a free computer; the first result wins
    }
    ctx = _obj(s.get("context"), "spec.context")
    context = None
    if ctx:
        values = _obj(ctx.get("values"), "spec.context.values")
        try:
            size = len(json.dumps(values, allow_nan=False))
        except (TypeError, ValueError):
            raise SpecError("spec.context.values", "should hold only JSON values") from None
        if size > MAX_CONTEXT:
            raise SpecError("spec.context.values", "is over 1 MB; put large reference data in a dataset")
        version = context_version(values)
        if ctx.get("version") is not None and ctx["version"] != version:
            raise SpecError("spec.context.version", "doesn't match the values: a context version names exactly one set of values")
        context = {"values": values, "version": version}
    out = _obj(s.get("output"), "spec.output")
    output = {
        "onBadRecord": _choice(out.get("onBadRecord"), "spec.output.onBadRecord", ("fail", "skip"), "fail"),
        "retentionDays": _num(out.get("retentionDays"), "spec.output.retentionDays", 0, 3650, 7),
        "spotCheck": _num(out.get("spotCheck"), "spec.output.spotCheck", 0, 1000, 0, integer=True),
    }
    return {
        "apiVersion": API_VERSION, "kind": "Job",
        "metadata": {"name": name.strip() or "Job", "labels": {str(k)[:60]: str(v)[:200] for k, v in list(labels.items())[:20]}},
        "spec": {"mode": _choice(s.get("mode"), "spec.mode", MODES, "automatic"), "workload": wl, "inputs": inputs,
                 "runtime": runtime, "partitioning": partitioning, "resources": resources, "security": security,
                 "retry": retry, "output": output, "context": context, "stages": stages if len(stages) > 1 else None},
    }


def _stages(s: dict) -> list[dict]:
    """The job's stages in order. One workload is one stage; spec.stages lists several, each starting
    after the one before it finished (a barrier), with the earlier stages' results in its context."""
    if s.get("stages") is None:
        return [{"name": "main", "workload": _workload(s.get("workload"), "spec.workload"), "from": "inputs"}]
    if s.get("workload") is not None:
        raise SpecError("spec.workload", "give either workload or stages, not both")
    raw = s["stages"]
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_STAGES:
        raise SpecError("spec.stages", f"should be a list of 1 to {MAX_STAGES} stages")
    out, names = [], []
    for i, st in enumerate(raw):
        p = f"spec.stages[{i}]"
        st = _obj(st, p)
        name = st.get("name", f"stage-{i + 1}")
        if not isinstance(name, str) or not STAGE_NAME.fullmatch(name) or name in names:
            raise SpecError(f"{p}.name", "should be a unique name of lowercase letters, digits and dashes")
        wl = _workload(st.get("workload"), f"{p}.workload")
        if wl["type"] == "batch.join" or wl.get("shuffle") is not None:
            raise SpecError(f"{p}.workload", "a join or a shuffled group-by runs as a job of its own, not as one of several stages")
        src = st.get("from", "inputs")
        if src != "inputs":
            if src not in names:
                raise SpecError(f"{p}.from", 'should be "inputs" or the name of an earlier stage')
            if not writes_records(out[names.index(src)]["workload"]):
                raise SpecError(f"{p}.from", f"stage {src} doesn't write records for a later stage to read")
            if wl["type"] == "batch.transform" and wl.get("rolling"):
                raise SpecError(f"{p}.workload.rolling", "a rolling mean reads only the job's inputs, not an earlier stage's records")
        names.append(name)
        out.append({"name": name, "workload": wl, "from": src})
    return out


def loads(text) -> dict:
    """JSON, or YAML when PyYAML is installed (pip install pyyaml)."""
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    try:
        return json.loads(text)
    except ValueError as json_error:
        try:
            import yaml  # optional
        except ImportError:
            raise SpecError("job", f"isn't valid JSON ({json_error}); YAML needs PyYAML installed") from None
        try:
            return yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise SpecError("job", f"isn't valid JSON or YAML ({e})") from None
