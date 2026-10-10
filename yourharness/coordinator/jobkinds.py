"""Job types. The coordinator plans and combines; devices compute.

Each type says which runtimes can compute it: ``js`` (a browser tab) and
``python`` (an agent). Built-in types run on both.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

BOTH = frozenset({"js", "python"})
MAX_CODE = 100_000
MAX_ITEMS_BYTES = 8 * 1024 * 1024


class UserError(Exception):
    """A problem the person can fix; the message is shown to them as is."""


def clamp_int(v, lo, hi, default):
    try:
        n = math.floor(float(v))
    except (TypeError, ValueError, OverflowError):
        return default
    return min(hi, max(lo, n))


def clamp_num(v, lo, hi, default):
    try:
        n = float(v)
    except (TypeError, ValueError):
        return default
    return min(hi, max(lo, n)) if math.isfinite(n) else default


def is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def fmt_int(n) -> str:
    return f"{int(n):,}"


def sanitize_name(name) -> str | None:
    clean = re.sub(r"[\x00-\x1f\x7f]", "", str(name)).replace("/", "_").replace("\\", "_").strip()
    if not clean or clean.startswith(".") or len(clean) > 200:
        return None
    return clean


@dataclass
class Plan:
    title: str
    total: int
    params: dict
    runtimes: frozenset = BOTH
    data: dict = field(default_factory=dict)  # kept on the coordinator, never broadcast


@dataclass
class Kind:
    plan: Callable[[dict, "PlanContext"], Plan]
    check: Callable[[object, int, object], bool]
    reduce: Callable[[object], dict]
    piece: Callable[[object, int], dict | None] = lambda job, idx: None


@dataclass
class PlanContext:
    shared_dir: Path


# --- Fractal
def _plan_mandelbrot(p, ctx):
    width = clamp_int(p.get("width"), 64, 4096, 1600)
    height = clamp_int(p.get("height"), 64, 4096, 1000)
    max_iter = clamp_int(p.get("maxIter"), 50, 60000, 3000)
    rows = clamp_int(p.get("rows"), 1, 64, 8)
    params = {
        "width": width, "height": height, "maxIter": max_iter, "rows": rows,
        "cx": clamp_num(p.get("cx"), -3, 3, -0.743643887037151),
        "cy": clamp_num(p.get("cy"), -3, 3, 0.13182590420533),
        "span": clamp_num(p.get("span"), 1e-13, 6, 0.006),
    }
    return Plan(f"Fractal, {width} × {height} at {fmt_int(max_iter)} iterations", math.ceil(height / rows), params)


def _check_mandelbrot(job, idx, v):
    if not isinstance(v, str):
        return False
    p = job.params
    strip_rows = min(p["rows"], p["height"] - idx * p["rows"])
    return len(v) == 4 * math.ceil(strip_rows * p["width"] * 2 / 3)


# --- Primes
def _plan_primes(p, ctx):
    limit = clamp_int(p.get("limit"), 1000, 10**11, 5 * 10**9)
    size = clamp_int(p.get("chunkSize"), 10**5, 10**8, 10**7)
    total = math.ceil(limit / size)
    if total > 20000:
        raise UserError("That range splits into too many pieces. Use a larger piece size.")
    return Plan(f"Count primes below {fmt_int(limit)}", total, {"limit": limit, "chunkSize": size})


# --- π
def _plan_pi(p, ctx):
    chunks = clamp_int(p.get("chunks"), 1, 20000, 400)
    samples = clamp_int(p.get("samples"), 10**4, 10**9, 2 * 10**7)
    return Plan(f"Estimate π from {fmt_int(chunks * samples)} random points", chunks, {"chunks": chunks, "samples": samples})


def _reduce_pi(job):
    n = job.params["chunks"] * job.params["samples"]
    return {"estimate": 4 * sum(job.results) / n, "samples": n}


# --- Search a shared file
def _plan_grep(p, ctx):
    name = sanitize_name(p.get("file", ""))
    if not name:
        raise UserError("Pick a shared file to search.")
    term = str(p.get("term") or "")
    if not term or len(term) > 200:
        raise UserError("Enter the text to find, up to 200 characters.")
    path = ctx.shared_dir / name
    if not path.is_file():
        raise UserError(f"There is no shared file named {name}.")
    size = path.stat().st_size
    if size == 0:
        raise UserError(f"{name} is empty, so there is nothing to search.")
    chunk = clamp_int(p.get("chunkMB"), 1, 256, 4) * 1024 * 1024
    params = {"file": name, "term": term, "ignoreCase": bool(p.get("ignoreCase")), "chunkBytes": chunk, "size": size}
    return Plan(f"Search {name} for “{term}”", math.ceil(size / chunk), params)


# --- Your own code, in JavaScript (browsers) or Python (agents)
def _plan_custom(p, ctx):
    code = str(p.get("code") or "")
    lang = "python" if p.get("lang") == "python" else "js"
    if not code.strip():
        raise UserError("Write a run(chunk, total) function first.")
    if len(code) > MAX_CODE:
        raise UserError("The code is over 100 KB. Load big inputs from a shared file instead.")
    chunks = clamp_int(p.get("chunks"), 1, 20000, 200)
    combine = p.get("combine") if p.get("combine") in ("sum", "max", "min", "list") else "sum"
    label = "Python" if lang == "python" else "JavaScript"
    return Plan(
        f"Your {label} code, split into {fmt_int(chunks)} pieces", chunks,
        {"code": code, "chunks": chunks, "combine": combine, "lang": lang}, runtimes=frozenset({lang}),
    )


def _reduce_custom(job):
    combine = job.params["combine"]
    if combine == "list":
        return {"value": job.results}
    if not all(is_num(v) for v in job.results):
        raise UserError('Every piece has to return a number to combine them that way. Choose "Keep as a list" to see raw results.')
    if combine == "sum":
        return {"value": sum(job.results)}
    return {"value": max(job.results) if combine == "max" else min(job.results)}


# --- pool.map(fn, items) from the workspace SDK
def _plan_pymap(p, ctx):
    code = str(p.get("code") or "")
    fn = str(p.get("fn") or "")
    items = p.get("items")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", fn):
        raise UserError("pool.map needs the name of a function defined with def.")
    if not code.strip() or len(code) > MAX_CODE:
        raise UserError("The function's source is missing or over 100 KB.")
    if not isinstance(items, list) or not items:
        raise UserError("pool.map needs a non-empty list of items.")
    if len(json.dumps(items)) > MAX_ITEMS_BYTES:
        raise UserError("The items are over 8 MB. Put large inputs in a shared file and pass file names instead.")
    per = clamp_int(p.get("perPiece"), 1, 100_000, 1)
    total = math.ceil(len(items) / per)
    if total > 20000:
        raise UserError("That splits into too many pieces. Pass a larger per_piece.")
    return Plan(
        f"pool.map({fn}) over {fmt_int(len(items))} items", total,
        {"code": code, "fn": fn, "perPiece": per, "count": len(items)}, runtimes=frozenset({"python"}), data={"items": items},
    )


def _piece_pymap(job, idx):
    per = job.params["perPiece"]
    return {"items": job.data["items"][idx * per:(idx + 1) * per]}


def _check_pymap(job, idx, v):
    per = job.params["perPiece"]
    return isinstance(v, list) and len(v) == len(job.data["items"][idx * per:(idx + 1) * per])


def _num_check(job, idx, v):
    return is_num(v)


KINDS: dict[str, Kind] = {
    "mandelbrot": Kind(_plan_mandelbrot, _check_mandelbrot, lambda job: {"pixels": job.params["width"] * job.params["height"]}),
    "primes": Kind(_plan_primes, _num_check, lambda job: {"count": int(sum(job.results))}),
    "pi": Kind(_plan_pi, _num_check, _reduce_pi),
    "grep": Kind(_plan_grep, _num_check, lambda job: {"count": int(sum(job.results))}),
    "custom": Kind(_plan_custom, lambda job, idx, v: True, _reduce_custom),
    "pymap": Kind(_plan_pymap, _check_pymap, lambda job: {"value": [x for part in job.results for x in part]}, _piece_pymap),
}
