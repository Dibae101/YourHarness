"""Compute code for every job type, run by agents inside worker processes.

The browser has its own copy in web/jobs.js. Both produce answers that can be
mixed in one job: piece 3 of a prime count may come from an iPad and piece 4
from a Windows PC.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import textwrap
import urllib.parse
import urllib.request
import zlib

import numpy as np

_base_primes = np.zeros(0, dtype=np.int64)
_base_limit = 0
_code_cache: dict[str, dict] = {}


def compute(task: dict, ctx: dict):
    kind = task.get("kind")
    p = task.get("params") or {}
    idx = int(task["idx"])
    if kind == "mandelbrot":
        return mandelbrot(p, idx)
    if kind == "primes":
        return count_primes(p, idx)
    if kind == "pi":
        return monte_carlo(p, idx, str(task.get("jobId", "")))
    if kind == "grep":
        return search_file(p, idx, ctx)
    if kind == "custom":
        return run_custom(p, idx, int(task["total"]))
    if kind == "pymap":
        return run_map(p, task.get("piece") or {})
    raise ValueError(f"Unknown job type: {kind}")


# --- Fractal: one strip of rows, iteration counts as base64 little-endian uint16
def mandelbrot(p: dict, idx: int) -> str:
    width, height, max_iter = int(p["width"]), int(p["height"]), int(p["maxIter"])
    cx, cy, span, rows = float(p["cx"]), float(p["cy"]), float(p["span"]), int(p["rows"])
    y0 = idx * rows
    y1 = min(height, y0 + rows)
    step = span / width
    left = cx - span / 2
    top = cy + (height * step) / 2
    xs = left + np.arange(width) * step
    ys = top - np.arange(y0, y1) * step
    c = (xs[None, :] + 1j * ys[:, None]).ravel()
    out = np.full(c.size, max_iter, dtype=np.uint16)
    alive = np.arange(c.size)
    z = np.zeros_like(c)
    for i in range(1, max_iter + 1):
        np.multiply(z, z, out=z)
        z += c
        escaped = (z.real * z.real + z.imag * z.imag) > 4.0
        if escaped.any():
            out[alive[escaped]] = i
            keep = ~escaped
            alive, z, c = alive[keep], z[keep], c[keep]
            if alive.size == 0:
                break
    return base64.b64encode(out.astype("<u2").tobytes()).decode("ascii")


# --- Primes: segmented sieve over [lo, hi)
def _primes_up_to(n: int) -> np.ndarray:
    global _base_primes, _base_limit
    if _base_limit >= n:
        return _base_primes
    sieve = np.ones(n + 1, dtype=bool)
    sieve[:2] = False
    for i in range(2, math.isqrt(n) + 1):
        if sieve[i]:
            sieve[i * i :: i] = False
    _base_primes = np.flatnonzero(sieve).astype(np.int64)
    _base_limit = n
    return _base_primes


def count_primes(p: dict, idx: int) -> int:
    limit, size = int(p["limit"]), int(p["chunkSize"])
    lo = idx * size
    hi = min(limit, lo + size)
    if hi <= lo:
        return 0
    seg = np.ones(hi - lo, dtype=bool)
    for q in _primes_up_to(math.isqrt(limit) + 1).tolist():
        if q * q >= hi:
            break
        first = max(q * q, -(-lo // q) * q)
        seg[first - lo :: q] = False
    if lo < 2:
        seg[: min(2 - lo, seg.size)] = False
    return int(np.count_nonzero(seg))


# --- π: random points in a unit square, seeded per piece so a re-run gives the same count
def monte_carlo(p: dict, idx: int, job_id: str) -> int:
    rng = np.random.default_rng([zlib.crc32(job_id.encode()), idx])
    remaining = int(p["samples"])
    hits = 0
    while remaining:
        n = min(remaining, 1_000_000)
        x = rng.random(n)
        y = rng.random(n)
        hits += int(np.count_nonzero(x * x + y * y <= 1.0))
        remaining -= n
    return hits


# --- Search: one byte range of a shared file, counting matches that start in it.
# Overlapping matches count, so the total is exact wherever the boundaries fall.
# "Ignore case" folds ASCII letters only, the same as the browser version.
def search_file(p: dict, idx: int, ctx: dict) -> int:
    lo = idx * int(p["chunkBytes"])
    hi = min(int(p["size"]), lo + int(p["chunkBytes"]))
    needle = str(p["term"]).encode("utf-8")
    end = min(int(p["size"]), hi + len(needle) - 1)
    url = f"{ctx['server']}/api/files/{urllib.parse.quote(p['file'], safe='')}"
    req = urllib.request.Request(url, headers={"Range": f"bytes={lo}-{end - 1}", "Authorization": f"Bearer {ctx['token']}"})
    with urllib.request.urlopen(req, timeout=120) as res:
        hay = res.read()
        if res.status == 200:  # the server ignored the range
            hay = hay[lo:end]
    if p.get("ignoreCase"):
        hay, needle = hay.lower(), needle.lower()
    limit = hi - lo
    count = 0
    at = hay.find(needle)
    while 0 <= at < limit:
        count += 1
        at = hay.find(needle, at + 1)
    return count


# --- Your own Python: compiled once per job in each worker process
def _namespace(code: str) -> dict:
    ns = _code_cache.get(code)
    if ns is None:
        ns = {"__name__": "__harness__"}
        exec(compile(code, "<your code>", "exec"), ns)  # noqa: S102 - running the pool's code is the point
        _code_cache.clear()
        _code_cache[code] = ns
    return ns


def _finish(value):
    if asyncio.iscoroutine(value):
        value = asyncio.run(value)
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as e:
        raise ValueError(f"The result can't be sent back as JSON ({e}). Return numbers, text, lists or dicts.") from None
    return value


def run_custom(p: dict, idx: int, total: int):
    fn = _namespace(p["code"]).get("run")
    if not callable(fn):
        raise ValueError("Define a function named run(chunk, total).")
    return _finish(fn(idx, total))


def run_map(p: dict, piece: dict) -> list:
    code = textwrap.dedent(p["code"])
    fn = _namespace(code).get(p["fn"])
    if not callable(fn):
        raise ValueError(f"The code doesn't define a function named {p['fn']}.")
    return [_finish(fn(item)) for item in piece.get("items", [])]
