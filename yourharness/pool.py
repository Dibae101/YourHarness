"""Use the pool from Python, inside the workspace or on any computer running an agent.

    from yourharness import pool

    def slow_square(n):
        import time
        time.sleep(1)
        return n * n

    pool.map(slow_square, range(64))   # 64 one-second calls spread over every agent

Functions are sent to other devices as source code, so define them with ``def``
and import what they need inside the function body. Arguments and return values
must be JSON (numbers, text, lists, dicts).
"""

from __future__ import annotations

import builtins
import inspect
import textwrap

from .client import Client, PoolError

__all__ = ["map", "run", "devices", "client", "PoolError"]

_client: Client | None = None


def client() -> Client:
    global _client
    if _client is None:
        _client = Client()
    return _client


def _source(fn) -> str:
    if not callable(fn) or getattr(fn, "__name__", "<lambda>") == "<lambda>":
        raise PoolError("pool.map needs a function defined with def, so its source can be sent to other devices.")
    try:
        src = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError):
        raise PoolError("Can't read this function's source. Define it in a .py file or a notebook cell.") from None
    lines = src.splitlines()
    while lines and lines[0].lstrip().startswith("@"):  # decorators may not exist on other devices
        lines.pop(0)
    return "\n".join(lines) + "\n"


def map(fn, items, per_piece: int = 1, quiet: bool = False) -> list:  # noqa: A001 - mirrors builtins.map on purpose
    """Call fn(item) for every item across the pool. Results come back in order."""
    items = builtins.list(items)
    if not items:
        return []
    job = client().run("pymap", {"code": _source(fn), "fn": fn.__name__, "items": items, "perPiece": per_piece}, quiet=quiet)
    return job["result"]["value"]


def run(code: str, pieces: int, combine: str = "sum", quiet: bool = False):
    """Run code that defines run(chunk, total) once per piece, on every agent, and combine the results."""
    job = client().run("custom", {"code": code, "chunks": pieces, "combine": combine, "lang": "python"}, quiet=quiet)
    return job["result"]["value"]


def devices() -> list[dict]:
    return client().devices()
