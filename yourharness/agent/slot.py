"""One worker process. Reads tasks as JSON lines on stdin and writes results as JSON lines.

Results go out on a private copy of stdout. Anything else written to stdout,
such as print() in your own code, is sent to stderr, which shows in the agent's
terminal and can't corrupt the results.
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback


def describe(e: BaseException) -> str:
    line = None
    for frame in traceback.extract_tb(e.__traceback__):
        if frame.filename == "<your code>":
            line = frame.lineno
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    return f"{text} (line {line} of your code)" if line else text


def main() -> None:
    results = os.fdopen(os.dup(1), "w", encoding="utf-8")
    os.dup2(2, 1)
    from yourharness.kernels import compute

    while True:
        line = sys.stdin.readline()
        if not line:
            return
        if not line.strip():
            continue
        msg = json.loads(line)
        t0 = time.perf_counter()
        try:
            value = compute(msg["task"], msg.get("ctx") or {})
            out = json.dumps({"ok": True, "value": value, "ms": round((time.perf_counter() - t0) * 1000)}, allow_nan=False)
        except BaseException as e:  # noqa: BLE001 - your code may raise anything, including SystemExit
            if isinstance(e, KeyboardInterrupt):
                return
            out = json.dumps({"ok": False, "error": describe(e), "ms": round((time.perf_counter() - t0) * 1000)})
        sys.stdout.flush()
        results.write(out + "\n")
        results.flush()


if __name__ == "__main__":
    main()
