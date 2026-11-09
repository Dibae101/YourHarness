"""A pytest suite split across your computers: the tests.pytest workload.

The test ids are collected where you start the job (`harness test`), with --rootdir set to
the project, so every computer names each test the same way. They are cut into shards of
about equal running time, longest first onto the emptiest shard, using how long each test
took last time when that's known; every test id lands in exactly one shard. One shard is
one partition: the computer unpacks the project archive (checked by its SHA-256), runs
pytest on just that shard's tests (a small plugin deselects the rest and records each
test's outcome), and reruns the ones that failed, each in a fresh copy of the project.

A test that failed and then passed on a rerun is reported as flaky, never as passed. A
test pytest didn't find on that computer is "missing", and a test file that didn't import
makes its tests "error". The checked state of a shard is the outcome of each of its tests;
timings and failure messages travel beside it (they differ from run to run), so a spot
check (output.spotCheck) compares outcomes only, and fails the job if a test's outcome
changes between computers.

Standard library only, like the rest of the worker; pytest itself must be importable by
the Python the job runs with (the agent's, the managed environment's, or the image's).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

OUTCOMES = ("passed", "failed", "error", "skipped", "xfailed", "xpassed", "flaky", "missing")
BAD = ("failed", "error", "missing")  # what makes the run not OK; flaky is reported but doesn't fail it
SHARD_SECONDS = 20.0  # an automatic shard holds about this much test time
MAX_SHARDS = 256
DEFAULT_TEST_SECONDS = 1.0
MESSAGE_CHARS = 2000

PLUGIN = r'''
"""YourHarness: run only this shard's tests and record each one's outcome (loaded with -p yh_select)."""
import json, os

_WANT = set(open(os.environ["YH_TESTS_FILE"], encoding="utf-8").read().splitlines())
_OUT = open(os.environ["YH_RESULTS_FILE"], "a", encoding="utf-8")


def _write(rec):
    _OUT.write(json.dumps(rec) + "\n")
    _OUT.flush()


def _text(report):
    try:
        return str(report.longreprtext)[-4000:]
    except Exception:
        return str(getattr(report, "longrepr", ""))[-4000:]


def pytest_collectreport(report):
    if report.failed:
        _write({"collectError": report.nodeid, "message": _text(report)})


def pytest_collection_modifyitems(session, config, items):
    keep = [i for i in items if i.nodeid in _WANT]
    drop = [i for i in items if i.nodeid not in _WANT]
    if drop:
        config.hook.pytest_deselected(items=drop)
    items[:] = keep
    found = {i.nodeid for i in keep}
    for t in sorted(_WANT - found):
        _write({"test": t, "missing": True})


def pytest_runtest_logreport(report):
    rec = {"test": report.nodeid, "when": report.when, "outcome": report.outcome, "seconds": round(float(report.duration or 0), 6)}
    if hasattr(report, "wasxfail"):
        rec["xfail"] = True
    if report.failed:
        rec["message"] = _text(report)
    _write(rec)
'''


class TestRunError(RuntimeError):
    """The shard couldn't run at all (no pytest, the project missing): the job's setup, not a test, is wrong."""


# ---------------------------------------------------------------------- sharding
def estimates(tests: list[str], durations: dict) -> dict[str, float]:
    """Each test's expected seconds: last time's, else the median of the known ones (when most are known), else 1 s."""
    def ok(v) -> bool:
        return isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0

    known = sorted(float(durations[t]) for t in tests if ok(durations.get(t)))
    fallback = known[len(known) // 2] if len(known) >= max(3, len(tests) // 2) else DEFAULT_TEST_SECONDS
    return {t: float(durations[t]) if ok(durations.get(t)) else fallback for t in tests}


def auto_shards(tests: list[str], durations: dict) -> int:
    total = sum(estimates(tests, durations).values())
    return max(1, min(len(tests), MAX_SHARDS, round(total / SHARD_SECONDS) or 1))


def shard(tests: list[str], durations: dict, n: int) -> list[list[str]]:
    """Every test in exactly one of n shards, longest first onto the shard with the least time so far.

    Deterministic: the same tests and durations always give the same shards. Each shard
    lists its tests in their collected order."""
    order = {t: i for i, t in enumerate(tests)}
    est = estimates(tests, durations)
    n = max(1, min(n, len(tests)))
    bins: list[list[str]] = [[] for _ in range(n)]
    load = [0.0] * n
    for t in sorted(tests, key=lambda t: (-est[t], order[t])):
        i = min(range(n), key=lambda k: (load[k], k))
        bins[i].append(t)
        load[i] += est[t]
    return [sorted(b, key=order.__getitem__) for b in bins if b]


# ---------------------------------------------------------------------- outcomes from pytest's reports
def outcomes(records: list[dict]) -> tuple[dict, dict, dict]:
    """({test: outcome}, {test: seconds}, {test: message}) from the plugin's records, the way pytest's summary counts them."""
    phases: dict[str, dict] = {}
    missing, collect_errors = set(), {}
    for r in records:
        if "collectError" in r:
            collect_errors[r["collectError"]] = r.get("message") or "collection failed"
        elif r.get("missing"):
            missing.add(r["test"])
        else:
            phases.setdefault(r["test"], {})[r["when"]] = r
    out, secs, msgs = {}, {}, {}
    for t, ph in phases.items():
        secs[t] = round(sum(float(p.get("seconds") or 0) for p in ph.values()), 6)
        setup, call, teardown = ph.get("setup"), ph.get("call"), ph.get("teardown")
        if setup and setup["outcome"] == "failed":
            out[t], msgs[t] = "error", setup.get("message")
        elif setup and setup["outcome"] == "skipped":
            out[t] = "xfailed" if setup.get("xfail") else "skipped"
        elif call is None:
            out[t] = "error"
            msgs[t] = "the test never ran its body"
        elif call["outcome"] == "failed":
            out[t], msgs[t] = "failed", call.get("message")
        elif call["outcome"] == "skipped":
            out[t] = "xfailed" if call.get("xfail") else "skipped"
        else:
            out[t] = "xpassed" if call.get("xfail") else "passed"
        if teardown and teardown["outcome"] == "failed" and out[t] in ("passed", "skipped", "xfailed", "xpassed"):
            out[t], msgs[t] = "error", teardown.get("message")
    for t in missing:
        if t in out:
            continue
        err = next((m for f, m in collect_errors.items() if t == f or t.startswith(f + "::")), None)
        out[t] = "error" if err else "missing"
        msgs[t] = err or "pytest didn't find this test on the computer that ran it"
        secs.setdefault(t, 0.0)
    return out, secs, {t: (m or "")[-MESSAGE_CHARS:] for t, m in msgs.items() if m}


# ---------------------------------------------------------------------- the operator, in the worker
class PytestOp:
    resumable = False

    def __init__(self, wl: dict, project: dict | None):
        if not project:
            raise TestRunError("tests.pytest needs its project")
        self.wl = wl
        self.project = Path(project["path"])
        self.result: dict[str, str] = {}
        self.seconds: dict[str, float] = {}
        self.messages: dict[str, str] = {}
        self.runs = 0
        self.pytest_seconds = 0.0

    def feed(self, tests) -> None:
        if not isinstance(tests, list) or not all(isinstance(t, str) for t in tests):
            raise TestRunError("a shard should be a list of test ids")
        out, secs, msgs = self._run(tests)
        for _ in range(int(self.wl.get("reruns", 1))):
            again = sorted(t for t in tests if out.get(t) in ("failed", "error"))
            if not again:
                break
            out2, secs2, _msgs2 = self._run(again)
            for t in again:
                if out2.get(t) in ("passed", "xpassed"):
                    out[t] = "flaky"  # the first failure's message stays: that is what happened
                    secs[t] = secs2.get(t, secs.get(t, 0.0))
        for t in tests:
            if t in self.result:
                raise TestRunError(f"{t} is in this shard twice")
            self.result[t] = out.get(t, "missing")
            self.seconds[t] = secs.get(t, 0.0)
            if t in msgs:
                self.messages[t] = msgs[t]
            elif self.result[t] == "missing":
                self.messages[t] = "pytest didn't report this test"

    def _run(self, tests: list[str]) -> tuple[dict, dict, dict]:
        work = Path(tempfile.mkdtemp(prefix="yh-tests-"))
        try:
            proj = work / "project"
            shutil.copytree(self.project, proj, symlinks=True)  # each run gets a fresh copy: tests may write into it
            plugin = work / "plugin"
            plugin.mkdir()
            (plugin / "yh_select.py").write_text(PLUGIN, encoding="utf-8")
            (work / "tests.txt").write_text("\n".join(tests) + "\n", encoding="utf-8")
            results = work / "results.jsonl"
            results.touch()
            files = sorted({t.split("::", 1)[0] for t in tests})
            env = {**os.environ, "YH_TESTS_FILE": str(work / "tests.txt"), "YH_RESULTS_FILE": str(results), "PYTHONDONTWRITEBYTECODE": "1",
                   "PYTHONPATH": os.pathsep.join([str(plugin), str(proj)] + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else []))}
            env.pop("HARNESS_TOKEN", None)
            cmd = [sys.executable, "-m", "pytest", "-p", "yh_select", "-p", "no:cacheprovider", f"--rootdir={proj}", "-q", "--color=no",
                   *self.wl.get("args", []), "--", *files]
            t0 = time.monotonic()
            try:
                proc = subprocess.run(cmd, cwd=proj, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                      timeout=float(self.wl.get("timeout") or 3600))
            except subprocess.TimeoutExpired:
                raise TestRunError(f"the shard's tests ran longer than {self.wl.get('timeout')} s") from None
            self.runs += 1
            self.pytest_seconds += time.monotonic() - t0
            tail = (proc.stdout + proc.stderr)[-1500:]
            if "No module named pytest" in tail:
                raise TestRunError("pytest isn't installed in the Python this job runs with: add it to the job's requirements "
                                   "(or install it where the agent runs)")
            recs = []
            for line in results.read_text(encoding="utf-8").splitlines():
                try:
                    recs.append(json.loads(line))
                except ValueError:
                    continue
            if proc.returncode not in (0, 1, 5) and not recs:  # 2: interrupted, 3: internal error, 4: usage error
                raise TestRunError(f"pytest stopped with exit code {proc.returncode}: {tail.strip()}")
            return outcomes(recs)
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def state(self) -> dict:
        return {"tests": {t: self.result[t] for t in sorted(self.result)}}

    def timings(self) -> dict:
        """Beside the checked state: how long each test took and why the failing ones failed."""
        return {"testSeconds": self.seconds, "testMessages": self.messages, "pytestRuns": self.runs, "pytestSeconds": round(self.pytest_seconds, 3)}


# ---------------------------------------------------------------------- combining the shards
def merge(states: list[dict]) -> dict:
    tests: dict[str, str] = {}
    for st in states:
        for t, o in st["tests"].items():
            if t in tests:
                from .reducer import ReduceError

                raise ReduceError(f"{t} was run by two partitions")
            if o not in OUTCOMES:
                from .reducer import ReduceError

                raise ReduceError(f"{t} has an unknown outcome {o!r}")
            tests[t] = o
    return {"tests": tests}


def final(wl: dict, st: dict) -> dict:
    tests = st["tests"]
    if len(tests) != int(wl["count"]):
        from .reducer import ReduceError

        raise ReduceError(f"{len(tests)} tests reported, {wl['count']} were collected")
    counts = {o: 0 for o in OUTCOMES}
    for o in tests.values():
        counts[o] += 1
    bad = sorted(t for t, o in tests.items() if o in BAD)
    return {"total": len(tests), **counts, "ok": not bad, "notPassing": [{"test": t, "outcome": tests[t]} for t in bad[:500]],
            "flakyTests": sorted(t for t, o in tests.items() if o == "flaky")[:500], "outcomes": tests}


def report(attempt_metrics: list[dict], result: dict) -> dict:
    """Timings and failure messages from the accepted attempts, added to the result for people (not checked)."""
    secs, msgs, runs, busy = {}, {}, 0, 0.0
    for m in attempt_metrics:
        secs.update(m.get("testSeconds") or {})
        msgs.update(m.get("testMessages") or {})
        runs += int(m.get("pytestRuns") or 0)
        busy += float(m.get("pytestSeconds") or 0)
    for row in result.get("notPassing", []):
        row["message"] = msgs.get(row["test"], "")
    flaky = [{"test": t, "message": msgs.get(t, "")} for t in result.get("flakyTests", [])]
    slowest = sorted(secs.items(), key=lambda kv: -kv[1])[:20]
    return {**result, "flakyTests": flaky, "slowest": [{"test": t, "seconds": round(s, 3)} for t, s in slowest],
            "testSeconds": round(sum(secs.values()), 3), "pytestRuns": runs, "pytestSeconds": round(busy, 3),
            "durations": {t: round(s, 4) for t, s in secs.items()}}


def junit_xml(result: dict, name: str = "yourharness") -> str:
    """The result as one JUnit XML test suite (what CI systems and IDEs read)."""
    from xml.sax.saxutils import escape, quoteattr

    msgs = {r["test"]: r.get("message", "") for r in result.get("notPassing", [])}
    msgs.update({r["test"]: r.get("message", "") for r in result.get("flakyTests", []) if isinstance(r, dict)})
    durations = result.get("durations") or {}
    c = result
    rows = []
    for t, o in sorted((result.get("outcomes") or {}).items()):
        file, _, rest = t.partition("::")
        cls = (file[:-3] if file.endswith(".py") else file).replace("/", ".") + ("." + rest.rsplit("::", 1)[0].replace("::", ".") if "::" in rest else "")
        case = rest.rsplit("::", 1)[-1] or t
        attrs = f"classname={quoteattr(cls)} name={quoteattr(case)} file={quoteattr(file)} time=\"{float(durations.get(t, 0)):.3f}\""
        m = escape(msgs.get(t, ""))
        if o == "failed":
            body = f'<failure message="failed">{m}</failure>'
        elif o in ("error", "missing"):
            body = f'<error message={quoteattr(o)}>{m}</error>'
        elif o in ("skipped", "xfailed"):
            body = f'<skipped message={quoteattr(o)}/>'
        elif o == "flaky":
            body = f"<system-out>flaky: failed, then passed on a rerun\n{m}</system-out>"
        else:
            body = ""
        rows.append(f"    <testcase {attrs}>{body}</testcase>" if body else f"    <testcase {attrs}/>")
    errors = c.get("error", 0) + c.get("missing", 0)
    head = (f'<testsuite name={quoteattr(name)} tests="{c.get("total", len(rows))}" failures="{c.get("failed", 0)}" errors="{errors}" '
            f'skipped="{c.get("skipped", 0) + c.get("xfailed", 0)}" time="{float(c.get("testSeconds", 0)):.3f}">')
    return '<?xml version="1.0" encoding="utf-8"?>\n<testsuites>\n  ' + head + "\n" + "\n".join(rows) + "\n  </testsuite>\n</testsuites>\n"
