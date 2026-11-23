"""A pytest suite in shards across computers (tests.pytest and harness test)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from conftest import start_coordinator
from test_engine_e2e import Engine
from yourharness.engine import spec as jobspec
from yourharness.engine import testing
from yourharness.engine.spec import SpecError

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------- sharding and outcomes
def test_every_test_lands_in_exactly_one_shard_and_the_shards_balance():
    tests = [f"tests/test_{i % 7}.py::test_{i}" for i in range(200)]
    durations = {t: (5.0 if i % 50 == 0 else 0.1 * (i % 9)) for i, t in enumerate(tests)}
    shards = testing.shard(tests, durations, 6)
    flat = [t for s in shards for t in s]
    assert sorted(flat) == sorted(tests) and len(flat) == len(set(flat))
    assert shards == testing.shard(list(tests), dict(durations), 6), "the same tests and timings give the same shards"
    loads = [sum(durations[t] for t in s) for s in shards]
    assert max(loads) - min(loads) <= max(durations.values()), loads
    assert all(s == sorted(s, key=tests.index) for s in shards), "each shard runs its tests in the collected order"
    assert testing.shard(tests[:3], {}, 10) == [[tests[0]], [tests[1]], [tests[2]]], "never more shards than tests"
    assert testing.auto_shards(tests, {}) == 10, "about 20 s per shard, 1 s for a test with no timing"
    assert testing.auto_shards(tests[:5], {}) == 1


def test_outcomes_are_counted_the_way_pytest_counts_them():
    recs = [
        {"test": "t.py::ok", "when": w, "outcome": "passed", "seconds": 0.1} for w in ("setup", "call", "teardown")
    ] + [
        {"test": "t.py::bad", "when": "setup", "outcome": "passed", "seconds": 0}, {"test": "t.py::bad", "when": "call", "outcome": "failed",
                                                                                   "seconds": 0.2, "message": "assert 1 == 2"},
        {"test": "t.py::fixture", "when": "setup", "outcome": "failed", "seconds": 0, "message": "fixture broke"},
        {"test": "t.py::skip", "when": "setup", "outcome": "skipped", "seconds": 0},
        {"test": "t.py::xf", "when": "setup", "outcome": "passed", "seconds": 0}, {"test": "t.py::xf", "when": "call", "outcome": "skipped", "xfail": True},
        {"test": "t.py::xp", "when": "setup", "outcome": "passed", "seconds": 0}, {"test": "t.py::xp", "when": "call", "outcome": "passed", "xfail": True},
        {"test": "t.py::td", "when": "setup", "outcome": "passed"}, {"test": "t.py::td", "when": "call", "outcome": "passed"},
        {"test": "t.py::td", "when": "teardown", "outcome": "failed", "message": "cleanup broke"},
        {"test": "gone.py::x", "missing": True},
        {"collectError": "broken.py", "message": "ImportError: no module named nope"}, {"test": "broken.py::y", "missing": True},
    ]
    out, secs, msgs = testing.outcomes(recs)
    assert out == {"t.py::ok": "passed", "t.py::bad": "failed", "t.py::fixture": "error", "t.py::skip": "skipped", "t.py::xf": "xfailed",
                   "t.py::xp": "xpassed", "t.py::td": "error", "gone.py::x": "missing", "broken.py::y": "error"}
    assert secs["t.py::ok"] == pytest.approx(0.3)
    assert msgs["t.py::bad"] == "assert 1 == 2" and "cleanup" in msgs["t.py::td"] and "nope" in msgs["broken.py::y"]
    with pytest.raises(Exception, match="two partitions"):
        testing.merge([{"tests": {"a": "passed"}}, {"tests": {"a": "failed"}}])
    res = testing.final({"count": 3}, testing.merge([{"tests": {"a": "passed", "b": "flaky"}}, {"tests": {"c": "failed"}}]))
    assert (res["passed"], res["flaky"], res["failed"], res["ok"]) == (1, 1, 1, False) and res["notPassing"] == [{"test": "c", "outcome": "failed"}]
    with pytest.raises(Exception, match="were collected"):
        testing.final({"count": 4}, {"tests": {"a": "passed"}})


def test_a_test_job_spec_becomes_its_shards():
    tests = [f"tests/t.py::test_{i}" for i in range(50)]
    doc = jobspec.parse({"spec": {"workload": {"type": "tests.pytest", "tests": tests, "shards": 4, "args": ["-k", "not slow"],
                                               "durations": {tests[0]: 30, "elsewhere::x": 9}},
                                  "inputs": {"project": {"ref": "model:m1a2b3c4d", "sha256": "ab" * 32}}}})
    s = doc["spec"]
    assert s["workload"] == {"type": "tests.pytest", "count": 50, "args": ["-k", "not slow"], "reruns": 1, "timeout": 3600, "shards": 4}
    assert len(s["inputs"]["items"]) == 4 and sorted(t for sh in s["inputs"]["items"] for t in sh) == sorted(tests)
    assert [tests[0]] in s["inputs"]["items"], "a 30 s test gets a shard of its own"
    assert s["partitioning"]["itemsPerPartition"] == 1 and s["runtime"] == {"kind": "native-python"}
    assert s["security"]["allowedTrust"] == "owned-only", "a test suite is your code: your computers only"
    for wl, inputs, field in (
        ({"type": "tests.pytest", "tests": []}, {"project": {"ref": "model:m1a2b3c4d"}}, "spec.workload.tests"),
        ({"type": "tests.pytest", "tests": ["a::b", "a::b"]}, {"project": {"ref": "model:m1a2b3c4d"}}, "spec.workload.tests"),
        ({"type": "tests.pytest", "tests": ["a::b"], "args": ["-p", "evil"]}, {"project": {"ref": "model:m1a2b3c4d"}}, "spec.workload.args"),
        ({"type": "tests.pytest", "tests": ["a::b"], "args": ["--junitxml=x.xml"]}, {"project": {"ref": "model:m1a2b3c4d"}}, "spec.workload.args"),
        ({"type": "tests.pytest", "tests": ["a::b"]}, {"project": {"ref": "dataset:d1a2b3c4"}}, "spec.inputs.project.ref"),
        ({"type": "tests.pytest", "tests": ["a::b"]}, {"project": {"ref": "model:m1a2b3c4d"}, "items": [1]}, "spec.inputs"),
    ):
        with pytest.raises(SpecError) as e:
            jobspec.parse({"spec": {"workload": wl, "inputs": inputs}})
        assert e.value.path == field, (wl, str(e.value))


def test_the_junit_report_is_well_formed():
    result = {"total": 4, "passed": 1, "failed": 1, "error": 0, "skipped": 1, "xfailed": 0, "xpassed": 0, "flaky": 1, "missing": 0, "testSeconds": 1.5,
              "outcomes": {"tests/a.py::test_ok": "passed", "tests/a.py::TestX::test_bad[1]": "failed", "tests/b.py::test_skip": "skipped",
                           "tests/b.py::test_flaky": "flaky"},
              "notPassing": [{"test": "tests/a.py::TestX::test_bad[1]", "outcome": "failed", "message": "assert <1> == 2 & more"}],
              "flakyTests": [{"test": "tests/b.py::test_flaky", "message": "first try"}], "durations": {"tests/a.py::test_ok": 0.25}}
    root = ET.fromstring(testing.junit_xml(result, "proj"))
    suite = root.find("testsuite")
    assert (suite.get("tests"), suite.get("failures"), suite.get("skipped")) == ("4", "1", "1")
    cases = {(c.get("classname"), c.get("name")): c for c in suite.iter("testcase")}
    bad = cases[("tests.a.TestX", "test_bad[1]")]
    assert bad.find("failure").text == "assert <1> == 2 & more"
    assert cases[("tests.a", "test_ok")].get("time") == "0.250"
    assert cases[("tests.b", "test_skip")].find("skipped") is not None and "flaky" in cases[("tests.b", "test_flaky")].find("system-out").text


# ---------------------------------------------------------------------- end to end
def make_project(d: Path) -> Path:
    mark = Path(os.environ.get("TMPDIR", "/tmp")) / f"yh-flaky-{uuid.uuid4().hex}"
    files = {
        "conftest.py": "",
        "calc.py": "def add(a, b):\n    return a + b\n",
        "tests/conftest.py": "import pytest\n\n@pytest.fixture\ndef base():\n    return 41\n",
        "tests/test_calc.py": ("import pytest\nfrom calc import add\n\n"
                               "def test_add():\n    assert add(2, 2) == 4\n\n"
                               "@pytest.mark.parametrize('n', range(12))\ndef test_many(n):\n    assert add(n, 1) == n + 1\n\n"
                               "def test_fixture(base):\n    assert add(base, 1) == 42\n\n"
                               "def test_wrong():\n    assert add(1, 1) == 3, 'one and one'\n\n"
                               "def test_skipped():\n    pytest.skip('not here')\n\n"
                               "@pytest.mark.xfail(reason='known')\ndef test_known_bug():\n    assert False\n\n"
                               "class TestGroup:\n    def test_in_class(self):\n        assert add(-1, 1) == 0\n"),
        "tests/test_flaky.py": (f"import os\nMARK = {str(mark)!r}\n\n"
                                "def test_flaky():\n    if not os.path.exists(MARK):\n        open(MARK, 'w').close()\n        assert False, 'first try'\n"),
        "tests/test_writes.py": "def test_writes_into_its_folder(tmp_path):\n    open('scratch.txt', 'w').write('x')\n    assert open('scratch.txt').read() == 'x'\n",
    }
    for name, text in files.items():
        (d / name).parent.mkdir(parents=True, exist_ok=True)
        (d / name).write_text(text)
    return d


def harness(coord, client, home: Path, *args, cwd=None):
    env = {**os.environ, "HARNESS_URL": coord.base, "HARNESS_TOKEN": client.token, "YOURHARNESS_HOME": str(home)}
    return subprocess.run([sys.executable, "-m", "yourharness", *args], capture_output=True, text=True, env=env, cwd=cwd or ROOT, timeout=300)


def test_harness_test_runs_a_suite_in_shards_and_reports_every_test(tmp_path):
    proj = make_project(tmp_path / "proj")
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("A", threads=1, extra=("--no-tasks",))
        coord.agent("B", threads=1, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"A", "B"})
        home = tmp_path / "home"
        out = harness(coord, eng.c, home, "test", str(proj), "--shards", "4", "--junit", str(tmp_path / "report.xml"))
        assert out.returncode == 1, out.stdout + out.stderr  # one test fails
        text = out.stdout
        assert "16 passed" in text and "1 failed" in text and "1 skipped" in text and "1 xfailed" in text and "1 flaky" in text, text
        assert "FAILED  tests/test_calc.py::test_wrong" in text and "one and one" in text and "FLAKY   tests/test_flaky.py::test_flaky" in text
        job = eng.c.engine_jobs()[0]
        full = eng.get(job["id"])
        res = full["result"]
        collected = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q", f"--rootdir={proj}", "-p", "no:cacheprovider"],
                                   cwd=proj, capture_output=True, text=True).stdout
        ids = {ln for ln in collected.splitlines() if "::" in ln}
        assert set(res["outcomes"]) == ids and res["total"] == len(ids) == 20, "every collected test ran exactly once"
        assert res["outcomes"]["tests/test_flaky.py::test_flaky"] == "flaky", "a pass on the rerun is reported as flaky, not passed"
        assert res["missing"] == 0 and res["error"] == 0
        assert {d["name"] for d in full["perDevice"]} == {"A", "B"} and full["total"] == 4
        assert full["validation"]["level"] == "structural"
        assert res["provenance"]["project"]["sha256"] == full["provenance"]["project"]["sha256"]
        suite = ET.parse(tmp_path / "report.xml").getroot().find("testsuite")
        assert suite.get("tests") == "20" and suite.get("failures") == "1"
        timings = list((home / "test-durations").glob("*.json"))
        assert len(timings) == 1 and len(json.loads(timings[0].read_text())) == 20, "the timings are kept for the next run's shards"
        # the deterministic tests again, checked: one shard runs a second time on another computer and must match
        again = harness(coord, eng.c, home, "test", str(proj), "--spot-check", "1", "--", "-k", "not flaky and not wrong")
        assert again.returncode == 0, again.stdout + again.stderr
        job2 = eng.get(eng.c.engine_jobs()[0]["id"])
        assert job2["result"]["ok"] and job2["result"]["passed"] == 16 and job2["result"]["total"] == 18 and job2["validation"]["level"] == "spot-checked", job2["validation"]
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()
