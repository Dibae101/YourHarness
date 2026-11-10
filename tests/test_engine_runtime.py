"""Managed Python environments: prepared once per computer with uv, only after you approve.

These tests need uv (pip install uv) and no network: the environments use the Python
that is already installed and no packages from PyPI, except the one that is meant
to fail.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import start_coordinator
from test_engine_e2e import Engine
from yourharness.engine import spec as jobspec
from yourharness.engine.runtime import RuntimeManager
from yourharness.runtime.envs import EnvManager, fingerprint, import_name

PY = ".".join(platform.python_version_tuple()[:2])
needs_uv = pytest.mark.skipif(shutil.which("uv") is None and not (Path(sys.prefix) / ("Scripts" if os.name == "nt" else "bin") / "uv").exists(),
                              reason="needs uv")


def make_wheel(folder: Path, name: str, value: int) -> str:
    """A tiny package as a wheel file, so installing it needs no network. Returns its requirement."""
    version = "1.0"
    files = {f"{name}/__init__.py": f"VALUE = {value}\n",
             f"{name}-{version}.dist-info/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
             f"{name}-{version}.dist-info/WHEEL": "Wheel-Version: 1.0\nGenerator: yourharness-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"}
    whl = folder / f"{name}-{version}-py3-none-any.whl"
    record = []
    with zipfile.ZipFile(whl, "w") as z:
        for path, text in files.items():
            data = text.encode()
            z.writestr(path, data)
            record.append(f"{path},sha256={base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()},{len(data)}")
        record.append(f"{name}-{version}.dist-info/RECORD,,")
        z.writestr(f"{name}-{version}.dist-info/RECORD", "\n".join(record) + "\n")
    return f"{name} @ {whl.resolve().as_uri()}"


def test_a_requirement_is_a_package_never_a_pip_option():
    base = {"spec": {"workload": {"type": "python.map", "code": "def process(x):\n    return x\n"}, "inputs": {"items": [1]},
                     "runtime": {"kind": "managed-python", "python": "3.12"}}}
    for bad in (["--index-url https://example.invalid/simple"], ["-r /etc/passwd"], ["-e ."], ["numpy\n--pre"]):
        base["spec"]["runtime"]["requirements"] = bad
        with pytest.raises(jobspec.SpecError) as e:
            jobspec.parse(base)
        assert e.value.path == "spec.runtime.requirements"
    base["spec"]["runtime"]["requirements"] = [" numpy==2.1.3 ", "pkg @ file:///tmp/pkg-1.0-py3-none-any.whl", "torch[cpu]>=2.5; sys_platform == 'linux'"]
    assert jobspec.parse(base)["spec"]["runtime"]["requirements"][0] == "numpy==2.1.3"


def test_a_running_job_stops_saying_its_environment_is_being_prepared(tmp_path):
    from yourharness.engine.db import EngineDB

    db = EngineDB(tmp_path / "engine.db")
    rm = RuntimeManager(SimpleNamespace(db=db))
    doc = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": "def process(x):\n    return x\n"}, "inputs": {"items": [1]},
                                  "runtime": {"kind": "managed-python", "python": "3.12", "requirements": ["numpy==2.1.3"]}}})
    other = jobspec.parse({"spec": {"workload": {"type": "python.map", "code": "def process(x):\n    return x\n"}, "inputs": {"items": [1]},
                                    "runtime": {"kind": "managed-python", "python": "3.11", "requirements": []}}})
    for job_id, spec, note in (("j1", doc, "Preparing its Python environment on Alpha."), ("j2", other, "Preparing its Python environment on Alpha."),
                               ("j3", doc, "Spread over 2 computers.")):
        db.insert("jobs", {"id": job_id, "name": "t", "spec": spec, "status": "running", "note": note, "created_at": 0, "updated_at": 0})
    rm._ready_notes(rm.fp(doc), SimpleNamespace(name="Alpha"))
    assert db.job("j1")["note"] == "Its Python environment is ready on Alpha."
    assert db.job("j2")["note"].startswith("Preparing"), "a job waiting for another environment still waits"
    assert db.job("j3")["note"] == "Spread over 2 computers.", "a note about something else is left alone"


def test_a_computer_that_restarted_while_preparing_is_not_waited_for_forever():
    doc = {"spec": {"runtime": {"kind": "managed-python", "python": "3.12", "requirements": []}}}
    stopped = []
    engine = SimpleNamespace(db=SimpleNamespace(all=lambda q, args=(): [{"id": "j1", "spec": doc}]), s=SimpleNamespace(devices={}),
                             _changed=lambda job_id: None, _stop_job=lambda job_id, status, error: stopped.append((job_id, status, error)))
    rm = RuntimeManager(engine)
    fp = rm.fp(doc)
    d = SimpleNamespace(id="a", name="Alpha", owned=True, closed=False, engine_info={"envs": [], "building": [fp], "envErrors": {}})
    engine.s.devices["a"] = d
    rm.status[("a", fp)] = {"phase": "packages", "note": "Installing", "error": None, "at": 0}
    rm.device_connected(d)  # it reconnected and is still building: keep waiting
    assert rm.status[("a", fp)]["phase"] == "packages" and not stopped
    d.engine_info = {"envs": [], "building": [], "envErrors": {}}
    rm.device_connected(d)  # it came back without the build: that try is over
    assert rm.status[("a", fp)]["phase"] == "failed" and stopped and "restarted" in stopped[0][2]
    rm.status[("a", fp)] = {"phase": "python", "note": "", "error": None, "at": 0}
    d.engine_info = {"envs": [], "building": [], "envErrors": {fp: "uv: no Python 3.12 found"}}
    rm.device_connected(d)  # the build failed while it was away: say why
    assert "no Python 3.12" in rm.status[("a", fp)]["error"]
    rm.status[("a", fp)] = {"phase": "python", "note": "", "error": None, "at": 0}
    d.engine_info = {"envs": [fp], "building": [], "envErrors": {}}
    rm.device_connected(d)  # it finished while it was away
    assert ("a", fp) not in rm.status and rm.why_not(doc, d) is None
    friend = SimpleNamespace(id="f", name="Friend", owned=False, closed=False, engine_info={"envs": [fp]})
    assert "friend's computer" in rm.why_not(doc, friend), "a friend's computer never runs your own code"


def test_an_environment_id_depends_only_on_python_and_requirements():
    a = fingerprint("3.12", ["numpy==2.1.3", "pandas>=2"])
    assert a == fingerprint("3.12", ["pandas>=2", "numpy==2.1.3"])
    assert a != fingerprint("3.11", ["numpy==2.1.3", "pandas>=2"])
    assert a.startswith("env-") and len(a) == 24
    assert [import_name(r) for r in ("scikit-learn>=1.5", "Pillow", "torch[cpu]==2.5", "py-yaml")] == ["sklearn", "PIL", "torch", "py_yaml"]


def test_unused_environments_go_first_when_the_cache_is_full(tmp_path):
    m = EnvManager(tmp_path, lambda msg: None)
    m.cap = 2500
    for i, fp in enumerate(["env-" + str(i) * 20 for i in range(4)]):
        d = tmp_path / "envs" / fp
        d.mkdir(parents=True)
        (d / "ready.json").write_text(json.dumps({"env": fp, "size": 1000}))
        os.utime(d / "ready.json", (time.time() - 100 + i, time.time() - 100 + i))
    m.in_use["env-" + "0" * 20] = 1  # in use: never deleted, even though it's the oldest
    gone = m.evict()
    assert gone == ["env-" + "1" * 20, "env-" + "2" * 20]
    assert sorted(m.ready_list()) == ["env-" + "0" * 20, "env-" + "3" * 20]


WHERE = {"type": "python.map", "code": "import sys\ndef process(x):\n    return [x * 2, sys.prefix]\n"}


@needs_uv
def test_a_job_with_its_own_environment_waits_for_approval_then_runs_inside_it(tmp_path):
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Alpha", threads=2, extra=("--no-tasks",))
        coord.agent("Beta", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Alpha", "Beta"})
        runtime = {"kind": "managed-python", "python": PY, "requirements": []}
        plan = eng.c.request("POST", "/api/engine/plan", {"job": eng.job_doc(WHERE, items=list(range(12)), runtime=runtime)})["plan"]
        assert plan["decision"] == "NEEDS_SETUP_APPROVAL"
        assert {s["name"] for s in plan["setup"]} == {"Alpha", "Beta"} and "uv venv --python" in plan["setup"][0]["steps"][0]
        job = eng.start(WHERE, items=list(range(12)), runtime=runtime, mode="distributed", partitioning={"itemsPerPartition": 2})
        assert job["status"] == "needs_approval" and job["setupPlan"]
        time.sleep(1)
        assert eng.get(job["id"])["counts"] == {"ready": 6}, "nothing runs before the setup is approved"
        eng.c.request("POST", f"/api/engine/jobs/{job['id']}/approve-setup", {"downloads": False})
        job = eng.wait(job["id"], timeout=180)
        assert job["status"] == "done", job
        values = job["result"]["value"]
        assert [v[0] for v in values] == [x * 2 for x in range(12)]
        assert all("/envs/env-" in v[1].replace("\\", "/") for v in values), "the code ran in the prepared environment"
        assert {s["phase"] for s in eng.get(job["id"])["setup"]} == {"ready"}
        # the same environment again: no approval, no setup
        t0 = time.monotonic()
        again = eng.wait(eng.start(WHERE, items=[5], runtime=runtime)["id"])
        assert again["status"] == "done" and again["decision"] != "NEEDS_SETUP_APPROVAL" and time.monotonic() - t0 < 30
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


@needs_uv
def test_a_setup_that_fails_says_why_and_leaves_nothing_half_built(tmp_path):
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Alpha", threads=1, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Alpha"})
        runtime = {"kind": "managed-python", "python": PY, "requirements": ["yourharness-no-such-package==9.9.9"]}
        job = eng.start(WHERE, items=[1, 2], runtime=runtime)
        assert job["status"] == "needs_approval"
        eng.c.request("POST", f"/api/engine/jobs/{job['id']}/approve-setup", {"downloads": False})
        job = eng.wait(job["id"], timeout=180)
        assert job["status"] == "failed" and "couldn't be prepared" in job["error"], job
        envs = tmp_path / "c" / "agent-Alpha" / "envs"
        assert not envs.exists() or not any(p.name.startswith(".") for p in envs.iterdir()), "no half-built environment is left"
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


def plus_package(x):
    import yhpkg_a

    return x + yhpkg_a.VALUE


def plus_other_package(x):
    import yhpkg_b

    return x + yhpkg_b.VALUE


@needs_uv
def test_packages_install_once_and_the_command_line_and_sdk_ask_before_setting_up(tmp_path, monkeypatch):
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Alpha", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Alpha"})
        wheels = tmp_path / "wheels"
        wheels.mkdir()
        req_a, req_b = make_wheel(wheels, "yhpkg_a", 40), make_wheel(wheels, "yhpkg_b", 1000)
        env = {**os.environ, "HARNESS_URL": coord.base, "HARNESS_TOKEN": eng.c.token}
        harness = [sys.executable, "-m", "yourharness"]
        code = "def process(x):\n    import yhpkg_a\n    return x + yhpkg_a.VALUE\n"
        job_file = tmp_path / "job.json"
        job_file.write_text(json.dumps(eng.job_doc({"type": "python.map", "code": code}, items=[1, 2, 3],
                                                   runtime={"kind": "managed-python", "python": PY, "requirements": [req_a]})))
        # the command line stops and says what would be installed, and how to approve it
        run = subprocess.run(harness + ["job", "run", str(job_file)], env=env, capture_output=True, text=True, timeout=60)
        assert run.returncode == 2, run.stderr
        assert "uv pip install yhpkg_a @ file:" in run.stderr and "harness job approve" in run.stderr
        job_id = re.search(r"harness job approve (j[0-9a-f]+)", run.stderr).group(1)
        approved = subprocess.run(harness + ["job", "approve", job_id], env=env, capture_output=True, text=True, timeout=60)
        assert approved.returncode == 0 and "Approved." in approved.stdout, approved.stderr
        waited = subprocess.run(harness + ["job", "wait", job_id], env=env, capture_output=True, text=True, timeout=240)
        assert waited.returncode == 0 and "41" in waited.stdout and "43" in waited.stdout, (waited.stdout, waited.stderr)
        envs = [p for p in (tmp_path / "c" / "agent-Alpha" / "envs").iterdir() if not p.name.startswith(".")]
        info = json.loads((envs[0] / "ready.json").read_text())
        assert info["imports"] == {"yhpkg_a": "ok"} and any(p.startswith("yhpkg") for p in info["packages"])

        # the Python SDK: the prepared environment is reused, a new one asks first
        from yourharness import jobs

        monkeypatch.setenv("HARNESS_URL", coord.base)
        monkeypatch.setenv("HARNESS_TOKEN", eng.c.token)
        jobs._client = None
        assert jobs.map_items(plus_package, [0, 10], python=PY, requirements=[req_a]) == [40, 50]
        with pytest.raises(jobs.SetupNeeded) as e:
            jobs.map_items(plus_other_package, [1], python=PY, requirements=[req_b])
        assert "yhpkg_b" in str(e.value) and e.value.job["status"] == "needs_approval"
        eng.c.request("POST", f"/api/engine/jobs/{e.value.job['id']}/cancel", {})
        assert jobs.map_items(plus_other_package, [1, 2], python=PY, requirements=[req_b], approve_setup=True) == [1001, 1002]
        assert len([p for p in (tmp_path / "c" / "agent-Alpha" / "envs").iterdir() if not p.name.startswith(".")]) == 2
        devs = {d["name"]: d for d in eng.c.devices()}
        assert devs["Alpha"]["engine"]["envs"] == 2
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()
