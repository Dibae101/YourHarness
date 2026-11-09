"""Who may run what, containers, revoking a device, and HTTPS.

The container tests run against a stand-in for Docker (FAKE below) that runs the engine's
worker on this computer with the container's folders mapped, and answers the isolation check
as told; it checks the plumbing: approval, the check, the flags, files in and out, no token
inside, no fallback. test_a_real_container_* runs the same job in a real Docker container
wherever Docker is running (it is on the Linux CI runners) and checks the isolation itself.
"""

from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from conftest import start_coordinator
from test_engine_e2e import AGG, Engine, sales
from yourharness.engine import isolation
from yourharness.engine import spec as jobspec
from yourharness.engine.spec import SpecError

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "registry.example/yh/python:3.12@sha256:" + "ab" * 32
LIMITS = {"cpus": 1.5, "memoryMB": 512, "pids": 128}
DOUBLE = {"type": "python.map", "code": "def process(x):\n    return x * 2\n"}


class Dev:
    def __init__(self, owned: bool, kind: str = "agent"):
        self.owned, self.kind = owned, kind


def parse(workload, runtime=None, **spec):
    doc = {"apiVersion": jobspec.API_VERSION, "kind": "Job", "metadata": {"name": "t"},
           "spec": {"workload": workload, "inputs": {"items": [1, 2]}, **({"runtime": runtime} if runtime else {}), **spec}}
    return jobspec.parse(doc)


# ---------------------------------------------------------------------- the rules
def test_the_trust_rules():
    rules = {(r["runtime"], r["tier"]): r["allowed"] for r in isolation.table()["rules"]}
    assert all(rules[("builtin", t)] for t in isolation.TIERS), "the engine's own operators run anywhere"
    for rt in ("native-python", "managed-python", "isolated-oci"):
        assert rules[(rt, "owned")] and not rules[(rt, "friend")] and not rules[(rt, "browser")], rt
    assert "not even in a container" in isolation.runtime_rule("isolated-oci", "friend")
    spec = parse(DOUBLE)["spec"]
    assert isolation.admit(spec, Dev(True), None, None) is None
    assert "own computers only" in isolation.admit(spec, Dev(False), None, None)
    shared = {"classification": "shareable"}
    agg = jobspec.parse({"spec": {"workload": AGG, "inputs": {"dataset": {"ref": "dataset:d12345678"}},
                                  "security": {"allowedTrust": "owned-and-friends"}}})["spec"]
    assert isolation.admit(agg, Dev(False), shared, None) is None
    assert "isn't marked shareable" in isolation.admit(agg, Dev(False), {"classification": "owned-private"}, None)
    assert isolation.admit(agg, Dev(False, "browser"), shared, None) is None


def test_a_container_job_names_its_image_by_digest_and_gets_no_network():
    rt = parse(DOUBLE, {"kind": "isolated-oci", "image": IMAGE, "limits": {"memoryMB": 512}})["spec"]["runtime"]
    assert rt == {"kind": "isolated-oci", "image": IMAGE, "network": "none", "limits": {"cpus": 1.0, "memoryMB": 512, "pids": 256}}
    for bad, field, words in (
        ({"kind": "isolated-oci", "image": "python:3.12-slim"}, "spec.runtime.image", "pinned by its digest"),
        ({"kind": "isolated-oci", "image": "python:latest"}, "spec.runtime.image", "can change under you"),
        ({"kind": "isolated-oci", "image": IMAGE, "network": "bridge"}, "spec.runtime.network", "no network"),
        ({"kind": "isolated-oci", "image": IMAGE, "limits": {"memoryMB": 16}}, "spec.runtime.limits.memoryMB", "between"),
        ({"kind": "native-python", "isolation": "isolated"}, "spec.runtime.isolation", "aren't sandboxes"),
        ({"kind": "isolated-oci", "image": IMAGE, "isolation": "sort-of"}, "spec.runtime.isolation", "owned-trusted"),
    ):
        with pytest.raises(SpecError) as e:
            parse(DOUBLE, bad)
        assert e.value.path == field and words in str(e.value), (bad, str(e.value))
    with pytest.raises(SpecError) as e:  # a container doesn't make a friend's computer a place for your code
        parse(DOUBLE, {"kind": "isolated-oci", "image": IMAGE}, security={"allowedTrust": "owned-and-friends"})
    assert "even when the job uses a container" in str(e.value)
    with pytest.raises(SpecError):
        parse(AGG, {"kind": "isolated-oci", "image": IMAGE})  # the engine's operators don't take a runtime


def test_the_check_inside_a_container_catches_what_isnt_isolated():
    from yourharness.runtime.oci import problems

    good = {"python": "3.12.1", "uid": 1000, "network": "blocked", "dns": "blocked", "rootfs": "read-only", "input": "read-only",
            "output": "writable", "tmp": "writable", "memory": str(512 * 2**20), "pids": "128", "sockets": [], "secrets": [], "worker": "ok"}
    assert problems(good, LIMITS, rootless=False) == []
    for change, words in (({"uid": 0}, "as root"), ({"network": "reachable"}, "network"), ({"dns": "resolves"}, "network"),
                          ({"rootfs": "writable"}, "system files"), ({"input": "writable"}, "input folder"), ({"memory": "max"}, "memory limit"),
                          ({"pids": "max"}, "process limit"), ({"sockets": ["/var/run/docker.sock"]}, "engine socket"),
                          ({"secrets": ["HARNESS_TOKEN"]}, "tokens"), ({"python": "3.8.10"}, "3.10 or newer"), ({"worker": "ImportError: x"}, "worker")):
        found = problems({**good, **change}, LIMITS, rootless=False)
        assert len(found) == 1 and words in found[0], (change, found)
    assert problems({**good, "uid": 0}, LIMITS, rootless=True) == [], "root in a rootless engine is the user's own account"


# ---------------------------------------------------------------------- containers (a stand-in for Docker)
FAKE = r'''
import json, os, subprocess, sys
from pathlib import Path
state = Path(os.environ["FAKE_OCI_STATE"])
args = sys.argv[1:]
with open(state / "calls.jsonl", "a") as f:
    f.write(json.dumps(args) + "\n")
images_file = state / "images.json"
images = json.loads(images_file.read_text()) if images_file.exists() else []
cmd = args[0]
if cmd == "version":
    print("99.0-fake")
elif cmd == "info":
    print("[name=seccomp,profile=builtin]")
elif cmd == "image" and args[1] == "inspect":
    sys.exit(0 if args[-1] in images else 1)
elif cmd == "image" and args[1] == "rm":
    images_file.write_text(json.dumps([i for i in images if i != args[-1]]))
elif cmd == "pull":
    images_file.write_text(json.dumps(images + [args[1]]))
elif cmd == "kill":
    pass
elif cmd == "run":
    i = next(k for k, a in enumerate(args) if "@sha256:" in a)
    opts, image, rest = args[1:i], args[i], args[i + 1:]
    if image not in images:
        sys.exit(125)
    mounts = {}
    for k, a in enumerate(opts):
        if a == "--mount":
            spec = dict(x.split("=", 1) for x in opts[k + 1].split(",") if "=" in x)
            mounts[spec["target"]] = spec["source"]
    if rest[:1] == ["-c"]:  # the isolation check: answer as an isolated container would, unless told otherwise
        mode = os.environ.get("FAKE_OCI_MODE", "isolated")
        out = {"python": "3.12.1", "uid": 1000, "network": "reachable" if mode == "leaky" else "blocked", "dns": "blocked",
               "rootfs": "read-only", "input": "read-only", "output": "writable", "tmp": "writable",
               "memory": str(int(opts[opts.index("--memory") + 1].rstrip("m")) * 2**20), "pids": opts[opts.index("--pids-limit") + 1],
               "sockets": [], "secrets": [], "worker": "ok"}
        print("YH-PREFLIGHT " + json.dumps(out))
        sys.exit(0)
    job = json.loads(sys.stdin.readline())
    if "token" in job or "server" in job or "ca" in job:
        print("the container was given the coordinator's address or a token", file=sys.stderr)
        sys.exit(3)
    job.update(staged=mounts["/in"], upDir=mounts["/out"] + "/up", outDir=mounts["/out"] + "/work", cache=mounts["/out"] + "/cache")
    env = {**os.environ, "PYTHONPATH": str(Path(mounts["/yh/yourharness"]).parent)}
    sys.exit(subprocess.run([sys.executable, "-m", "yourharness.engine.worker"], input=json.dumps(job) + "\n", text=True, env=env).returncode)
else:
    sys.exit(2)
'''


def fake_engine(tmp: Path, mode: str = "isolated") -> dict:
    state = tmp / "fake-oci"
    state.mkdir(parents=True, exist_ok=True)
    exe = tmp / "bin" / "fake-docker"
    exe.parent.mkdir(parents=True, exist_ok=True)
    exe.write_text(f"#!{sys.executable}\n{FAKE}")
    exe.chmod(0o755)
    return {"HARNESS_OCI_ENGINE": str(exe), "FAKE_OCI_STATE": str(state), "FAKE_OCI_MODE": mode}


def calls(env: dict) -> list[list[str]]:
    return [json.loads(line) for line in (Path(env["FAKE_OCI_STATE"]) / "calls.jsonl").read_text().splitlines()]


def wait_runtime(eng: Engine, name: str, runtime: str = "isolated-oci", timeout: float = 20) -> dict:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        d = next((d for d in eng.c.devices() if d["name"] == name), None)
        if d and runtime in ((d.get("engine") or {}).get("runtimes") or []):
            return d
        time.sleep(0.2)
    raise AssertionError(f"{name} never reported {runtime}")


@pytest.mark.skipif(os.name == "nt", reason="the stand-in engine is a POSIX script")
def test_a_job_in_a_container_waits_for_approval_runs_with_no_network_and_starts_at_once_next_time(tmp_path, monkeypatch):
    fake = fake_engine(tmp_path)
    for k, v in fake.items():
        monkeypatch.setenv(k, v)
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Box", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Box"})
        box = wait_runtime(eng, "Box")
        assert box["engine"]["oci"] == {"engine": "docker", "version": "99.0-fake", "rootless": False}
        runtime = {"kind": "isolated-oci", "image": IMAGE, "limits": LIMITS}
        job = eng.start(DOUBLE, items=list(range(20)), runtime=runtime, mode="distributed", partitioning={"itemsPerPartition": 5})
        assert job["status"] == "needs_approval"
        steps = job["setupPlan"][0]["steps"]
        assert steps[0].startswith(f"docker pull {IMAGE}") and "no network" in steps[1] and "512 MB memory" in steps[1]
        time.sleep(0.5)
        assert eng.get(job["id"])["counts"] == {"ready": 4}, "nothing runs before the approval"
        eng.c.request("POST", f"/api/engine/jobs/{job['id']}/approve-setup", {"downloads": True})
        job = eng.wait(job["id"], timeout=120)
        assert job["status"] == "done", job
        assert job["result"]["value"] == [x * 2 for x in range(20)]
        env = eng.get(job["id"])["provenance"]["environment"]
        assert env["kind"] == "isolated-oci" and env["image"] == IMAGE and env["network"] == "none" and env["limits"] == LIMITS
        log = calls(fake)
        assert ["pull", IMAGE] in log, "pulled only after the approval allowed downloads"
        runs = [c for c in log if c[0] == "run" and c[-2:] == ["-m", "yourharness.engine.worker"]]
        assert len(runs) == 4
        flags = runs[0]
        for want in (["--network", "none"], ["--read-only"], ["--cap-drop", "ALL"], ["--security-opt", "no-new-privileges"], ["--pull", "never"],
                     ["--memory", "512m"], ["--memory-swap", "512m"], ["--pids-limit", "128"], ["--cpus", "1.5"], ["--entrypoint", "python3"]):
            assert any(flags[i:i + len(want)] == want for i in range(len(flags))), want
        mounts = [flags[i + 1] for i, a in enumerate(flags) if a == "--mount"]
        assert len(mounts) == 3 and all(",readonly" in m for m in mounts if "target=/out" not in m)
        assert not any("--privileged" in a or "docker.sock" in a or a.startswith("--volume") for a in flags)
        # the same image with the same limits: no approval, no new check
        before = len(calls(fake))
        again = eng.wait(eng.start(DOUBLE, items=[7], runtime=runtime)["id"])
        assert again["status"] == "done" and again["result"]["value"] == [14]
        assert not any(c[0] in ("pull", "image") for c in calls(fake)[before:])
        # different limits are a different check
        other = eng.start(DOUBLE, items=[1], runtime={**runtime, "limits": {**LIMITS, "memoryMB": 1024}})
        assert other["status"] == "needs_approval"
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


@pytest.mark.skipif(os.name == "nt", reason="the stand-in engine is a POSIX script")
def test_a_container_that_isnt_isolated_never_runs_the_job_and_nothing_falls_back(tmp_path, monkeypatch):
    fake = fake_engine(tmp_path, mode="leaky")
    for k, v in fake.items():
        monkeypatch.setenv(k, v)
    Path(fake["FAKE_OCI_STATE"], "images.json").write_text(json.dumps([IMAGE]))  # already on the computer
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Box", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Box"})
        wait_runtime(eng, "Box")
        job = eng.start(DOUBLE, items=[1, 2, 3], runtime={"kind": "isolated-oci", "image": IMAGE, "limits": LIMITS})
        eng.c.request("POST", f"/api/engine/jobs/{job['id']}/approve-setup", {"downloads": False})
        job = eng.wait(job["id"], timeout=60)
        assert job["status"] == "failed" and "couldn't be checked" in job["error"] and "could reach the network" in job["error"], job
        assert job["perDevice"] == [], "no partition ran, in a container or outside one"
        assert not any(c[0] == "pull" for c in calls(fake)), "downloads weren't allowed"
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


def _docker_works() -> bool:
    if os.name == "nt" or not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(not _docker_works() and os.environ.get("YH_EXPECT_DOCKER") != "1", reason="needs a running Docker")
def test_a_real_container_has_no_network_no_host_files_and_its_limits(tmp_path):
    assert _docker_works(), "YH_EXPECT_DOCKER is set (the Linux CI runners), so Docker must be running here"
    subprocess.run(["docker", "pull", "-q", "python:3.12-slim"], check=True, capture_output=True, timeout=600)
    digest = subprocess.run(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", "python:3.12-slim"],
                            check=True, capture_output=True, text=True).stdout.strip()
    coord = start_coordinator(tmp_path / "c")
    try:
        coord.agent("Box", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Box"})
        wait_runtime(eng, "Box", timeout=60)
        home = str(tmp_path / "c")
        code = ("import os, socket\n"
                "def process(x):\n"
                "    try:\n"
                "        socket.create_connection(('1.1.1.1', 53), timeout=2).close()\n"
                "        net = 'reachable'\n"
                "    except OSError:\n"
                "        net = 'blocked'\n"
                f"    return [x * 2, net, os.getuid(), os.path.exists({home!r}), open('/sys/fs/cgroup/memory.max').read().strip()]\n")
        job = eng.start({"type": "python.map", "code": code}, items=list(range(6)), mode="distributed", partitioning={"itemsPerPartition": 3},
                        runtime={"kind": "isolated-oci", "image": digest, "limits": {"memoryMB": 256, "pids": 64}})
        assert job["status"] == "needs_approval"
        eng.c.request("POST", f"/api/engine/jobs/{job['id']}/approve-setup", {"downloads": False})
        job = eng.wait(job["id"], timeout=300)
        assert job["status"] == "done", job
        for i, (doubled, net, uid, sees_home, mem) in enumerate(job["result"]["value"]):
            assert doubled == i * 2
            assert net == "blocked", "the container reached the network"
            assert uid != 0 and not sees_home and mem == str(256 * 2**20)
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


# ---------------------------------------------------------------------- revoking a device
def test_removing_a_friends_computer_hands_back_its_work_cleans_it_up_and_keeps_the_record(tmp_path):
    coord = start_coordinator(tmp_path / "c")
    try:
        eng = Engine(coord)
        code = eng.c.request("POST", "/api/invites", {"label": "Sam"})["code"]
        sam = coord.agent("Sams PC", threads=2, link=code, extra=("--server", coord.base, "--no-tasks"))
        coord.agent("Mine", threads=2, extra=("--no-tasks",))
        eng.ready({"Sams PC", "Mine"})
        ds = eng.upload("shared.jsonl", sales(3000), classification="shareable")
        job = eng.run(AGG, ds, mode="distributed", partitioning={"targetBytes": 8 * 1024}, security={"allowedTrust": "owned-and-friends"})
        assert job["status"] == "done" and "Sams PC" in {d["name"] for d in job["perDevice"]}, job
        sam_home = tmp_path / "c" / "agent-Sams-PC"
        (sam_home / "engine-cache" / "artifacts").mkdir(parents=True, exist_ok=True)
        (sam_home / "engine-cache" / "artifacts" / "leftover").write_bytes(b"pool data")
        sam_id = next(d["id"] for d in eng.c.devices() if d["name"] == "Sams PC")
        eng.c.request("DELETE", f"/api/devices/{sam_id}")
        sam.wait_for("Deleted what this computer kept for the pool", timeout=20)
        assert sam.p.wait(10) == 1
        assert not (sam_home / "engine-cache").exists() and not (sam_home / "agent.json").exists()
        record = [json.loads(line) for line in (sam_home / "activity.jsonl").read_text().splitlines()]
        assert any(r["kind"] == "partition" and r["what"] == "finished" and r["job"] == job["id"] and r["workload"] == "batch.aggregate"
                   for r in record)
        assert any(r["kind"] == "pool" and "removed" in r["what"] for r in record)
        shown = subprocess.run([sys.executable, "-m", "yourharness", "agent", "--activity"], capture_output=True, text=True, cwd=ROOT,
                               env={**os.environ, "YOURHARNESS_HOME": str(sam_home)}, timeout=60).stdout
        assert f"finished a partition of job {job['id']} (batch.aggregate" in shown
        # the owner can still see what the removed computer had
        act = eng.c.request("GET", f"/api/devices/{sam_id}/activity")
        assert act["device"]["removed"] and act["device"]["friend"] and act["device"]["name"] == "Sams PC"
        (j,) = [j for j in act["jobs"] if j["job"] == job["id"]]
        assert j["dataset"] == {"name": "shared.jsonl", "classification": "shareable"} and j["accepted"] >= 1 and j["bytes"] > 0
        assert not j["code"] and j["workload"] == "batch.aggregate"
        actions = [a["action"] for a in act["audit"]]
        assert actions[:2] == ["removed", "joined"] and act["audit"][0]["by"] == "engine tests"
        assert any(a["action"] == "friend code used" for a in eng.c.request("GET", "/api/audit")["audit"])
        rules = eng.c.request("GET", "/api/engine/policy")["rules"]
        assert {"runtime": "isolated-oci", "tier": "friend", "allowed": False} == {k: r[k] for r in rules for k in ("runtime", "tier", "allowed")
                                                                                  if r["runtime"] == "isolated-oci" and r["tier"] == "friend"}
        mine = next(d["id"] for d in eng.c.devices() if d["name"] == "Mine")
        assert eng.c.request("GET", f"/api/devices/{mine}/activity")["device"]["friend"] is False
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


def test_removing_a_computer_mid_partition_gives_it_to_another_at_once(tmp_path):
    coord = start_coordinator(tmp_path / "c")
    try:
        eng = Engine(coord)
        coord.agent("Slow", threads=1, extra=("--no-tasks",))
        eng.ready({"Slow"})
        code = "import time\ndef process(x):\n    time.sleep(2)\n    return x + 1\n"
        job = eng.start({"type": "python.map", "code": code}, items=[1, 2], mode="distributed", partitioning={"itemsPerPartition": 2})
        end = time.monotonic() + 20
        while time.monotonic() < end and not any(p["status"] == "leased" for p in eng.get(job["id"])["partitions"]):
            time.sleep(0.1)
        coord.agent("Other", threads=1, extra=("--no-tasks",))
        eng.ready({"Other"})
        slow = next(d["id"] for d in eng.c.devices() if d["name"] == "Slow")
        t0 = time.monotonic()
        eng.c.request("DELETE", f"/api/devices/{slow}")
        job = eng.wait(job["id"], timeout=40)
        assert job["status"] == "done" and job["result"]["value"] == [2, 3], job
        assert time.monotonic() - t0 < 30, "it didn't wait for the 45 s lease to run out"
        lost = [e for e in eng.get(job["id"])["events"] if e["type"] == "lost"]
        assert lost and "Slow was removed from the pool" in lost[0]["data"]["why"]
        assert {d["name"] for d in job["perDevice"]} == {"Other"}
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()


# ---------------------------------------------------------------------- HTTPS
def _make_ca_and_cert(d: Path) -> tuple[Path, Path, Path]:
    """A private authority and a certificate it signed for 127.0.0.1 and localhost, as mkcert would make."""
    openssl = shutil.which("openssl")
    run = lambda *a: subprocess.run([openssl, *a], check=True, capture_output=True, cwd=d, timeout=60)  # noqa: E731
    # the extensions strict verification (Python 3.13+) expects of an authority and of a server certificate
    (d / "ca.cnf").write_text("[req]\ndistinguished_name = dn\nprompt = no\nx509_extensions = v3_ca\n[dn]\nCN = Test authority\n"
                              "[v3_ca]\nbasicConstraints = critical, CA:TRUE\nkeyUsage = critical, keyCertSign, cRLSign\nsubjectKeyIdentifier = hash\n")
    (d / "leaf.cnf").write_text("[req]\ndistinguished_name = dn\nprompt = no\n[dn]\nCN = localhost\n")
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-config", "ca.cnf", "-keyout", "ca.key", "-out", "ca.pem")
    run("req", "-new", "-newkey", "rsa:2048", "-nodes", "-config", "leaf.cnf", "-keyout", "key.pem", "-out", "cert.csr")
    (d / "ext.cnf").write_text("subjectAltName = IP:127.0.0.1, DNS:localhost\nbasicConstraints = CA:FALSE\n"
                               "keyUsage = critical, digitalSignature, keyEncipherment\nextendedKeyUsage = serverAuth\n"
                               "subjectKeyIdentifier = hash\nauthorityKeyIdentifier = keyid, issuer\n")
    run("x509", "-req", "-in", "cert.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "2", "-extfile", "ext.cnf", "-out", "cert.pem")
    return d / "ca.pem", d / "cert.pem", d / "key.pem"


@pytest.mark.skipif(shutil.which("openssl") is None, reason="needs openssl to make a test certificate")
def test_an_https_pool(tmp_path, monkeypatch):
    certs = tmp_path / "certs"
    certs.mkdir()
    ca, cert, key = _make_ca_and_cert(certs)
    env = {**os.environ, "HARNESS_STALE_S": "2", "NO_QR": "1"}
    from conftest import Coord, Proc

    proc = Proc([sys.executable, "-m", "yourharness", "serve", "--host", "127.0.0.1", "--port", "0", "--data-dir", str(tmp_path / "c" / "data"),
                 "--shared-dir", str(tmp_path / "c" / "shared"), "--no-qr", "--no-local-agent", "--tls-cert", str(cert), "--tls-key", str(key)], env)
    coord = None
    try:
        port = int(proc.wait_for("Listening on port").rsplit(" ", 1)[1])
        assert any("https://" in line and "#join=" in line for line in proc.lines), "the printed join links are https"
        base = f"https://127.0.0.1:{port}"
        token = json.loads((tmp_path / "c" / "data" / "coordinator.json").read_text())["join_token"]
        coord = Coord(base, token, tmp_path / "c", proc)
        with pytest.raises(urllib.error.URLError):  # a client that doesn't trust the authority is refused, not downgraded
            urllib.request.urlopen(f"{base}/api/me", timeout=10)
        trusted = ssl.create_default_context(cafile=str(ca))
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(f"{base}/api/me", timeout=10, context=trusted)
        assert e.value.code == 401
        monkeypatch.setenv("HARNESS_CA_FILE", str(ca))  # what `harness agent --ca` sets for the agent and its workers
        coord.agent("Secure", threads=2, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Secure"})
        job = eng.run(DOUBLE, items=list(range(8)), mode="distributed", partitioning={"itemsPerPartition": 2})
        assert job["status"] == "done" and job["result"]["value"] == [x * 2 for x in range(8)], job
        ds = eng.upload("s.jsonl", sales(500))
        assert eng.run(AGG, ds, mode="distributed")["status"] == "done"
    finally:
        for a in coord.agents if coord else []:
            a.stop()
        proc.stop()
