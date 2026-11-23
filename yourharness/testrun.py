"""Run a project's pytest suite across your computers, from the computer you're on.

    harness test [PROJECT] [--shards N] [--reruns N] [--junit report.xml] [-- PYTEST OPTIONS]

This collects the test ids here (pytest --collect-only, with --rootdir set to the project),
packs the project into a .zip (the files git tracks, or everything but caches and virtual
environments), uploads it with its SHA-256, and starts a tests.pytest job: shards of about
equal running time, using how long each test took last time (kept in
~/.yourharness/test-durations/), each run on one of your computers. It prints the counts,
every test that failed, errored, went missing or was flaky, and writes a JUnit XML report
when asked. The exit code is 0 when every test passed or was skipped, 1 otherwise, 2 when
the job waits for a setup approval.

The computers run the tests with the Python the job says: the agent's own by default (it
needs pytest and the project's packages), a managed environment (--python, --requirement),
or a container image (--container IMAGE@sha256:...).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

SKIP_DIRS = {".git", ".hg", ".svn", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".tox", ".nox", ".venv", "venv",
             "env", "node_modules", ".eggs", "build", "dist", ".idea", ".vscode"}
MAX_FILES = 50_000


class TestSetupError(RuntimeError):
    pass


def collect(project: Path, args: list[str], python: str | None = None) -> list[str]:
    """The test ids pytest collects in the project, named relative to it."""
    cmd = [python or sys.executable, "-m", "pytest", "--collect-only", "-q", f"--rootdir={project}", "-p", "no:cacheprovider", *args]
    proc = subprocess.run(cmd, cwd=project, capture_output=True, text=True, timeout=900, stdin=subprocess.DEVNULL)
    ids = [ln.strip() for ln in proc.stdout.splitlines() if "::" in ln and not ln.startswith((" ", "<", "="))]
    if proc.returncode not in (0, 5) or (not ids and proc.returncode != 5):
        tail = (proc.stdout + proc.stderr).strip()[-2000:]
        raise TestSetupError(f"pytest couldn't collect the tests here (exit code {proc.returncode}):\n{tail}")
    return list(dict.fromkeys(ids))


def project_files(project: Path) -> list[Path]:
    """The files to send: what git tracks (and untracked files it doesn't ignore), else everything but caches."""
    try:
        out = subprocess.run(["git", "-C", str(project), "ls-files", "-co", "--exclude-standard", "-z"], capture_output=True, timeout=60)
        if out.returncode == 0:
            files = [project / p for p in out.stdout.decode("utf-8", "replace").split("\0") if p]
            return [f for f in files if f.is_file() and not (set(f.relative_to(project).parts) & {".git"})]
    except (OSError, subprocess.TimeoutExpired):
        pass
    found = []
    for root, dirs, names in os.walk(project):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.endswith(".egg-info")]
        found += [Path(root) / n for n in names if not n.endswith((".pyc", ".pyo"))]
    return found


def pack(project: Path, dest: Path) -> tuple[int, str]:
    """A .zip of the project at dest: (files, SHA-256). Symbolic links are stored as the files they point to inside it."""
    files = sorted(project_files(project))
    if len(files) > MAX_FILES:
        raise TestSetupError(f"the project has more than {MAX_FILES:,} files; run it from a smaller folder")
    root = project.resolve()
    n = 0
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for f in files:
            real = f.resolve()
            if root not in real.parents and real != root:  # a link that leaves the project: not sent
                continue
            info = zipfile.ZipInfo(f.relative_to(project).as_posix(), date_time=(1980, 1, 1, 0, 0, 0))  # the same files make the same archive
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o755 if os.access(real, os.X_OK) else 0o644) << 16
            z.writestr(info, real.read_bytes())
            n += 1
    h = hashlib.sha256()
    with open(dest, "rb") as f:
        while data := f.read(1 << 20):
            h.update(data)
    return n, h.hexdigest()


def durations_path(project: Path) -> Path:
    from .client import home

    key = hashlib.sha256(str(project.resolve()).encode("utf-8")).hexdigest()[:24]
    return home() / "test-durations" / f"{key}.json"


def load_durations(project: Path) -> dict:
    try:
        return json.loads(durations_path(project).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_durations(project: Path, durations: dict) -> None:
    p = durations_path(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    old = load_durations(project)
    p.write_text(json.dumps({**old, **durations}), encoding="utf-8")


def run(project: str | Path = ".", args: list[str] | None = None, *, shards: int | None = None, reruns: int = 1, python: str | None = None,
        requirements=None, container: str | None = None, limits: dict | None = None, mode: str = "distributed",
        approve_setup: bool = False, downloads: bool = False, spot_check: int = 0, quiet: bool = False) -> dict:
    """Collect, pack, upload, run; returns the finished job with its full result (outcomes, durations, messages)."""
    from .client import Client, PoolError
    from .jobs import SetupNeeded

    project = Path(project).resolve()
    args = list(args or [])
    tests = collect(project, args)
    if not tests:
        raise TestSetupError(f"pytest found no tests in {project}")
    c = Client()
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / f"{project.name or 'project'}.zip"
        files, sha = pack(project, archive)
        model = c.add_model(archive, name=archive.name, adapter="project")
    if model["sha"] != sha:
        raise PoolError("the coordinator stored a different archive than was sent")
    runtime = None
    if container:
        runtime = {"kind": "isolated-oci", "image": container, **({"limits": limits} if limits else {})}
    elif python or requirements:
        import platform

        runtime = {"kind": "managed-python", "python": python or ".".join(platform.python_version_tuple()[:2]), "requirements": list(requirements or [])}
    wl = {"type": "tests.pytest", "tests": tests, "durations": load_durations(project), "args": args, "reruns": reruns}
    if shards:
        wl["shards"] = shards
    job = {"apiVersion": "yourharness.dev/v1alpha1", "kind": "Job", "metadata": {"name": f"tests: {project.name}"},
           "spec": {"mode": mode, "workload": wl, "inputs": {"project": {"ref": model["ref"], "sha256": sha}},
                    **({"runtime": runtime} if runtime else {}), "output": {"spotCheck": spot_check}}}
    started = c.start_job(job)
    if started["status"] == "needs_approval":
        if not approve_setup:
            raise SetupNeeded(started)
        started = c.approve_setup(started["id"], downloads=downloads)
    if not quiet:
        print(f"{len(tests):,} tests from {files:,} files, in {started['total']} shards. Job {started['id']}.", file=sys.stderr, flush=True)
    done = c.wait_engine_job(started["id"], quiet=quiet)
    if done["status"] == "needs_approval":
        raise SetupNeeded(done)
    if done["status"] == "done":
        full = done["result"]
        if done.get("resultTruncated"):  # a large suite: the page got a preview, the whole result is a download
            got = c.engine_result(done["id"])
            full = got if isinstance(got, dict) else json.loads(got)
        done = {**done, "result": full}
        if full.get("durations"):
            save_durations(project, full["durations"])
    return done


def summary(job: dict) -> str:
    r = job.get("result") or {}
    if job["status"] != "done":
        return f"The test job {job['status']}: {job.get('error') or job.get('note') or ''}".strip()
    counts = [f"{r.get(k, 0):,} {k}" for k in ("passed", "failed", "error", "skipped", "xfailed", "xpassed", "flaky", "missing") if r.get(k)]
    per = ", ".join(f"{d['name']} {d['n']}" for d in job.get("perDevice", []))
    took = (job.get("finishedAt") or 0) - (job.get("startedAt") or 0)
    lines = [f"{', '.join(counts) or 'no tests'} in {took:.1f} s ({job['total']} shards{f': {per}' if per else ''}; "
             f"{r.get('testSeconds', 0):.1f} s of test time)"]
    for row in r.get("notPassing", [])[:50]:
        msg = (row.get("message") or "").strip().splitlines()
        why = next((ln[1:].strip() for ln in msg if ln.startswith("E ")), msg[-1] if msg else "")  # pytest's "E   ..." lines say what failed
        lines.append(f"  {row['outcome'].upper():<7} {row['test']}" + (f"\n          {why[:300]}" if why else ""))
    for row in r.get("flakyTests", [])[:50]:
        lines.append(f"  FLAKY   {row['test'] if isinstance(row, dict) else row} (failed, then passed on a rerun)")
    if len(r.get("notPassing", [])) > 50:
        lines.append(f"  … and {len(r['notPassing']) - 50} more: see the JUnit report (--junit)")
    return "\n".join(lines)
