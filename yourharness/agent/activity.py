"""What this computer did for a pool, kept on this computer, and cleaning up after leaving one.

Every partition and agent task this computer runs is written to activity.jsonl in its
YourHarness folder (~/.yourharness): when, for which pool, which job, what kind of work,
and how much it read. It's this computer's own record, so a friend who lends a computer
can see what it was asked to do without trusting the pool owner's word:

    harness agent --activity

When the computer is removed from a pool, a friend's computer deletes what it kept for
that pool at once: partition files and caches, downloaded models, prepared Python
environments, checked container images it pulled, agent task folders, and its sign-in.
An owner's computer keeps them (they may be your own work in progress) and says how to
delete them; on any computer:

    harness agent --cleanup

The activity record itself stays, and the YourHarness program and anything outside the
folders listed in POOL_DATA are never touched.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

LOG = "activity.jsonl"
MAX_BYTES = 5 * 2**20
# What a pool leaves in the YourHarness folder: partition work, caches, environments, task folders.
POOL_DATA = ("engine", "engine-cache", "work", "store", "taskrun", "tasks", "envs", "uv-cache", "oci", "run")


class ActivityLog:
    def __init__(self, home: Path, pool: str):
        self.path = Path(home) / LOG
        self.pool = pool

    def add(self, rec: dict) -> None:
        line = json.dumps({"at": round(time.time(), 3), "pool": self.pool, **rec}, separators=(",", ":"), default=str)
        try:
            if self.path.exists() and self.path.stat().st_size > MAX_BYTES:  # keep one older file
                self.path.replace(self.path.with_suffix(".jsonl.1"))
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass  # never stop work for the log


def read(home: Path, limit: int = 200) -> list[dict]:
    out = []
    for p in (Path(home) / (LOG + ".1"), Path(home) / LOG):
        try:
            for line in p.read_text(encoding="utf-8").splitlines():
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
        except OSError:
            continue
    return out[-limit:]


def describe(rec: dict) -> str:
    when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rec.get("at") or 0))
    pool = rec.get("pool") or "?"
    if rec.get("kind") == "partition":
        size = f", read {rec['bytes']:,} bytes" if isinstance(rec.get("bytes"), int) else ""
        return (f"{when}  {pool}  {rec.get('what')} a partition of job {rec.get('job') or '?'} ({rec.get('workload')}, "
                f"runtime {rec.get('runtime')}){size}")
    if rec.get("kind") == "task":
        return f"{when}  {pool}  {rec.get('what')} agent task {rec.get('task') or '?'} ({rec.get('agent') or 'commands'}) on {rec.get('repo') or '?'}"
    if rec.get("kind") == "pool":
        return f"{when}  {pool}  {rec.get('what')}"
    return f"{when}  {pool}  {json.dumps(rec)}"


def cleanup(home: Path, signin: bool = True) -> list[str]:
    """Delete what pools left in this computer's YourHarness folder (and its sign-in); returns what went."""
    home = Path(home)
    gone = []
    for name in POOL_DATA:
        p = home / name
        if p.is_symlink() or p.is_file():
            p.unlink(missing_ok=True)
            gone.append(name)
        elif p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
            gone.append(name)
    if signin and (home / "agent.json").exists():
        (home / "agent.json").unlink(missing_ok=True)
        gone.append("agent.json")
    return gone
