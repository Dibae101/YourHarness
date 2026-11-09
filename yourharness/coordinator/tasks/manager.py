"""Agent tasks: the coordinator's side.

A run is a list of tasks for one repository. Each task goes to a computer, either
the one you picked or any of your own. The computer works in its own Git worktree
at the run's base commit and sends its commits back as a bundle. The coordinator
checks them: the files the task may change, protected files, links, sizes and
anything that looks like a secret. It cherry-picks the accepted ones onto the
run's branch, pushes that branch, and opens a draft pull request if you asked for one.

The GitHub token never leaves this process. A computer gets a token that is good
for one attempt of one task: downloading its starting point and uploading its result.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import time
from pathlib import Path

from ..state import CAP_SUBMIT
from . import rules
from .db import TaskDB
from .github import GitHub, GitHubError
from .gitops import GitError, GitOps
from .rules import ADAPTERS, RunError
from .secrets import SecretStore

log = logging.getLogger("yourharness")

LEASE_S = float(os.environ.get("HARNESS_TASK_LEASE_S", 120))  # how long a disconnected computer keeps its task
MAX_LOST = 3
LOG_LIMIT = 5 * 1024 * 1024
ACTIVE = ("queued", "running", "verifying")
FINISHED = ("done", "failed", "cancelled")
WORKER_FIELDS = ("kind", "adapter", "model", "instructions", "command", "autonomy", "scope", "deny", "checks", "setup",
                 "time_limit_min", "max_usd", "rounds", "results_dir")
SPEC_FIELDS = ("title", "device", *WORKER_FIELDS)


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _cell(text) -> str:
    return str(text or "").replace("|", "\\|").replace("\n", " ")[:120]


def clean_result(res) -> dict:
    """What a computer reports about a finished attempt, trimmed to known fields."""
    res = res if isinstance(res, dict) else {}

    def num(v):
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    checks = []
    for c in (res.get("checks") or [])[:20]:
        if isinstance(c, dict):
            checks.append({"cmd": rules.clean_line(c.get("cmd"), 300), "code": c.get("code") if isinstance(c.get("code"), int) else None,
                           "ms": num(c.get("ms"))})
    timings = {k: int(v) for k, v in (res.get("timings_ms") or {}).items() if k in ("fetch", "setup", "agent", "checks", "commit", "upload") and num(v) is not None}
    commit = str(res.get("commit") or "")
    return {
        "summary": str(res.get("summary") or "")[:4000],
        "commit": commit if len(commit) in (40, 64) and all(c in "0123456789abcdef" for c in commit) else None,
        "files": int(num(res.get("files")) or 0),
        "noChanges": bool(res.get("no_changes")),
        "checks": checks,
        "rounds": int(num(res.get("rounds")) or 1),
        "usd": round(float(num(res.get("usd"))), 4) if num(res.get("usd")) is not None else None,
        "timings": timings,
        "adapterVersion": rules.clean_line(res.get("adapter_version"), 80),
    }


def pr_body(run: dict, tasks: list[dict]) -> str:
    rows, details = [], []
    for t in tasks:
        spec, res = t["spec"] or {}, t["result"] or {}
        agent = ADAPTERS.get(spec.get("adapter"), spec.get("adapter") or "")
        if spec.get("model"):
            agent += f" ({spec['model']})"
        state = "included" if t["integrated"] else t["status"]
        rows.append(f"| {_cell(t['title'])} | {_cell(t['device_name'])} | {_cell(agent)} | {state} | {res.get('changed') and len(res['changed']) or res.get('files', 0)} |")
        lines = [f"### {t['title']}"]
        if res.get("summary"):
            lines.append(res["summary"][:1500])
        for c in res.get("checks") or []:
            outcome = "passed" if c.get("code") == 0 else f"exit code {c.get('code')}"
            lines.append(f"- `{c.get('cmd')}`: {outcome}")
        if res.get("override"):
            lines.append(f"> Accepted by {res['override']} although the checks flagged: {'; '.join(res.get('problems') or [])[:500]}")
        if t["error"] and not t["integrated"]:
            lines.append(f"> {t['error'][:500]}")
        details.append("\n".join(lines))
    head = (f"Results of the YourHarness run **{run['title']}** (`{run['id']}`), based on `{run['base_branch']}` "
            f"at `{(run['base_commit'] or '')[:12]}`.\n\n| Task | Computer | Agent | Status | Files |\n|---|---|---|---|---|\n")
    return head + "\n".join(rows) + "\n\n" + "\n\n".join(details) + "\n\n---\nMade by YourHarness. Nothing was merged: review this and merge it yourself."


class TaskManager:
    def __init__(self, sched, data_dir: Path):
        self.s = sched
        self.dir = Path(data_dir) / "agent-tasks"
        for sub in ("logs", "results"):
            (self.dir / sub).mkdir(parents=True, exist_ok=True)
        self.db = TaskDB(self.dir / "tasks.db")
        self.secrets = SecretStore(data_dir)
        self.gitops = GitOps(self.dir, self.secrets)
        self.github = GitHub(self.secrets)
        self.grace: dict[str, float] = {}  # task id -> when its disconnected computer is given up on
        self.refused: dict[tuple[str, str], tuple[float, str]] = {}  # (task, device) -> (when, why)
        self.run_locks: dict[str, asyncio.Lock] = {}
        self.full_logs: set[str] = set()
        self._bg: set[asyncio.Future] = set()
        self._last_sweep = 0.0
        self.started = False

    # ------------------------------------------------------------------ start-up
    def start(self) -> None:
        """Pick up where a restart left off. Called once the event loop is running."""
        if self.started:
            return
        self.started = True
        now = time.time()
        for t in self.db.tasks_in(("running",)):
            self.grace[t["id"]] = now + LEASE_S  # its computer gets time to reconnect and say it's still at it
        for t in self.db.tasks_in(("verifying",)):
            self._spawn(self._verify(t["id"], t["attempt"]))
        for r in self.db.runs_in(("preparing",)):
            self._spawn(self._prepare(r["id"]))
        for r in self.db.runs_in(("integrating",)):
            self.db.update_run(r["id"], status="running")
            self._check_run(r["id"])

    def _spawn(self, coro) -> None:
        fut = asyncio.ensure_future(coro)
        self._bg.add(fut)

        def done(f):
            self._bg.discard(f)
            if not f.cancelled() and f.exception() is not None:
                log.error("agent task work failed", exc_info=f.exception())

        fut.add_done_callback(done)

    # ------------------------------------------------------------------ computers
    @staticmethod
    def tasks_on(d) -> bool:
        info = d.tasks_info
        if d.kind != "agent" or not info or not info.get("git"):
            return False
        return d.owned if info.get("enabled") is None else bool(info["enabled"])

    def device_view(self, d) -> dict | None:
        if d.kind != "agent":
            return None
        info = d.tasks_info or {}
        on = self.tasks_on(d)
        why = None
        if not info:
            why = "This computer runs an older YourHarness without agent tasks. Update it."
        elif info.get("problem"):
            why = info["problem"]
        elif not info.get("git"):
            why = "Git isn't installed on this computer" + (" inside Ubuntu (WSL)." if "WSL" in (d.exec or {}).get("os", "") else ".")
        elif not on:
            why = ("Agent tasks are switched off on this computer (it was started with --no-tasks)." if d.owned
                   else "Your friend hasn't allowed agent tasks on this computer.")
        return {"on": on, "why": why, "slots": info.get("slots", 1), "busy": self.db.busy(d.id), "agents": info.get("agents", [])}

    def device_connected(self, d) -> None:
        info = d.tasks_info or {}
        reported = {(r["task"], r["attempt"]) for r in info.get("running", [])}
        for t in self.db.running_on(d.id):
            if (t["id"], t["attempt"]) in reported:
                self.grace.pop(t["id"], None)
            else:
                self._lost(t, f"{d.name} came back without the task (it may have restarted).")
        for task_id, attempt in reported:
            t = self.db.task(task_id)
            if not t or t["status"] != "running" or t["device_id"] != d.id or t["attempt"] != attempt:
                d.send({"type": "task.cancel", "task": task_id, "attempt": attempt})
        self.dispatch()

    def device_gone(self, d) -> None:
        deadline = time.time() + LEASE_S
        for t in self.db.running_on(d.id):
            self.grace.setdefault(t["id"], deadline)

    def sweep(self) -> None:
        now = time.time()
        if not self.started or now - self._last_sweep < 2:
            return
        self._last_sweep = now
        for task_id, deadline in list(self.grace.items()):
            if now < deadline:
                continue
            self.grace.pop(task_id, None)
            t = self.db.task(task_id)
            if t and t["status"] == "running":
                d = self.s.devices.get(t["device_id"])
                if d is None or d.closed:
                    self._lost(t, f"{t['device_name']} disconnected.")
        for t in self.db.tasks_in(("running",)):
            limit = (t["spec"].get("time_limit_min") or 30) * 60 + 600
            if t["started_at"] and now - t["started_at"] > limit:
                self.cancel_task(t["id"], "its time limit", status="failed")
        for key, (when, _) in list(self.refused.items()):
            if now - when > 600:
                self.refused.pop(key, None)
        self.dispatch()

    # ------------------------------------------------------------------ placing tasks
    def dispatch(self) -> None:
        if not self.started:
            return
        runs: dict[str, dict] = {}
        for t in self.db.tasks_in(("queued",)):
            run = runs.get(t["run_id"]) or runs.setdefault(t["run_id"], self.db.run(t["run_id"]))
            if not run or run["status"] not in ("running", "integrating"):
                continue
            pinned = (t["spec"] or {}).get("device") or ""
            if pinned and self.s.store.get(pinned) is None:
                self._set(t["id"], status="failed", error="The computer this task was given to was removed from the pool.", note="")
                self._check_run(t["run_id"])
                continue
            d = self._pick(t)
            if d is None:
                note = self._why_waiting(t)
                if note != t["note"]:
                    self._set(t["id"], note=note)
                continue
            self._start(t, d, run)

    def _eligible(self, t: dict, d) -> bool:
        spec = t["spec"]
        if d.closed or d.stale or not self.tasks_on(d):
            return False
        if spec["adapter"] not in {a["id"] for a in d.tasks_info.get("agents", [])}:
            return False
        if (t["id"], d.id) in self.refused:
            return False
        return self.db.busy(d.id) < d.tasks_info.get("slots", 1)

    def _pick(self, t: dict):
        pinned = (t["spec"] or {}).get("device") or ""
        if pinned:
            d = self.s.devices.get(pinned)
            return d if d is not None and self._eligible(t, d) else None
        # "Any of my computers" never means a friend's computer: that has to be picked by name
        cands = [d for d in self.s.devices.values() if d.owned and self._eligible(t, d)]
        if not cands:
            return None
        return min(cands, key=lambda d: (self.db.busy(d.id), (d.load or {}).get("cpu") or 0.0, -(d.info.get("memoryFreeGB") or 0), d.name))

    def _why_waiting(self, t: dict) -> str:
        spec = t["spec"]
        adapter = ADAPTERS.get(spec["adapter"], spec["adapter"])
        pinned = spec.get("device") or ""
        if not pinned:
            return f"Waiting for one of your computers with {adapter} to be free."
        d = self.s.devices.get(pinned)
        name = d.name if d else (self.s.store.get(pinned) or {}).get("name", "that computer")
        if d is None or d.closed:
            return f"Waiting for {name} to connect."
        if d.stale:
            return f"Waiting for {name}, which isn't responding."
        view = self.device_view(d) or {}
        if not view.get("on"):
            return view.get("why") or f"{name} can't run agent tasks."
        if spec["adapter"] not in {a["id"] for a in view.get("agents", [])}:
            return f"{adapter} isn't installed on {name}. Install it there, or pick another computer."
        if (t["id"], d.id) in self.refused:
            return self.refused[(t["id"], d.id)][1]
        return f"Waiting for {name} to finish its other task."

    def _start(self, t: dict, d, run: dict) -> None:
        attempt = (t["attempt"] or 0) + 1
        secret = secrets.token_urlsafe(24)
        start = t["start_commit"] or run["base_commit"]
        self._append_log(t["id"], f"\n--- Attempt {attempt} on {d.name} ---\n")
        self._set(t["id"], status="running", attempt=attempt, device_id=d.id, device_name=d.name, token_hash=_hash(secret),
                  start_commit=start, started_at=time.time(), finished_at=None, phase="starting", note=f"Starting on {d.name}.", error=None)
        self.grace.pop(t["id"], None)
        spec = t["spec"]
        d.send({"type": "task.start", "task": t["id"], "attempt": attempt, "token": f"{t['id']}.{attempt}.{secret}",
                "run": run["id"], "repo": run["repo_id"], "base": start, "title": t["title"],
                "spec": {k: spec.get(k) for k in WORKER_FIELDS}})

    def _lost(self, t: dict, why: str) -> None:
        d = self.s.devices.get(t["device_id"])
        if d is not None and not d.closed:
            d.send({"type": "task.cancel", "task": t["id"], "attempt": t["attempt"]})
        self.grace.pop(t["id"], None)
        lost = (t["lost"] or 0) + 1
        if lost > MAX_LOST:
            self._set(t["id"], status="failed", lost=lost, phase=None, finished_at=time.time(), error=f"{why} Gave up after {MAX_LOST} tries.")
            self._check_run(t["run_id"])
        else:
            self._set(t["id"], status="queued", lost=lost, phase=None, note=f"{why} It will start again.")
            self.dispatch()

    # ------------------------------------------------------------------ messages from computers
    def handle(self, d, msg: dict) -> None:
        kind = msg.get("type")
        task_id = str(msg.get("task") or "")
        attempt = msg.get("attempt")
        if kind == "task.done":
            d.send({"type": "task.ack", "task": task_id, "attempt": attempt})  # it can stop resending, whatever happens next
        t = self.db.task(task_id)
        current = t is not None and t["device_id"] == d.id and t["attempt"] == attempt and t["status"] == "running"
        if not current:
            if kind == "task.update" and t is not None:
                d.send({"type": "task.cancel", "task": task_id, "attempt": attempt})  # this attempt was given up on
            return
        if kind == "task.update":
            phase = rules.clean_line(msg.get("phase"), 30) or t["phase"]
            note = rules.clean_line(msg.get("note"), 300)
            if phase != t["phase"] or (note and note != t["note"]):
                self._set(task_id, phase=phase, note=note or t["note"])
        elif kind == "task.log":
            data = str(msg.get("data") or "")[:65536]
            if data:
                self._append_log(task_id, data)
                self.s.broadcast({"type": "agents.log", "task": task_id, "data": data}, need=CAP_SUBMIT)
        elif kind == "task.refuse":
            why = rules.clean_line(msg.get("reason"), 300, "The computer couldn't take the task.")
            self.refused[(task_id, d.id)] = (time.time(), f"{d.name}: {why}")
            if (t["spec"] or {}).get("device"):
                self._set(task_id, status="failed", phase=None, finished_at=time.time(), error=f"{d.name} couldn't take it: {why}")
                self._check_run(t["run_id"])
            else:
                self._set(task_id, status="queued", phase=None, note=f"{d.name} couldn't take it: {why}")
                self.dispatch()
        elif kind == "task.done":
            self._on_done(t, msg)

    def _on_done(self, t: dict, msg: dict) -> None:
        result = clean_result(msg.get("result"))
        now = time.time()
        self.grace.pop(t["id"], None)
        if not msg.get("ok"):
            error = rules.clean_line(msg.get("error"), 2000, "The task failed.")
            self._set(t["id"], status="failed", phase=None, finished_at=now, error=error, result=result, note="")
            self._check_run(t["run_id"])
        elif result["noChanges"]:
            self._set(t["id"], status="done", phase=None, finished_at=now, result=result, note="Finished without changing any files.")
            self._check_run(t["run_id"])
        else:
            self._set(t["id"], status="verifying", phase="checking", result=result, note="Checking the result…")
            self._spawn(self._verify(t["id"], t["attempt"]))

    async def _verify(self, task_id: str, attempt: int) -> None:
        t = self.db.task(task_id)
        if not t or t["status"] != "verifying" or t["attempt"] != attempt:
            return
        run = self.db.run(t["run_id"])
        repo = self.db.repo(run["repo_id"])
        bundle = self.result_path(task_id, attempt)
        res = dict(t["result"] or {})
        try:
            if not bundle.exists():
                raise GitError("The computer reported a result, but the result file never arrived.")
            ref = await self.gitops.import_result(repo, task_id, attempt, bundle)
            info = await self.gitops.inspect(repo, t["start_commit"] or run["base_commit"], ref)
            res.update(ref=ref, commits=info.get("count", 0), changed=[p for _, p in info.get("changes", [])][:300])
            problems = rules.verify(info, t["spec"])
        except GitError as e:
            problems = [str(e)]
        t = self.db.task(task_id)
        if not t or t["status"] != "verifying" or t["attempt"] != attempt:
            return  # cancelled while it was being checked
        res["problems"] = problems
        if problems:
            self._set(task_id, status="failed", phase=None, finished_at=time.time(), result=res, note="",
                      error="Not accepted. " + " ".join(problems[:4]))
        else:
            self._set(task_id, status="done", phase=None, finished_at=time.time(), result=res,
                      note=f"Checked: {len(res.get('changed') or [])} file(s) changed.")
        if res.get("ref"):
            bundle.unlink(missing_ok=True)  # its commits are in the bare copy now
        self._check_run(t["run_id"])

    # ------------------------------------------------------------------ finishing a run
    def _check_run(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if not run:
            return
        tasks = self.db.tasks(run_id)
        if run["status"] == "running" and not any(t["status"] in ACTIVE for t in tasks):
            self._spawn(self._finish_run(run_id))
        else:
            self._broadcast_run(run_id)

    async def _finish_run(self, run_id: str, force: bool = False) -> None:
        lock = self.run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            run = self.db.run(run_id)
            if not run or run["status"] in ("cancelled", "preparing") or not run["base_commit"]:
                return
            if not force and any(t["status"] in ACTIVE for t in self.db.tasks(run_id)):
                return
            repo = self.db.repo(run["repo_id"])
            self._set_run(run_id, status="integrating", note="Combining the results…")
            try:
                for _ in range(5):  # results that arrive while one batch is pushed go in the next batch
                    ready = [t for t in self.db.tasks(run_id) if t["status"] == "done" and not t["integrated"] and (t["result"] or {}).get("ref")]
                    if ready:
                        out = await self.gitops.integrate(repo, run_id, run["base_commit"], [
                            {"id": t["id"], "ref": t["result"]["ref"], "start": t["start_commit"] or run["base_commit"]} for t in ready])
                        for tid in out["merged"]:
                            self.db.update_task(tid, integrated=1)
                            self._broadcast_task(self.db.task(tid))
                        for tid, files in out["conflicts"].items():
                            ct = self.db.task(tid)
                            self._set(tid, status="failed", result={**(ct["result"] or {}), "conflict": True},
                                      error="Conflicts with an earlier task in " + ", ".join(files[:8]) + ". Retry it to redo the work on top of the others.")
                    tasks = self.db.tasks(run_id)
                    head = await self.gitops.run_head(repo, run_id) if any(t["integrated"] for t in tasks) else None
                    run = self.db.run(run_id)
                    if head and head != run["pushed_commit"]:
                        self._set_run(run_id, note=f"Pushing to {run['results_branch']}…")
                        await self.gitops.push(repo, run_id, run["results_branch"])
                        self._set_run(run_id, pushed_commit=head)
                        self.db.event(run_id, None, "pushed", {"commit": head, "branch": run["results_branch"]})
                        run = self.db.run(run_id)
                        if run["open_pr"] and repo["kind"] == "github" and not run["pr_url"]:
                            self._set_run(run_id, note="Opening a pull request…")
                            url = await self.github.open_pr(repo["name"], run["results_branch"], run["base_branch"],
                                                            f"YourHarness: {run['title']}", pr_body(run, tasks))
                            self._set_run(run_id, pr_url=url)
                    if not [t for t in self.db.tasks(run_id) if t["status"] == "done" and not t["integrated"] and (t["result"] or {}).get("ref")]:
                        break
                tasks = self.db.tasks(run_id)
                if any(t["status"] in ACTIVE for t in tasks):
                    status = "running"
                elif any(t["integrated"] for t in tasks) or all(t["status"] == "done" for t in tasks):
                    status = "done"
                else:
                    status = "failed"
                self._set_run(run_id, status=status, error=None, note=self._run_note(run_id))
            except (GitError, GitHubError) as e:
                self._set_run(run_id, status="failed", error=self.secrets.redact(str(e)), note="Use “Push now” to try again.")

    def _run_note(self, run_id: str) -> str:
        run = self.db.run(run_id)
        tasks = self.db.tasks(run_id)
        n = len(tasks)
        merged = sum(1 for t in tasks if t["integrated"])
        failed = sum(1 for t in tasks if t["status"] == "failed")
        active = sum(1 for t in tasks if t["status"] in ACTIVE)
        parts = []
        if merged:
            parts.append(f"{merged} of {n} task{'s' if n != 1 else ''} pushed to {run['results_branch']}")
        elif not active and run["status"] in ("done", "integrating") and not failed:
            parts.append("No task changed any files, so nothing was pushed")
        if active:
            parts.append(f"{active} still working")
        if failed:
            parts.append(f"{failed} failed")
        return (", ".join(parts) + ".") if parts else ""

    # ------------------------------------------------------------------ starting a run
    def create_run(self, body, created_by: str) -> dict:
        spec = rules.clean_run(body)
        repo = spec["repo"]
        if repo["kind"] == "github" and not self.github.connected:
            raise RunError("Connect GitHub first: add a token in the GitHub tab.")
        if repo["kind"] == "local" and "://" not in repo["name"]:
            p = Path(repo["name"]).expanduser()
            if not p.exists():
                raise RunError(f"There's no folder at {repo['name']} on the coordinator's computer.")
            repo["name"] = str(p.resolve())
        for t in spec["tasks"]:
            if t["device"] and self.s.store.get(t["device"]) is None:
                raise RunError(f"The computer picked for “{t['title']}” isn't in the pool anymore.")
        rid = rules.repo_id(repo["kind"], repo["name"])
        self.db.upsert_repo(rid, repo["kind"], repo["name"])
        run_id = rules.new_id("r")
        self.db.add_run({"id": run_id, "title": spec["title"], "repo_id": rid, "base_branch": spec["base_branch"],
                         "results_branch": spec["results_branch"], "open_pr": int(spec["open_pr"]), "status": "preparing",
                         "note": "Getting the repository ready…", "created_by": created_by, "spec": {"version": 1}})
        for i, ts in enumerate(spec["tasks"]):
            self.db.add_task({"id": rules.new_id("t"), "run_id": run_id, "seq": i, "title": ts["title"], "spec": ts,
                              "status": "queued", "note": "Waiting for the repository."})
        self.db.event(run_id, None, "created", {"by": created_by, "tasks": len(spec["tasks"])})
        self._broadcast_run(run_id)
        self._spawn(self._prepare(run_id))
        return self.run_view(self.db.run(run_id), with_spec=True)

    async def _prepare(self, run_id: str) -> None:
        run = self.db.run(run_id)
        repo = self.db.repo(run["repo_id"])
        try:
            await self.gitops.fetch(repo)
            default = await self.gitops.default_branch(repo)
            if default:
                self.db.update_repo(repo["id"], default_branch=default)
            branches = await self.gitops.branches(repo)
            if not branches:
                raise RunError(f"{repo['name']} has no commits yet. Add a first commit (a README is enough), then start the run again.")
            base = run["base_branch"] or default or ("main" if "main" in branches else branches[0])
            sha = await self.gitops.resolve(repo, base)
            results = run["results_branch"] or f"yh/{run_id}"
            rules.check_results_branch(results, base, default)
            await self.gitops.bundle_for(repo, sha)
            self.db.update_run(run_id, base_branch=base, base_commit=sha, results_branch=results)
            for t in self.db.tasks(run_id):
                if t["status"] == "queued":
                    self.db.update_task(t["id"], start_commit=sha, note="")
            if self.db.run(run_id)["status"] == "cancelled":
                return
            self._set_run(run_id, status="running", note="")
            self.dispatch()
        except (GitError, RunError) as e:
            self._set_run(run_id, status="failed", error=self.secrets.redact(str(e)), note="")
            for t in self.db.tasks(run_id):
                if t["status"] == "queued":
                    self._set(t["id"], status="failed", error="The run couldn't start.", note="")

    # ------------------------------------------------------------------ what you can do from the web app
    def cancel_task(self, task_id: str, by: str, status: str = "cancelled") -> dict | None:
        t = self.db.task(task_id)
        if not t or t["status"] in FINISHED:
            return t
        if t["status"] == "running":
            d = self.s.devices.get(t["device_id"])
            if d is not None:
                d.send({"type": "task.cancel", "task": task_id, "attempt": t["attempt"]})
        self.grace.pop(task_id, None)
        error = f"Cancelled by {by}." if status == "cancelled" else f"Stopped: it ran past {by}."
        t = self._set(task_id, status=status, phase=None, finished_at=time.time(), error=error, note="")
        self._check_run(t["run_id"])
        return t

    async def retry_task(self, task_id: str) -> dict:
        t = self.db.task(task_id)
        if not t:
            raise RunError("There's no task with that id.")
        if t["status"] not in ("failed", "cancelled"):
            raise RunError("Only a failed or cancelled task can be retried.")
        run = self.db.run(t["run_id"])
        if run["status"] == "cancelled":
            raise RunError("The run was cancelled. Start it again as a new run.")
        if not run["base_commit"]:
            raise RunError("This run never started. Start it again as a new run.")
        start = t["start_commit"] or run["base_commit"]
        if (t["result"] or {}).get("conflict"):  # redo it on top of what the others already did
            repo = self.db.repo(run["repo_id"])
            start = await self.gitops.run_head(repo, run["id"]) or start
            await self.gitops.bundle_for(repo, start)
        t = self._set(task_id, status="queued", error=None, phase=None, note="Queued again.", lost=0, result={}, integrated=0,
                      start_commit=start, finished_at=None)
        if run["status"] != "running":
            self._set_run(run["id"], status="running", error=None)
        self.dispatch()
        return t

    def accept_task(self, task_id: str, by: str) -> dict:
        t = self.db.task(task_id)
        res = (t or {}).get("result") or {}
        if not t or t["status"] != "failed" or not res.get("ref") or not res.get("problems"):
            raise RunError("Only a result that the checks flagged can be accepted anyway.")
        t = self._set(task_id, status="done", error=None, note=f"Accepted by {by}.", result={**res, "override": by})
        run = self.db.run(t["run_id"])
        if run["status"] != "running":
            self._set_run(run["id"], status="running", error=None)
        self._check_run(run["id"])
        return t

    def cancel_run(self, run_id: str, by: str) -> None:
        run = self.db.run(run_id)
        if not run:
            raise RunError("There's no run with that id.")
        self._set_run(run_id, status="cancelled", note=f"Cancelled by {by}.")
        for t in self.db.tasks(run_id):
            if t["status"] in ACTIVE:
                self.cancel_task(t["id"], by)

    def push_run(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if not run:
            raise RunError("There's no run with that id.")
        if not run["base_commit"]:
            raise RunError("This run never started, so there's nothing to push.")
        if run["status"] == "cancelled":
            raise RunError("This run was cancelled.")
        self._spawn(self._finish_run(run_id, force=True))

    # ------------------------------------------------------------------ files for a computer, by attempt token
    def task_for_token(self, token: str | None, task_id: str) -> dict | None:
        parts = (token or "").split(".")
        if len(parts) != 3 or parts[0] != task_id:
            return None
        t = self.db.task(task_id)
        if not t or t["status"] != "running" or str(t["attempt"]) != parts[1] or not t["token_hash"]:
            return None
        return t if hmac.compare_digest(t["token_hash"], _hash(parts[2])) else None

    def result_path(self, task_id: str, attempt: int) -> Path:
        return self.dir / "results" / f"{task_id}-{attempt}.bundle"

    async def start_bundle(self, t: dict) -> Path:
        run = self.db.run(t["run_id"])
        return await self.gitops.bundle_for(self.db.repo(run["repo_id"]), t["start_commit"] or run["base_commit"])

    # ------------------------------------------------------------------ logs and diffs
    def _append_log(self, task_id: str, text: str) -> None:
        path = self.dir / "logs" / f"{task_id}.log"
        try:
            size = path.stat().st_size if path.exists() else 0
            if size > LOG_LIMIT:
                if task_id not in self.full_logs:
                    self.full_logs.add(task_id)
                    with open(path, "a", encoding="utf-8") as f:
                        f.write("\n[The log is over 5 MB; later output isn't kept.]\n")
                return
            with open(path, "a", encoding="utf-8") as f:
                f.write(text)
        except OSError:
            log.exception("couldn't write a task log")

    def log_tail(self, task_id: str, limit: int = 256 * 1024) -> str:
        path = self.dir / "logs" / f"{task_id}.log"
        if not path.exists():
            return ""
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - limit))
            return f.read().decode("utf-8", "replace")

    async def diff(self, task_id: str) -> str:
        t = self.db.task(task_id)
        res = (t or {}).get("result") or {}
        if not res.get("ref"):
            return ""
        run = self.db.run(t["run_id"])
        return await self.gitops.diff_text(self.db.repo(run["repo_id"]), t["start_commit"] or run["base_commit"], res["ref"])

    # ------------------------------------------------------------------ views for the web app
    def task_view(self, t: dict, with_spec: bool = False) -> dict:
        spec, res = t["spec"] or {}, t["result"] or {}
        view = {
            "id": t["id"], "runId": t["run_id"], "seq": t["seq"], "title": t["title"], "kind": spec.get("kind"),
            "adapter": spec.get("adapter"), "adapterName": ADAPTERS.get(spec.get("adapter"), spec.get("adapter")),
            "model": spec.get("model") or "", "device": spec.get("device") or "", "deviceName": t["device_name"] or "",
            "status": t["status"], "phase": t["phase"], "note": t["note"] or "", "error": t["error"] or "", "attempt": t["attempt"],
            "startedAt": t["started_at"], "finishedAt": t["finished_at"], "integrated": bool(t["integrated"]),
            "hasDiff": bool(res.get("ref")), "canAccept": t["status"] == "failed" and bool(res.get("ref")) and bool(res.get("problems")),
            "result": {k: res.get(k) for k in ("summary", "checks", "usd", "rounds", "timings", "commits", "noChanges", "override", "conflict")},
            "changed": (res.get("changed") or [])[:50],
        }
        if with_spec:
            view["spec"] = {k: spec.get(k) for k in SPEC_FIELDS}
            view["spec"]["title"] = t["title"]
        return view

    def run_view(self, run: dict, tasks: list[dict] | None = None, with_spec: bool = False) -> dict:
        repo = self.db.repo(run["repo_id"]) or {"kind": "?", "name": "?"}
        tasks = self.db.tasks(run["id"]) if tasks is None else tasks
        branch_url = None
        if repo["kind"] == "github" and run["pushed_commit"]:
            branch_url = f"https://github.com/{repo['name']}/tree/{run['results_branch']}"
        return {
            "id": run["id"], "title": run["title"], "repo": {"kind": repo["kind"], "name": repo["name"]},
            "baseBranch": run["base_branch"] or "", "baseCommit": run["base_commit"] or "", "resultsBranch": run["results_branch"] or "",
            "openPr": bool(run["open_pr"]), "status": run["status"], "note": run["note"] or "", "error": run["error"] or "",
            "createdBy": run["created_by"], "createdAt": run["created_at"], "updatedAt": run["updated_at"],
            "pushedCommit": run["pushed_commit"] or "", "prUrl": run["pr_url"] or "", "branchUrl": branch_url,
            "tasks": [self.task_view(t, with_spec) for t in tasks],
        }

    def overview(self) -> dict:
        return {
            "runs": [self.run_view(r) for r in self.db.runs(30)],
            "repos": [{"kind": r["kind"], "name": r["name"], "defaultBranch": r["default_branch"]} for r in self.db.repos()],
            "github": {"connected": self.github.connected, "user": self.secrets.get("github_user") or ""},
            "adapters": ADAPTERS,
        }

    # ------------------------------------------------------------------ bookkeeping
    def _set(self, task_id: str, **fields) -> dict:
        before = self.db.task(task_id)
        self.db.update_task(task_id, **fields)
        t = self.db.task(task_id)
        if "status" in fields and before and before["status"] != t["status"]:
            self.db.event(t["run_id"], task_id, "status", {"status": t["status"], "device": t["device_name"], "attempt": t["attempt"],
                                                          "error": t["error"]})
        self._broadcast_task(t)
        return t

    def _set_run(self, run_id: str, **fields) -> None:
        before = self.db.run(run_id)
        self.db.update_run(run_id, **fields)
        if "status" in fields and before and before["status"] != fields["status"]:
            self.db.event(run_id, None, "status", {"status": fields["status"], "error": fields.get("error")})
        self._broadcast_run(run_id)

    def _broadcast_task(self, t: dict) -> None:
        self.s.broadcast({"type": "agents.task", "task": self.task_view(t)}, need=CAP_SUBMIT)

    def _broadcast_run(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if run:
            self.s.broadcast({"type": "agents.run", "run": self.run_view(run)}, need=CAP_SUBMIT)
