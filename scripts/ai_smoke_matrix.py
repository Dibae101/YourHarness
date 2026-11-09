#!/usr/bin/env python3
"""The real-model smoke matrix for agent tasks. Not run in CI, and never by YourHarness itself.

It runs one tiny task with each AI agent CLI you name (Claude Code, Codex, Kiro)
through the whole agent-task path, on this computer only:

  * a coordinator on 127.0.0.1 with this computer as its one agent,
  * a scratch Git repository whose "GitHub" is a folder here, and a fake GitHub API
    on 127.0.0.1, so nothing reaches github.com,
  * the task's own check, then the run's final check on the combined branch, then a
    draft pull request (to the fake GitHub).

For each agent it records whether the CLI was found and signed in, whether it took
the flags YourHarness passes, the task's status and error, what it changed, what it
cost when the CLI reports it, how long each step took, the final check, and what
the pull request said. The CLIs use your own sign-in (your HOME), as they would on
your computers in the pool.

It spends money: each task asks the model for one small edit, with a budget cap
where the CLI has one (Claude Code's --max-budget-usd). Nothing runs without --yes.

    python scripts/ai_smoke_matrix.py --dry-run
    python scripts/ai_smoke_matrix.py --yes --agents claude,codex --report smoke.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIS = {"claude": "claude", "codex": "codex", "kiro": "kiro-cli"}
TOKEN = "smoke-matrix-fake-github-token"
INSTRUCTIONS = ('In greet.py, change greet() so it returns "hello, world" instead of "hello". '
                "Change nothing else and don't add files.")
CHECK = "python3 -c \"import greet; assert greet.greet() == 'hello, world', greet.greet()\""


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--agents", default=",".join(CLIS), help="comma-separated: claude, codex, kiro (default: all three)")
    p.add_argument("--model", action="append", default=[], metavar="AGENT=MODEL", help="a model for one agent, e.g. claude=sonnet")
    p.add_argument("--budget", type=float, default=0.5, help="USD cap per task where the CLI supports one (default 0.50)")
    p.add_argument("--timeout-min", type=int, default=10, help="time limit per task in minutes (default 10)")
    p.add_argument("--report", help="write the results as JSON here")
    p.add_argument("--keep", action="store_true", help="keep the scratch folder (repositories, logs, coordinator data)")
    p.add_argument("--dry-run", action="store_true", help="say what would run, and run nothing")
    p.add_argument("--yes", action="store_true", help="really run the agents (this uses your AI subscriptions or API credits)")
    return p.parse_args(argv)


# ---------------------------------------------------------------- a fake GitHub on 127.0.0.1
class FakeGitHub:
    def __init__(self):
        self.calls: list[tuple[str, str, dict]] = []
        gh = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _reply(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _body(self):
                return json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")

            def do_GET(self):
                gh.calls.append(("GET", self.path, {}))
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    return self._reply(401, {"message": "Bad credentials"})
                if self.path == "/user":
                    return self._reply(200, {"login": "smoke"})
                if self.path.startswith("/user/repos"):
                    return self._reply(200, [{"full_name": "smoke/demo", "private": True, "default_branch": "main", "permissions": {"push": True}}])
                self._reply(404, {"message": "Not Found"})

            def do_POST(self):
                body = self._body()
                gh.calls.append(("POST", self.path, body))
                n = sum(1 for m, p, _ in gh.calls if m == "POST" and p.endswith("/pulls"))
                self._reply(201, {"html_url": f"https://github.invalid/smoke/demo/pull/{n}", "number": n})

            def do_PATCH(self):
                gh.calls.append(("PATCH", self.path, self._body()))
                self._reply(200, {})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"


# ---------------------------------------------------------------- the pool
def git(*args, cwd=None):
    env = {**os.environ, "GIT_AUTHOR_NAME": "Smoke", "GIT_AUTHOR_EMAIL": "smoke@localhost", "GIT_COMMITTER_NAME": "Smoke",
           "GIT_COMMITTER_EMAIL": "smoke@localhost"}
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout


def scratch_repo(tmp: Path) -> Path:
    work = tmp / "work"
    work.mkdir()
    git("init", "-q", "-b", "main", str(work))
    (work / "README.md").write_text("A scratch repository for the YourHarness smoke matrix.\n")
    (work / "greet.py").write_text('def greet():\n    return "hello"\n')
    git("-C", str(work), "add", ".")
    git("-C", str(work), "commit", "-q", "-m", "first")
    bare = tmp / "gh" / "smoke" / "demo.git"
    bare.parent.mkdir(parents=True)
    git("clone", "-q", "--bare", str(work), str(bare))
    return bare


class Proc:
    def __init__(self, args, env, log: Path):
        self.f = open(log, "w")
        self.p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=self.f, stderr=subprocess.STDOUT, text=True)
        self.log = log

    def wait_for(self, text: str, timeout: float = 60) -> str:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for line in self.log.read_text(errors="replace").splitlines():
                if text in line:
                    return line
            if self.p.poll() is not None:
                break
            time.sleep(0.2)
        raise SystemExit(f"never saw {text!r} in {self.log}")

    def stop(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(10)
            except subprocess.TimeoutExpired:
                self.p.kill()
        self.f.close()


def request(base: str, token: str, method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Authorization": f"Bearer {token}", **({"Content-Type": "application/json"} if data else {})})
    with urllib.request.urlopen(req, timeout=60) as res:
        raw = res.read()
        return json.loads(raw) if res.headers.get_content_type() == "application/json" else raw.decode("utf-8", "replace")


def run_matrix(args, agents: list[str], models: dict[str, str]) -> list[dict]:
    tmp = Path(tempfile.mkdtemp(prefix="yh-smoke-"))
    print(f"Scratch folder: {tmp}")
    gh = FakeGitHub()
    scratch_repo(tmp)
    env = {**os.environ, "HARNESS_GITHUB_API": gh.url, "HARNESS_GITHUB_GIT": str(tmp / "gh"), "NO_QR": "1"}
    procs = []
    results = []
    try:
        coord = Proc([sys.executable, "-m", "yourharness", "serve", "--host", "127.0.0.1", "--port", "0", "--data-dir", str(tmp / "data"),
                      "--shared-dir", str(tmp / "shared"), "--no-qr", "--no-local-agent"], env, tmp / "coordinator.log")
        procs.append(coord)
        port = int(coord.wait_for("Listening on port").rsplit(" ", 1)[1])
        base = f"http://127.0.0.1:{port}"
        join = json.loads((tmp / "data" / "coordinator.json").read_text())["join_token"]
        agent_env = {**env, "YOURHARNESS_HOME": str(tmp / "agent")}  # HOME stays yours: the CLIs need your sign-in
        agent_env.pop("HARNESS_AGENT_CLIS", None)
        agent = Proc([sys.executable, "-m", "yourharness", "agent", "--join", f"{base}/#join={join}", "--threads", "1", "--name", "smoke",
                      "--tasks", "--no-share"], agent_env, tmp / "agent.log")
        procs.append(agent)
        agent.wait_for("Joined the pool")
        req = urllib.request.Request(f"{base}/api/join", data=json.dumps({"token": join, "kind": "agent", "name": "smoke client"}).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req) as res:
            token = json.loads(res.read())["deviceToken"]
        request(base, token, "POST", "/api/github/token", {"token": TOKEN})
        time.sleep(3)  # the agent reports the CLIs it found
        found = {}
        for d in request(base, token, "GET", "/api/devices")["devices"]:
            if d["name"] == "smoke":
                found = {a["id"]: a.get("version") for a in ((d.get("agentTasks") or {}).get("agents") or [])}
        for adapter in agents:
            row = {"agent": adapter, "cli": CLIS[adapter], "found": adapter in found, "version": found.get(adapter)}
            results.append(row)
            if not row["found"]:
                row["status"] = "not found"
                print(f"{adapter}: not found on this computer, skipped")
                continue
            print(f"{adapter}: running…", flush=True)
            t0 = time.monotonic()
            task = {"title": f"Smoke: {adapter}", "kind": "code", "adapter": adapter, "model": models.get(adapter, ""), "instructions": INSTRUCTIONS,
                    "checks": [CHECK], "autonomy": "edit", "rounds": 1, "time_limit_min": args.timeout_min, "max_usd": args.budget,
                    "scope": ["greet.py"]}
            run = request(base, token, "POST", "/api/agent-runs", {
                "title": f"Smoke matrix: {adapter}", "repo": {"kind": "github", "name": "smoke/demo"}, "results_branch": f"yh/smoke-{adapter}-{int(time.time())}",
                "open_pr": True, "tasks": [task], "final_checks": [CHECK]})["run"]
            end = time.monotonic() + args.timeout_min * 60 + 120
            while time.monotonic() < end:
                run = request(base, token, "GET", f"/api/agent-runs/{run['id']}")["run"]
                if run["status"] in ("done", "failed", "cancelled"):
                    break
                time.sleep(3)
            t = run["tasks"][0]
            res = t.get("result") or {}
            log = request(base, token, "GET", f"/api/agent-tasks/{t['id']}/log")
            prs = [b for m, p, b in gh.calls if m == "POST" and p.endswith("/pulls") and b.get("head") == run["resultsBranch"]]
            row.update({
                "status": run["status"], "taskStatus": t["status"], "error": t["error"] or run["error"], "summary": (res.get("summary") or "")[:500],
                "changed": t.get("changed"), "usd": res.get("usd"), "timingsMs": res.get("timings"), "checks": res.get("checks"),
                "seconds": round(time.monotonic() - t0, 1), "validation": (run.get("validation") or {}).get("status"),
                "pr": bool(prs), "prSaysValidated": bool(prs) and "**Validated:" in prs[0].get("body", ""),
                "signInProblem": any(w in log.lower() for w in ("not logged in", "login", "unauthorized", "401", "api key")),
                "flagProblem": any(w in log.lower() for w in ("unknown option", "unexpected argument", "unrecognized", "invalid value")),
                "logTail": log[-2000:],
            })
            print(f"{adapter}: {row['status']} in {row['seconds']} s, task {row['taskStatus']}, final check {row['validation']}, "
                  f"cost {row['usd'] if row['usd'] is not None else 'not reported'}", flush=True)
    finally:
        for p in reversed(procs):
            p.stop()
        gh.server.shutdown()
        if not args.keep:
            shutil.rmtree(tmp, ignore_errors=True)
    return results


def main(argv=None) -> int:
    args = parse_args(argv)
    agents = [a.strip() for a in args.agents.split(",") if a.strip()]
    bad = [a for a in agents if a not in CLIS]
    if bad:
        print(f"Unknown agents: {', '.join(bad)}. Use: {', '.join(CLIS)}.", file=sys.stderr)
        return 2
    models = dict(m.split("=", 1) for m in args.model if "=" in m)
    plan = [f"  {a}: {CLIS[a]} ({'on the PATH' if shutil.which(CLIS[a]) else 'not on the PATH: skipped'})"
            f"{f', model {models[a]}' if a in models else ''}, edit-only, budget ${args.budget:.2f}, {args.timeout_min} min" for a in agents]
    print("The smoke matrix would run one small task with:\n" + "\n".join(plan))
    print("Each one edits greet.py in a scratch repository; its check and the run's final check confirm the edit;\n"
          "the draft pull request goes to a fake GitHub on 127.0.0.1. Your AI sign-ins are used and may cost money.")
    if args.dry_run or not args.yes:
        if not args.dry_run:
            print("\nNothing was run. Add --yes to run it.")
        return 0
    results = run_matrix(args, agents, models)
    print("\nAgent    Found  Status     Task       Final check  PR   Cost      Time")
    for r in results:
        print(f"{r['agent']:<8} {'yes' if r['found'] else 'no ':<6} {str(r.get('status')):<10} {str(r.get('taskStatus', '')):<10} "
              f"{str(r.get('validation') or ''):<12} {'yes' if r.get('pr') else 'no ':<4} "
              f"{('$%.3f' % r['usd']) if r.get('usd') is not None else '-':<9} {r.get('seconds', '')}")
        if r.get("signInProblem"):
            print(f"         {r['agent']}: the log mentions signing in; check that {r['cli']} is logged in on this computer")
        if r.get("flagProblem"):
            print(f"         {r['agent']}: the log mentions an unknown option; this CLI version may need different flags")
    if args.report:
        Path(args.report).write_text(json.dumps({"at": time.time(), "results": results}, indent=2))
        print(f"\nWrote {args.report}")
    return 0 if all(r.get("status") in ("done", "not found") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
