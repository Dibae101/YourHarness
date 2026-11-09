"""Agent tasks, end to end, with no AI involved.

Every task here uses the "Command (no AI)" adapter, so the whole path runs for
real: placing tasks, Git worktrees on each computer, bundles back to the
coordinator, the checks on the result, cherry-picking onto the run's branch and
pushing it. HARNESS_AGENT_CLIS="" keeps the agents from even looking for AI CLIs.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from conftest import start_coordinator
from yourharness.agent.adapters import OutputReader, agent_command, parse_detect
from yourharness.agent.prompts import build_prompt
from yourharness.coordinator.state import LENDER_CAPS, Store
from yourharness.coordinator.tasks import rules

GIT_ENV = {"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.com", "GIT_COMMITTER_NAME": "Test",
           "GIT_COMMITTER_EMAIL": "test@example.com"}


def git(*args, cwd=None) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env={**os.environ, **GIT_ENV}).stdout


def make_repo(path: Path, bare: bool = False) -> Path:
    """A repository with one commit on main. With bare=True, a bare one, like GitHub's side."""
    work = path if not bare else path.parent / (path.name + "-work")
    work.mkdir(parents=True, exist_ok=True)
    git("init", "-q", "-b", "main", str(work))
    (work / "README.md").write_text("hello\n")
    (work / "docs").mkdir()
    (work / "docs" / "notes.md").write_text("notes\n")
    git("-C", str(work), "add", ".")
    git("-C", str(work), "commit", "-q", "-m", "first")
    if bare:
        git("clone", "-q", "--bare", str(work), str(path))
    return path


# ---------------------------------------------------------------------- rules, no processes
def test_scope_patterns():
    assert rules.matches("src/a/b.py", ["src/**"])
    assert rules.matches("src/a/b.py", ["src"]) and rules.matches("src/a/b.py", ["src/"])
    assert not rules.matches("srcx/a.py", ["src"])
    assert rules.matches("README.md", ["*.md"]) and not rules.matches("docs/a.md", ["*.md"])
    assert rules.matches("docs/a.md", ["**/*.md"]) and rules.matches("a.md", ["**/*.md"])
    assert rules.matches(".github/workflows/ci.yml", rules.PROTECTED)
    assert rules.matches("anything/at/all", ["**"])


def test_links_out_of_the_repository_are_caught():
    assert rules.symlink_escapes("a/link", "../../etc/passwd")
    assert rules.symlink_escapes("link", "/etc/passwd")
    assert not rules.symlink_escapes("a/link", "../b/file")


def test_secret_scan_finds_credentials_in_added_lines_only():
    key = "AKIA" + "ABCDEFGHIJKLMNOP"  # built here, so this file holds nothing that looks like a key
    token = "gh" + "p_" + "a1B2c3D4" * 5
    diff = (f"+++ b/config.py\n@@ -0,0 +1,2 @@\n+aws = '{key}'\n+ok = 1\n"
            f"+++ b/old.txt\n@@ -3 +2,0 @@\n-old = '{token}'\n")
    found = rules.scan_secrets(diff)
    assert found == ["config.py:1 (an AWS access key)"]


def test_results_never_go_to_the_main_branch():
    for name in ("main", "master", "trunk"):
        with pytest.raises(rules.RunError):
            rules.check_results_branch(name, "dev", "main")
    with pytest.raises(rules.RunError):
        rules.check_results_branch("dev", "dev", "main")
    with pytest.raises(rules.RunError):
        rules.check_results_branch("bad name", "main", "main")
    rules.check_results_branch("yh/r1234", "main", "main")


def test_a_run_is_checked_and_filled_in():
    run = rules.clean_run({"repo": {"kind": "github", "name": "https://github.com/me/app.git"}, "tasks": [
        {"title": "Write the report", "kind": "general", "adapter": "claude", "instructions": "Summarize"},
        {"title": "Build", "adapter": "command", "command": "make", "allow_workflows": True},
    ]})
    assert run["repo"] == {"kind": "github", "name": "me/app"}
    report, build = run["tasks"]
    assert report["scope"][0].startswith("results/write-the-report-") and report["results_dir"]
    assert ".github/workflows/**" in report["deny"]
    assert ".github/workflows/**" not in build["deny"] and build["rounds"] == 1
    for bad in ({"repo": {"kind": "github", "name": "x"}, "tasks": [{"title": "a", "adapter": "command", "command": "x"}]},
                {"repo": {"kind": "local", "name": "/tmp/x"}, "tasks": []},
                {"repo": {"kind": "local", "name": "/tmp/x"}, "tasks": [{"title": "a", "adapter": "claude"}]},
                {"repo": {"kind": "local", "name": "/tmp/x"}, "tasks": [{"title": "a", "adapter": "gpt"}]}):
        with pytest.raises(rules.RunError):
            rules.clean_run(bad)


def test_friend_invites_are_one_time_and_expire(tmp_path):
    s = Store(tmp_path)
    invite, code = s.create_invite("Sam's PC", hours=1)
    assert invite["state"] == "open" and len(code) == 19 and code.count("-") == 3
    assert s.caps_for_join(code) == LENDER_CAPS
    assert s.caps_for_join(code.lower().replace("-", " ")) == LENDER_CAPS  # typed loosely still works
    s.use_invite(code)
    assert s.caps_for_join(code) is None, "an invite is good for one computer"
    _, late = s.create_invite("Late", hours=1)
    s.data["invites"][-1]["expires"] = time.time() - 1
    assert s.caps_for_join(late) is None
    other, code3 = s.create_invite("Revoked", hours=1)
    assert s.revoke_invite(other["id"]) and s.caps_for_join(code3) is None
    assert "hash" not in json.dumps(s.invites()), "the API never shows the stored hash"
    assert code3.replace("-", "") not in (tmp_path / "coordinator.json").read_text(), "only a hash is stored"


def test_adapter_commands_use_the_documented_flags():
    spec = {"model": "sonnet", "autonomy": "full", "max_usd": 2}
    claude = agent_command("claude", spec, "/w/p.md")
    assert claude.startswith("claude -p \"$(cat /w/p.md)\"") and "--output-format stream-json" in claude
    assert "--permission-mode bypassPermissions" in claude and "--max-budget-usd 2.00" in claude and "--model sonnet" in claude
    assert "--permission-mode acceptEdits" in agent_command("claude", {"autonomy": "edit"}, "/p")
    codex = agent_command("codex", {"autonomy": "edit", "model": "gpt-5"}, "/p")
    assert codex.startswith("codex exec --json") and "-s workspace-write" in codex and "-m gpt-5" in codex
    assert "--dangerously-bypass-approvals-and-sandbox" in agent_command("codex", {}, "/p")
    kiro = agent_command("kiro", {"autonomy": "edit"}, "/p")
    assert kiro.startswith("kiro-cli chat --no-interactive") and "--trust-tools=fs_read,fs_write" in kiro
    assert agent_command("command", {"command": "make test"}, "/p") == "bash -c 'make test'"


def test_agent_output_becomes_readable_lines():
    r = OutputReader("claude")
    out = r.feed('{"type":"system","subtype":"init","model":"m"}\n{"type":"assistant","message":{"content":'
                 '[{"type":"text","text":"Hi"},{"type":"tool_use","name":"Edit","input":{"file_path":"a.py"}}]}}\n')
    out += r.feed('{"type":"result","result":"Done it","total_cost_usd":0.25,"num_turns":3}\n')
    assert "Hi" in out and "→ Edit: a.py" in out and "$0.25" in out
    assert r.summary == "Done it" and r.usd == 0.25
    c = OutputReader("codex")
    c.feed('{"type":"item.completed","item":{"type":"agent_message","text":"Changed it"}}\n')
    assert c.summary == "Changed it"
    assert parse_detect("::yh-tool::git::git version 2.4\n::yh-tool::codex::codex-cli 1\n") == {
        "git": "git version 2.4", "curl": None, "agents": [{"id": "command", "version": ""}, {"id": "codex", "version": "codex-cli 1"}]}


def test_steps_left_by_a_killed_agent_are_stopped_on_restart(tmp_path, monkeypatch):
    """A step whose agent was killed outright (kill -9, power loss) is found by its pid file and stopped."""
    import asyncio
    import signal

    from yourharness.agent import taskhost

    if os.name == "nt":
        pytest.skip("POSIX process groups")
    started = {}

    async def fake_start_time(pid):  # ps may be unavailable where the tests run
        return started.get(pid, "")

    monkeypatch.setattr(taskhost, "_start_time", fake_start_time)
    shell = taskhost.Shell(tmp_path)
    left = subprocess.Popen(["sleep", "60"], start_new_session=True)
    other = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        started[left.pid] = started[other.pid] = "Mon 9 Nov 05:00:00 2026"
        (shell.dir / "pids").mkdir(parents=True)
        (shell.dir / "pids" / "t1-1-agent1.pid").write_text(f"{left.pid}\nMon 9 Nov 05:00:00 2026\n")
        (shell.dir / "pids" / "t2-1-agent1.pid").write_text(f"{other.pid}\nTue 1 Jan 00:00:00 2030\n")  # pid reused since
        assert asyncio.run(shell.reap()) == 1
        assert left.wait(5) == -signal.SIGTERM
        assert other.poll() is None, "a process that only shares the pid is left alone"
        assert not list((shell.dir / "pids").glob("*.pid"))
    finally:
        for p in (left, other):
            if p.poll() is None:
                p.kill()
                p.wait()


def test_the_prompt_states_the_rules():
    p = build_prompt("Add login", {"instructions": "Add a login form", "scope": ["src/**"], "deny": [".github/workflows/**"],
                                   "checks": ["npm test"]}, feedback="test failed")
    assert "Add a login form" in p and "src/**" in p and "npm test" in p and "Don't commit" in p and "test failed" in p


# ---------------------------------------------------------------------- end to end
class Pool:
    def __init__(self, coord):
        self.coord = coord
        self.c = coord.client("tests")

    def device(self, name: str) -> dict:
        end = time.monotonic() + 20
        while time.monotonic() < end:
            for d in self.c.devices():
                if d["name"] == name and (d.get("agentTasks") or {}).get("agents"):
                    return d
            time.sleep(0.2)
        raise AssertionError(f"{name} never offered agent tasks: {self.c.devices()}")

    def start(self, body: dict) -> dict:
        return self.c.request("POST", "/api/agent-runs", body)["run"]

    def wait(self, run_id: str, until=("done", "failed", "cancelled"), timeout: float = 90) -> dict:
        end = time.monotonic() + timeout
        run = None
        while time.monotonic() < end:
            run = self.c.request("GET", f"/api/agent-runs/{run_id}")["run"]
            if run["status"] in until:
                return run
            time.sleep(0.3)
        raise AssertionError(f"run stayed {run and run['status']}: {json.dumps(run, indent=1)[:3000]}")

    def wait_task(self, run_id: str, pred, timeout: float = 60) -> dict:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            run = self.c.request("GET", f"/api/agent-runs/{run_id}")["run"]
            for t in run["tasks"]:
                if pred(t):
                    return t
            time.sleep(0.3)
        raise AssertionError(f"no task matched: {json.dumps(run['tasks'], indent=1)[:3000]}")


@pytest.fixture(scope="module")
def pool(tmp_path_factory):
    os.environ["HARNESS_AGENT_CLIS"] = ""  # agents inherit it: look for no AI CLIs at all
    coord = start_coordinator(tmp_path_factory.mktemp("agent-tasks"))
    coord.agent("Alpha", threads=1, extra=("--tasks", "--no-share"))
    coord.agent("Beta", threads=1, extra=("--tasks", "--no-share"))
    yield Pool(coord)
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


def task(title, command, device="", **extra) -> dict:
    return {"title": title, "kind": "code", "adapter": "command", "command": command, "device": device, **extra}


def test_two_computers_each_do_a_task_and_the_results_are_pushed(pool):
    origin = make_repo(pool.coord.tmp / "origin")
    alpha, beta = pool.device("Alpha"), pool.device("Beta")
    run = pool.start({"title": "Two files", "repo": {"kind": "local", "name": str(origin)}, "results_branch": "yh/two-files",
                      "tasks": [task("Alpha's file", "echo alpha > alpha.txt", alpha["id"], checks=["test -f alpha.txt"]),
                                task("Beta's file", "mkdir -p src && echo beta > src/beta.txt", beta["id"])]})
    run = pool.wait(run["id"])
    assert run["status"] == "done", run
    by_title = {t["title"]: t for t in run["tasks"]}
    assert by_title["Alpha's file"]["deviceName"] == "Alpha" and by_title["Beta's file"]["deviceName"] == "Beta"
    assert all(t["integrated"] for t in run["tasks"])
    assert by_title["Alpha's file"]["result"]["checks"][0]["code"] == 0
    assert git("-C", str(origin), "show", "yh/two-files:alpha.txt") == "alpha\n"
    assert git("-C", str(origin), "show", "yh/two-files:src/beta.txt") == "beta\n"
    assert git("-C", str(origin), "rev-parse", "main").strip() == run["baseCommit"], "main is never touched"
    alpha_id, beta_id = by_title["Alpha's file"]["id"], by_title["Beta's file"]["id"]
    assert b"Workspace ready" in pool.c.request("GET", f"/api/agent-tasks/{alpha_id}/log")
    assert b"src/beta.txt" in pool.c.request("GET", f"/api/agent-tasks/{beta_id}/diff")


def test_the_page_has_the_agents_app(pool):
    import urllib.request

    page = urllib.request.urlopen(pool.coord.base + "/").read().decode()
    assert 'id="win-agents"' in page and 'src="agents.js"' in page and 'data-open="agents"' in page
    script = urllib.request.urlopen(pool.coord.base + "/agents.js").read().decode()
    assert "window.YHAgents" in script


def test_changes_outside_the_scope_are_rejected_until_you_accept_them(pool):
    origin = make_repo(pool.coord.tmp / "origin-scope")
    run = pool.start({"repo": {"kind": "local", "name": str(origin)}, "tasks": [
        task("Docs only", "echo x > x.txt && echo more >> docs/notes.md", scope=["docs/**"])]})
    run = pool.wait(run["id"])
    t = run["tasks"][0]
    assert t["status"] == "failed" and "x.txt" in t["error"] and t["canAccept"], t
    assert run["status"] == "failed" and not run["pushedCommit"]
    pool.c.request("POST", f"/api/agent-tasks/{t['id']}/accept", {})
    run = pool.wait(run["id"], until=("done",))
    assert git("-C", str(origin), "show", f"{run['resultsBranch']}:x.txt") == "x\n"


def test_secrets_and_workflow_changes_are_stopped(pool):
    origin = make_repo(pool.coord.tmp / "origin-secret")
    run = pool.start({"repo": {"kind": "local", "name": str(origin)}, "tasks": [
        task("Leaks a key", "printf 'key=%s%s\\n' AKIA ABCDEFGHIJKLMNOP > settings.txt"),
        task("Edits CI", "mkdir -p .github/workflows && echo 'on: push' > .github/workflows/ci.yml"),
    ]})
    run = pool.wait(run["id"])
    leak, ci = run["tasks"]
    assert leak["status"] == "failed" and "secret" in leak["error"] and "settings.txt" in leak["error"], leak
    assert ci["status"] == "failed" and "protected" in ci["error"], ci
    assert run["status"] == "failed"


def test_failing_checks_and_cancelling(pool):
    origin = make_repo(pool.coord.tmp / "origin-fail")
    run = pool.start({"repo": {"kind": "local", "name": str(origin)}, "tasks": [
        task("Fails its check", "echo y > y.txt", checks=["test -f missing.txt"]),
        task("Takes too long", "sleep 60"),
    ]})
    slow = pool.wait_task(run["id"], lambda t: t["title"] == "Takes too long" and t["phase"] == "agent")
    pool.c.request("POST", f"/api/agent-tasks/{slow['id']}/cancel", {})
    run = pool.wait(run["id"])
    failing, slow = run["tasks"]
    assert failing["status"] == "failed" and "checks still fail" in failing["error"], failing
    assert slow["status"] == "cancelled"
    # retrying the cancelled one runs it again
    retried = pool.c.request("POST", f"/api/agent-tasks/{slow['id']}/retry", {})["task"]
    assert retried["status"] in ("queued", "running")
    pool.c.request("POST", f"/api/agent-runs/{run['id']}/cancel", {})
    assert pool.wait(run["id"])["status"] == "cancelled"


def test_a_friends_computer_takes_tasks_only_when_its_owner_allows_it(pool):
    origin = make_repo(pool.coord.tmp / "origin-friend")
    coord = pool.coord
    invite = pool.c.request("POST", "/api/invites", {"label": "Sam"})
    assert invite["link"].endswith(invite["code"])
    # one friend joined with a one-time invite code and allowed tasks; another used the lend link and didn't
    coord.agent("Sams PC", threads=1, link=f"{coord.base}/#join={invite['code']}", extra=("--tasks", "--no-share"))
    coord.agent("Kims PC", threads=1, link=f"{coord.base}/#join={coord.lend_token}", extra=("--no-share",))
    sam = pool.device("Sams PC")
    kim = next(d for d in pool.c.devices() if d["name"] == "Kims PC")
    assert not sam["owned"] and sam["agentTasks"]["on"]
    assert not kim["owned"] and not kim["agentTasks"]["on"] and "friend" in kim["agentTasks"]["why"]
    assert pool.c.request("GET", "/api/invites")["invites"][0]["state"] == "used"
    run = pool.start({"repo": {"kind": "local", "name": str(origin)}, "tasks": [
        task("On Sam's", "echo sam > sam.txt", sam["id"]),
        task("On Kim's", "echo kim > kim.txt", kim["id"]),
        task("Anywhere", "echo any > any.txt"),
    ]})
    kim_task = pool.wait_task(run["id"], lambda t: t["title"] == "On Kim's" and "friend" in t["note"])
    assert kim_task["status"] == "queued"
    sam_task = pool.wait_task(run["id"], lambda t: t["title"] == "On Sam's" and t["status"] == "done")
    assert sam_task["deviceName"] == "Sams PC"
    anywhere = pool.wait_task(run["id"], lambda t: t["title"] == "Anywhere" and t["status"] == "done")
    assert anywhere["deviceName"] in ("Alpha", "Beta"), "“any of my computers” never means a friend's"
    pool.c.request("POST", f"/api/agent-runs/{run['id']}/cancel", {})


# ---------------------------------------------------------------------- GitHub, faked
class FakeGitHub:
    """Just enough of the GitHub API for the coordinator: who am I, my repositories, open a pull request."""

    def __init__(self, token: str):
        self.token = token
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

            def _authed(self):
                if self.headers.get("Authorization") != f"Bearer {gh.token}":
                    self._reply(401, {"message": "Bad credentials"})
                    return False
                return True

            def do_GET(self):
                gh.calls.append(("GET", self.path, {}))
                if not self._authed():
                    return
                if self.path == "/user":
                    return self._reply(200, {"login": "tester"})
                if self.path.startswith("/user/repos"):
                    return self._reply(200, [{"full_name": "owner/demo", "private": True, "default_branch": "main", "permissions": {"push": True}}])
                if self.path.startswith("/repos/owner/demo/branches"):
                    return self._reply(200, [{"name": "main"}])
                self._reply(404, {"message": "Not Found"})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                gh.calls.append(("POST", self.path, body))
                if not self._authed():
                    return
                if self.path == "/repos/owner/demo/pulls":
                    return self._reply(201, {"html_url": "https://github.example/owner/demo/pull/1", "number": 1})
                self._reply(404, {"message": "Not Found"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"


def test_results_go_to_github_as_a_draft_pull_request(tmp_path):
    os.environ["HARNESS_AGENT_CLIS"] = ""
    token = "fake-token-for-tests-0123456789"
    gh = FakeGitHub(token)
    git_base = tmp_path / "gh"
    make_repo(git_base / "owner" / "demo.git", bare=True)
    coord = start_coordinator(tmp_path / "pool", extra_env={"HARNESS_GITHUB_API": gh.url, "HARNESS_GITHUB_GIT": str(git_base)})
    try:
        coord.agent("Worker", threads=1, extra=("--tasks", "--no-share"))
        p = Pool(coord)
        with pytest.raises(Exception, match="Connect GitHub"):
            p.start({"repo": {"kind": "github", "name": "owner/demo"}, "tasks": [task("x", "true")]})
        assert p.c.request("POST", "/api/github/token", {"token": token}) == {"connected": True, "user": "tester"}
        secrets_file = coord.tmp / "data" / "secrets.json"
        assert stat.S_IMODE(secrets_file.stat().st_mode) == 0o600
        assert p.c.request("GET", "/api/github/repos")["repos"][0]["name"] == "owner/demo"
        overview = p.c.request("GET", "/api/agents")
        assert overview["github"] == {"connected": True, "user": "tester"} and token not in json.dumps(overview)
        p.device("Worker")
        run = p.start({"title": "Add a file", "repo": {"kind": "github", "name": "owner/demo"},
                       "tasks": [task("Add hello", "echo hi > hello.txt")]})
        run = p.wait(run["id"])
        assert run["status"] == "done", run
        assert run["prUrl"] == "https://github.example/owner/demo/pull/1"
        bare = git_base / "owner" / "demo.git"
        assert git("-C", str(bare), "show", f"{run['resultsBranch']}:hello.txt") == "hi\n"
        posted = [b for m, path, b in gh.calls if m == "POST"]
        assert posted[0]["head"] == run["resultsBranch"] and posted[0]["base"] == "main" and posted[0]["draft"] is True
        assert "Add hello" in posted[0]["body"]
        log = p.c.request("GET", f"/api/agent-tasks/{run['tasks'][0]['id']}/log").decode()
        assert token not in log
        # the coordinator restarts: runs and tasks are still there
        coord.proc.stop()
        again = start_coordinator(tmp_path / "pool", extra_env={"HARNESS_GITHUB_API": gh.url, "HARNESS_GITHUB_GIT": str(git_base)})
        coord.proc = again.proc
        coord.base = again.base
        p2 = Pool(again)
        kept = p2.c.request("GET", f"/api/agent-runs/{run['id']}")["run"]
        assert kept["status"] == "done" and kept["prUrl"] == run["prUrl"]
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()
        gh.server.shutdown()
