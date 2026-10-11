"""Test helpers: a real coordinator and real agents as subprocesses, plus fake browsers.

Fake browsers sign in with a cookie like a real page, speak the same WebSocket
protocol, and compute built-in pieces with the Python kernels.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
import pytest

from yourharness import kernels

ROOT = Path(__file__).resolve().parent.parent


class Proc:
    """A subprocess whose output is drained in the background so it never blocks."""

    def __init__(self, args: list[str], env: dict):
        self.lines: list[str] = []
        self.p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._drain, daemon=True).start()

    def _drain(self):
        for line in self.p.stdout:
            self.lines.append(line.rstrip("\n"))

    def wait_for(self, text: str, timeout: float = 20) -> str:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for line in self.lines:
                if text in line:
                    return line
            if self.p.poll() is not None:
                break
            time.sleep(0.05)
        raise AssertionError(f"never saw {text!r}; output:\n" + "\n".join(self.lines))

    def stop(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(5)
            except subprocess.TimeoutExpired:
                self.p.kill()


@dataclass
class Coord:
    base: str
    token: str
    tmp: Path
    proc: Proc
    agents: list = field(default_factory=list)

    @property
    def link(self) -> str:
        return f"{self.base}/#join={self.token}"

    def agent(self, name: str, threads: int = 2, workspace: bool = False) -> Proc:
        home = self.tmp / f"agent-{name.replace(' ', '-')}"
        (home / "home").mkdir(parents=True, exist_ok=True)
        # HOME is the folder the Files app may browse; keep it inside the test's temp folder
        env = {**os.environ, "YOURHARNESS_HOME": str(home), "HARNESS_BEAT_S": "0.5", "HOME": str(home / "home")}
        args = [sys.executable, "-m", "yourharness", "agent", "--join", self.link, "--threads", str(threads), "--name", name]
        if workspace:
            args.append("--workspace")
        proc = Proc(args, env)
        proc.wait_for("Joined the pool")
        self.agents.append(proc)
        return proc

    def client(self, name: str = "test client"):
        """A REST client with its own device sign-in, like the workspace CLI."""
        from yourharness.client import Client

        import urllib.request

        req = urllib.request.Request(f"{self.base}/api/join", data=json.dumps({"token": self.token, "kind": "agent", "name": name}).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req) as res:
            token = json.loads(res.read())["deviceToken"]
        return Client(self.base, token)


def start_coordinator(tmp: Path, stale_s: str = "2", extra_env: dict | None = None) -> Coord:
    env = {**os.environ, "HARNESS_STALE_S": stale_s, "NO_QR": "1", "HARNESS_REPAIR_S": "1", **(extra_env or {})}
    args = [sys.executable, "-m", "yourharness", "serve", "--host", "127.0.0.1", "--port", "0",
            "--data-dir", str(tmp / "data"), "--shared-dir", str(tmp / "shared"), "--no-qr", "--no-local-agent"]
    proc = Proc(args, env)
    port = int(proc.wait_for("Listening on port").rsplit(" ", 1)[1])
    token = json.loads((tmp / "data" / "coordinator.json").read_text())["join_token"]
    return Coord(f"http://127.0.0.1:{port}", token, tmp, proc)


@pytest.fixture(scope="module")
def coord(tmp_path_factory):
    c = start_coordinator(tmp_path_factory.mktemp("pool"))
    yield c
    for a in c.agents:
        a.stop()
    c.proc.stop()


@pytest.fixture
def fresh_coord(tmp_path):
    c = start_coordinator(tmp_path)
    yield c
    for a in c.agents:
        a.stop()
    c.proc.stop()


class FakeBrowser:
    """Signs in with a cookie and computes pieces like a browser tab would."""

    def __init__(self, coord: Coord, name: str, threads: int = 2, *, stall: bool = False, beat: bool = True, origin: str | None = None):
        self.coord, self.name, self.threads = coord, name, threads
        self.stall, self.beat = stall, beat
        self.origin = origin or coord.base
        self.msgs: list[dict] = []
        self.params: dict[str, dict] = {}
        self.computed = 0
        self.closed_code = None

    async def __aenter__(self):
        self.http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True))
        r = await self.http.post(f"{self.coord.base}/api/join", json={"token": self.coord.token, "name": self.name})
        assert r.status == 200, await r.text()
        self.ws = await self.http.ws_connect(self.coord.base.replace("http", "ws") + "/ws", headers={"Origin": self.origin})
        await self.ws.send_json({"type": "hello", "name": self.name, "threads": self.threads, "sharing": True, "runtimes": ["js"], "info": {"platform": "Test", "cores": self.threads}})
        self.reader = asyncio.ensure_future(self._read())
        self.beater = asyncio.ensure_future(self._beat())
        return self

    async def __aexit__(self, *exc):
        self.reader.cancel()
        self.beater.cancel()
        await self.ws.close()
        await self.http.close()

    async def _beat(self):
        while self.beat:
            await asyncio.sleep(0.5)
            await self.ws.send_json({"type": "beat"})

    async def _read(self):
        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(self.threads)
        async for m in self.ws:
            if m.type != aiohttp.WSMsgType.TEXT:
                continue
            msg = json.loads(m.data)
            self.msgs.append(msg)
            if msg["type"] == "chunk" and not self.stall:
                if "params" in msg:
                    self.params[msg["jobId"]] = msg["params"]
                task = {**msg, "params": self.params[msg["jobId"]]}

                async def work(task=task):
                    async with sem:
                        if task["kind"] == "custom":  # stands in for running the JavaScript
                            value = task["idx"]
                        else:
                            value = await loop.run_in_executor(None, kernels.compute, task, {"server": self.coord.base, "token": self.file_token})
                        self.computed += 1
                        await self.ws.send_json({"type": "result", "jobId": task["jobId"], "idx": task["idx"], "value": value, "ms": 1})

                asyncio.ensure_future(work())
        self.closed_code = self.ws.close_code

    file_token: str = ""

    def first(self, kind: str) -> dict | None:
        return next((m for m in self.msgs if m["type"] == kind), None)

    async def wait_for(self, pred, timeout: float = 15):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for m in self.msgs:
                if pred(m):
                    return m
            await asyncio.sleep(0.05)
        raise AssertionError(f"timed out; saw types {[m['type'] for m in self.msgs][-20:]}")

    async def run(self, kind: str, params: dict, timeout: float = 60) -> dict:
        start = len(self.msgs)
        await self.ws.send_json({"type": "submit", "job": {"kind": kind, "params": params}})
        end = time.monotonic() + timeout
        job_id = None
        while time.monotonic() < end:
            for m in self.msgs[start:]:
                if m["type"] == "error":
                    raise AssertionError(m["message"])
                if m["type"] == "job" and m["job"]["createdBy"] == self.name:
                    job_id = job_id or m["job"]["id"]
                    if m["job"]["id"] == job_id and m["job"]["status"] in ("done", "failed", "cancelled"):
                        return m["job"]
            await asyncio.sleep(0.05)
        raise AssertionError(f"{kind} job did not finish")
