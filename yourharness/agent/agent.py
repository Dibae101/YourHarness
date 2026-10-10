"""The agent's connection to the coordinator."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from .. import __version__, sysinfo
from .slots import SlotPool

log = logging.getLogger("yourharness")

BEAT_S = float(os.environ.get("HARNESS_BEAT_S", 4))  # the coordinator marks a device as not responding after 20 s
INFO_S = 30.0


class SignInError(Exception):
    pass


def default_home() -> Path:
    return Path(os.environ.get("YOURHARNESS_HOME") or Path.home() / ".yourharness")


def parse_join_link(link: str) -> tuple[str, str]:
    """Split ``http://host:8420/#join=TOKEN`` into the coordinator address and the token."""
    parts = urllib.parse.urlsplit(link.strip())
    token = urllib.parse.parse_qs(parts.fragment).get("join", [""])[0] or urllib.parse.parse_qs(parts.query).get("join", [""])[0]
    if parts.scheme not in ("http", "https") or not parts.netloc or not token:
        raise SignInError("That doesn't look like a join link. It should look like http://192.168.1.20:8420/#join=...")
    return f"{parts.scheme}://{parts.netloc}", token


def join(link: str, name: str) -> dict:
    server, token = parse_join_link(link)
    req = urllib.request.Request(
        f"{server}/api/join", data=json.dumps({"token": token, "kind": "agent", "name": name}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            body = json.loads(res.read())
    except urllib.error.HTTPError as e:
        try:
            message = json.loads(e.read()).get("error")
        except ValueError:
            message = None
        raise SignInError(message or f"The coordinator answered {e.code}.") from None
    except (urllib.error.URLError, OSError) as e:
        raise SignInError(f"Can't reach the coordinator at {server} ({e}). Check that it's running and that this computer is on the same network.") from None
    return {"server": server, "token": body["deviceToken"], "deviceId": body["deviceId"], "name": body["name"]}


@dataclass
class AgentConfig:
    server: str
    token: str
    name: str
    threads: int
    sharing: bool = True
    workspace: bool = False
    home: Path = field(default_factory=default_home)


class Agent:
    def __init__(self, cfg: AgentConfig):
        self.cfg = cfg
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.outbox: asyncio.Queue | None = None
        self.params: dict[str, dict] = {}
        self.pool = SlotPool({"server": cfg.server, "token": cfg.token}, self._piece_done, cfg.home / "work")
        self.terms = None
        if cfg.workspace:
            if os.name != "posix":
                raise SystemExit("The workspace needs Linux or macOS. On Windows, run the agent inside WSL (Ubuntu).")
            from .terminal import TerminalHost

            self.terms = TerminalHost(self.send, cfg.home, {"HARNESS_URL": cfg.server, "HARNESS_TOKEN": cfg.token})

    # ------------------------------------------------------------------ sending
    def send(self, msg: dict) -> None:
        if self.outbox is not None:
            self.outbox.put_nowait(json.dumps(msg, separators=(",", ":")))

    async def _writer(self, ws, outbox: asyncio.Queue) -> None:
        while True:
            data = await outbox.get()
            await ws.send_str(data)

    def _piece_done(self, task: dict, res: dict) -> None:
        if res.get("ok"):
            self.send({"type": "result", "jobId": task["jobId"], "idx": task["idx"], "value": res.get("value"), "ms": res.get("ms", 0)})
        else:
            self.send({"type": "failed", "jobId": task["jobId"], "idx": task["idx"], "error": res.get("error")})

    # ------------------------------------------------------------------ main loop
    async def run(self) -> int:
        await self.pool.resize(self.cfg.threads if self.cfg.sharing else 0)
        delay = 1.0
        try:
            while True:
                try:
                    code = await self._connect_once()
                    if code == 4401:
                        print("The coordinator refused this agent's sign-in. It may have been removed from the pool.\n"
                              "Join again with a current link:  harness agent --join '<join link>'", file=sys.stderr)
                        return 1
                    if code == 4002:
                        print("This agent connected again from another process, so this one is stopping.", file=sys.stderr)
                        return 1
                    delay = 1.0
                    log.warning("Lost the connection to the coordinator. Reconnecting.")
                    await asyncio.sleep(1.0)
                except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
                    log.warning("Can't reach the coordinator at %s (%s). Trying again in %.0f s.", self.cfg.server, e or type(e).__name__, delay)
                    await asyncio.sleep(delay)
                    delay = min(30.0, delay * 2)
        finally:
            if self.terms:
                self.terms.close_all()
            await self.pool.close()

    def _hello(self) -> dict:
        return {
            "type": "hello", "kind": "agent", "name": self.cfg.name, "threads": self.cfg.threads, "sharing": self.cfg.sharing,
            "runtimes": ["python"], "info": sysinfo.collect(str(self.cfg.home)), "version": __version__,
            "workspace": self.terms.describe() if self.terms else None,
        }

    async def _connect_once(self) -> int | None:
        url = self.cfg.server.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/ws"
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(url, headers={"Authorization": f"Bearer {self.cfg.token}"}, heartbeat=10.0, max_msg_size=64 * 1024 * 1024) as ws:
                self.ws = ws
                self.outbox = asyncio.Queue()
                self.send(self._hello())
                tasks = [asyncio.ensure_future(t) for t in (self._writer(ws, self.outbox), self._beat(), self._refresh_info())]
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except ValueError:
                                continue
                            try:
                                self._handle(data)
                            except Exception:
                                log.exception("handling %s failed", data.get("type"))
                finally:
                    for t in tasks:
                        t.cancel()
                    self.ws = None
                    self.outbox = None
                    self.params.clear()
                    self.pool.take_queued()  # the coordinator hands these to other devices
                    if self.terms:
                        self.terms.close_all()
                return ws.close_code

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(BEAT_S)
            self.send({"type": "beat"})

    async def _refresh_info(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(INFO_S)
            info = await loop.run_in_executor(None, sysinfo.collect, str(self.cfg.home))
            self.send({"type": "update", "info": info})

    def _handle(self, m: dict) -> None:
        kind = m.get("type")
        if kind == "welcome":
            role = " and hosting the workspace" if self.terms else ""
            sharing = f"sharing {self.cfg.threads} worker processes" if self.cfg.sharing else "not sharing compute"
            print(f"Joined the pool as {m.get('name')}, {sharing}{role}.", flush=True)
        elif kind == "denied":
            print(m.get("reason") or "The coordinator refused this agent.", file=sys.stderr)
        elif kind == "chunk":
            if "params" in m:
                self.params[m["jobId"]] = m["params"]
            params = self.params.get(m["jobId"])
            if params is None:
                self.send({"type": "failed", "jobId": m["jobId"], "idx": m["idx"], "error": "This agent missed the job's settings."})
                return
            self.pool.submit({**m, "params": params})
        elif kind == "abort":
            self.pool.abort(m.get("jobId"))
            self.params.pop(m.get("jobId"), None)
        elif kind == "job":
            job = m.get("job") or {}
            if job.get("status") in ("done", "failed", "cancelled"):
                self.params.pop(job.get("id"), None)
        elif kind == "error":
            log.warning("%s", m.get("message"))
        elif isinstance(kind, str) and kind.startswith("term."):
            self._terminal(m)

    def _terminal(self, m: dict) -> None:
        if self.terms is None:
            return
        kind, sid = m.get("type"), m.get("sid")
        if not isinstance(sid, str):
            return
        if kind == "term.open":
            try:
                self.terms.open(sid, int(m.get("cols", 80)), int(m.get("rows", 24)), str(m.get("user", "")))
                log.info("Opened a workspace terminal for %s", m.get("user"))
            except OSError as e:
                self.send({"type": "term.out", "sid": sid, "data": base64.b64encode(f"Could not start a shell: {e}\r\n".encode()).decode()})
                self.send({"type": "term.exit", "sid": sid, "code": 127})
        elif kind == "term.in" and isinstance(m.get("data"), str):
            self.terms.write(sid, m["data"])
        elif kind == "term.resize":
            self.terms.resize(sid, int(m.get("cols", 80)), int(m.get("rows", 24)))
        elif kind == "term.close":
            self.terms.close(sid)
