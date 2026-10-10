"""The agent's connection to the coordinator."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
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
LOAD_S = float(os.environ.get("HARNESS_LOAD_S", 2))  # how often live CPU and memory use is reported


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


def join(link_or_code: str, name: str, server: str | None = None) -> dict:
    """Join with a 4-digit code plus the coordinator's address, or with a full join link."""
    text = link_or_code.strip()
    if text.isdigit():
        if not server:
            raise SignInError("A join code needs the coordinator's address too: harness agent --server http://192.168.1.20:8420 --join 1234")
        token = text
        server = server if "://" in server else f"http://{server}"
        server = server.rstrip("/")
    else:
        server, token = parse_join_link(text)
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
    coordinator: bool = False


class Agent:
    def __init__(self, cfg: AgentConfig):
        self.cfg = cfg
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.outbox: asyncio.Queue | None = None
        self.params: dict[str, dict] = {}
        self.pool = SlotPool({"server": cfg.server, "token": cfg.token}, self._piece_done, cfg.home / "work")
        self.terms = None
        self.files = None
        self.runner = None
        self.cpu = sysinfo.CpuMeter()
        if cfg.workspace:  # a computer that hosts a terminal also lets the pool browse its home folder
            from .files import HomeFiles

            self.files = HomeFiles()
            from .runner import Runner

            if Runner.available():  # the pool shell can run commands here
                self.runner = Runner(cfg.server, cfg.token, cfg.home, self.send, {"HARNESS_URL": cfg.server})
        if cfg.workspace:
            env = {"HARNESS_URL": cfg.server, "HARNESS_TOKEN": cfg.token}
            if os.name == "posix":
                from .terminal import TerminalHost

                self.terms = TerminalHost(self.send, cfg.home, env)
            else:
                try:
                    from .terminal_win import WinTerminalHost

                    self.terms = WinTerminalHost(self.send, cfg.home, env)
                except ImportError as e:  # pywinpty missing: share compute without a terminal
                    log.warning("This computer can't host a terminal (%s). It still shares its processors.", e)

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
                    if code == 4429:
                        log.warning("The coordinator is temporarily refusing this address after failed sign-ins. Retrying in 60 s.")
                        await asyncio.sleep(60)
                        continue
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
            "workspace": self.terms.describe() if self.terms else None, "coordinator": self.cfg.coordinator,
            "files": self.files.describe() if self.files else None,
            "exec": self.runner.describe() if self.runner else None,
        }

    async def _connect_once(self) -> int | None:
        url = self.cfg.server.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/ws"
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(url, headers={"Authorization": f"Bearer {self.cfg.token}"}, heartbeat=10.0, max_msg_size=64 * 1024 * 1024) as ws:
                self.ws = ws
                self.outbox = asyncio.Queue()
                self.send(self._hello())
                tasks = [asyncio.ensure_future(t) for t in (self._writer(ws, self.outbox), self._beat(), self._refresh_info(), self._report_load())]
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
                    if self.runner:
                        for run_id in list(self.runner.procs):  # the coordinator has already given up on these
                            self.runner.kill(run_id)
                    self.pool.dropped.clear()  # job ids restart if the coordinator restarts
                    self.pool.take_queued()  # the coordinator hands these to other devices
                    if self.terms:
                        self.terms.close_all()
                return ws.close_code

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(BEAT_S)
            self.send({"type": "beat"})

    async def _report_load(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(LOAD_S)
            cpu = self.cpu.sample()
            total, avail = await loop.run_in_executor(None, sysinfo.memory)
            used = round((total - avail) / sysinfo.GB, 1) if total and avail else None
            self.send({"type": "update", "load": {"cpu": cpu, "memUsedGB": used}})

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
        elif kind in ("store.put", "store.get", "store.del"):
            asyncio.ensure_future(self._store(m))
        elif kind == "fs":
            asyncio.ensure_future(self._fs(m))
        elif kind == "exec.start" and self.runner:
            asyncio.ensure_future(self.runner.run(m))
        elif kind == "exec.cancel" and self.runner:
            self.runner.kill(str(m.get("id")))
        elif isinstance(kind, str) and kind.startswith("term."):
            self._terminal(m)

    async def _store(self, m: dict) -> None:
        """Keep, return or delete a chunk of pooled storage. Chunks are named by their SHA-256."""
        h, rid = m.get("hash"), m.get("rid")
        try:
            if not isinstance(h, str) or not re.fullmatch(r"[0-9a-f]{64}", h):
                raise ValueError("bad chunk name")
            path = self.cfg.home / "store" / h
            loop = asyncio.get_running_loop()
            if m["type"] == "store.put":
                data = base64.b64decode(m["data"])
                if hashlib.sha256(data).hexdigest() != h:
                    raise ValueError("chunk failed its checksum")
                await loop.run_in_executor(None, self._write_chunk, path, data)
                self.send({"type": "store.ack", "rid": rid})
            elif m["type"] == "store.get":
                data = await loop.run_in_executor(None, path.read_bytes)
                self.send({"type": "store.data", "rid": rid, "data": base64.b64encode(data).decode("ascii")})
            else:
                path.unlink(missing_ok=True)
        except Exception as e:
            if rid:
                self.send({"type": "store.error", "rid": rid, "error": f"{type(e).__name__}: {e}"})

    async def _fs(self, m: dict) -> None:
        rid = m.get("rid")
        if self.files is None:
            self.send({"type": "fs.error", "rid": rid, "error": "This computer doesn't allow file access."})
            return
        from .files import FileAccessError

        try:
            res = await asyncio.get_running_loop().run_in_executor(None, self.files.handle, str(m.get("op")), m)
            self.send({"type": "fs.reply", "rid": rid, **res})
        except FileAccessError as e:
            self.send({"type": "fs.error", "rid": rid, "error": str(e)})
        except OSError as e:
            self.send({"type": "fs.error", "rid": rid, "error": e.strerror or str(e)})
        except Exception as e:  # keep the agent running whatever a request does
            self.send({"type": "fs.error", "rid": rid, "error": f"{type(e).__name__}: {e}"})

    @staticmethod
    def _write_chunk(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

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
