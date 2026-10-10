"""HTTP and WebSocket front door of the coordinator."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
from dataclasses import dataclass, field
from pathlib import Path

from aiohttp import WSMsgType, web

from .. import __version__
from . import files as shared_files
from .auth import COOKIE, Limiter, client_ip, same_origin, token_from
from .install import agent_tarball, install_ps1, install_script
from .jobkinds import UserError
from .scheduler import Scheduler, clean_text
from .state import Store

log = logging.getLogger("yourharness")

WEB_DIR = Path(__file__).resolve().parent.parent / "web"
MIME = {
    ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml", ".json": "application/json", ".png": "image/png", ".ico": "image/x-icon", ".webmanifest": "application/manifest+json",
}
SECURITY_HEADERS = {"X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY"}
COOKIE_MAX_AGE = 400 * 24 * 3600


def _reject_constant(name: str):
    raise ValueError(f"{name} is not valid JSON")


def default_home() -> Path:
    return Path.home() / ".yourharness"


@dataclass
class Config:
    host: str = "0.0.0.0"
    port: int = 8420
    data_dir: Path = field(default_factory=lambda: default_home() / "coordinator")
    shared_dir: Path = field(default_factory=lambda: default_home() / "shared")
    max_upload_mb: int = 4096
    stale_s: float = 20.0
    chunk_timeout_s: float = 300.0
    max_auth_fails: int = 10


_ERRORS = {400: web.HTTPBadRequest, 401: web.HTTPUnauthorized, 403: web.HTTPForbidden, 404: web.HTTPNotFound, 429: web.HTTPTooManyRequests}


def fail(status: int, message: str, headers: dict | None = None) -> web.HTTPException:
    return _ERRORS[status](text=json.dumps({"error": message}), content_type="application/json", headers=headers)


@web.middleware
async def middleware(request: web.Request, handler):
    try:
        resp = await handler(request)
    except web.HTTPException as e:
        if e.status < 400 or not request.path.startswith("/api/") or e.content_type == "application/json":
            raise
        message = e.text or e.reason
        if message.startswith(f"{e.status}:"):
            message = e.reason
        resp = web.json_response({"error": message}, status=e.status)
    if not resp.prepared:
        for k, v in SECURITY_HEADERS.items():
            resp.headers.setdefault(k, v)
    return resp


class Coordinator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        cfg.shared_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(cfg.data_dir)
        self.sched = Scheduler(self.store, cfg.shared_dir, stale_s=cfg.stale_s, chunk_timeout_s=cfg.chunk_timeout_s)
        self.limiter = Limiter(max_fails=cfg.max_auth_fails)
        self.code_guesses = Limiter(max_fails=20)  # wrong join codes from anywhere; a 4-digit code can't survive more
        self.static = {"/": WEB_DIR / "index.html"}
        for path in WEB_DIR.rglob("*"):
            if path.is_file() and path.suffix in MIME:
                self.static["/" + path.relative_to(WEB_DIR).as_posix()] = path
        self._maintenance: asyncio.Task | None = None
        self.lan_ip = primary_ip() or ""

    # ------------------------------------------------------------------ app
    def build(self) -> web.Application:
        app = web.Application(middlewares=[middleware], client_max_size=2 * 1024 * 1024)
        r = app.router
        r.add_post("/api/join", self.join)
        r.add_get("/api/me", self.me)
        r.add_post("/api/leave", self.leave)
        r.add_get("/api/devices", self.devices)
        r.add_delete("/api/devices/{device_id}", self.remove_device)
        r.add_get("/api/jobs", self.list_jobs)
        r.add_post("/api/jobs", self.submit_job)
        r.add_get("/api/jobs/{job_id}", self.get_job)
        r.add_post("/api/jobs/{job_id}/cancel", self.cancel_job)
        r.add_get("/api/files", self.list_files)
        r.add_get("/api/files/{name:.+}", self.download)
        r.add_put("/api/files/{name:.+}", self.upload)
        r.add_delete("/api/files/{name:.+}", self.delete_file)
        r.add_get("/api/pool", self.pool_list)
        r.add_put("/api/pool/{name:.+}", self.pool_put)
        r.add_get("/api/pool/{name:.+}", self.pool_get)
        r.add_delete("/api/pool/{name:.+}", self.pool_delete)
        r.add_get("/install.sh", self.install_sh)
        r.add_get("/install.ps1", self.install_ps1)
        r.add_get("/agent.tar.gz", self.agent_tar)
        r.add_get("/ws", self.websocket)
        r.add_get("/{path:.*}", self.serve_static)
        app.on_startup.append(self._on_startup)
        app.on_shutdown.append(self._on_shutdown)
        return app

    async def _on_startup(self, app):
        self._maintenance = asyncio.ensure_future(self.sched.run_maintenance())

    async def _on_shutdown(self, app):
        if self._maintenance:
            self._maintenance.cancel()
        for d in list(self.sched.devices.values()):
            d.close(1001, "The coordinator is stopping.")
        await asyncio.sleep(0.1)

    # ------------------------------------------------------------------ sign-in
    def _check_blocked(self, request) -> str:
        ip = client_ip(request)
        wait = self.limiter.retry_after(ip)
        if wait:
            minutes = max(1, round(wait / 60))
            raise fail(429, f"Too many failed sign-ins from this address. Try again in {minutes} minute{'s' if minutes > 1 else ''}.", {"Retry-After": str(int(wait))})
        return ip

    def authed(self, request: web.Request, write: bool = False) -> dict:
        ip = self._check_blocked(request)
        token, how = token_from(request)
        rec = self.store.device_for_token(token) if token else None
        if rec is None:
            if token:
                self.limiter.fail(ip)
            raise fail(401, "This device isn't signed in. Open a join link from the coordinator.")
        if how == "cookie" and write and not same_origin(request):
            raise fail(403, "That request came from another website, so it was refused.")
        return rec

    async def join(self, request: web.Request) -> web.Response:
        ip = self._check_blocked(request)
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise fail(400, "Send the join token as JSON.")
        if self.code_guesses.retry_after("all"):
            raise fail(429, "Too many wrong join codes. The code was changed; read the new one from the coordinator's terminal.")
        if not self.store.join_ok(body.get("token")):
            self.limiter.fail(ip)
            self.code_guesses.fail("all")
            if self.code_guesses.retry_after("all"):
                self.store.rotate_join_code()
                self.code_guesses = Limiter(max_fails=20)
                log.warning("20 wrong join codes in a row; the join code was changed.")
            raise fail(401, "That join code or link isn't valid. Check the coordinator's terminal for the current one.")
        kind = "agent" if body.get("kind") == "agent" else "browser"
        name = clean_text(body.get("name"), 40, "Unnamed device")
        if kind == "browser":
            existing, how = token_from(request)
            rec = self.store.device_for_token(existing) if how == "cookie" else None
            if rec is not None:  # opening the join link again on a signed-in browser
                return web.json_response({"deviceId": rec["id"], "name": rec["name"]})
        rec, token = self.store.add_device(kind, name)
        if kind == "agent":
            return web.json_response({"deviceId": rec["id"], "name": rec["name"], "deviceToken": token})
        resp = web.json_response({"deviceId": rec["id"], "name": rec["name"]})
        resp.set_cookie(COOKIE, token, max_age=COOKIE_MAX_AGE, httponly=True, samesite="Strict", path="/", secure=request.secure)
        return resp

    async def me(self, request):
        rec = self.authed(request)
        return web.json_response({"deviceId": rec["id"], "name": rec["name"], "kind": rec["kind"], "version": __version__})

    async def leave(self, request):
        rec = self.authed(request, write=True)
        self.sched.remove_device(rec["id"])
        resp = web.json_response({"ok": True})
        resp.del_cookie(COOKIE, path="/")
        return resp

    async def devices(self, request):
        self.authed(request)
        return web.json_response({"devices": self.sched.device_list()})

    async def remove_device(self, request):
        rec = self.authed(request, write=True)
        target = request.match_info["device_id"]
        if target == rec["id"]:
            raise fail(400, "Use “Leave the pool” to remove the device you're on.")
        if not self.sched.remove_device(target):
            raise fail(404, "There is no device with that id.")
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ jobs
    async def list_jobs(self, request):
        self.authed(request)
        return web.json_response({"jobs": [j.summary() for j in self.sched.jobs]})

    async def submit_job(self, request):
        rec = self.authed(request, write=True)
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise fail(400, "Send the job as JSON: {\"kind\": ..., \"params\": {...}}.")
        live = self.sched.devices.get(rec["id"])
        try:
            job = self.sched.create_job(body.get("kind"), body.get("params"), live.name if live else rec["name"])
        except UserError as e:
            raise fail(400, str(e))
        return web.json_response({"job": job.summary()}, status=201)

    async def get_job(self, request):
        self.authed(request)
        job = self.sched.find_job(request.match_info["job_id"])
        if job is None:
            raise fail(404, "There is no job with that id. Only the last 20 runs are kept.")
        try:
            wait = min(60.0, max(0.0, float(request.query.get("wait", 0))))
        except ValueError:
            wait = 0.0
        if wait:
            await self.sched.wait_job(job, wait)
        return web.json_response({"job": job.summary()})

    async def cancel_job(self, request):
        rec = self.authed(request, write=True)
        job = self.sched.find_job(request.match_info["job_id"])
        if job is None:
            raise fail(404, "There is no job with that id.")
        self.sched.stop_job(job, "cancelled", f"Cancelled by {rec['name']}.")
        return web.json_response({"job": job.summary()})

    # ------------------------------------------------------------------ shared folder
    def _name(self, request) -> str:
        name = shared_files.safe_name(request.match_info["name"])
        if not name:
            raise fail(400, "That file name isn't allowed.")
        return name

    async def list_files(self, request):
        self.authed(request)
        return web.json_response({"files": self.sched.list_files()})

    async def download(self, request):
        self.authed(request)
        return shared_files.download(self.cfg.shared_dir, self._name(request))

    async def upload(self, request):
        rec = self.authed(request, write=True)
        name = self._name(request)
        size = await shared_files.receive(request, self.cfg.shared_dir, name, self.cfg.max_upload_mb * 1024 * 1024)
        live = self.sched.devices.get(rec["id"])
        self.sched.file_meta[name] = {"by": live.name if live else rec["name"], "color": live.color if live else None}
        self.sched.broadcast_files()
        return web.json_response({"ok": True, "name": name, "size": size}, status=201)

    async def delete_file(self, request):
        self.authed(request, write=True)
        name = self._name(request)
        try:
            (self.cfg.shared_dir / name).unlink()
        except FileNotFoundError:
            raise fail(404, f"There is no shared file named {name}.")
        self.sched.file_meta.pop(name, None)
        self.sched.broadcast_files()
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ pooled storage
    async def pool_list(self, request):
        self.authed(request)
        return web.json_response(self.sched.pool_store.listing())

    async def pool_put(self, request):
        rec = self.authed(request, write=True)
        name = self._name(request)
        live = self.sched.devices.get(rec["id"])
        try:
            size = await self.sched.pool_store.put_file(name, request.content, live.name if live else rec["name"], self.cfg.max_upload_mb * 1024 * 1024)
        except UserError as e:
            raise fail(400, str(e))
        return web.json_response({"ok": True, "name": name, "size": size}, status=201)

    async def pool_get(self, request):
        self.authed(request)
        try:
            return await self.sched.pool_store.stream(request, self._name(request))
        except UserError as e:
            raise fail(404, str(e))

    async def pool_delete(self, request):
        self.authed(request, write=True)
        name = self._name(request)
        if not self.sched.pool_store.delete(name):
            raise fail(404, f"There is no pooled file named {name}.")
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ agent install
    async def install_sh(self, request):
        if not re.fullmatch(r"[A-Za-z0-9.:\-\[\]]+", request.host):
            raise web.HTTPBadRequest(text="Bad host name.")
        return web.Response(text=install_script(f"{request.scheme}://{request.host}"), content_type="text/x-shellscript")

    async def install_ps1(self, request):
        if not re.fullmatch(r"[A-Za-z0-9.:\-\[\]]+", request.host):
            raise web.HTTPBadRequest(text="Bad host name.")
        return web.Response(text=install_ps1(f"{request.scheme}://{request.host}"), content_type="text/plain")

    async def agent_tar(self, request):
        return web.Response(body=agent_tarball(), content_type="application/gzip")

    # ------------------------------------------------------------------ portal
    async def serve_static(self, request):
        path = self.static.get("/" + request.match_info["path"])
        if path is None:
            raise web.HTTPNotFound(text="Not found")
        return web.FileResponse(path, headers={"Content-Type": MIME[path.suffix], "Cache-Control": "no-cache"})

    def host_of(self, ip: str) -> str:
        """A device on this machine connects as 127.0.0.1 or as the LAN address; call both the same."""
        return self.lan_ip if ip in ("127.0.0.1", "::1") and self.lan_ip else ip

    # ------------------------------------------------------------------ live connection
    async def websocket(self, request: web.Request):
        ws = web.WebSocketResponse(heartbeat=10.0, max_msg_size=16 * 1024 * 1024)
        ip = client_ip(request)
        token, how = token_from(request)
        blocked = self.limiter.retry_after(ip)
        rec = self.store.device_for_token(token) if token and not blocked else None
        await ws.prepare(request)
        if blocked or rec is None or (how == "cookie" and not same_origin(request)):
            if token and rec is None and not blocked:
                self.limiter.fail(ip)
            reason = ("Too many failed sign-ins from this address. Try again later." if blocked
                      else "This device isn't signed in. Open a join link from the coordinator.")
            await ws.send_json({"type": "denied", "reason": reason})
            await ws.close(code=4429 if blocked else 4401, message=b"Sign in")
            return ws
        try:
            hello = await ws.receive_json(timeout=10)
        except (asyncio.TimeoutError, TypeError, ValueError):
            await ws.close(code=4000, message=b"Expected hello")
            return ws
        if not isinstance(hello, dict) or hello.get("type") != "hello":
            await ws.close(code=4000, message=b"Expected hello")
            return ws
        device = self.sched.connect(rec, ws, hello, self.host_of(ip))
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data, parse_constant=_reject_constant)
                except ValueError:
                    continue
                if isinstance(data, dict):
                    try:
                        self.sched.handle(device, data)
                    except Exception:
                        log.exception("handling %s from %s failed", data.get("type"), device.name)
        finally:
            self.sched.disconnect(device)
        return ws


def primary_ip() -> str | None:
    """The address other devices on the network use to reach this machine."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 9))  # no packet is sent; this only picks the outgoing interface
        ip = s.getsockname()[0]
        return None if ip.startswith("127.") else ip
    except OSError:
        return None
    finally:
        s.close()


async def run_local_agent(cfg: Config, coord: "Coordinator", port: int) -> None:
    """The coordinator's own computer joins the pool and hosts a workspace, so there is a terminal at once."""
    from .. import sysinfo
    from ..agent.agent import Agent, AgentConfig

    path = cfg.data_dir / "local-agent.json"
    token = None
    if path.exists():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if coord.store.device_for_token(saved.get("token")):
            token = saved["token"]
    if not token:
        _, token = coord.store.add_device("agent", f"{sysinfo.platform_name()} (this computer)")
        path.write_text(json.dumps({"token": token}), encoding="utf-8")
        os.chmod(path, 0o600)
    home = default_home() / "local-agent"
    home.mkdir(parents=True, exist_ok=True)
    agent_cfg = AgentConfig(server=f"http://127.0.0.1:{port}", token=token, name=f"{sysinfo.platform_name()} (this computer)",
                            threads=max(1, (os.cpu_count() or 2) - 1), workspace=True, home=home)
    await Agent(agent_cfg).run()


async def serve(cfg: Config, public_url: str | None = None, show_qr: bool = True, local_agent: bool = True) -> None:
    coord = Coordinator(cfg)
    runner = web.AppRunner(coord.build(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, cfg.host, cfg.port)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    base = (public_url or f"http://{primary_ip() or 'localhost'}:{port}").rstrip("/")
    coord.sched.public_url = base
    link = f"{base}/#join={coord.store.join_token}"
    print(f"\nYourHarness coordinator {__version__} is running.\n")
    print("Phones, tablets and browsers: open this address and enter the join code")
    print(f"  {base}\n")
    print(f"  JOIN CODE: {coord.store.join_code}\n")
    print("Windows PC (sets up Ubuntu for you): in PowerShell run")
    print(f"  irm {base}/install.ps1 | iex\n")
    print("Mac or Linux computers: run this in a terminal")
    print(f"  curl -fsSL {base}/install.sh | bash -s -- --join {coord.store.join_code} --workspace")
    print("  Leave out --workspace on computers that shouldn't host a terminal.\n")
    print(f"Or skip typing the code by opening: {link}\n")
    print(f"Shared folder: {cfg.shared_dir}")
    print(f"Sign-ins are stored in: {cfg.data_dir}")
    print(f"Listening on port {port}", flush=True)
    if show_qr:
        try:
            import qrcode  # optional

            print("\nScan with a phone camera to join:")
            qr = qrcode.QRCode(border=1)
            qr.add_data(link)
            qr.print_ascii(invert=True)
        except ImportError:
            pass
    from . import discovery
    beacon = await discovery.start(port, int(os.environ.get("HARNESS_BEACON_PORT", discovery.BEACON_PORT)))
    local = asyncio.ensure_future(run_local_agent(cfg, coord, port)) if local_agent and os.name == "posix" else None
    try:
        await asyncio.Event().wait()
    finally:
        if beacon:
            beacon.close()
        if local:
            local.cancel()
        await runner.cleanup()
