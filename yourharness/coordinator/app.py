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
from .state import CAP_ADMIN, CAP_FILES, CAP_SHELL, CAP_SUBMIT, CAP_REFUSALS, Store, caps_of

log = logging.getLogger("yourharness")

WEB_DIR = Path(__file__).resolve().parent.parent / "web"
MIME = {
    ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml", ".json": "application/json", ".png": "image/png", ".ico": "image/x-icon", ".webmanifest": "application/manifest+json",
}
# 'unsafe-eval' is needed on purpose: the "Your own code" job compiles the code you
# type with new Function() inside a worker, which is the whole point of that job.
# Everything else is locked down, so an injected <script> or a stray external URL
# still cannot run or phone home. font-src is 'self' only, so the page needs no
# network but the pool's own.
CSP = (
    "default-src 'self'; script-src 'self' 'unsafe-eval'; worker-src 'self' blob:; "
    "style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self'; "
    "connect-src 'self' ws: wss:; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY",
    "Content-Security-Policy": CSP,
}
COOKIE_MAX_AGE = 400 * 24 * 3600
CODE_RE = re.compile(r"\d{4}")  # the shape of a join code, so junk doesn't count as guessing one
CODE_GUESSES = 20  # wrong codes the whole pool may make before code joins turn off: a 0.2% chance to hit


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
        # Wrong join codes from anywhere. A 4-digit code is only safe if the whole pool
        # gets a few guesses at it, so once these run out the code is replaced and code
        # joins stay off until an owner asks for a new code (or the coordinator restarts).
        self.code_fails = 0
        self.static = {"/": WEB_DIR / "index.html"}
        for path in WEB_DIR.rglob("*"):
            if path.is_file() and path.suffix in MIME:
                self.static["/" + path.relative_to(WEB_DIR).as_posix()] = path
        self._maintenance: asyncio.Task | None = None
        self.lan_ip = primary_ip() or ""
        # An online pool accepts join LINKS only. The 4-digit code has 10,000
        # possibilities, which is fine on a network you own and far too few once the
        # coordinator can be reached from the internet. Set HARNESS_ONLINE=1 there.
        self.online = os.environ.get("HARNESS_ONLINE", "").strip().lower() in ("1", "true", "yes", "on")

    # ------------------------------------------------------------------ app
    def build(self) -> web.Application:
        app = web.Application(middlewares=[middleware], client_max_size=2 * 1024 * 1024)
        r = app.router
        r.add_post("/api/join", self.join)
        r.add_post("/api/join-code", self.new_join_code)
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
        r.add_get("/api/os/list", self.os_list)
        r.add_get("/api/os/manifest", self.os_manifest)
        r.add_get("/api/os/file", self.os_get)
        r.add_put("/api/os/file", self.os_put)
        r.add_delete("/api/os/file", self.os_delete)
        r.add_post("/api/os/mkdir", self.os_mkdir)
        r.add_post("/api/os/rename", self.os_rename)
        r.add_get("/api/fs/{device_id}", self.fs_list)
        r.add_get("/api/fs/{device_id}/download", self.fs_download)
        r.add_put("/api/fs/{device_id}/upload", self.fs_upload)
        r.add_post("/api/fs/{device_id}/mkdir", self.fs_mkdir)
        r.add_post("/api/fs/{device_id}/rename", self.fs_rename)
        r.add_delete("/api/fs/{device_id}", self.fs_delete)
        r.add_post("/api/fs-copy", self.fs_copy)
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

    def authed(self, request: web.Request, write: bool = False, need: str | None = None) -> dict:
        token, how = token_from(request)
        rec = self.store.device_for_token(token) if token else None
        if rec is None:
            # A signed-in device is never locked out: behind a proxy every device shares
            # one address, and a stranger's wrong guesses must not cut the others off.
            ip = self._check_blocked(request)
            if token:
                self.limiter.fail(ip)
            raise fail(401, "This device isn't signed in. Open a join link from the coordinator.")
        if how == "cookie" and write and not same_origin(request):
            raise fail(403, "That request came from another website, so it was refused.")
        if need is not None and need not in caps_of(rec):
            # a refusal, not a failed sign-in: the device is who it says it is
            raise fail(403, CAP_REFUSALS.get(need, "This device isn't allowed to do that."))
        return rec

    async def join(self, request: web.Request) -> web.Response:
        if not same_origin(request):
            raise fail(403, "That request came from another website, so it was refused.")
        ip = self._check_blocked(request)
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise fail(400, "Send the join token as JSON.")
        offered = body.get("token")
        code_shaped = isinstance(offered, str) and CODE_RE.fullmatch(offered.strip()) is not None
        if code_shaped and self.store.code_locked:
            raise fail(429, "Too many wrong join codes, so joining by code is off. Use the join link, or ask an owner for a new code.")
        caps = self.store.caps_for_join(offered, allow_code=not self.online)
        if caps is None:
            self.limiter.fail(ip)
            # Only a code-shaped guess counts against the code. Otherwise anyone who can
            # reach this page could switch codes off with junk.
            if code_shaped:
                self.code_fails += 1
                if self.code_fails >= CODE_GUESSES:
                    self.store.rotate_join_code()
                    self.store.code_locked = True
                    self.sched.broadcast({"type": "joinCode", "joinCode": None}, need=CAP_ADMIN)
                    log.warning("%d wrong join codes; joining by code is off until an owner makes a new code.", CODE_GUESSES)
            if self.online and code_shaped:
                raise fail(401, "This pool is reachable from outside your network, so it only accepts join links. Ask for a link.")
            raise fail(401, "That join code or link isn't valid. Check the coordinator's terminal for the current one.")
        kind = "agent" if body.get("kind") == "agent" else "browser"
        name = clean_text(body.get("name"), 40, "Unnamed device")
        if kind == "browser":
            existing, how = token_from(request)
            rec = self.store.device_for_token(existing) if how == "cookie" else None
            # Reopening the same kind of link on a signed-in browser keeps the device it
            # already is. A link granting something different signs it in afresh, so an
            # owner link opened on a lender's browser is not silently ignored.
            if rec is not None and caps_of(rec) == caps:
                return web.json_response({"deviceId": rec["id"], "name": rec["name"], "caps": list(caps)})
        rec, token = self.store.add_device(kind, name, caps)
        if kind == "agent":
            return web.json_response({"deviceId": rec["id"], "name": rec["name"], "deviceToken": token, "caps": list(caps)})
        resp = web.json_response({"deviceId": rec["id"], "name": rec["name"], "caps": list(caps)})
        resp.set_cookie(COOKIE, token, max_age=COOKIE_MAX_AGE, httponly=True, samesite="Strict", path="/", secure=request.secure)
        return resp

    async def new_join_code(self, request):
        """An owner asks for a fresh 4-digit code, which also turns code joins back on."""
        self.authed(request, write=True, need=CAP_ADMIN)
        code = self.store.rotate_join_code()
        self.code_fails, self.store.code_locked = 0, False
        self.sched.broadcast({"type": "joinCode", "joinCode": code}, need=CAP_ADMIN)
        return web.json_response({"joinCode": code})

    async def me(self, request):
        rec = self.authed(request)
        return web.json_response({"deviceId": rec["id"], "name": rec["name"], "kind": rec["kind"],
                                  "caps": list(caps_of(rec)), "version": __version__})

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
        rec = self.authed(request, write=True, need=CAP_ADMIN)
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
        rec = self.authed(request, write=True, need=CAP_SUBMIT)
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
        rec = self.authed(request, write=True, need=CAP_SUBMIT)
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
        self.authed(request, need=CAP_FILES)
        return web.json_response({"files": self.sched.list_files()})

    async def download(self, request):
        self.authed(request, need=CAP_FILES)
        return shared_files.download(self.cfg.shared_dir, self._name(request))

    async def upload(self, request):
        rec = self.authed(request, write=True, need=CAP_FILES)
        name = self._name(request)
        size = await shared_files.receive(request, self.cfg.shared_dir, name, self.cfg.max_upload_mb * 1024 * 1024)
        live = self.sched.devices.get(rec["id"])
        self.sched.file_meta[name] = {"by": live.name if live else rec["name"], "color": live.color if live else None}
        self.sched.broadcast_files()
        return web.json_response({"ok": True, "name": name, "size": size}, status=201)

    async def delete_file(self, request):
        self.authed(request, write=True, need=CAP_FILES)
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
        self.authed(request, need=CAP_FILES)
        return web.json_response(self.sched.pool_store.listing())

    async def pool_put(self, request):
        rec = self.authed(request, write=True, need=CAP_FILES)
        name = self._name(request)
        live = self.sched.devices.get(rec["id"])
        try:
            size = await self.sched.pool_store.put_file(name, request.content, live.name if live else rec["name"], self.cfg.max_upload_mb * 1024 * 1024)
        except UserError as e:
            raise fail(400, str(e))
        return web.json_response({"ok": True, "name": name, "size": size}, status=201)

    async def pool_get(self, request):
        self.authed(request, need=CAP_FILES)
        try:
            return await self.sched.pool_store.stream(request, self._name(request))
        except UserError as e:
            raise fail(404, str(e))

    async def pool_delete(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        name = self._name(request)
        if not self.sched.pool_store.delete(name):
            raise fail(404, f"There is no pooled file named {name}.")
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ the OS disk (/pool)
    def _os(self, fn, *args):
        try:
            return fn(*args)
        except UserError as e:
            raise fail(400, str(e))

    async def os_list(self, request):
        self.authed(request, need=CAP_FILES)
        path = request.query.get("path", "")
        from .pool_store import clean_path

        store = self.sched.pool_store
        entries = self._os(store.list_dir, path)
        return web.json_response({"path": self._os(clean_path, path), "entries": entries, "free": round(store.listing()["freeGB"] * 2**30), "agents": len(store.live_agents())})

    async def os_manifest(self, request):
        self.authed(request, need=CAP_FILES)
        return web.json_response(self.sched.pool_store.manifest())

    async def os_get(self, request):
        self.authed(request, need=CAP_FILES)
        from .pool_store import clean_path

        path = self._os(clean_path, request.query.get("path", ""))
        store = self.sched.pool_store
        if path not in store.files:
            raise fail(404, f"There is no file at /pool/{path}.")
        try:
            return await store.stream(request, path)
        except UserError as e:
            raise fail(404, str(e))

    async def os_put(self, request):
        rec = self.authed(request, write=True, need=CAP_FILES)
        live = self.sched.devices.get(rec["id"])
        try:
            size = await self.sched.pool_store.put_file(request.query.get("path", ""), request.content, live.name if live else rec["name"], self.cfg.max_upload_mb * 1024 * 1024)
        except UserError as e:
            raise fail(400, str(e))
        from .pool_store import clean_path

        f = self.sched.pool_store.files[clean_path(request.query.get("path", ""))]
        return web.json_response({"ok": True, "size": size, "digest": f["digest"], "mtime": f["mtime"]}, status=201)

    async def os_delete(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        if not self._os(self.sched.pool_store.delete, request.query.get("path", ""), request.query.get("recursive") == "1"):
            raise fail(404, "There is nothing at that path.")
        return web.json_response({"ok": True})

    async def os_mkdir(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        body = await self._fs_body(request)
        self._os(self.sched.pool_store.mkdir, str(body.get("path", "")))
        return web.json_response({"ok": True})

    async def os_rename(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        body = await self._fs_body(request)
        self._os(self.sched.pool_store.rename, str(body.get("path", "")), str(body.get("to", "")))
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ files on each computer
    async def _fs(self, coro):
        from .remote_fs import RemoteFSError

        try:
            return await coro
        except RemoteFSError as e:
            raise fail(400, str(e))

    async def fs_list(self, request):
        self.authed(request, need=CAP_FILES)
        res = await self._fs(self.sched.remote_fs.ask(request.match_info["device_id"], "list", path=request.query.get("path", "")))
        return web.json_response({k: res[k] for k in ("path", "entries", "truncated", "free", "total")})

    async def fs_download(self, request):
        from urllib.parse import quote

        from .remote_fs import RemoteFSError

        self.authed(request, need=CAP_FILES)
        dev, path = request.match_info["device_id"], request.query.get("path", "")
        info = await self._fs(self.sched.remote_fs.ask(dev, "stat", path=path))
        if info.get("dir"):
            raise fail(400, "That's a folder. Pick a file.")
        resp = web.StreamResponse(headers={
            "Content-Type": "application/octet-stream", "Content-Length": str(info["size"]),
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(info['name'])}",
            "X-Content-Type-Options": "nosniff", "Content-Security-Policy": "sandbox", "Cache-Control": "no-store",
        })
        await resp.prepare(request)
        try:
            async for piece in self.sched.remote_fs.read_pieces(dev, path):
                await resp.write(piece)
        except RemoteFSError:
            pass  # the computer left mid-download; the browser sees a short file and reports it
        await resp.write_eof()
        return resp

    async def fs_upload(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        dev, path = request.match_info["device_id"], request.query.get("path", "")
        limit = self.cfg.max_upload_mb * 1024 * 1024

        async def body():
            seen = 0
            async for part in request.content.iter_chunked(256 * 1024):
                seen += len(part)
                if seen > limit:
                    raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=seen, text=f"That file is over the {limit // 2**20} MB limit.")
                yield part

        size = await self._fs(self.sched.remote_fs.write_stream(dev, path, body()))
        self.sched.broadcast({"type": "fs.changed", "device": dev, "path": path.rsplit("/", 1)[0] if "/" in path else ""}, need=CAP_FILES)
        return web.json_response({"ok": True, "size": size}, status=201)

    async def _fs_body(self, request) -> dict:
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise fail(400, "Send JSON.")
        return body

    async def fs_mkdir(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        body = await self._fs_body(request)
        res = await self._fs(self.sched.remote_fs.ask(request.match_info["device_id"], "mkdir", path=str(body.get("path", ""))))
        return web.json_response({"ok": True, "path": res.get("path")})

    async def fs_rename(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        body = await self._fs_body(request)
        await self._fs(self.sched.remote_fs.ask(request.match_info["device_id"], "rename", path=str(body.get("path", "")), to=str(body.get("to", ""))))
        return web.json_response({"ok": True})

    async def fs_delete(self, request):
        self.authed(request, write=True, need=CAP_FILES)
        await self._fs(self.sched.remote_fs.ask(request.match_info["device_id"], "delete", path=request.query.get("path", ""),
                                                 recursive=request.query.get("recursive") == "1"))
        return web.json_response({"ok": True})

    async def fs_copy(self, request):
        """Copy a file from one computer to a folder on another, piece by piece through the coordinator."""
        self.authed(request, write=True, need=CAP_FILES)
        body = await self._fs_body(request)
        src, dst = body.get("from") or {}, body.get("to") or {}
        fs = self.sched.remote_fs
        info = await self._fs(fs.ask(str(src.get("device", "")), "stat", path=str(src.get("path", ""))))
        if info.get("dir"):
            raise fail(400, "Copying whole folders isn't supported yet. Pick a file.")
        target = "/".join(x for x in (str(dst.get("dir", "")).strip("/"), info["name"]) if x)
        size = await self._fs(fs.write_stream(str(dst.get("device", "")), target, fs.read_pieces(str(src.get("device", "")), str(src.get("path", "")))))
        self.sched.broadcast({"type": "fs.changed", "device": str(dst.get("device", "")), "path": str(dst.get("dir", "")).strip("/")}, need=CAP_FILES)
        return web.json_response({"ok": True, "size": size, "path": target})

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
        rec = self.store.device_for_token(token) if token else None
        blocked = self.limiter.retry_after(ip) if rec is None else 0
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
        _, token = coord.store.add_device("agent", sysinfo.computer_name())
        path.write_text(json.dumps({"token": token}), encoding="utf-8")
        os.chmod(path, 0o600)
    home = default_home() / "local-agent"
    home.mkdir(parents=True, exist_ok=True)
    agent_cfg = AgentConfig(server=f"http://127.0.0.1:{port}", token=token, name=sysinfo.computer_name(),
                            threads=max(1, (os.cpu_count() or 2) - 1), workspace=True, home=home, coordinator=True)
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
    lend = f"{base}/#join={coord.store.lend_token}"
    print(f"\nYourHarness coordinator {__version__} is running.\n")
    if coord.online:
        print("ONLINE MODE: this pool accepts join links only; the 4-digit code is turned off.\n")
    else:
        print("Phones, tablets and browsers: open this address and enter the join code")
        print(f"  {base}\n")
        print(f"  JOIN CODE: {coord.store.join_code}\n")
        print("Windows PC (sets up Ubuntu for you): in PowerShell run")
        print(f"  irm {base}/install.ps1 | iex\n")
        print("Mac or Linux computers: run this in a terminal")
        print(f"  curl -fsSL {base}/install.sh | bash -s -- --join {coord.store.join_code} --workspace")
        print("  Leave out --workspace on computers that shouldn't host a terminal.\n")
        print(f"Or skip typing the code by opening: {link}\n")
    print("Your own devices -- full control of the pool, including shells and files:")
    print(f"  {link}\n")
    print("Lend link -- give this to someone else. Their computer joins and computes, and")
    print("cannot start runs, open a terminal, or read files on any computer here:")
    print(f"  {lend}\n")
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
    local = asyncio.ensure_future(run_local_agent(cfg, coord, port)) if local_agent else None
    try:
        await asyncio.Event().wait()
    finally:
        if beacon:
            beacon.close()
        if local:
            local.cancel()
        await runner.cleanup()
