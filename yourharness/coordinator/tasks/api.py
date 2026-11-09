"""HTTP routes for agent tasks, GitHub and friend invites.

Owner routes use the same sign-in as the rest of the coordinator. The two routes a
computer uses while working on a task (downloading its starting point, uploading
its result) take that attempt's own token instead, which is good for nothing else.
"""

from __future__ import annotations

import os
import re
import secrets
from typing import TYPE_CHECKING

from aiohttp import web

from ..auth import token_from
from ..state import CAP_ADMIN, CAP_SUBMIT, caps_of
from .github import GitHubError
from .gitops import GitError
from .rules import RunError, clean_line

if TYPE_CHECKING:
    from ..app import Coordinator

MAX_RESULT = 500 * 1024 * 1024


class TaskAPI:
    def __init__(self, coord: "Coordinator"):
        self.c = coord
        self.tm = coord.sched.tasks

    def register(self, r: web.UrlDispatcher) -> None:
        r.add_get("/api/agents", self.overview)
        r.add_post("/api/agent-runs", self.create_run)
        r.add_get("/api/agent-runs/{run_id}", self.get_run)
        r.add_post("/api/agent-runs/{run_id}/cancel", self.cancel_run)
        r.add_post("/api/agent-runs/{run_id}/push", self.push_run)
        r.add_post("/api/agent-tasks/{task_id}/cancel", self.cancel_task)
        r.add_post("/api/agent-tasks/{task_id}/retry", self.retry_task)
        r.add_post("/api/agent-tasks/{task_id}/accept", self.accept_task)
        r.add_get("/api/agent-tasks/{task_id}/log", self.task_log)
        r.add_get("/api/agent-tasks/{task_id}/diff", self.task_diff)
        r.add_get("/api/agent-tasks/{task_id}/start.bundle", self.start_bundle)
        r.add_put("/api/agent-tasks/{task_id}/result.bundle", self.upload_result)
        r.add_post("/api/github/token", self.set_token)
        r.add_delete("/api/github/token", self.delete_token)
        r.add_get("/api/github/repos", self.github_repos)
        r.add_get("/api/github/branches", self.github_branches)
        r.add_get("/api/invites", self.list_invites)
        r.add_post("/api/invites", self.create_invite)
        r.add_delete("/api/invites/{invite_id}", self.revoke_invite)

    # ------------------------------------------------------------------ helpers
    def _who(self, request, write: bool = False, need: str = CAP_SUBMIT) -> dict:
        return self.c.authed(request, write=write, need=need)

    def _name(self, rec: dict) -> str:
        live = self.c.sched.devices.get(rec["id"])
        return live.name if live else rec["name"]

    @staticmethod
    async def _json(request) -> dict:
        from ..app import fail

        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise fail(400, "Send JSON.")
        return body

    @staticmethod
    def _fail(status: int, message: str):
        from ..app import fail

        return fail(status, message)

    def _task(self, request) -> dict:
        t = self.tm.db.task(request.match_info["task_id"])
        if t is None:
            raise self._fail(404, "There's no task with that id.")
        return t

    # ------------------------------------------------------------------ overview and runs
    async def overview(self, request):
        rec = self._who(request)
        data = self.tm.overview()
        data["isAdmin"] = CAP_ADMIN in caps_of(rec)
        return web.json_response(data)

    async def create_run(self, request):
        rec = self._who(request, write=True)
        body = await self._json(request)
        try:
            run = self.tm.create_run(body, self._name(rec))
        except RunError as e:
            raise self._fail(400, str(e))
        return web.json_response({"run": run}, status=201)

    async def get_run(self, request):
        self._who(request)
        run = self.tm.db.run(request.match_info["run_id"])
        if run is None:
            raise self._fail(404, "There's no run with that id.")
        return web.json_response({"run": self.tm.run_view(run, with_spec=True), "events": self.tm.db.events(run["id"], 200)})

    async def cancel_run(self, request):
        rec = self._who(request, write=True)
        try:
            self.tm.cancel_run(request.match_info["run_id"], self._name(rec))
        except RunError as e:
            raise self._fail(400, str(e))
        return web.json_response({"ok": True})

    async def push_run(self, request):
        self._who(request, write=True)
        try:
            self.tm.push_run(request.match_info["run_id"])
        except RunError as e:
            raise self._fail(400, str(e))
        return web.json_response({"ok": True})

    async def cancel_task(self, request):
        rec = self._who(request, write=True)
        t = self.tm.cancel_task(self._task(request)["id"], self._name(rec))
        return web.json_response({"task": self.tm.task_view(t)})

    async def retry_task(self, request):
        self._who(request, write=True)
        try:
            t = await self.tm.retry_task(self._task(request)["id"])
        except (RunError, GitError) as e:
            raise self._fail(400, str(e))
        return web.json_response({"task": self.tm.task_view(t)})

    async def accept_task(self, request):
        rec = self._who(request, write=True)
        try:
            t = self.tm.accept_task(self._task(request)["id"], self._name(rec))
        except RunError as e:
            raise self._fail(400, str(e))
        return web.json_response({"task": self.tm.task_view(t)})

    async def task_log(self, request):
        self._who(request)
        t = self._task(request)
        return web.Response(text=self.tm.log_tail(t["id"]), content_type="text/plain", charset="utf-8",
                            headers={"Cache-Control": "no-store"})

    async def task_diff(self, request):
        self._who(request)
        t = self._task(request)
        try:
            text = await self.tm.diff(t["id"])
        except GitError as e:
            raise self._fail(400, str(e))
        return web.Response(text=text, content_type="text/plain", charset="utf-8", headers={"Cache-Control": "no-store"})

    # ------------------------------------------------------------------ a computer working on a task
    def _attempt(self, request) -> dict:
        ip = self.c._check_blocked(request)
        token, how = token_from(request)
        t = self.tm.task_for_token(token if how == "bearer" else None, request.match_info["task_id"])
        if t is None:
            self.c.limiter.fail(ip)
            raise self._fail(401, "This task's sign-in isn't valid anymore. The task may have been cancelled or given to another computer.")
        return t

    async def start_bundle(self, request):
        t = self._attempt(request)
        try:
            path = await self.tm.start_bundle(t)
        except GitError as e:
            raise self._fail(400, str(e))
        return web.FileResponse(path, headers={"Content-Type": "application/octet-stream", "Cache-Control": "no-store"})

    async def upload_result(self, request):
        t = self._attempt(request)
        dest = self.tm.result_path(t["id"], t["attempt"])
        tmp = dest.with_name(f".{dest.name}.{secrets.token_hex(3)}")
        size = 0
        try:
            with open(tmp, "wb") as f:
                async for part in request.content.iter_chunked(1024 * 1024):
                    size += len(part)
                    if size > MAX_RESULT:
                        raise self._fail(400, f"The result is over {MAX_RESULT // 2**20} MB.")
                    f.write(part)
            os.replace(tmp, dest)
        finally:
            if tmp.exists():
                tmp.unlink()
        return web.json_response({"ok": True, "size": size}, status=201)

    # ------------------------------------------------------------------ GitHub
    async def set_token(self, request):
        self._who(request, write=True, need=CAP_ADMIN)
        body = await self._json(request)
        token = str(body.get("token") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_\-.]{20,255}", token):
            raise self._fail(400, "That doesn't look like a GitHub token. Paste a fine-grained personal access token.")
        try:
            user = await self.tm.github.user(token)
        except GitHubError as e:
            raise self._fail(400, f"GitHub didn't accept that token: {e}")
        self.tm.secrets.set("github_token", token)
        self.tm.secrets.set("github_user", user)
        return web.json_response({"connected": True, "user": user})

    async def delete_token(self, request):
        self._who(request, write=True, need=CAP_ADMIN)
        self.tm.secrets.delete("github_token", "github_user")
        return web.json_response({"connected": False})

    async def github_repos(self, request):
        self._who(request)
        try:
            return web.json_response({"repos": await self.tm.github.repos()})
        except GitHubError as e:
            raise self._fail(400, str(e))

    async def github_branches(self, request):
        self._who(request)
        name = clean_line(request.query.get("repo"), 200)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name):
            raise self._fail(400, "Name the repository as owner/name.")
        try:
            return web.json_response({"branches": await self.tm.github.branches(name)})
        except GitHubError as e:
            raise self._fail(400, str(e))

    # ------------------------------------------------------------------ friend invites
    def _invite_link(self, code: str) -> str:
        return f"{self.c.sched.public_url or ''}/#join={code}"

    async def list_invites(self, request):
        self._who(request, need=CAP_ADMIN)
        return web.json_response({"invites": self.c.store.invites(), "address": self.c.sched.public_url})

    async def create_invite(self, request):
        self._who(request, write=True, need=CAP_ADMIN)
        body = await self._json(request)
        label = clean_line(body.get("label"), 60, "A friend")
        try:
            hours = min(24 * 30, max(1, float(body.get("hours") or 24)))
        except (TypeError, ValueError):
            hours = 24
        invite, code = self.c.store.create_invite(label, hours)
        return web.json_response({"invite": invite, "code": code, "link": self._invite_link(code), "address": self.c.sched.public_url}, status=201)

    async def revoke_invite(self, request):
        self._who(request, write=True, need=CAP_ADMIN)
        if not self.c.store.revoke_invite(request.match_info["invite_id"]):
            raise self._fail(404, "There's no invite with that id.")
        return web.json_response({"ok": True})
