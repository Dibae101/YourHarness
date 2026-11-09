"""HTTP routes of the job engine.

Your devices use /api/engine/* with their normal sign-in. A computer working on a
partition uses only /api/engine/attempts/<partition>/<attempt>/... with that
attempt's own token: it can read that partition's input and store that attempt's
outputs, and nothing else, and the token stops working when the attempt ends.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import quote

from aiohttp import web

from ..coordinator import files as shared_files
from ..coordinator.auth import token_from
from ..coordinator.state import CAP_SUBMIT
from .manager import EngineError
from .store import StoreError

if TYPE_CHECKING:
    from ..coordinator.app import Coordinator

ID = re.compile(r"[a-z0-9-]{1,80}")


def _fail(status: int, message: str):
    from ..coordinator.app import fail

    return fail(status, message)


async def _body_chunks(request):
    async for part in request.content.iter_chunked(256 * 1024):
        yield part


class EngineAPI:
    def __init__(self, coord: "Coordinator"):
        self.c = coord
        self.m = coord.sched.engine

    def register(self, r: web.UrlDispatcher) -> None:
        r.add_get("/api/engine", self.overview)
        r.add_put("/api/engine/datasets", self.upload_dataset)
        r.add_post("/api/engine/datasets/import", self.import_dataset)
        r.add_get("/api/engine/datasets/{ds}/preview", self.preview)
        r.add_post("/api/engine/datasets/{ds}", self.update_dataset)
        r.add_delete("/api/engine/datasets/{ds}", self.delete_dataset)
        r.add_post("/api/engine/plan", self.plan)
        r.add_post("/api/engine/jobs", self.create_job)
        r.add_get("/api/engine/jobs/{job}", self.get_job)
        r.add_post("/api/engine/jobs/{job}/cancel", self.cancel_job)
        r.add_post("/api/engine/jobs/{job}/retry", self.retry_job)
        r.add_delete("/api/engine/jobs/{job}", self.delete_job)
        r.add_get("/api/engine/jobs/{job}/result", self.result)
        r.add_get("/api/engine/attempts/{pid}/{n}/input", self.attempt_input)
        r.add_put("/api/engine/attempts/{pid}/{n}/outputs/{name}", self.attempt_output)

    # ------------------------------------------------------------------ helpers
    def _who(self, request, write: bool = False) -> dict:
        return self.c.authed(request, write=write, need=CAP_SUBMIT)

    def _name(self, rec: dict) -> str:
        live = self.c.sched.devices.get(rec["id"])
        return live.name if live else rec["name"]

    @staticmethod
    async def _json(request) -> dict:
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            body = None
        if not isinstance(body, dict):
            raise _fail(400, "Send JSON.")
        return body

    @staticmethod
    def _id(request, key: str) -> str:
        v = request.match_info[key]
        if not ID.fullmatch(v):
            raise _fail(404, "Not found.")
        return v

    def _run(self, fn, *args):
        try:
            return fn(*args)
        except EngineError as e:
            raise _fail(400, str(e))

    # ------------------------------------------------------------------ overview, datasets
    async def overview(self, request):
        self._who(request)
        data = self.m.overview()
        data["devices"] = [{"id": d.id, "name": d.name, "kind": d.kind, "owned": d.owned, "engine": self.m.device_view(d)}
                           for d in self.c.sched.devices.values()]
        return web.json_response(data)

    async def upload_dataset(self, request):
        rec = self._who(request, write=True)
        q = request.query
        try:
            ds = await self.m.add_dataset(q.get("name", ""), q.get("format") or None, q.get("classification") or None,
                                          _body_chunks(request), request.content_length, self._name(rec), "upload")
        except (EngineError, StoreError) as e:
            raise _fail(400, str(e))
        return web.json_response({"dataset": ds}, status=201)

    async def import_dataset(self, request):
        rec = self._who(request, write=True)
        body = await self._json(request)
        source = body.get("source")
        path = str(body.get("path") or "")
        name = str(body.get("name") or path.rsplit("/", 1)[-1])
        if source == "shared":
            safe = shared_files.safe_name(path)
            f = self.c.cfg.shared_dir / safe if safe else None
            if f is None or not f.is_file():
                raise _fail(400, f"There's no file called {path} in the shared folder.")
            chunks, size = _file_chunks(f), f.stat().st_size
        elif source == "pool":
            from ..coordinator.pool_store import clean_path

            store = self.c.sched.pool_store
            try:
                clean = clean_path(path)
            except Exception:
                raise _fail(400, "That path isn't allowed.")
            entry = store.files.get(clean)
            if entry is None:
                raise _fail(400, f"There's no file at /pool/{clean}.")
            chunks, size = _pool_chunks(store, entry), entry["size"]
        else:
            raise _fail(400, 'Say where the file is: "source" is "shared" (the shared folder) or "pool" (/pool).')
        try:
            ds = await self.m.add_dataset(name, body.get("format") or None, body.get("classification") or None, chunks, size,
                                          self._name(rec), f"{source}:{path}")
        except (EngineError, StoreError) as e:
            raise _fail(400, str(e))
        except (ConnectionError, RuntimeError) as e:
            raise _fail(400, f"Couldn't read the file: {e}")
        return web.json_response({"dataset": ds}, status=201)

    async def preview(self, request):
        self._who(request)
        try:
            return web.json_response({"records": await self.m.preview(self._id(request, "ds"))})
        except EngineError as e:
            raise _fail(400, str(e))

    async def update_dataset(self, request):
        self._who(request, write=True)
        body = await self._json(request)
        return web.json_response({"dataset": self._run(self.m.set_classification, self._id(request, "ds"), body.get("classification"))})

    async def delete_dataset(self, request):
        self._who(request, write=True)
        self._run(self.m.delete_dataset, self._id(request, "ds"))
        return web.json_response({"ok": True})

    # ------------------------------------------------------------------ jobs
    async def plan(self, request):
        self._who(request)
        return web.json_response(self._run(self.m.plan, await self._json(request)))

    async def create_job(self, request):
        rec = self._who(request, write=True)
        body = await self._json(request)
        return web.json_response({"job": self._run(self.m.create_job, body, self._name(rec))}, status=201)

    async def get_job(self, request):
        self._who(request)
        job = self.m.db.job(self._id(request, "job"))
        if job is None:
            raise _fail(404, "There's no job with that id.")
        return web.json_response({"job": self.m.job_view(job, full=True)})

    async def cancel_job(self, request):
        rec = self._who(request, write=True)
        return web.json_response({"job": self._run(self.m.cancel_job, self._id(request, "job"), self._name(rec))})

    async def retry_job(self, request):
        self._who(request, write=True)
        return web.json_response({"job": self._run(self.m.retry_job, self._id(request, "job"))})

    async def delete_job(self, request):
        self._who(request, write=True)
        self._run(self.m.delete_job, self._id(request, "job"))
        return web.json_response({"ok": True})

    async def result(self, request):
        self._who(request)
        job_id = self._id(request, "job")
        job = self.m.db.job(job_id)
        if job is None or job["status"] != "done":
            raise _fail(400, "The job hasn't finished.")
        records = bool(job["outputs"])
        filename = f"{job['name']}-{job_id}." + ("jsonl" if records else "json")
        resp = web.StreamResponse(headers={
            "Content-Type": "application/x-ndjson" if records else "application/json",
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}", "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff"})
        await resp.prepare(request)
        async for chunk in self.m.result_stream(job_id):
            await resp.write(chunk)
        await resp.write_eof()
        return resp

    # ------------------------------------------------------------------ a computer working on a partition
    def _attempt(self, request):
        ip = self.c._check_blocked(request)
        token, how = token_from(request)
        try:
            n = int(request.match_info["n"])
        except ValueError:
            n = -1
        got = self.m.attempt_for_token(token if how == "bearer" else None, request.match_info["pid"], n)
        if got is None:
            self.c.limiter.fail(ip)
            raise _fail(401, "This partition's sign-in isn't valid anymore. The attempt may have been cancelled or finished elsewhere.")
        return got

    async def attempt_input(self, request):
        p, _a = self._attempt(request)
        if p["input"]["kind"] != "range":
            raise _fail(400, "This partition's input came with the task.")
        try:
            from_offset = int(request.query["from"]) if "from" in request.query else None
        except ValueError:
            raise _fail(400, "from should be a byte offset.")
        sha, start, end = self.m.input_range(p, request.query.get("halo") == "1", from_offset)
        resp = web.StreamResponse(headers={"Content-Type": "application/octet-stream", "Content-Length": str(max(0, end - start)),
                                           "Cache-Control": "no-store"})
        await resp.prepare(request)
        async for chunk in self.m.store.stream_range(sha, start, end):
            await resp.write(chunk)
        await resp.write_eof()
        return resp

    async def attempt_output(self, request):
        p, a = self._attempt(request)
        try:
            saved = await self.m.save_output(p, a, request.match_info["name"], _body_chunks(request))
        except (EngineError, StoreError) as e:
            raise _fail(400, str(e))
        return web.json_response(saved, status=201)


async def _file_chunks(path):
    import asyncio

    loop = asyncio.get_running_loop()
    with open(path, "rb") as f:
        while True:
            data = await loop.run_in_executor(None, f.read, 1024 * 1024)
            if not data:
                return
            yield data


async def _pool_chunks(store, entry: dict):
    for h in entry["chunks"]:
        yield await store._get_chunk(h)
