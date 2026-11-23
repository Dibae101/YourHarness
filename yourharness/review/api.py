"""The job owner and each invited browser share only a leased review frame at a time."""

import asyncio
import hashlib
import json
import math
import os
import secrets
import shutil
import sqlite3
import subprocess
import time
import zipfile
from pathlib import Path

from aiohttp import web
from PIL import UnidentifiedImageError

from ..coordinator.state import CAP_SUBMIT
from . import media
from .plan import estimate

LEASE = 90
PREFIX = "/api/reviews"
TERMINAL = {"ready", "cancelled", "failed"}


def bad(message):
    return web.HTTPBadRequest(text=message)


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


def number(value, low, high, label):
    try:
        n = float(value)
    except (TypeError, ValueError):
        raise bad(f"Invalid {label}.") from None
    if not math.isfinite(n) or not low <= n <= high:
        raise bad(f"Invalid {label}.")
    return n


class Reviews:
    def __init__(self, coord, app):
        self.coord = coord
        self.root = coord.cfg.data_dir / "reviews"
        self.root.mkdir(exist_ok=True)
        self.db = sqlite3.connect(self.root / "reviews.sqlite")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, name TEXT, kind TEXT, status TEXT, created REAL, deadline REAL,
                total INTEGER, fps REAL, w INTEGER, h INTEGER, bytes INTEGER, error TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS frames (
                job TEXT, n INTEGER, size INTEGER, probe INTEGER, done INTEGER DEFAULT 0, actor TEXT,
                ticket TEXT, until REAL DEFAULT 0, seconds REAL, label TEXT, PRIMARY KEY(job,n));
            CREATE TABLE IF NOT EXISTS grants (
                invite TEXT PRIMARY KEY, job TEXT, session TEXT UNIQUE, expires REAL, workers INTEGER,
                quota INTEGER, label TEXT, stopped INTEGER DEFAULT 0);
            CREATE INDEX IF NOT EXISTS frame_next ON frames(job,done,until,n);
        """)
        columns = {r["name"] for r in self.db.execute("PRAGMA table_info(jobs)")}
        with self.db:
            for column in ("encoding_at", "finished_at"):
                if column not in columns:
                    self.db.execute(f"ALTER TABLE jobs ADD COLUMN {column} REAL")
        frame_columns = {r["name"] for r in self.db.execute("PRAGMA table_info(frames)")}
        with self.db:
            if "issued" not in frame_columns:
                self.db.execute("ALTER TABLE frames ADD COLUMN issued REAL")
            if "compute_seconds" not in frame_columns:
                self.db.execute("ALTER TABLE frames ADD COLUMN compute_seconds REAL")
            if "result_bytes" not in frame_columns:
                self.db.execute("ALTER TABLE frames ADD COLUMN result_bytes INTEGER")
        self.tasks = {}
        self.encoder_lock = asyncio.Lock()
        r = app.router
        r.add_get(PREFIX, self.listing)
        r.add_post(PREFIX, self.create)
        r.add_get(PREFIX + "/exporter", self.exporter)
        r.add_get(PREFIX + "/invitation", self.invitation)
        r.add_post(PREFIX + "/accept", self.accept)
        base = PREFIX + "/{job:[0-9a-f]{20}}"
        for path, method, handler in [
            ("", "GET", self.detail),
            ("", "DELETE", self.delete),
            ("/loan", "GET", self.loan_status),
            ("/plan", "GET", self.plan),
            ("/action", "POST", self.action),
            ("/invites", "POST", self.invite),
            ("/claim", "POST", self.claim),
            ("/release", "POST", self.release),
            ("/lease/{ticket}", "GET", self.input),
            ("/lease/{ticket}", "PUT", self.result),
            ("/clip", "GET", self.clip),
            ("/poster", "GET", self.poster),
        ]:
            r.add_route(method, base + path, handler)
        app.on_startup.append(self.startup)
        app.on_cleanup.append(self.cleanup)

    def one(self, query, *args):
        return self.db.execute(query, args).fetchone()

    async def startup(self, app):
        for folder in self.root.glob("*.removing"):
            job_id = folder.name.split(".")[0]
            if self.one("SELECT 1 FROM jobs WHERE id=?", job_id):
                folder.rename(self.root / job_id)
            else:
                shutil.rmtree(folder)
        with self.db:
            self.db.execute("UPDATE frames SET ticket=NULL, until=0 WHERE done=0")
        for row in self.db.execute("SELECT id FROM jobs WHERE status='encoding'"):
            self.schedule_encode(row["id"])

    async def cleanup(self, app):
        if self.tasks:
            await asyncio.gather(*list(self.tasks.values()), return_exceptions=True)
        self.db.close()

    async def fields(self, req):
        try:
            data = await req.json()
        except (ValueError, UnicodeError):
            raise bad("Send a JSON object.") from None
        if not isinstance(data, dict):
            raise bad("Send a JSON object.")
        return data

    def owner(self, req):
        return self.coord.authed(req, write=req.method != "GET", need=CAP_SUBMIT)

    def job(self, req):
        row = self.one("SELECT * FROM jobs WHERE id=?", req.match_info["job"])
        if row is None:
            raise web.HTTPNotFound(text="Review not found.")
        return row

    def actor(self, req, job):
        token = req.headers.get("X-Review-Session")
        if token:
            row = self.one(
                "SELECT * FROM grants WHERE job=? AND session=? AND stopped=0 AND expires>?",
                job["id"],
                digest(token),
                time.time(),
            )
            if not row:
                raise web.HTTPForbidden(text="This loan has ended.")
            return row["session"], row["label"], row["workers"], row
        rec = self.owner(req)
        # Each owner's tab may release only its own outstanding leases.
        tab = req.headers.get("X-Review-Tab", "default")[:80]
        return "owner/" + rec["id"] + "/" + tab, rec["name"], 4, None

    def view(self, j):
        data = dict(j)
        data["done"] = self.one("SELECT count(*) FROM frames WHERE job=? AND done=1", j["id"])[0]
        data["contributors"] = [
            dict(row)
            for row in self.db.execute(
                "SELECT label, count(*) AS frames FROM frames WHERE job=? AND done=1 GROUP BY label ORDER BY frames DESC",
                (j["id"],),
            )
        ]
        data["preview"] = bool(data["done"])
        return data

    async def listing(self, req):
        self.owner(req)
        return web.json_response(
            [self.view(j) for j in self.db.execute("SELECT * FROM jobs ORDER BY created DESC LIMIT 100")]
        )

    async def detail(self, req):
        self.owner(req)
        j = self.job(req)
        view = self.view(j)
        manifest = self.root / j["id"] / "sources.json"
        view["sources"] = json.loads(manifest.read_text(encoding="utf-8")) if manifest.exists() else []
        view["loans"] = [
            dict(row)
            for row in self.db.execute(
                "SELECT rowid AS id,expires,workers,quota,label,stopped,session IS NOT NULL AS accepted,"
                "(SELECT count(*) FROM frames f WHERE f.job=grants.job AND f.actor=grants.session AND done=1) AS done "
                "FROM grants WHERE job=? ORDER BY rowid DESC LIMIT 30",
                (j["id"],),
            )
        ]
        return web.json_response(view)

    async def delete(self, req):
        self.owner(req)
        j = self.job(req)
        if j["status"] not in TERMINAL:
            raise web.HTTPConflict(text="Cancel the active review before removing its files.")
        folder = self.root / j["id"]
        removed = folder.with_name(j["id"] + ".removing")
        folder.rename(removed)
        try:
            with self.db:
                for table in ("grants", "frames"):
                    self.db.execute(f"DELETE FROM {table} WHERE job=?", (j["id"],))
                self.db.execute("DELETE FROM jobs WHERE id=?", (j["id"],))
        except BaseException:
            removed.rename(folder)
            raise
        await asyncio.to_thread(shutil.rmtree, removed)
        return web.json_response({"ok": True})

    async def loan_status(self, req):
        j = self.job(req)
        self.actor(req, j)
        return web.json_response({"status": j["status"]})

    async def exporter(self, req):
        return web.FileResponse(
            Path(__file__).with_name("blender_script.py"),
            headers={"Content-Disposition": 'attachment; filename="yourharness_motion_review.py"'},
        )

    async def create(self, req):
        self.owner(req)
        now = time.time()
        deadline = number(req.query.get("deadline"), now + 1, now + 31 * 86400, "deadline")
        width = int(number(req.query.get("width", 640), 320, 960, "preview size"))
        name = req.query.get("name", "").strip()
        if not name or len(name) > 100:
            raise bad("Give the review a name of up to 100 characters.")
        kind = req.query.get("kind")
        job_id = secrets.token_hex(10)
        folder = self.root / job_id
        inputs = folder / "in"
        inputs.mkdir(parents=True)
        (folder / "out").mkdir()
        incoming = folder / "upload"
        incoming.mkdir()
        names = []
        try:
            if kind == "demo":
                names.append("Built-in turntable geometry")
                count, fps, (w, h) = await asyncio.to_thread(media.demo, inputs, width)
                kind = "mesh"
            elif kind in ("mesh", "images"):
                if not req.content_type.startswith("multipart/"):
                    raise bad("Select the files to upload.")
                reader = await req.multipart()
                paths, size = [], 0
                async for part in reader:
                    if len(paths) >= media.MAX_FRAMES or part.name != "file" or not part.filename:
                        raise bad("Unexpected upload field or too many files.")
                    path = incoming / str(len(paths))
                    with path.open("wb") as f:
                        while chunk := await part.read_chunk(256 * 1024):
                            size += len(chunk)
                            if size > media.MAX_BYTES:
                                raise bad("The upload limit is 128 MiB.")
                            f.write(chunk)
                    paths.append(path)
                    names.append(Path(part.filename.replace(chr(92), "/")).name[:160])
                if not paths:
                    raise bad("Select at least one file.")
                if kind == "mesh":
                    if len(paths) != 1:
                        raise bad("Select one .yhreview package.")
                    count, fps, (w, h) = await asyncio.to_thread(media.mesh_package, paths[0], inputs, width)
                else:
                    fps = number(req.query.get("fps", 12), 1, 60, "frame rate")
                    count = len(paths)
                    w, h = await asyncio.to_thread(media.import_images, paths, inputs, width)
            else:
                raise bad("Unknown source.")
            (folder / "sources.json").write_text(json.dumps(names), encoding="utf-8")
            sizes = [(inputs / str(i)).stat().st_size for i in range(count)]
            probes = {0, count // 2, count - 1}
            with self.db:
                self.db.execute(
                    "INSERT INTO jobs(id,name,kind,status,created,deadline,total,fps,w,h,bytes,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (job_id, name, kind, "measuring", now, deadline, count, fps, w, h, sum(sizes), ""),
                )
                self.db.executemany(
                    "INSERT INTO frames(job,n,size,probe) VALUES(?,?,?,?)",
                    [(job_id, i, size, int(i in probes)) for i, size in enumerate(sizes)],
                )
        except BaseException as exc:
            shutil.rmtree(folder, ignore_errors=True)
            if isinstance(
                exc,
                (
                    ValueError,
                    TypeError,
                    KeyError,
                    OSError,
                    zipfile.BadZipFile,
                    UnidentifiedImageError,
                    RuntimeError,
                ),
            ):
                raise bad("Cannot import this project. " + str(exc)[:140]) from None
            raise
        shutil.rmtree(incoming)
        return web.json_response({"id": job_id}, status=201)

    async def plan(self, req):
        self.owner(req)
        job = self.job(req)
        rows = self.db.execute(
            "SELECT seconds FROM frames WHERE job=? AND done=1 AND actor LIKE 'owner/%' ORDER BY n", (job["id"],)
        ).fetchall()
        pending = self.one("SELECT count(*), coalesce(sum(size),0) FROM frames WHERE job=? AND done=0", job["id"])
        helper, mbps = req.query.get("helper"), req.query.get("mbps")
        if helper is not None or mbps is not None:
            helper = number(helper, 0.001, 86400, "helper speed")
            mbps = number(mbps, 0.001, 100000, "transfer speed")
        avg_output = self.one("SELECT avg(result_bytes) FROM frames WHERE job=? AND done=1", job["id"])[0] or 0
        wire_bytes = pending[1] + pending[0] * avg_output
        # Assembly includes queueing for the encoder. Expose only measured history.
        history = self.db.execute(
            "SELECT (finished_at-encoding_at)/total FROM jobs WHERE status='ready' "
            "AND encoding_at IS NOT NULL AND finished_at>=encoding_at ORDER BY finished_at DESC LIMIT 10"
        )
        rates = sorted(r[0] for r in history)
        assembly = round(rates[len(rates) // 2] * job["total"], 2) if rates else None
        budget = job["deadline"] - time.time() - (assembly or 0)
        result = estimate(pending[0], [r[0] for r in rows], wire_bytes, budget, helper, mbps)
        # Only use observations for the same source type and resolution.
        profiles = self.db.execute(
            "SELECT f.label,j.name AS project,avg(f.compute_seconds) AS compute,"
            "8.0*sum(f.size+f.result_bytes)/1e6 / max(0.001,sum(max(0.001,f.seconds-f.compute_seconds))) AS mbps,"
            "count(*) AS frames FROM frames f JOIN jobs j ON j.id=f.job WHERE f.done=1 AND f.compute_seconds IS NOT NULL "
            "AND f.actor NOT LIKE 'owner/%' AND j.kind=? AND j.w=? AND j.h=? GROUP BY f.actor,j.id "
            "ORDER BY max(f.issued) DESC LIMIT 10",
            (job["kind"], job["w"], job["h"]),
        )
        result["profiles"] = [dict(row) for row in profiles]
        result["assemblySeconds"] = assembly
        return web.json_response(result)

    async def action(self, req):
        self.owner(req)
        data = await self.fields(req)
        op = data.get("action")
        j = self.job(req)
        status = j["status"]
        if op == "start" and status in ("measuring", "planned", "paused"):
            status = "rendering"
        elif op == "pause" and status in ("measuring", "rendering"):
            status = "paused"
        elif op == "cancel" and status in ("measuring", "planned", "paused", "rendering", "failed"):
            status = "cancelled"
        elif op == "retry" and status == "failed":
            status = "encoding"
        elif op == "revoke":
            grant_id = data.get("grant")
            if grant_id is not None and type(grant_id) is not int:
                raise bad("Invalid invitation.")
            with self.db:
                self.db.execute(
                    "UPDATE frames SET until=0,ticket=NULL WHERE job=? AND done=0 AND actor IN "
                    "(SELECT session FROM grants WHERE job=? AND (? IS NULL OR rowid=?))",
                    (j["id"], j["id"], grant_id, grant_id),
                )
                self.db.execute(
                    "UPDATE grants SET stopped=1 WHERE job=? AND (? IS NULL OR rowid=?)",
                    (j["id"], grant_id, grant_id),
                )
            return web.json_response({"ok": True})
        else:
            raise web.HTTPConflict(text="This action is unavailable in the current phase.")
        with self.db:
            self.db.execute("UPDATE jobs SET status=?,error='' WHERE id=?", (status, j["id"]))
            if op in ("pause", "cancel"):
                self.db.execute("UPDATE frames SET ticket=NULL,until=0 WHERE job=? AND done=0", (j["id"],))
        if status == "encoding":
            self.schedule_encode(j["id"])
        elif status == "rendering":
            self.advance(j["id"])
        return web.json_response({"ok": True})

    async def invite(self, req):
        self.owner(req)
        data = await self.fields(req)
        j = self.job(req)
        seconds = number(data.get("minutes", 30), 1, 1440, "loan duration") * 60
        workers = int(number(data.get("workers", 1), 1, 4, "worker limit"))
        quota = int(number(data.get("frames", j["total"]), 1, j["total"], "frame allowance"))
        if j["status"] in TERMINAL | {"encoding"}:
            raise web.HTTPConflict(text="This review is closed.")
        token = secrets.token_urlsafe(32)
        with self.db:
            self.db.execute(
                "INSERT INTO grants VALUES(?,?,NULL,?,?,?,'',0)",
                (digest(token), j["id"], time.time() + seconds, workers, quota),
            )
        base = getattr(self.coord.sched, "public_url", None) or f"{req.scheme}://{req.host}"
        return web.json_response(
            {"token": token, "url": base.rstrip("/") + "/reviews.html#invite=" + token}, status=201
        )

    def grant(self, req):
        token = digest(req.headers.get("X-Review-Invite", ""))
        g = self.one(
            "SELECT * FROM grants WHERE invite=? AND expires>? AND stopped=0 AND session IS NULL",
            token,
            time.time(),
        )
        if not g:
            raise web.HTTPGone(text="This invitation was used, revoked or expired.")
        j = self.one("SELECT * FROM jobs WHERE id=?", g["job"])
        if j["status"] in TERMINAL | {"encoding"}:
            raise web.HTTPGone(text="This review has ended.")
        return g, j

    async def invitation(self, req):
        g, j = self.grant(req)
        return web.json_response(
            {k: j[k] for k in ("name", "kind", "id", "bytes", "deadline", "total", "w", "h")}
            | {"workers": g["workers"], "expires": g["expires"], "quota": g["quota"]}
        )

    async def accept(self, req):
        data = await self.fields(req)
        g, j = self.grant(req)
        name = str(data.get("name", "")).strip()[:60]
        if not name:
            raise bad("Enter a name for your contribution.")
        workers = int(number(data.get("workers", 1), 1, g["workers"], "workers"))
        expires = min(g["expires"], time.time() + number(data.get("minutes", 15), 1, 1440, "minutes") * 60)
        token = secrets.token_urlsafe(32)
        with self.db:
            self.db.execute(
                "UPDATE grants SET session=?, label=?, workers=?, expires=? WHERE invite=?",
                (digest(token), name, workers, expires, g["invite"]),
            )
        return web.json_response({"id": j["id"], "session": token, "expires": expires, "workers": workers})

    async def claim(self, req):
        j = self.job(req)
        key, label, limit, g = self.actor(req, j)
        now = time.time()
        status = j["status"]
        waiting = {"status": status, "done": self.view(j)["done"], "total": j["total"]}
        if status != "rendering" and (status != "measuring" or g):
            return web.json_response(waiting)
        held = self.one(
            "SELECT count(*) FROM frames WHERE job=? AND actor=? AND done=0 AND until>?", j["id"], key, now
        )[0]
        if g:
            done = self.one("SELECT count(*) FROM frames WHERE job=? AND actor=? AND done=1", j["id"], key)[0]
            if done >= g["quota"]:
                return web.json_response(dict(waiting, status="fulfilled"))
            limit = min(limit, g["quota"] - done)
        if held >= limit:
            return web.json_response(waiting)
        f = self.one(
            "SELECT n FROM frames WHERE job=? AND done=0 AND until<=? AND (probe=1 OR ?='rendering') ORDER BY n LIMIT 1",
            j["id"],
            now,
            status,
        )
        if not f:
            return web.json_response(waiting)
        t = secrets.token_hex(16)
        with self.db:
            self.db.execute(
                "UPDATE frames SET actor=?,label=?,ticket=?,issued=?,until=? WHERE job=? AND n=?",
                (key, label, t, now, min(g["expires"], now + LEASE) if g else now + LEASE, j["id"], f["n"]),
            )
        return web.json_response(
            dict(waiting, ticket=t, frame=f["n"], kind=j["kind"], width=j["w"], height=j["h"])
        )

    def lease(self, req):
        j = self.job(req)
        key, *_ = self.actor(req, j)
        f = self.one(
            "SELECT n FROM frames WHERE job=? AND ticket=? AND actor=? AND done=0 AND until>?",
            j["id"],
            req.match_info["ticket"],
            key,
            time.time(),
        )
        if not f:
            raise web.HTTPConflict(text="This frame has been reassigned.")
        return j, f["n"]

    async def input(self, req):
        j, n = self.lease(req)
        return web.FileResponse(
            self.root / j["id"] / "in" / str(n),
            headers={
                "Cache-Control": "no-store",
                "Content-Type": "image/jpeg" if j["kind"] == "images" else "application/octet-stream",
            },
        )

    async def result(self, req):
        j, n = self.lease(req)
        measured = req.headers.get("X-Render-Seconds")
        compute = number(measured, 0.001, LEASE, "render time") if measured is not None else None
        data = await req.read()
        tmp = self.root / j["id"] / "out" / (secrets.token_hex(12) + ".tmp")
        try:
            try:
                await asyncio.to_thread(media.save_result, data, tmp, j["w"], j["h"])
            except (ValueError, OSError) as e:
                raise bad(str(e)) from None
            self.lease(req)  # decoding yielded; a pause, expiry or revocation takes precedence
            os.replace(tmp, tmp.with_name(f"{n:05}.jpg"))
            with self.db:
                self.db.execute(
                    "UPDATE frames SET done=1,seconds=max(0.001,?-issued),result_bytes=?,compute_seconds=?,ticket=NULL,until=0 WHERE job=? AND n=?",
                    (time.time(), len(data), compute, j["id"], n),
                )
            self.advance(j["id"])
        finally:
            tmp.unlink(missing_ok=True)
        return web.json_response({"ok": True})

    def advance(self, jid):
        j = self.one("SELECT status FROM jobs WHERE id=?", jid)
        if j["status"] == "measuring":
            if not self.one("SELECT 1 FROM frames WHERE job=? AND done=0 AND probe=1", jid):
                with self.db:
                    self.db.execute("UPDATE jobs SET status='planned' WHERE id=?", (jid,))
        elif j["status"] == "rendering" and not self.one("SELECT 1 FROM frames WHERE job=? AND done=0", jid):
            with self.db:
                self.db.execute("UPDATE jobs SET status='encoding' WHERE id=?", (jid,))
            self.schedule_encode(jid)

    def schedule_encode(self, jid):
        if jid not in self.tasks:
            with self.db:
                self.db.execute("UPDATE jobs SET encoding_at=? WHERE id=?", (time.time(), jid))
            self.tasks[jid] = asyncio.create_task(self.encode(jid))

    async def encode(self, jid):
        j = self.one("SELECT * FROM jobs WHERE id=?", jid)
        try:
            async with self.encoder_lock:
                await asyncio.to_thread(media.encode, self.root / jid, j["fps"], j["total"])
            status, error = "ready", ""
        except (OSError, RuntimeError, ImportError, subprocess.SubprocessError) as exc:
            status, error = (
                "failed",
                f"MP4 assembly failed ({type(exc).__name__}). Check that FFmpeg is installed and retry.",
            )
        finally:
            self.tasks.pop(jid, None)
        with self.db:
            self.db.execute(
                "UPDATE jobs SET status=?, error=?, finished_at=? WHERE id=?", (status, error, time.time(), jid)
            )

    async def release(self, req):
        j = self.job(req)
        key, _, _, g = self.actor(req, j)
        with self.db:
            self.db.execute(
                "UPDATE frames SET until=0,ticket=NULL WHERE job=? AND actor=? AND done=0", (j["id"], key)
            )
            if g and req.query.get("end") == "1":
                self.db.execute("UPDATE grants SET stopped=1 WHERE session=?", (key,))
        return web.json_response({"ok": True})

    async def clip(self, req):
        self.owner(req)
        j = self.job(req)
        if j["status"] != "ready":
            raise web.HTTPConflict(text="The review cut is still being prepared.")
        return web.FileResponse(self.root / j["id"] / "review.mp4", headers={"Content-Type": "video/mp4"})

    async def poster(self, req):
        self.owner(req)
        j = self.job(req)
        row = self.one("SELECT min(n) FROM frames WHERE job=? AND done=1", j["id"])
        if row[0] is None:
            raise web.HTTPNotFound(text="A preview is being prepared.")
        return web.FileResponse(
            self.root / j["id"] / "out" / f"{row[0]:05}.jpg", headers={"Content-Type": "image/jpeg"}
        )
