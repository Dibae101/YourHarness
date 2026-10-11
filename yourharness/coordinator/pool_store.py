"""Storage pooled across computers.

A file is cut into 4 MB chunks named by their SHA-256. Each chunk is kept on two
different agents (one if only one is connected). The coordinator holds only the
index of which chunk lives where, and relays chunk bytes over the agents'
existing connections, so agents behind WSL or a firewall need no open ports.
When a computer leaves, a repair pass copies its chunks to other computers.
Browsers (phones and tablets) don't hold chunks; they only upload and download.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import time
from collections import Counter
from pathlib import Path

from .jobkinds import UserError
from .state import CAP_FILES

log = logging.getLogger("yourharness")


def clean_path(raw) -> str:
    """A path inside /pool as "a/b/c.txt" (no leading slash, no "." or ".." parts). "" is the top folder."""
    parts = [p for p in str(raw or "").replace("\\", "/").split("/") if p not in ("", ".")]
    for p in parts:
        if p == ".." or len(p) > 200 or any(ord(c) < 32 or ord(c) == 127 for c in p):
            raise UserError("That path isn't allowed.")
    return "/".join(parts)


def file_digest(chunks: list[str]) -> str:
    """One fingerprint for a whole file, made from its chunk fingerprints."""
    return hashlib.sha256("".join(chunks).encode("ascii")).hexdigest()


CHUNK = 4 * 1024 * 1024
REPLICAS = 2
REQUEST_TIMEOUT = 60.0


class PoolStore:
    def __init__(self, sched, data_dir: Path):
        self.s = sched
        self.path = Path(data_dir) / "pool-index.json"
        self.files: dict[str, dict] = {}
        self.holders: dict[str, list[str]] = {}
        self.dirs: set[str] = set()  # folders made on purpose; folders that hold files exist without being listed here
        # chunks deleted while a computer holding them was offline: it is told to delete them when it's back
        self.dead: dict[str, list[str]] = {}
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.files, self.holders = data.get("files", {}), data.get("holders", {})
            self.dirs = set(data.get("dirs", []))
            self.dead = data.get("dead", {})
        # chunks that an unfinished upload or a repair is counting on, so _gc leaves them alone
        self.pending: Counter[str] = Counter()
        self.reqs: dict[str, tuple[asyncio.Future, str]] = {}
        self._repairing = False

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"files": self.files, "holders": self.holders, "dirs": sorted(self.dirs), "dead": self.dead}), encoding="utf-8")
        os.replace(tmp, self.path)

    # ------------------------------------------------------------------ devices
    def live_agents(self):
        return [d for d in self.s.devices.values() if d.trusted and not d.closed and not d.stale]

    def _live_holders(self, h: str):
        return [self.s.devices[i] for i in self.holders.get(h, []) if i in self.s.devices and self.s.devices[i] in self.live_agents()]

    async def _ask(self, dev, msg: dict) -> dict:
        rid = secrets.token_hex(8)
        fut = asyncio.get_running_loop().create_future()
        self.reqs[rid] = (fut, dev.id)
        dev.send({**msg, "rid": rid})
        try:
            return await asyncio.wait_for(fut, REQUEST_TIMEOUT)
        finally:
            self.reqs.pop(rid, None)

    def handle(self, dev, msg: dict) -> None:
        entry = self.reqs.get(msg.get("rid"))
        if entry and entry[1] == dev.id and not entry[0].done():
            if msg.get("type") == "store.error":
                entry[0].set_exception(RuntimeError(str(msg.get("error") or "the agent could not store that")[:200]))
            else:
                entry[0].set_result(msg)

    def device_gone(self, dev) -> None:
        for fut, dev_id in list(self.reqs.values()):
            if dev_id == dev.id and not fut.done():
                fut.set_exception(ConnectionError(f"{dev.name} disconnected"))

    # ------------------------------------------------------------------ chunks
    def _targets(self, h: str, size: int) -> list:
        live = self.live_agents()
        need = min(REPLICAS, len(live))
        held = {d.id for d in self._live_holders(h)}
        # unknown free space counts as plenty; a reported 0 (a full disk) is 0, not unknown
        room = lambda d: 1e9 if d.info.get("diskFreeGB") is None else d.info["diskFreeGB"]  # noqa: E731
        cands = [d for d in live if d.id not in held and room(d) * 1e9 > size + 5e8]
        cands.sort(key=lambda d: -room(d))
        return cands[: max(0, need - len(held))]

    async def _put_chunk(self, h: str, data: bytes) -> None:
        if not self.live_agents():
            raise UserError("No computer is connected to store files. Add a Mac, Linux or Windows computer as an agent first.")
        targets = self._targets(h, len(data))
        b64 = base64.b64encode(data).decode("ascii")
        results = await asyncio.gather(*(self._ask(d, {"type": "store.put", "hash": h, "data": b64}) for d in targets), return_exceptions=True)
        stored = [d.id for d, r in zip(targets, results) if not isinstance(r, BaseException)]
        for d, r in zip(targets, results):
            if isinstance(r, BaseException):
                log.warning("%s could not store a chunk: %s", d.name, r)
        have = list(dict.fromkeys(self.holders.get(h, []) + stored))
        if not [i for i in have if i in {d.id for d in self.live_agents()}]:
            raise UserError("No computer could store that chunk. Check that the agents have free disk space.")
        self.holders[h] = have

    async def _get_chunk(self, h: str) -> bytes:
        last: Exception | None = None
        for dev in self._live_holders(h):
            try:
                res = await self._ask(dev, {"type": "store.get", "hash": h})
                data = base64.b64decode(res["data"])
                if hashlib.sha256(data).hexdigest() == h:
                    return data
                last = ValueError("chunk failed its checksum")
            except Exception as e:  # try the next copy
                last = e
        raise UserError(f"Part of this file is on a computer that is offline ({last or 'no copy is connected'}). It will be readable again when that computer reconnects.")

    # ------------------------------------------------------------------ files
    # ------------------------------------------------------------------ folders and paths
    def is_dir(self, path: str) -> bool:
        if path == "":
            return True
        pre = path + "/"
        return path in self.dirs or any(d.startswith(pre) for d in self.dirs) or any(f.startswith(pre) for f in self.files)

    def _check_parents(self, path: str) -> None:
        parts = path.split("/")
        for k in range(1, len(parts)):
            if "/".join(parts[:k]) in self.files:
                raise UserError(f"{'/'.join(parts[:k])} is a file, so nothing can be saved inside it.")

    def mkdir(self, path: str) -> None:
        path = clean_path(path)
        if not path:
            raise UserError("Name the new folder.")
        if path in self.files or self.is_dir(path):
            raise UserError("Something with that name is already there.")
        self._check_parents(path)
        self.dirs.add(path)
        self.save()
        self.changed()

    def list_dir(self, path: str) -> list[dict]:
        path = clean_path(path)
        if path in self.files:
            raise UserError("That's a file, not a folder.")
        if not self.is_dir(path):
            raise UserError("That folder doesn't exist.")
        pre = path + "/" if path else ""
        live_ids = {d.id for d in self.live_agents()}
        out: dict[str, dict] = {}
        for full, f in self.files.items():
            if not full.startswith(pre):
                continue
            rest = full[len(pre):]
            head, _, tail = rest.partition("/")
            if tail:
                e = out.setdefault(head, {"name": head, "dir": True, "size": None, "mtime": 0})
                e["mtime"] = max(e["mtime"], f["mtime"])
            else:
                copies = min((len([i for i in self.holders.get(h, []) if i in live_ids]) for h in f["chunks"]), default=1)
                on = sorted({self.s.devices[i].name for h in f["chunks"] for i in self.holders.get(h, []) if i in live_ids and i in self.s.devices})
                out[head] = {"name": head, "dir": False, "size": f["size"], "mtime": f["mtime"], "available": copies >= 1, "copies": copies, "on": on}
        for d in self.dirs:
            if d.startswith(pre):
                head = d[len(pre):].partition("/")[0]
                if head:
                    out.setdefault(head, {"name": head, "dir": True, "size": None, "mtime": 0})
        return sorted(out.values(), key=lambda e: (not e["dir"], e["name"].lower()))

    def rename(self, src: str, dst: str) -> None:
        src, dst = clean_path(src), clean_path(dst)
        if not src or not dst:
            raise UserError("Name both the old and the new path.")
        if src == dst:
            return
        if dst == src or dst.startswith(src + "/"):
            raise UserError("A folder can't be moved into itself.")
        if dst in self.files or self.is_dir(dst):
            raise UserError("Something with the new name is already there.")
        self._check_parents(dst)
        if src in self.files:
            self.files[dst] = self.files.pop(src)
        elif self.is_dir(src):
            pre = src + "/"
            for full in [f for f in self.files if f.startswith(pre)]:
                self.files[dst + "/" + full[len(pre):]] = self.files.pop(full)
            self.dirs = {dst + d[len(src):] if d == src or d.startswith(pre) else d for d in self.dirs}
            self.dirs.add(dst)
        else:
            raise UserError("That doesn't exist.")
        self.save()
        self.changed()

    def manifest(self) -> dict:
        files = {}
        for path, f in self.files.items():
            f.setdefault("digest", file_digest(f["chunks"]))
            files[path] = {"size": f["size"], "mtime": f["mtime"], "digest": f["digest"]}
        return {"files": files, "dirs": sorted(self.dirs)}

    async def put_file(self, name: str, content, by: str | None, max_bytes: int) -> int:
        name = clean_path(name)
        if not name:
            raise UserError("Name the file.")
        if self.is_dir(name):
            raise UserError("A folder with that name is already there.")
        self._check_parents(name)
        size, hashes, sizes, fresh = 0, [], [], []
        buf = bytearray()

        async def flush(chunk: bytes):
            h = hashlib.sha256(chunk).hexdigest()
            self.pending[h] += 1  # held until this upload commits or fails
            fresh.append(h)
            await self._put_chunk(h, chunk)
            hashes.append(h)
            sizes.append(len(chunk))

        try:
            async for part in content.iter_chunked(256 * 1024):
                size += len(part)
                if size > max_bytes:
                    raise UserError(f"That file is over the {max_bytes // 2**20} MB limit.")
                buf += part
                while len(buf) >= CHUNK:
                    chunk = bytes(buf[:CHUNK])
                    del buf[:CHUNK]
                    await flush(chunk)
            if buf:
                await flush(bytes(buf))
        except BaseException:
            self._release(fresh)
            self._gc(fresh)
            raise
        old = self.files.get(name)
        self.files[name] = {"size": size, "chunks": hashes, "sizes": sizes, "mtime": int(time.time() * 1000), "by": by, "digest": file_digest(hashes)}
        self._release(fresh)
        if old:
            self._gc(old["chunks"])
        self.save()
        self.changed()
        return size

    def delete(self, name: str, recursive: bool = False) -> bool:
        name = clean_path(name)
        old = self.files.pop(name, None)
        if old is not None:
            self._gc(old["chunks"])
        elif name and self.is_dir(name):
            pre = name + "/"
            inside = [f for f in self.files if f.startswith(pre)]
            if (inside or any(d.startswith(pre) for d in self.dirs)) and not recursive:
                raise UserError("That folder isn't empty.")
            gone: list[str] = []
            for f in inside:
                gone += self.files.pop(f)["chunks"]
            self.dirs = {d for d in self.dirs if d != name and not d.startswith(pre)}
            self._gc(gone)
        else:
            return False
        self.save()
        self.changed()
        return True

    def _release(self, hashes: list[str]) -> None:
        for h in hashes:
            self.pending[h] -= 1
            if self.pending[h] <= 0:
                del self.pending[h]

    def _referenced(self) -> set[str]:
        return {h for f in self.files.values() for h in f["chunks"]} | set(self.pending)

    def _gc(self, hashes: list[str]) -> None:
        still = self._referenced()
        for h in set(hashes):
            if h in still:
                continue
            live = self._live_holders(h)
            for dev in live:
                dev.send({"type": "store.del", "hash": h})
            offline = [i for i in self.holders.pop(h, []) if i not in {d.id for d in live}]
            if offline:
                self.dead[h] = list(dict.fromkeys(self.dead.get(h, []) + offline))

    def _sweep(self) -> bool:
        """Tell computers that are back to delete chunks removed while they were away,
        and take copies off computers that shouldn't hold them (lenders) once a trusted
        copy exists. Returns True if the index changed."""
        did = False
        online = {d.id: d for d in self.s.devices.values() if d.kind == "agent" and not d.closed and not d.stale}
        known = self.s.store.data.get("devices", {})
        still = self._referenced()
        for h, who in list(self.dead.items()):
            if h in still:  # stored again since; whoever has it now keeps it
                self.dead.pop(h)
                did = True
                continue
            for i in who:
                if i in online:
                    online[i].send({"type": "store.del", "hash": h})
            left = [i for i in who if i not in online and i in known]  # removed devices never come back
            if left != who:
                did = True
                if left:
                    self.dead[h] = left
                else:
                    self.dead.pop(h)
        for h, who in list(self.holders.items()):
            untrusted = [i for i in who if i in online and not online[i].trusted]
            if untrusted and self._live_holders(h):
                for i in untrusted:
                    online[i].send({"type": "store.del", "hash": h})
                self.holders[h] = [i for i in who if i not in untrusted]
                did = True
        return did

    async def stream(self, request, name: str):
        from aiohttp import web

        f = self.files.get(name)
        if f is None:
            raise web.HTTPNotFound(text=f"There is no pooled file named {name}.")
        # fetch the first chunk before sending headers, so an offline copy is a clean error
        first = await self._get_chunk(f["chunks"][0]) if f["chunks"] else b""
        from urllib.parse import quote

        resp = web.StreamResponse(headers={
            "Content-Type": "application/octet-stream", "Content-Length": str(f["size"]),
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}",
            "X-Content-Type-Options": "nosniff", "Content-Security-Policy": "sandbox", "Cache-Control": "no-store",
        })
        await resp.prepare(request)
        await resp.write(first)
        for h in f["chunks"][1:]:
            await resp.write(await self._get_chunk(h))
        await resp.write_eof()
        return resp

    # ------------------------------------------------------------------ health
    def listing(self, names: bool = True) -> dict:
        live_ids = {d.id for d in self.live_agents()}
        want = min(REPLICAS, len(live_ids)) or 1
        files = []
        for name, f in self.files.items():
            copies = min((len([i for i in self.holders.get(h, []) if i in live_ids]) for h in f["chunks"]), default=want)
            on = sorted({self.s.devices[i].name for h in f["chunks"] for i in self.holders.get(h, []) if i in live_ids and i in self.s.devices})
            files.append({"name": name, "size": f["size"], "mtime": f["mtime"], "by": f["by"], "chunks": len(f["chunks"]),
                          "copies": copies, "wanted": REPLICAS, "available": copies >= 1, "on": on})
        files.sort(key=lambda x: -x["mtime"])
        stored = sum(sum(f["sizes"]) for f in self.files.values())
        free = sum(d.info.get("diskFreeGB") or 0 for d in self.live_agents())
        return {"files": files if names else [], "agents": len(live_ids), "storedBytes": stored, "freeGB": round(free, 1), "replicas": REPLICAS}

    def changed(self) -> None:
        self.s.broadcast({"type": "pool", "pool": self.listing()}, need=CAP_FILES)

    async def repair(self) -> None:
        """Copy chunks that have fewer copies than they should onto other computers."""
        if self._repairing:
            return
        self._repairing = True
        try:
            did = self._sweep()
            budget = 6
            for f in list(self.files.values()):
                for h, size in zip(f["chunks"], f["sizes"]):
                    if budget <= 0:
                        break
                    if not self._live_holders(h) or not self._targets(h, size):
                        continue
                    self.pending[h] += 1  # the file may be deleted while this copy is made
                    try:
                        await self._put_chunk(h, await self._get_chunk(h))
                        did = True
                        budget -= 1
                    except Exception as e:
                        log.warning("repair of a chunk failed: %s", e)
                    finally:
                        self._release([h])
                        self._gc([h])
            if did:
                self.save()
                self.changed()
        finally:
            self._repairing = False
