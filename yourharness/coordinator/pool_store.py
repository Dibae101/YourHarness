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
from pathlib import Path

from .jobkinds import UserError

log = logging.getLogger("yourharness")

CHUNK = 4 * 1024 * 1024
REPLICAS = 2
REQUEST_TIMEOUT = 60.0


class PoolStore:
    def __init__(self, sched, data_dir: Path):
        self.s = sched
        self.path = Path(data_dir) / "pool-index.json"
        self.files: dict[str, dict] = {}
        self.holders: dict[str, list[str]] = {}
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.files, self.holders = data.get("files", {}), data.get("holders", {})
        self.reqs: dict[str, tuple[asyncio.Future, str]] = {}
        self._repairing = False

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"files": self.files, "holders": self.holders}), encoding="utf-8")
        os.replace(tmp, self.path)

    # ------------------------------------------------------------------ devices
    def live_agents(self):
        return [d for d in self.s.devices.values() if d.kind == "agent" and not d.closed and not d.stale]

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
        room = lambda d: (d.info.get("diskFreeGB") or 1e9)  # noqa: E731 - unknown free space counts as plenty
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
    async def put_file(self, name: str, content, by: str | None, max_bytes: int) -> int:
        size, hashes, sizes, fresh = 0, [], [], []
        buf = bytearray()

        async def flush(chunk: bytes):
            h = hashlib.sha256(chunk).hexdigest()
            await self._put_chunk(h, chunk)
            hashes.append(h)
            sizes.append(len(chunk))
            fresh.append(h)

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
            self._gc(fresh)
            raise
        old = self.files.get(name)
        self.files[name] = {"size": size, "chunks": hashes, "sizes": sizes, "mtime": int(time.time() * 1000), "by": by}
        if old:
            self._gc(old["chunks"])
        self.save()
        self.changed()
        return size

    def delete(self, name: str) -> bool:
        old = self.files.pop(name, None)
        if old is None:
            return False
        self._gc(old["chunks"])
        self.save()
        self.changed()
        return True

    def _gc(self, hashes: list[str]) -> None:
        still = {h for f in self.files.values() for h in f["chunks"]}
        for h in set(hashes):
            if h in still:
                continue
            for dev in self._live_holders(h):
                dev.send({"type": "store.del", "hash": h})
            self.holders.pop(h, None)

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
    def listing(self) -> dict:
        live_ids = {d.id for d in self.live_agents()}
        want = min(REPLICAS, len(live_ids)) or 1
        files = []
        for name, f in self.files.items():
            copies = min((len([i for i in self.holders.get(h, []) if i in live_ids]) for h in f["chunks"]), default=want)
            files.append({"name": name, "size": f["size"], "mtime": f["mtime"], "by": f["by"], "chunks": len(f["chunks"]),
                          "copies": copies, "wanted": REPLICAS, "available": copies >= 1})
        files.sort(key=lambda x: -x["mtime"])
        stored = sum(sum(f["sizes"]) for f in self.files.values())
        free = sum(d.info.get("diskFreeGB") or 0 for d in self.live_agents())
        return {"files": files, "agents": len(live_ids), "storedBytes": stored, "freeGB": round(free, 1), "replicas": REPLICAS}

    def changed(self) -> None:
        self.s.broadcast({"type": "pool", "pool": self.listing()})

    async def repair(self) -> None:
        """Copy chunks that have fewer copies than they should onto other computers."""
        if self._repairing:
            return
        self._repairing = True
        try:
            did = False
            budget = 6
            for f in list(self.files.values()):
                for h, size in zip(f["chunks"], f["sizes"]):
                    if budget <= 0:
                        break
                    if not self._live_holders(h) or not self._targets(h, size):
                        continue
                    try:
                        await self._put_chunk(h, await self._get_chunk(h))
                        did = True
                        budget -= 1
                    except Exception as e:
                        log.warning("repair of a chunk failed: %s", e)
            if did:
                self.save()
                self.changed()
        finally:
            self._repairing = False
