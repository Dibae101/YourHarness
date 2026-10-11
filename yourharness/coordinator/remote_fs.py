"""Relays the Files app's requests to the computer that holds the files.

Every computer that hosts a terminal also lets the pool browse its home folder.
Bytes travel over the computer's existing connection in 4 MB pieces, so a file
can go from the Mac to the Windows PC (or to a phone's download) without either
computer opening a port.
"""

from __future__ import annotations

import asyncio
import base64
import secrets

PIECE = 4 * 1024 * 1024
TIMEOUT = 60.0


class RemoteFSError(Exception):
    pass


class RemoteFS:
    def __init__(self, sched):
        self.s = sched
        self.reqs: dict[str, tuple[asyncio.Future, str]] = {}

    def device(self, device_id: str):
        d = self.s.devices.get(device_id)
        if d is None or d.closed:
            raise RemoteFSError("That computer isn't connected right now.")
        if not d.files:
            raise RemoteFSError(f"{d.name} doesn't allow file access.")
        return d

    async def ask(self, device_id: str, op: str, **args) -> dict:
        d = self.device(device_id)
        rid = secrets.token_hex(8)
        fut = asyncio.get_running_loop().create_future()
        self.reqs[rid] = (fut, d.id)
        d.send({"type": "fs", "op": op, "rid": rid, **args})
        try:
            return await asyncio.wait_for(fut, TIMEOUT)
        except asyncio.TimeoutError:
            raise RemoteFSError(f"{d.name} didn't answer in time.") from None
        finally:
            self.reqs.pop(rid, None)

    def handle(self, dev, msg: dict) -> None:
        entry = self.reqs.get(msg.get("rid"))
        if not entry or entry[1] != dev.id or entry[0].done():
            return
        if msg.get("type") == "fs.error":
            entry[0].set_exception(RemoteFSError(str(msg.get("error") or "The file operation failed.")[:300]))
        else:
            entry[0].set_result(msg)

    def device_gone(self, dev) -> None:
        for fut, dev_id in list(self.reqs.values()):
            if dev_id == dev.id and not fut.done():
                fut.set_exception(RemoteFSError(f"{dev.name} disconnected."))

    # ------------------------------------------------------------------ streaming helpers
    async def read_pieces(self, device_id: str, path: str):
        """Yield the file's bytes piece by piece."""
        info = await self.ask(device_id, "stat", path=path)
        if info.get("dir"):
            raise RemoteFSError("That's a folder. Pick a file.")
        offset, size = 0, int(info.get("size") or 0)
        while offset < size:
            res = await self.ask(device_id, "read", path=path, offset=offset, length=PIECE)
            data = base64.b64decode(res.get("data") or "")
            if not data:
                break
            offset += len(data)
            yield data

    async def write_stream(self, device_id: str, path: str, pieces) -> int:
        """Write an async iterator of byte pieces to a file on that computer."""
        offset = 0
        buf = bytearray()
        try:
            async for part in pieces:
                buf += part
                while len(buf) >= PIECE:
                    chunk = bytes(buf[:PIECE])
                    del buf[:PIECE]
                    await self.ask(device_id, "write", path=path, offset=offset, data=base64.b64encode(chunk).decode("ascii"), final=False)
                    offset += len(chunk)
            await self.ask(device_id, "write", path=path, offset=offset, data=base64.b64encode(bytes(buf)).decode("ascii"), final=True)
            return offset + len(buf)
        except BaseException:
            try:
                await self.ask(device_id, "abort", path=path)
            except Exception:
                pass
            raise
