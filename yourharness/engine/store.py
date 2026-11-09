"""Immutable files named by their SHA-256: datasets, indexes, partial results, outputs.

A file is written to a temporary name while it streams in, hashed on the way, then
moved to artifacts/<first two hex>/<sha>. Storing the same bytes twice keeps one
copy. Reads stream byte ranges from disk without loading the file. Nothing is ever
changed in place; a new version is a new file.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
from pathlib import Path
from typing import AsyncIterator, Callable

READ_CHUNK = 1024 * 1024


class StoreError(ValueError):
    pass


class ArtifactStore:
    def __init__(self, root: Path, db):
        self.root = Path(root)
        self.tmp = self.root / "tmp"
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.db = db
        for leftover in self.tmp.iterdir():  # uploads a crash left half-done
            try:
                leftover.unlink()
            except OSError:
                pass

    def path(self, sha: str) -> Path:
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise StoreError("not an artifact id")
        return self.root / sha[:2] / sha

    def has(self, sha: str) -> bool:
        try:
            return self.path(sha).is_file()
        except StoreError:
            return False

    def size(self, sha: str) -> int:
        return self.path(sha).stat().st_size

    def _commit(self, tmp: Path, sha: str, size: int, kind: str) -> None:
        dest = self.path(sha)
        if dest.exists():
            tmp.unlink(missing_ok=True)  # the same bytes are already stored
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, dest)
        now = time.time()
        if self.db.one("SELECT sha FROM artifacts WHERE sha = ?", (sha,)) is None:
            self.db.insert("artifacts", {"sha": sha, "size": size, "kind": kind, "created_at": now, "last_used": now})
        else:
            self.db.update("artifacts", {"sha": sha}, {"last_used": now})

    async def put_stream(self, chunks: AsyncIterator[bytes], kind: str, max_bytes: int,
                         observe: Callable[[bytes], None] | None = None) -> tuple[str, int]:
        """Store a stream. observe(chunk) sees every byte in order (the indexer uses it)."""
        loop = asyncio.get_running_loop()
        tmp = self.tmp / f"{secrets.token_hex(8)}.part"
        h = hashlib.sha256()
        size = 0
        f = open(tmp, "wb")
        buf = bytearray()

        def write(data: bytes) -> None:
            f.write(data)
            h.update(data)
            if observe is not None:
                observe(data)

        try:
            async for part in chunks:
                size += len(part)
                if size > max_bytes:
                    raise StoreError(f"That's over the {max_bytes / 2**30:g} GB limit.")
                buf += part
                if len(buf) >= READ_CHUNK:
                    data, buf = bytes(buf), bytearray()
                    await loop.run_in_executor(None, write, data)
            if buf:
                await loop.run_in_executor(None, write, bytes(buf))
            f.close()
            sha = h.hexdigest()
            self._commit(tmp, sha, size, kind)
            return sha, size
        except BaseException:
            f.close()
            tmp.unlink(missing_ok=True)
            raise

    def put_bytes(self, data: bytes, kind: str) -> str:
        sha = hashlib.sha256(data).hexdigest()
        if not self.has(sha):
            tmp = self.tmp / f"{secrets.token_hex(8)}.part"
            tmp.write_bytes(data)
            self._commit(tmp, sha, len(data), kind)
        else:
            self._commit(self.tmp / "unused", sha, len(data), kind)
        return sha

    def put_json(self, value, kind: str) -> str:
        return self.put_bytes(json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8"), kind)

    def read_json(self, sha: str):
        return json.loads(self.path(sha).read_bytes())

    async def stream_range(self, sha: str, start: int, end: int) -> AsyncIterator[bytes]:
        """The bytes [start, end) of an artifact, read from disk a megabyte at a time."""
        loop = asyncio.get_running_loop()
        f = open(self.path(sha), "rb")
        try:
            f.seek(start)
            left = end - start
            while left > 0:
                data = await loop.run_in_executor(None, f.read, min(READ_CHUNK, left))
                if not data:
                    break
                left -= len(data)
                yield data
        finally:
            f.close()

    def delete(self, sha: str) -> None:
        try:
            self.path(sha).unlink(missing_ok=True)
        except StoreError:
            return
        self.db.conn.execute("DELETE FROM artifacts WHERE sha = ?", (sha,))

    def collect_garbage(self, keep: set[str], older_than_s: float = 3600) -> int:
        """Delete artifacts nothing refers to anymore, once they're an hour old."""
        cutoff = time.time() - older_than_s
        gone = 0
        for row in self.db.all("SELECT sha, created_at, pinned FROM artifacts"):
            if row["sha"] in keep or row["pinned"] or (row["created_at"] or 0) > cutoff:
                continue
            self.delete(row["sha"])
            gone += 1
        return gone

    def usage(self) -> dict:
        r = self.db.conn.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM artifacts").fetchone()
        return {"artifacts": r[0], "bytes": r[1]}
