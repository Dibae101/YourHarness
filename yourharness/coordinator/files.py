"""The shared folder: every device can list, add, read byte ranges of, and delete files.

Files are always served as downloads (octet-stream, nosniff, attachment), so an
HTML file someone adds can never run as a page on the pool's own origin.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from urllib.parse import quote

from aiohttp import web

from .jobkinds import sanitize_name


def safe_name(raw: str) -> str | None:
    return sanitize_name(raw)


def list_files(shared_dir: Path, meta: dict) -> list[dict]:
    out = []
    try:
        entries = list(os.scandir(shared_dir))
    except OSError:
        return out
    for e in entries:
        if e.name.startswith(".") or not e.is_file():
            continue
        try:
            st = e.stat()
        except OSError:
            continue
        m = meta.get(e.name, {})
        out.append({"name": e.name, "size": st.st_size, "mtime": int(st.st_mtime * 1000), "by": m.get("by"), "color": m.get("color")})
    out.sort(key=lambda f: f["mtime"], reverse=True)
    return out


async def receive(request: web.Request, shared_dir: Path, name: str, max_bytes: int) -> int:
    """Stream an upload to a temporary file, then move it into place. Returns its size."""
    declared = request.content_length
    if declared is not None and declared > max_bytes:
        raise web.HTTPRequestEntityTooLarge(max_size=max_bytes, actual_size=declared, text=f"That file is over the {max_bytes // 2**20} MB limit.")
    tmp = shared_dir / f".upload-{secrets.token_hex(6)}"
    received = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.content.iter_chunked(256 * 1024):
                received += len(chunk)
                if received > max_bytes:
                    raise web.HTTPRequestEntityTooLarge(max_size=max_bytes, actual_size=received, text=f"That file is over the {max_bytes // 2**20} MB limit.")
                f.write(chunk)
        os.replace(tmp, shared_dir / name)
    finally:
        if tmp.exists():
            tmp.unlink()
    return received


def download(shared_dir: Path, name: str) -> web.StreamResponse:
    path = shared_dir / name
    if not path.is_file():
        raise web.HTTPNotFound(text=f"There is no shared file named {name}.")
    return web.FileResponse(path, headers={
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "sandbox",
        "Cache-Control": "no-store",
    })
