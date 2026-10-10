"""File access on a computer that allows it, for the Files app.

Paths are relative to the user's home folder and can't leave it: "Documents/a.txt",
never "/etc/passwd" or "../other-user". Reads and writes go in pieces of up to
4 MB, so files of any size pass through the coordinator without filling memory.
"""

from __future__ import annotations

import base64
import os
import shutil
import stat
from pathlib import Path

PIECE = 4 * 1024 * 1024
MAX_ENTRIES = 3000


class FileAccessError(Exception):
    pass


class HomeFiles:
    def __init__(self, root: Path | None = None):
        self.root = (root or Path.home()).resolve()

    def describe(self) -> dict:
        return {"root": str(self.root), "sep": os.sep}

    def _path(self, rel) -> Path:
        rel = str(rel or "").replace("\\", "/").strip("/")
        if rel.startswith("~"):
            rel = rel[1:].strip("/")
        p = (self.root / rel).resolve() if rel else self.root
        if p != self.root and self.root not in p.parents:
            raise FileAccessError("That path is outside the home folder.")
        return p

    def _rel(self, p: Path) -> str:
        return p.relative_to(self.root).as_posix() if p != self.root else ""

    # ------------------------------------------------------------------ operations
    def list(self, rel) -> dict:
        p = self._path(rel)
        if not p.is_dir():
            raise FileAccessError("That folder doesn't exist.")
        out = []
        try:
            with os.scandir(p) as it:
                for e in it:
                    if len(out) >= MAX_ENTRIES:
                        break
                    try:
                        st = e.stat(follow_symlinks=True)
                    except OSError:
                        continue
                    out.append({
                        "name": e.name, "dir": stat.S_ISDIR(st.st_mode), "size": st.st_size if not stat.S_ISDIR(st.st_mode) else None,
                        "mtime": int(st.st_mtime * 1000), "hidden": e.name.startswith("."),
                    })
        except PermissionError:
            raise FileAccessError("This computer won't let YourHarness read that folder.") from None
        out.sort(key=lambda x: (not x["dir"], x["name"].lower()))
        usage = shutil.disk_usage(p)
        return {"path": self._rel(p), "entries": out, "truncated": len(out) >= MAX_ENTRIES, "free": usage.free, "total": usage.total}

    def stat(self, rel) -> dict:
        p = self._path(rel)
        if not p.exists():
            raise FileAccessError("That file doesn't exist.")
        st = p.stat()
        return {"path": self._rel(p), "dir": p.is_dir(), "size": st.st_size, "mtime": int(st.st_mtime * 1000), "name": p.name}

    def read(self, rel, offset: int, length: int) -> dict:
        p = self._path(rel)
        if not p.is_file():
            raise FileAccessError("That file doesn't exist.")
        length = max(0, min(int(length), PIECE))
        with open(p, "rb") as f:
            f.seek(max(0, int(offset)))
            data = f.read(length)
        return {"data": base64.b64encode(data).decode("ascii"), "size": p.stat().st_size}

    def write(self, rel, offset: int, data_b64: str, final: bool) -> dict:
        """Write one piece. Pieces go to a hidden .part file that replaces the target at the end."""
        p = self._path(rel)
        if p.is_dir():
            raise FileAccessError("A folder with that name is already there.")
        if not p.parent.is_dir():
            raise FileAccessError("The folder to save into doesn't exist.")
        part = p.with_name(f".{p.name}.yhpart")
        data = base64.b64decode(data_b64) if data_b64 else b""
        with open(part, "wb" if int(offset) == 0 else "r+b") as f:
            f.seek(int(offset))
            f.write(data)
        if final:
            os.replace(part, p)
        return {"ok": True}

    def abort(self, rel) -> dict:
        part = self._path(rel)
        part = part.with_name(f".{part.name}.yhpart")
        part.unlink(missing_ok=True)
        return {"ok": True}

    def mkdir(self, rel) -> dict:
        p = self._path(rel)
        if p.exists():
            raise FileAccessError("Something with that name is already there.")
        p.mkdir(parents=False)
        return {"ok": True, "path": self._rel(p)}

    def rename(self, rel, to_rel) -> dict:
        src, dst = self._path(rel), self._path(to_rel)
        if src == self.root:
            raise FileAccessError("The home folder itself can't be renamed.")
        if dst.exists():
            raise FileAccessError("Something with the new name is already there.")
        os.replace(src, dst)
        return {"ok": True}

    def delete(self, rel, recursive: bool) -> dict:
        p = self._path(rel)
        if p == self.root:
            raise FileAccessError("The home folder itself can't be deleted.")
        if p.is_dir() and not p.is_symlink():
            if recursive:
                shutil.rmtree(p)
            else:
                try:
                    p.rmdir()
                except OSError:
                    raise FileAccessError("That folder isn't empty.") from None
        elif p.exists() or p.is_symlink():
            p.unlink()
        else:
            raise FileAccessError("That file doesn't exist.")
        return {"ok": True}

    def handle(self, op: str, m: dict) -> dict:
        if op == "list":
            return self.list(m.get("path"))
        if op == "stat":
            return self.stat(m.get("path"))
        if op == "read":
            return self.read(m.get("path"), m.get("offset", 0), m.get("length", PIECE))
        if op == "write":
            return self.write(m.get("path"), m.get("offset", 0), m.get("data", ""), bool(m.get("final")))
        if op == "abort":
            return self.abort(m.get("path"))
        if op == "mkdir":
            return self.mkdir(m.get("path"))
        if op == "rename":
            return self.rename(m.get("path"), m.get("to"))
        if op == "delete":
            return self.delete(m.get("path"), bool(m.get("recursive")))
        raise FileAccessError("Unknown file operation.")
