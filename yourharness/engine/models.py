"""Model files on a computer that evaluates them: fetched once per SHA-256, checked, cached.

A worker downloads its job's model with the attempt's token, hashes it on the way,
and refuses it when the hash isn't the one the job was started with, so a model
changed on the way (or on the coordinator's disk) never produces predictions. A
.zip is unpacked with every path checked to stay inside the model's folder. The
folder is built under a temporary name and renamed into place when complete, so a
worker that crashes halfway leaves nothing a later worker would trust. Models not
used for a while go first when the cache is over its limit.

Standard library only, like the rest of the worker.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import zipfile
from pathlib import Path, PurePosixPath

FORMATS = ("py", "zip", "pt")
CACHE_BYTES = float(os.environ.get("HARNESS_MODEL_CACHE_GB", 5)) * 2**30
MAX_UNPACKED = 8 * 2**30


class ModelIntegrityError(RuntimeError):
    """The model that arrived isn't the one the job names."""


def detect_format(filename: str, head: bytes) -> str | None:
    """py, zip or pt from the file's name and first bytes; None when it's none of them."""
    name = filename.lower()
    if name.endswith(".py"):
        return "py"
    if head.startswith(b"PK"):
        return "pt" if name.endswith((".pt", ".pth", ".torchscript")) else "zip"
    return None


def safe_extract(z: zipfile.ZipFile, dest: Path) -> None:
    total = 0
    for info in z.infolist():
        name = info.filename
        parts = PurePosixPath(name).parts
        if name.startswith(("/", "\\")) or ".." in parts or ":" in name or (info.external_attr >> 16) & 0o170000 == 0o120000:
            raise ModelIntegrityError(f"the model's .zip has a path that leaves its folder: {name}")
        total += info.file_size
        if total > MAX_UNPACKED:
            raise ModelIntegrityError("the model's .zip unpacks to more than 8 GB")
        if name.endswith("/"):
            (dest / name).mkdir(parents=True, exist_ok=True)
            continue
        target = dest / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with z.open(info) as src, open(target, "wb") as out:
            shutil.copyfileobj(src, out, 1024 * 1024)


def fetch(chunks, cache: Path, model: dict) -> Path:
    """The model's folder on this computer. chunks() streams the model's bytes from the coordinator."""
    sha, fmt = model["sha"], model["format"]
    if fmt not in FORMATS or len(sha) != 64:
        raise ModelIntegrityError("the job names a model this worker can't use")
    root = cache / "models"
    dest = root / sha
    if (dest / ".complete").is_file():
        os.utime(dest / ".complete")
        return dest
    root.mkdir(parents=True, exist_ok=True)
    tmp = root / f".{sha[:16]}.{secrets.token_hex(4)}"
    tmp.mkdir()
    try:
        blob = tmp / ".download"
        h = hashlib.sha256()
        with open(blob, "wb") as f:
            for data in chunks():
                f.write(data)
                h.update(data)
        if h.hexdigest() != sha:
            raise ModelIntegrityError(f"the model that arrived has SHA-256 {h.hexdigest()[:12]}…, not the job's {sha[:12]}…: it was not used")
        if fmt == "py":
            os.replace(blob, tmp / "model.py")
        elif fmt == "pt":
            os.replace(blob, tmp / "model.pt")
        else:
            with zipfile.ZipFile(blob) as z:
                safe_extract(z, tmp)
            blob.unlink()
            if model.get("adapter") != "project" and not (tmp / "model.py").is_file():  # a test job's project has no model.py
                raise ModelIntegrityError("the model's .zip has no model.py at its top level")
        (tmp / ".complete").write_text(sha, encoding="ascii")
        try:
            os.replace(tmp, dest)
        except OSError:
            if not (dest / ".complete").is_file():  # another worker got there first, or it really failed
                raise
            shutil.rmtree(tmp, ignore_errors=True)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    evict(root, keep=sha)
    return dest


def evict(root: Path, keep: str, cap: float = CACHE_BYTES) -> list[str]:
    """Remove the least recently used models while the cache is over its limit."""
    entries = []
    for d in root.iterdir():
        mark = d / ".complete"
        if d.name.startswith(".") or not mark.is_file():
            continue
        size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
        entries.append((mark.stat().st_mtime, d.name, size))
    total = sum(s for _, _, s in entries)
    gone = []
    for _t, name, size in sorted(entries):
        if total <= cap:
            break
        if name == keep:
            continue
        shutil.rmtree(root / name, ignore_errors=True)
        total -= size
        gone.append(name)
    return gone
