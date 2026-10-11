"""Helpers for reaching Ubuntu (or another distribution) through WSL from Windows. Standard library only."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def wsl_distro() -> str | None:
    wsl = shutil.which("wsl.exe")
    if not wsl:
        return None
    try:
        out = subprocess.run([wsl, "-l", "-q"], capture_output=True, timeout=8, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    names = [n.strip() for n in out.decode("utf-16-le", "ignore").replace("\x00", "").splitlines() if n.strip()]
    if not names:
        return None
    return next((n for n in names if "ubuntu" in n.lower()), names[0])


def to_wsl_path(p: Path | str) -> str:
    """C:\\Users\\Ana Lee\\x -> /mnt/c/Users/Ana Lee/x (WSL's default mount)."""
    s = str(p)
    if len(s) > 1 and s[1] == ":":
        return f"/mnt/{s[0].lower()}{s[2:].replace(chr(92), '/')}"
    return s.replace(chr(92), "/")
