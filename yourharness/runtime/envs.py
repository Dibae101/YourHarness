"""Python environments for jobs, prepared with uv on this computer.

A job that needs packages names its Python version and requirements. The same pair
always gives the same fingerprint, and the environment for it is built once per
computer in <home>/envs/<fingerprint> and reused by every later job that asks for
the same thing. Nothing is installed into this computer's own Python.

Building only happens when the pool's owner approved it for this computer. It goes:
  1. find uv (next to this program, in this program's Python, or on the PATH), or,
     if allowed, download the pinned uv release from GitHub and check it against
     its published SHA-256;
  2. uv venv --python <version>  (uv may download that Python, if allowed);
  3. uv pip install the requirements;
  4. a preflight in the new environment: the job engine's worker imports, the
     requested packages import, and any GPU PyTorch can see is reported.
It happens in a temporary folder that becomes the environment only once every step
passed, so a failure leaves nothing half-built behind. Environments not used for a
while are deleted, oldest first, when they take more than the cache limit.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import platform
import re
import secrets
import shutil
import sys
import tarfile
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable

UV_VERSION = "0.13.7"
UV_RELEASES = os.environ.get("HARNESS_UV_RELEASES", "https://github.com/astral-sh/uv/releases/download")
PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)
WINDOWS = os.name == "nt"

PREFLIGHT = r"""
import importlib, json, platform, sys
out = {"python": platform.python_version(), "imports": {}, "accelerators": []}
import yourharness.engine.worker  # the job engine's worker runs here: standard library only
for name in json.loads(sys.argv[1]):
    try:
        importlib.import_module(name)
        out["imports"][name] = "ok"
    except Exception as e:
        out["imports"][name] = f"{type(e).__name__}: {e}"[:300]
try:
    import torch
    if torch.cuda.is_available():
        out["accelerators"].append("cuda:" + torch.cuda.get_device_name(0))
    mps = getattr(getattr(torch, "backends", None), "mps", None)
    if mps is not None and mps.is_available():
        out["accelerators"].append("mps")
except Exception:
    pass
print(json.dumps(out))
"""


def fingerprint(python: str, requirements: list[str]) -> str:
    """The same Python version and requirements always give the same environment id."""
    doc = json.dumps({"python": python, "requirements": sorted(r.strip() for r in requirements)}, sort_keys=True)
    return "env-" + hashlib.sha256(doc.encode("utf-8")).hexdigest()[:20]


def import_name(requirement: str) -> str | None:
    """The module a requirement probably provides ("scikit-learn>=1.5" -> "sklearn" is beyond this; it gives "scikit_learn")."""
    m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    if not m:
        return None
    name = m.group(1).lower().replace("-", "_").replace(".", "_")
    return {"pillow": "PIL", "scikit_learn": "sklearn", "pyyaml": "yaml", "opencv_python": "cv2", "beautifulsoup4": "bs4"}.get(name, name)


def uv_target() -> str | None:
    machine = platform.machine().lower()
    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64", "amd64": "x86_64"}.get(machine)
    if arch is None:
        return None
    if sys.platform == "darwin":
        return f"uv-{arch}-apple-darwin.tar.gz"
    if sys.platform.startswith("linux"):
        return f"uv-{arch}-unknown-linux-gnu.tar.gz"
    if WINDOWS:
        return f"uv-{arch}-pc-windows-msvc.zip"
    return None


class EnvError(RuntimeError):
    pass


class EnvManager:
    def __init__(self, home: Path, send: Callable[[dict], None]):
        self.home = Path(home)
        self.root = self.home / "envs"
        self.bin = self.home / "bin"
        self.send = send
        self.building: dict[str, asyncio.Future] = {}
        self.failed: dict[str, str] = {}  # the last error per environment, reported again after a reconnect
        self.in_use: dict[str, int] = {}
        self.cap = float(os.environ.get("HARNESS_ENV_CACHE_GB", 10)) * 2**30

    def describe(self) -> dict:
        """For the coordinator: the environments here, the ones being built, and the last failures."""
        return {"envs": self.ready_list(), "building": sorted(self.building), "envErrors": dict(list(self.failed.items())[-20:])}

    # ------------------------------------------------------------------ what's here
    def uv(self) -> str | None:
        exe = "uv.exe" if WINDOWS else "uv"
        candidates = [os.environ.get("YH_UV"), self.bin / exe, Path(sys.executable).parent / exe,
                      Path(sys.prefix) / ("Scripts" if WINDOWS else "bin") / exe, shutil.which("uv")]
        for c in candidates:
            if c and Path(c).is_file() and os.access(c, os.X_OK):
                return str(c)
        return None

    def available(self) -> bool:
        """Whether this computer can run jobs in environments: it needs uv (or a uv release to download) and
        the job engine's worker as source files, which a packaged desktop app doesn't have."""
        if not (Path(PACKAGE_PARENT) / "yourharness" / "engine" / "worker.py").is_file():
            return False
        return self.uv() is not None or uv_target() is not None

    def env_dir(self, fp: str) -> Path:
        if not re.fullmatch(r"env-[0-9a-f]{20}", fp or ""):
            raise EnvError("not an environment id")
        return self.root / fp

    def has(self, fp) -> bool:
        try:
            return (self.env_dir(fp) / "ready.json").is_file()
        except EnvError:
            return False

    def ready_list(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(p.name for p in self.root.iterdir() if (p / "ready.json").is_file())

    def python(self, fp: str) -> str:
        d = self.env_dir(fp)
        self._touch(fp)
        return str(d / ("Scripts/python.exe" if WINDOWS else "bin/python"))

    def environ(self, env: dict, fp: str | None = None) -> dict:
        """The environment variables a worker runs with inside an environment."""
        env = dict(env)
        if fp:
            d = self.env_dir(fp)
            env["VIRTUAL_ENV"] = str(d)
            env["PATH"] = str(d / ("Scripts" if WINDOWS else "bin")) + os.pathsep + env.get("PATH", "")
        env.pop("PYTHONHOME", None)
        return env

    def _touch(self, fp: str) -> None:
        try:
            os.utime(self.env_dir(fp) / "ready.json")
        except OSError:
            pass

    # ------------------------------------------------------------------ building
    def prepare(self, m: dict) -> None:
        fp = str(m.get("env") or "")
        if fp in self.building:
            return
        if self.has(fp):
            self.send({"type": "env.ready", "env": fp, "info": self._info(fp)})
            return
        self.failed.pop(fp, None)
        self.building[fp] = asyncio.ensure_future(self._build(fp, m))

    def _progress(self, fp: str, phase: str, note: str) -> None:
        self.send({"type": "env.progress", "env": fp, "phase": phase, "note": note})

    async def _run(self, argv: list[str], env: dict, timeout: float) -> str:
        proc = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, env=env)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise EnvError(f"{Path(argv[0]).name} {argv[1]} took more than {int(timeout)} s") from None
        text = out.decode("utf-8", "replace")
        if proc.returncode != 0:
            raise EnvError(text.strip()[-1500:] or f"{Path(argv[0]).name} exited with code {proc.returncode}")
        return text

    async def _ensure_uv(self, allow_download: bool) -> str:
        found = self.uv()
        if found:
            return found
        target = uv_target()
        if not allow_download or target is None:
            raise EnvError("uv isn't installed on this computer" + ("" if target else ", and there's no uv release for it"))
        url = f"{UV_RELEASES}/{UV_VERSION}/{target}"
        loop = asyncio.get_running_loop()

        def fetch(u: str) -> bytes:
            with urllib.request.urlopen(u, timeout=300) as res:
                return res.read()

        data = await loop.run_in_executor(None, fetch, url)
        published = (await loop.run_in_executor(None, fetch, url + ".sha256")).decode("ascii", "replace").split()[0].lower()
        digest = hashlib.sha256(data).hexdigest()
        if digest != published:
            raise EnvError(f"the uv download doesn't match its published checksum ({digest[:12]}… vs {published[:12]}…)")
        exe = "uv.exe" if WINDOWS else "uv"
        self.bin.mkdir(parents=True, exist_ok=True)
        if target.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                member = next(n for n in z.namelist() if n.endswith("/" + exe) or n == exe)
                blob = z.read(member)
        else:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as t:
                member = next(x for x in t.getmembers() if x.isfile() and x.name.rsplit("/", 1)[-1] == exe)
                blob = t.extractfile(member).read()
        tmp = self.bin / f".{exe}.{secrets.token_hex(3)}"
        tmp.write_bytes(blob)
        tmp.chmod(0o755)
        os.replace(tmp, self.bin / exe)
        return str(self.bin / exe)

    async def _build(self, fp: str, m: dict) -> None:
        python = str(m.get("python") or "")
        reqs = [str(r) for r in m.get("requirements") or []]
        downloads = bool(m.get("downloads"))
        tmp = self.root / f".{fp}.{secrets.token_hex(3)}"
        try:
            if fingerprint(python, reqs) != fp:
                raise EnvError("the environment id doesn't match its Python version and requirements")
            if not re.fullmatch(r"3\.\d{1,2}", python) or any(not r[:1].isalnum() or "\n" in r or "\r" in r for r in reqs):
                raise EnvError("a requirement isn't a package requirement (pip options aren't allowed)")
            self._progress(fp, "uv", "Finding uv" + (" (it will be downloaded if missing)" if downloads else ""))
            uv = await self._ensure_uv(downloads)
            env = {**os.environ, "UV_CACHE_DIR": str(self.home / "uv-cache"), "UV_PYTHON_DOWNLOADS": "automatic" if downloads else "never",
                   "VIRTUAL_ENV": "", "UV_NO_PROGRESS": "1"}
            if not downloads:
                env["UV_OFFLINE"] = "1"
            self.root.mkdir(parents=True, exist_ok=True)
            self._progress(fp, "python", f"Making a Python {python} environment")
            await self._run([uv, "venv", "--quiet", "--python", python, str(tmp)], env, 900)
            py = str(tmp / ("Scripts/python.exe" if WINDOWS else "bin/python"))
            if reqs:
                (tmp / "requirements.in").write_text("\n".join(reqs) + "\n", encoding="utf-8")
                self._progress(fp, "packages", f"Installing {len(reqs)} package{'s' if len(reqs) != 1 else ''}")
                await self._run([uv, "pip", "install", "--quiet", "--python", py, "-r", str(tmp / "requirements.in")], env, 3600)
            self._progress(fp, "preflight", "Checking the environment")
            names = [n for n in (import_name(r) for r in reqs) if n]
            check = await self._run([py, "-c", PREFLIGHT, json.dumps(names)], {**self.environ(os.environ), "PYTHONPATH": PACKAGE_PARENT}, 300)
            report = json.loads(check.strip().splitlines()[-1])
            frozen = await self._run([uv, "pip", "freeze", "--python", py], env, 300) if reqs else ""
            info = {"env": fp, "python": report["python"], "imports": report["imports"], "accelerators": report["accelerators"],
                    "packages": [line for line in frozen.splitlines() if line and not line.startswith("#")][:500],
                    "preparedAt": time.time(), "size": _du(tmp)}
            (tmp / "ready.json").write_text(json.dumps(info), encoding="utf-8")
            dest = self.env_dir(fp)
            if dest.exists():
                shutil.rmtree(dest, ignore_errors=True)
            os.replace(tmp, dest)
            self.evict(keep={fp})
            self.send({"type": "env.ready", "env": fp, "info": self._info(fp)})
        except Exception as e:  # report it; never leave a half-built environment
            shutil.rmtree(tmp, ignore_errors=True)
            error = str(e)[-1500:] or type(e).__name__
            self.failed[fp] = error[-600:]
            self.send({"type": "env.failed", "env": fp, "error": error})
        finally:
            self.building.pop(fp, None)

    def _info(self, fp: str) -> dict:
        try:
            return json.loads((self.env_dir(fp) / "ready.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"env": fp}

    # ------------------------------------------------------------------ the cache
    def evict(self, keep: set[str] = frozenset()) -> list[str]:
        """Delete the least recently used environments while they take more than the cache limit."""
        envs = []
        for fp in self.ready_list():
            info = self._info(fp)
            try:
                used = (self.env_dir(fp) / "ready.json").stat().st_mtime
            except OSError:
                continue
            envs.append((used, fp, int(info.get("size") or 0)))
        total = sum(s for _, _, s in envs)
        gone = []
        for _used, fp, size in sorted(envs):
            if total <= self.cap:
                break
            if fp in keep or self.in_use.get(fp):
                continue
            shutil.rmtree(self.env_dir(fp), ignore_errors=True)
            total -= size
            gone.append(fp)
        return gone


def _du(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total
