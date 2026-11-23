"""Containers for your code: the isolated-oci runtime, on this computer's side.

A job with runtime "isolated-oci" names a container image by its digest. Its partitions
run in that image with Docker or Podman, and each container gets:

  no network (--network none): this computer downloads the partition's input first and
      uploads what it wrote afterwards, so the code inside never sees the attempt's token
  read-only system files (--read-only) and a small private /tmp; it isn't root (--user),
      has no capabilities (--cap-drop ALL) and can't gain any (no-new-privileges)
  the job's CPU, memory and process limits (--cpus, --memory, --pids-limit)
  three folders and nothing else: the engine's worker code (read-only), the partition's
      input (read-only), and an output folder

Before the first partition, with your approval, the computer gets the image if it doesn't
have it (only when downloads are allowed) and runs a check in it with exactly those flags:
no network, read-only, not root, the limits applied, no container engine socket inside, no
tokens in the environment, and the engine's worker starts. A computer whose check fails
doesn't run the job: there is no fallback to running it outside a container. The image is
never pulled while a job runs (--pull never).

A container isolates your own code on your own computers. It isn't treated as a sandbox for
a friend's computer (engine/isolation.py), which never gets a job's code.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import shutil
import time
from pathlib import Path
from typing import Callable

PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)
WORKER_PKG = Path(PACKAGE_PARENT) / "yourharness"
ENGINES = ("docker", "podman")
ENV_RE = re.compile(r"oci-[0-9a-f]{20}")
IMAGE_RE = re.compile(r"[a-z0-9][a-z0-9._\-/:]{0,250}@sha256:[0-9a-f]{64}")
TMP_MB = 256

# Run inside the image, with the same flags as a partition, before any partition runs there.
PREFLIGHT = r"""
import json, os, platform, socket
out = {"python": platform.python_version(), "uid": os.getuid()}
def tcp():
    try:
        socket.create_connection(("1.1.1.1", 53), timeout=3).close()
        return "reachable"
    except OSError:
        return "blocked"
def dns():
    try:
        socket.getaddrinfo("example.com", 443)
        return "resolves"
    except OSError:
        return "blocked"
def writable(path):
    try:
        with open(path, "w") as f:
            f.write("x")
        os.unlink(path)
        return "writable"
    except OSError:
        return "read-only"
def first(*paths):
    for p in paths:
        try:
            with open(p) as f:
                return f.read().strip()
        except OSError:
            pass
    return None
out["network"] = tcp()
out["dns"] = dns()
out["rootfs"] = writable("/usr/.yh-check")
out["input"] = writable("/in/.yh-check")
out["output"] = writable("/out/.yh-check")
out["tmp"] = writable("/tmp/.yh-check")
out["memory"] = first("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes")
out["pids"] = first("/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/pids/pids.max")
out["sockets"] = [p for p in ("/var/run/docker.sock", "/run/docker.sock", "/run/podman/podman.sock") if os.path.exists(p)]
out["secrets"] = sorted(k for k in os.environ if k.startswith("HARNESS") or "TOKEN" in k.upper() or "SECRET" in k.upper())
try:
    import yourharness.engine.worker  # noqa: F401
    out["worker"] = "ok"
except Exception as e:
    out["worker"] = type(e).__name__ + ": " + str(e)
print("YH-PREFLIGHT " + json.dumps(out))
"""


class OciError(RuntimeError):
    pass


def fingerprint(image: str, limits: dict) -> str:
    """The id of an image with a job's limits: a check passed in one says nothing about other limits."""
    text = json.dumps({"image": image, "limits": {k: limits[k] for k in sorted(limits)}}, sort_keys=True, separators=(",", ":"))
    return "oci-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:20]


def problems(out: dict, limits: dict, rootless: bool) -> list[str]:
    """What the check inside the image found wrong; empty when the container is as isolated as it should be."""
    bad = []
    if out.get("uid") == 0 and not rootless:
        bad.append("the code would run as root")
    if out.get("network") != "blocked" or out.get("dns") != "blocked":
        bad.append("the container could reach the network")
    if out.get("rootfs") != "read-only":
        bad.append("its system files were writable")
    if out.get("input") != "read-only":
        bad.append("its input folder was writable")
    if out.get("output") != "writable" or out.get("tmp") != "writable":
        bad.append("its output folder or /tmp wasn't writable")
    if out.get("memory") != str(int(limits["memoryMB"]) * 2**20):
        bad.append(f"the memory limit wasn't applied (the container saw {out.get('memory') or 'no limit'})")
    if out.get("pids") != str(int(limits["pids"])):
        bad.append(f"the process limit wasn't applied (the container saw {out.get('pids') or 'no limit'})")
    if out.get("sockets"):
        bad.append("a container engine socket was inside: " + ", ".join(out["sockets"]))
    if out.get("secrets"):
        bad.append("tokens were in its environment: " + ", ".join(out["secrets"]))
    try:
        major, minor = (int(x) for x in str(out.get("python") or "0.0").split(".")[:2])
    except ValueError:
        major, minor = 0, 0
    if (major, minor) < (3, 10):
        bad.append(f"the image's Python is {out.get('python') or 'missing'}; the engine needs 3.10 or newer as python3")
    elif out.get("worker") != "ok":
        bad.append(f"the engine's worker didn't start there ({out.get('worker')})")
    return bad


class OciManager:
    def __init__(self, home: Path, send: Callable[[dict], None]):
        self.home = Path(home)
        self.root = self.home / "oci"
        self.send = send
        self.engine: str | None = None  # the docker or podman program
        self.kind = ""  # "docker" or "podman"
        self.version = ""
        self.rootless = False
        self.why = "not checked yet"
        self.building: dict[str, asyncio.Future] = {}
        self.failed: dict[str, str] = {}

    # ------------------------------------------------------------------ what's here
    async def detect(self) -> None:
        """Find a container engine that answers: Docker or Podman (HARNESS_OCI_ENGINE names one)."""
        self.engine = None
        if not (WORKER_PKG / "engine" / "worker.py").is_file():
            self.why = "this build of YourHarness has no worker source to mount into a container"
            return
        names = [os.environ["HARNESS_OCI_ENGINE"]] if os.environ.get("HARNESS_OCI_ENGINE") else list(ENGINES)
        why = "neither Docker nor Podman is installed"
        for name in names:
            exe = name if os.path.isabs(name) and os.access(name, os.X_OK) else shutil.which(name)
            if not exe:
                continue
            kind = "podman" if "podman" in Path(exe).name else "docker"
            fmt = "{{.Version}}" if kind == "podman" else "{{.Server.Version}}"
            try:
                version = (await self._run([exe, "version", "--format", fmt], 30)).strip()
                if kind == "podman":
                    rootless = (await self._run([exe, "info", "--format", "{{.Host.Security.Rootless}}"], 30)).strip() == "true"
                else:
                    rootless = "rootless" in await self._run([exe, "info", "--format", "{{.SecurityOptions}}"], 30)
            except OciError as e:
                why = f"{kind} is installed but doesn't answer ({str(e)[:200]})"
                continue
            self.engine, self.kind, self.version, self.rootless, self.why = exe, kind, version[:40], rootless, ""
            return
        self.why = why

    def available(self) -> bool:
        return self.engine is not None

    def describe(self) -> dict:
        return {"oci": {"engine": self.kind, "version": self.version, "rootless": self.rootless} if self.engine else None,
                "ociWhy": self.why if not self.engine else ""}

    def _record(self, fp: str) -> Path:
        if not ENV_RE.fullmatch(fp or ""):
            raise OciError("not a container environment id")
        return self.root / f"{fp}.json"

    def info(self, fp: str) -> dict | None:
        try:
            return json.loads(self._record(fp).read_text(encoding="utf-8"))
        except (OSError, ValueError, OciError):
            return None

    def has(self, fp) -> bool:
        return self.engine is not None and self.info(fp) is not None

    def ready_list(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(p.stem for p in self.root.glob("oci-*.json") if ENV_RE.fullmatch(p.stem))

    def summary(self) -> dict:
        """For the coordinator, merged with the Python environments: what's checked, being checked, or failed."""
        return {"envs": self.ready_list() if self.engine else [], "building": sorted(self.building), "envErrors": dict(list(self.failed.items())[-20:])}

    # ------------------------------------------------------------------ running
    def argv(self, name: str, in_dir: Path, out_dir: Path, image: str, limits: dict) -> list[str]:
        """The container engine command for one partition (or the check): see the module's docstring for each flag."""
        if self.engine is None:
            raise OciError(self.why or "no container engine")
        if not IMAGE_RE.fullmatch(image):
            raise OciError("the image isn't pinned by its digest")
        for p in (WORKER_PKG, in_dir, out_dir):
            if "," in str(p) or "=" in str(p):
                raise OciError(f"the folder {p} has a comma or an equals sign, which a container mount can't name")
        uid, gid = (os.getuid(), os.getgid()) if hasattr(os, "getuid") else (65534, 65534)
        if self.rootless and self.kind == "docker":
            uid = gid = 0  # root in a rootless engine is this computer's own unprivileged user
        elif uid == 0:
            uid = gid = 65534
        mem = int(limits["memoryMB"])
        args = [self.engine, "run", "--rm", "-i", "--pull", "never", "--name", name, "--label", "dev.yourharness.partition=1",
                "--network", "none", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--pids-limit", str(int(limits["pids"])), "--memory", f"{mem}m", "--memory-swap", f"{mem}m", "--cpus", f"{float(limits['cpus']):g}",
                "--user", f"{uid}:{gid}", "--tmpfs", f"/tmp:rw,nosuid,nodev,size={TMP_MB}m",
                "--mount", f"type=bind,source={WORKER_PKG},target=/yh/yourharness,readonly",
                "--mount", f"type=bind,source={in_dir},target=/in,readonly",
                "--mount", f"type=bind,source={out_dir},target=/out",
                "--env", "PYTHONPATH=/yh", "--env", "PYTHONUNBUFFERED=1", "--env", "PYTHONDONTWRITEBYTECODE=1", "--env", "HOME=/tmp",
                "--workdir", "/tmp", "--entrypoint", "python3"]
        if self.kind == "podman" and self.rootless:
            args[2:2] = ["--userns", "keep-id"]
        return args + [image]

    def environ(self) -> dict:
        """The container engine program's own environment (DOCKER_HOST and the like); the container gets only the --env above."""
        return {k: v for k, v in os.environ.items() if not k.startswith("HARNESS_TOKEN")}

    def open_dirs(self, *dirs: Path) -> None:
        """Make the folders writable for the container's user when that isn't this computer's own user."""
        if hasattr(os, "getuid") and os.getuid() == 0:
            for d in dirs:
                os.chmod(d, 0o777)

    async def kill(self, name: str) -> None:
        if self.engine is None:
            return
        try:
            await self._run([self.engine, "kill", name], 30)
        except OciError:
            pass  # it already ended

    # ------------------------------------------------------------------ preparing
    def prepare(self, m: dict) -> None:
        fp = str(m.get("env") or "")
        if fp in self.building:
            return
        if self.has(fp):
            self.send({"type": "env.ready", "env": fp, "info": self.info(fp)})
            return
        self.failed.pop(fp, None)
        self.building[fp] = asyncio.ensure_future(self._prepare(fp, m))

    def _progress(self, fp: str, phase: str, note: str) -> None:
        self.send({"type": "env.progress", "env": fp, "phase": phase, "note": note})

    async def _prepare(self, fp: str, m: dict) -> None:
        image = str(m.get("image") or "")
        limits = m.get("limits") if isinstance(m.get("limits"), dict) else {}
        box = self.root / f".check-{secrets.token_hex(4)}"
        try:
            if not IMAGE_RE.fullmatch(image):
                raise OciError("the image isn't pinned by its digest")
            try:
                limits = {"cpus": float(limits["cpus"]), "memoryMB": int(limits["memoryMB"]), "pids": int(limits["pids"])}
            except (KeyError, TypeError, ValueError):
                raise OciError("the job's limits are incomplete") from None
            if fingerprint(image, limits) != fp:
                raise OciError("the environment id doesn't match the image and its limits")
            if self.engine is None:
                await self.detect()
            if self.engine is None:
                raise OciError(self.why)
            self._progress(fp, "image", "Looking for the image")
            pulled = False
            try:
                await self._run([self.engine, "image", "inspect", "--format", "{{.Id}}", image], 60)
            except OciError:
                if not m.get("downloads"):
                    raise OciError("the image isn't on this computer, and downloads weren't allowed") from None
                self._progress(fp, "image", f"Downloading {image.split('@')[0]}")
                await self._run([self.engine, "pull", image], 3600)
                pulled = True
            self._progress(fp, "preflight", "Checking the container: no network, read-only, not root, the limits")
            (box / "in").mkdir(parents=True)
            (box / "out").mkdir()
            self.open_dirs(box / "out")
            argv = self.argv(f"yh-check-{secrets.token_hex(4)}", box / "in", box / "out", image, limits) + ["-c", PREFLIGHT]
            text = await self._run(argv, 300)
            line = next((ln for ln in reversed(text.splitlines()) if ln.startswith("YH-PREFLIGHT ")), None)
            if line is None:
                raise OciError("the check didn't run in the image: " + text.strip()[-500:])
            found = json.loads(line[len("YH-PREFLIGHT "):])
            bad = problems(found, limits, self.rootless)
            if bad:
                raise OciError("the container isn't isolated enough here: " + "; ".join(bad))
            info = {"env": fp, "image": image, "limits": limits, "engine": self.kind, "version": self.version, "rootless": self.rootless,
                    "pulled": pulled, "python": found.get("python"), "checkedAt": time.time()}
            self.root.mkdir(parents=True, exist_ok=True)
            tmp = self._record(fp).with_suffix(".tmp")
            tmp.write_text(json.dumps(info), encoding="utf-8")
            os.replace(tmp, self._record(fp))
            self.send({"type": "env.ready", "env": fp, "info": info})
        except Exception as e:  # report it; the job never falls back to running outside a container
            error = str(e)[-1500:] or type(e).__name__
            self.failed[fp] = error[-600:]
            self.send({"type": "env.failed", "env": fp, "error": error})
        finally:
            shutil.rmtree(box, ignore_errors=True)
            self.building.pop(fp, None)

    async def forget(self) -> list[str]:
        """Remove the checks and the images this computer downloaded for the pool (when it leaves or is removed)."""
        gone = []
        for fp in self.ready_list():
            info = self.info(fp) or {}
            if info.get("pulled") and self.engine and info.get("image"):
                try:
                    await self._run([self.engine, "image", "rm", info["image"]], 120)
                    gone.append(info["image"])
                except OciError:
                    pass  # in use by something else on this computer: leave it
        shutil.rmtree(self.root, ignore_errors=True)
        return gone

    async def _run(self, argv: list[str], timeout: float) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                                                        stdin=asyncio.subprocess.DEVNULL, env=self.environ())
        except OSError as e:
            raise OciError(str(e)) from None
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise OciError(f"{Path(argv[0]).name} {argv[1]} took more than {int(timeout)} s") from None
        text = out.decode("utf-8", "replace")
        if proc.returncode != 0:
            raise OciError(text.strip()[-1500:] or f"{Path(argv[0]).name} exited with code {proc.returncode}")
        return text
