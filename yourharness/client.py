"""A small client for the coordinator's API. Standard library only.

Inside the workspace, HARNESS_URL and HARNESS_TOKEN are already set. On any
computer that joined as an agent, the sign-in saved in ~/.yourharness/agent.json
is used instead.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

FINISHED = ("done", "failed", "cancelled")


class PoolError(Exception):
    pass


def home() -> Path:
    return Path(os.environ.get("YOURHARNESS_HOME") or Path.home() / ".yourharness")


def saved_signin() -> tuple[str, str]:
    url, token = os.environ.get("HARNESS_URL"), os.environ.get("HARNESS_TOKEN")
    cfg = home() / "agent.json"
    if not (url and token) and cfg.exists():
        data = json.loads(cfg.read_text(encoding="utf-8"))
        url, token = url or data.get("server"), token or data.get("token")
    if not (url and token):
        raise PoolError("This computer isn't signed in to a pool. Run this inside the workspace, "
                        "or join first with: harness agent --join '<join link>'")
    return url.rstrip("/"), token


class Client:
    def __init__(self, server: str | None = None, token: str | None = None):
        if server and token:
            self.server, self.token = server.rstrip("/"), token
        else:
            self.server, self.token = saved_signin()

    def request(self, method: str, path: str, body=None, data: bytes | None = None, headers: dict | None = None, timeout: float = 90):
        hdrs = {"Authorization": f"Bearer {self.token}", **(headers or {})}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            hdrs["Content-Type"] = "application/json"
        req = urllib.request.Request(self.server + path, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                raw = res.read()
                return json.loads(raw) if res.headers.get_content_type() == "application/json" else raw
        except urllib.error.HTTPError as e:
            try:
                message = json.loads(e.read()).get("error")
            except ValueError:
                message = None
            raise PoolError(message or f"The coordinator answered {e.code}.") from None
        except (urllib.error.URLError, OSError) as e:
            raise PoolError(f"Can't reach the coordinator at {self.server} ({e}).") from None

    # --- devices and jobs
    def devices(self) -> list[dict]:
        return self.request("GET", "/api/devices")["devices"]

    def jobs(self) -> list[dict]:
        return self.request("GET", "/api/jobs")["jobs"]

    def job(self, job_id: str, wait: float = 0) -> dict:
        return self.request("GET", f"/api/jobs/{urllib.parse.quote(job_id)}?wait={wait}", timeout=wait + 30)["job"]

    def submit(self, kind: str, params: dict) -> dict:
        return self.request("POST", "/api/jobs", {"kind": kind, "params": params})["job"]

    def cancel(self, job_id: str) -> dict:
        return self.request("POST", f"/api/jobs/{urllib.parse.quote(job_id)}/cancel", {})["job"]

    def wait(self, job: dict, quiet: bool = False) -> dict:
        """Wait for a job to finish, showing progress on stderr unless quiet."""
        show = not quiet and sys.stderr.isatty()
        try:
            while job["status"] not in FINISHED:
                job = self.job(job["id"], wait=2 if show else 25)
                if show:
                    sys.stderr.write("\r\033[K" + progress_line(job))
                    sys.stderr.flush()
        except KeyboardInterrupt:
            self.cancel(job["id"])
            if show:
                sys.stderr.write("\n")
            raise PoolError("Cancelled.") from None
        if show:
            sys.stderr.write("\r\033[K")
        return job

    def run(self, kind: str, params: dict, quiet: bool = False) -> dict:
        job = self.wait(self.submit(kind, params), quiet=quiet)
        if job["status"] != "done":
            raise PoolError(job.get("error") or f"The job was {job['status']}.")
        return job

    # --- shared folder
    def files(self) -> list[dict]:
        return self.request("GET", "/api/files")["files"]

    def upload(self, path: str | Path, name: str | None = None) -> dict:
        path = Path(path)
        target = urllib.parse.quote(name or path.name, safe="")
        hdrs = {"Authorization": f"Bearer {self.token}", "Content-Length": str(path.stat().st_size)}
        with open(path, "rb") as f:
            req = urllib.request.Request(f"{self.server}/api/files/{target}", data=f, headers=hdrs, method="PUT")
            try:
                with urllib.request.urlopen(req, timeout=3600) as res:
                    return json.loads(res.read())
            except urllib.error.HTTPError as e:
                try:
                    message = json.loads(e.read()).get("error")
                except ValueError:
                    message = None
                raise PoolError(message or f"The coordinator answered {e.code}.") from None

    def download(self, name: str, dest: str | Path) -> Path:
        dest = Path(dest)
        if dest.is_dir():
            dest = dest / name
        req = urllib.request.Request(f"{self.server}/api/files/{urllib.parse.quote(name, safe='')}", headers={"Authorization": f"Bearer {self.token}"})
        try:
            with urllib.request.urlopen(req, timeout=3600) as res, open(dest, "wb") as out:
                while chunk := res.read(1024 * 1024):
                    out.write(chunk)
        except urllib.error.HTTPError as e:
            raise PoolError(f"There is no shared file named {name}." if e.code == 404 else f"The coordinator answered {e.code}.") from None
        return dest

    def delete(self, name: str) -> None:
        self.request("DELETE", f"/api/files/{urllib.parse.quote(name, safe='')}")


def progress_line(job: dict) -> str:
    per = sorted(job.get("perDevice", {}).values(), key=lambda p: -p["chunks"])
    who = ", ".join(f"{p['name']} {p['chunks']}" for p in per[:4])
    return f"{job['done']}/{job['total']} pieces" + (f" · {who}" if who else "") + (" · waiting" if job["status"] == "queued" else "")


def breakdown(job: dict) -> str:
    per = sorted(job.get("perDevice", {}).values(), key=lambda p: -p["chunks"])
    width = max((len(p["name"]) for p in per), default=0)
    lines = []
    for p in per:
        share = round(p["chunks"] / job["total"] * 100) if job["total"] else 0
        lines.append(f"  {p['name']:<{width}}  {p['chunks']:>5} pieces  {share:>3}%  {p['ms'] / 1000:8.1f} s compute")
    if job.get("startedAt") and job.get("finishedAt"):
        lines.append(f"  took {(job['finishedAt'] - job['startedAt']) / 1000:.1f} s")
    return "\n".join(lines)

