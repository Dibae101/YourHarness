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


def setup_text(job: dict) -> str:
    """What a job that waits for a setup approval would prepare, and how to approve it."""
    lines = [f"Job {job['id']} needs its Python environment prepared before it can start:"]
    for s in job.get("setupPlan") or []:
        lines.append(f"  {s['name']}:")
        lines += [f"    {step}" for step in s["steps"]]
    if job.get("setupPlan"):
        lines.append(f"  {job['setupPlan'][0]['downloads']}")
    lines.append(f"Approve it with: harness job approve {job['id']}  (add --downloads to let the computers download what's missing)")
    return "\n".join(lines)


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

    # --- pooled storage: chunks spread over the computers, two copies of each
    def pool_files(self) -> dict:
        return self.request("GET", "/api/pool")

    def pool_put(self, path: str | Path, name: str | None = None) -> dict:
        path = Path(path)
        target = urllib.parse.quote(name or path.name, safe="")
        hdrs = {"Authorization": f"Bearer {self.token}", "Content-Length": str(path.stat().st_size)}
        with open(path, "rb") as f:
            req = urllib.request.Request(f"{self.server}/api/pool/{target}", data=f, headers=hdrs, method="PUT")
            try:
                with urllib.request.urlopen(req, timeout=3600) as res:
                    return json.loads(res.read())
            except urllib.error.HTTPError as e:
                try:
                    message = json.loads(e.read()).get("error")
                except ValueError:
                    message = None
                raise PoolError(message or f"The coordinator answered {e.code}.") from None

    def pool_get(self, name: str, dest: str | Path) -> Path:
        dest = Path(dest)
        if dest.is_dir():
            dest = dest / name
        req = urllib.request.Request(f"{self.server}/api/pool/{urllib.parse.quote(name, safe='')}", headers={"Authorization": f"Bearer {self.token}"})
        try:
            with urllib.request.urlopen(req, timeout=3600) as res, open(dest, "wb") as out:
                while chunk := res.read(1024 * 1024):
                    out.write(chunk)
        except urllib.error.HTTPError as e:
            try:
                message = json.loads(e.read()).get("error")
            except ValueError:
                message = None
            raise PoolError(message or f"The coordinator answered {e.code}.") from None
        return dest

    def pool_rm(self, name: str) -> None:
        self.request("DELETE", f"/api/pool/{urllib.parse.quote(name, safe='')}")

    # --- the job engine: datasets and batch jobs
    def datasets(self) -> list[dict]:
        return self.request("GET", "/api/engine")["datasets"]

    def add_dataset(self, path: str | Path, name: str | None = None, fmt: str | None = None, classification: str | None = None) -> dict:
        path = Path(path)
        q = {"name": name or path.name}
        if fmt:
            q["format"] = fmt
        if classification:
            q["classification"] = classification
        return self._put_file("/api/engine/datasets", q, path)["dataset"]

    def _put_file(self, route: str, query: dict, path: Path) -> dict:
        hdrs = {"Authorization": f"Bearer {self.token}", "Content-Length": str(path.stat().st_size), "Content-Type": "application/octet-stream"}
        with open(path, "rb") as f:
            req = urllib.request.Request(f"{self.server}{route}?{urllib.parse.urlencode(query)}", data=f, headers=hdrs, method="PUT")
            try:
                with urllib.request.urlopen(req, timeout=3600) as res:
                    return json.loads(res.read())
            except urllib.error.HTTPError as e:
                try:
                    message = json.loads(e.read()).get("error")
                except ValueError:
                    message = None
                raise PoolError(message or f"The coordinator answered {e.code}.") from None

    def models(self) -> list[dict]:
        return self.request("GET", "/api/engine")["models"]

    def add_model(self, path: str | Path, name: str | None = None, adapter: str | None = None) -> dict:
        """Upload a model for ml.evaluate: model.py, a .zip with model.py at its top, or a TorchScript .pt."""
        path = Path(path)
        q = {"name": name or path.name}
        if adapter:
            q["adapter"] = adapter
        return self._put_file("/api/engine/models", q, path)["model"]

    def delete_model(self, model_id: str) -> None:
        self.request("DELETE", f"/api/engine/models/{urllib.parse.quote(model_id)}")

    def import_dataset(self, source: str, path: str, **opts) -> dict:
        return self.request("POST", "/api/engine/datasets/import", {"source": source, "path": path, **opts}, timeout=3600)["dataset"]

    def delete_dataset(self, ds_id: str) -> None:
        self.request("DELETE", f"/api/engine/datasets/{urllib.parse.quote(ds_id)}")

    def plan_job(self, job: dict) -> dict:
        return self.request("POST", "/api/engine/plan", {"job": job})["plan"]

    def start_job(self, job: dict) -> dict:
        return self.request("POST", "/api/engine/jobs", {"job": job})["job"]

    def engine_jobs(self) -> list[dict]:
        return self.request("GET", "/api/engine")["jobs"]

    def engine_job(self, job_id: str) -> dict:
        return self.request("GET", f"/api/engine/jobs/{urllib.parse.quote(job_id)}")["job"]

    def cancel_engine_job(self, job_id: str) -> dict:
        return self.request("POST", f"/api/engine/jobs/{urllib.parse.quote(job_id)}/cancel", {})["job"]

    def retry_engine_job(self, job_id: str) -> dict:
        return self.request("POST", f"/api/engine/jobs/{urllib.parse.quote(job_id)}/retry", {})["job"]

    def approve_setup(self, job_id: str, devices: list[str] | None = None, downloads: bool = False) -> dict:
        """Let the computers prepare the job's Python environment (all that need it, or the ones named by id)."""
        body: dict = {"downloads": bool(downloads)}
        if devices is not None:
            body["devices"] = list(devices)
        return self.request("POST", f"/api/engine/jobs/{urllib.parse.quote(job_id)}/approve-setup", body)["job"]

    def wait_engine_job(self, job_id: str, quiet: bool = False, poll: float = 0.5) -> dict:
        """Wait until the job finishes, or stops to wait for a setup approval (status "needs_approval")."""
        import time

        show = not quiet and sys.stderr.isatty()
        try:
            while True:
                job = self.engine_job(job_id)
                if job["status"] in FINISHED or job["status"] == "needs_approval":
                    break
                if show:
                    c = job["counts"]
                    sys.stderr.write(f"\r\033[K{c.get('done', 0)}/{job['total']} partitions · {job['status']}")
                    sys.stderr.flush()
                time.sleep(poll)
        except KeyboardInterrupt:
            self.cancel_engine_job(job_id)
            raise PoolError("Cancelled.") from None
        finally:
            if show:
                sys.stderr.write("\r\033[K")
        return job

    def engine_result(self, job_id: str, dest: str | Path | None = None):
        """The job's result: parsed JSON, or the raw records (JSON Lines) saved to dest."""
        req = urllib.request.Request(f"{self.server}/api/engine/jobs/{urllib.parse.quote(job_id)}/result",
                                     headers={"Authorization": f"Bearer {self.token}"})
        try:
            with urllib.request.urlopen(req, timeout=3600) as res:
                if dest is not None:
                    with open(dest, "wb") as out:
                        while chunk := res.read(1024 * 1024):
                            out.write(chunk)
                    return Path(dest)
                raw = res.read()
                return json.loads(raw) if res.headers.get_content_type() == "application/json" else raw
        except urllib.error.HTTPError as e:
            try:
                message = json.loads(e.read()).get("error")
            except ValueError:
                message = None
            raise PoolError(message or f"The coordinator answered {e.code}.") from None


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

