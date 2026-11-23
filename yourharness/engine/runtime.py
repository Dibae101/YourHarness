"""Prepared runtimes, from the coordinator's side: managed Python environments and container images.

A job with runtime "managed-python" runs in an environment with exactly the Python
version and packages it names; a job with runtime "isolated-oci" runs in a container
image named by its digest, with the job's limits. Before a computer can take such a
job, it has to have that environment, or have that image and have passed a check in
it (no network, read-only, not root, the limits applied). Preparing either installs or
downloads software, so it happens only when you approve it, per job and per computer.
Approving shows exactly what each computer will run and whether it may download. Once
a computer has one, later jobs that ask for the same one start at once.

The "doc" arguments below are anything whose ["spec"]["runtime"] is the job's
runtime: a JobSpec document, or {"spec": <its spec>} as the planner passes it.
"""

from __future__ import annotations

import time

from ..runtime.envs import UV_VERSION, fingerprint
from ..runtime.oci import fingerprint as oci_fingerprint
from . import isolation

SETUP_NEEDED = "needs the job's Python environment prepared first: approve the setup"
OCI_SETUP_NEEDED = "needs the job's container image checked first: approve the setup"
ACTIVE = ("queued", "uv", "python", "packages", "preflight", "image")  # phases of a build that is still going
KINDS = ("managed-python", "isolated-oci")


def needs_setup(why: str | None) -> bool:
    return why in (SETUP_NEEDED, OCI_SETUP_NEEDED)


def what(kind: str) -> str:
    return "container image" if kind == "isolated-oci" else "Python environment"


class RuntimeManager:
    def __init__(self, engine):
        self.e = engine
        self.status: dict[tuple[str, str], dict] = {}  # (device id, environment) -> how preparing it goes

    @staticmethod
    def fp(doc: dict) -> str:
        rt = doc["spec"]["runtime"]
        if rt["kind"] == "isolated-oci":
            return oci_fingerprint(rt["image"], rt["limits"])
        return fingerprint(rt["python"], rt.get("requirements") or [])

    def env_for(self, job_row: dict, d) -> str:
        return self.fp(job_row["spec"])

    def why_not(self, doc: dict, d) -> str | None:
        kind = doc["spec"]["runtime"]["kind"]
        rule = isolation.runtime_rule(kind, isolation.tier(d))
        if rule:
            return rule
        fp = self.fp(doc)
        if fp in (d.engine_info or {}).get("envs", []):
            return None
        st = self.status.get((d.id, fp))
        oci = kind == "isolated-oci"
        if st and st["phase"] == "failed":
            return f"couldn't {'check' if oci else 'prepare'} the job's {what(kind)}: {st['error']}"
        if st:
            return f"is {'checking' if oci else 'preparing'} the job's {what(kind)} ({st['note'] or st['phase']})"
        return OCI_SETUP_NEEDED if oci else SETUP_NEEDED

    def setup_plan(self, doc: dict, devices: list) -> list[dict]:
        """What each computer that needs it would run to prepare the environment: shown before you approve."""
        rt = doc["spec"]["runtime"]
        out = []
        if rt["kind"] == "isolated-oci":
            lim = rt["limits"]
            for d in devices:
                engine = ((d.engine_info or {}).get("oci") or {}).get("engine") or "docker"
                steps = [f"{engine} pull {rt['image']} (only if this computer doesn't have it)",
                         f"a check in that image with the job's limits ({lim['cpus']:g} CPU, {lim['memoryMB']} MB memory, {lim['pids']} processes): "
                         "no network, read-only system files, not root, no container engine socket, no tokens, and the engine's worker starts"]
                out.append({"id": d.id, "name": d.name, "steps": steps,
                            "downloads": "If missing: the image from its registry, by its digest (nothing else)."})
            return out
        reqs = rt.get("requirements") or []
        fp = self.fp(doc)
        for d in devices:
            steps = [f"uv venv --python {rt['python']} <YourHarness folder>/envs/{fp}"]
            if reqs:
                steps.append("uv pip install " + " ".join(reqs))
            steps.append("a check that the packages import and the job's worker starts")
            out.append({"id": d.id, "name": d.name, "steps": steps,
                        "downloads": f"If missing: uv {UV_VERSION} from GitHub (checksum checked), Python {rt['python']}, and the packages from PyPI."})
        return out

    def _why(self, doc: dict) -> dict:
        """Why each computer can't run the job's partitions now (None: it can)."""
        ref = doc["spec"]["inputs"].get("dataset", {}).get("ref")
        ds = self.e.db.dataset(ref.split(":", 1)[1]) if ref else None
        meta = self.e.dataset_meta(ds) if ds else None
        return {d.id: (d, self.e.planner.eligible({"spec": doc["spec"], "plan": {}}, meta, d)) for d in self.e.devices()}

    def candidates(self, doc: dict) -> list:
        """The computers that could run the job once they prepared its environment."""
        return [d for d, why in self._why(doc).values() if needs_setup(why)]

    def approve(self, job_id: str, device_ids: list[str] | None, downloads: bool, by: str) -> dict:
        job = self.e.db.job(job_id)
        if job is None:
            raise ValueError("There's no job with that id.")
        if job["status"] != "needs_approval":
            raise ValueError("This job isn't waiting for a setup approval.")
        doc = job["spec"]
        rt = doc["spec"]["runtime"]
        oci = rt["kind"] == "isolated-oci"
        fp = self.fp(doc)
        why = self._why(doc)
        candidates = {i: d for i, (d, w) in why.items() if needs_setup(w)}
        ready = [d for d, w in why.values() if w is None]
        chosen = [i for i in (device_ids if device_ids is not None else sorted(candidates)) if i in candidates]
        if not chosen and not ready:
            raise ValueError(f"None of those computers can prepare this {what(rt['kind'])} right now.")
        for dev_id in chosen:
            self.status[(dev_id, fp)] = {"phase": "queued", "note": "Asked to " + ("check it" if oci else "prepare it"), "error": None, "at": time.time()}
            if oci:
                msg = {"type": "env.prepare", "kind": "isolated-oci", "env": fp, "image": rt["image"], "limits": rt["limits"], "downloads": bool(downloads)}
            else:
                msg = {"type": "env.prepare", "env": fp, "python": rt["python"], "requirements": rt.get("requirements") or [], "downloads": bool(downloads)}
            candidates[dev_id].send(msg)
        names = ", ".join(candidates[i].name for i in chosen)
        if chosen:
            note = f"Checking its container image on {names}." if oci else f"Preparing its Python environment on {names}."
        else:
            note = "Its container image is ready." if oci else "Its Python environment is ready."
        self.e.db.update("jobs", {"id": job_id}, {"status": "running", "updated_at": time.time(), "note": note})
        self.e.db.event(job_id, "setup-approved", {"by": by, "devices": [candidates[i].name for i in chosen], "downloads": bool(downloads), "env": fp})
        self.e._changed(job_id)
        self.e.dispatch()
        return self.e.job_view(self.e.db.job(job_id))

    # ------------------------------------------------------------------ what the computers report
    def handle(self, d, msg: dict) -> None:
        fp = str(msg.get("env") or "")
        key = (d.id, fp)
        kind = msg.get("type")
        if kind == "env.progress" and key in self.status:
            self.status[key].update(phase=str(msg.get("phase") or "")[:30], note=str(msg.get("note") or "")[:200])
        elif kind == "env.ready":
            self.status.pop(key, None)
            envs = (d.engine_info or {}).setdefault("envs", [])
            if fp not in envs:
                envs.append(fp)
            info = msg.get("info") if isinstance(msg.get("info"), dict) else {}
            bad = {k: v for k, v in (info.get("imports") or {}).items() if v != "ok"}
            if bad:
                self.status[key] = {"phase": "warning", "note": "Some packages didn't import: " + ", ".join(bad)[:200], "error": None, "at": time.time()}
            self._ready_notes(fp, d)
            self.e.s.broadcast_devices()
            self.e.dispatch()
        elif kind == "env.failed":
            self.status[key] = {"phase": "failed", "note": "", "error": str(msg.get("error") or "unknown error")[-600:], "at": time.time()}
        else:
            return
        self._touch_jobs(fp)
        if kind == "env.failed":
            self._fail_if_hopeless(fp)

    def device_connected(self, d) -> None:
        """A computer came back: it says which environments it has, is building, or failed to build."""
        info = d.engine_info or {}
        for (dev_id, fp), st in list(self.status.items()):
            if dev_id != d.id or st["phase"] not in ACTIVE:
                continue
            if fp in info.get("envs", []):
                self.status.pop((dev_id, fp), None)
            elif fp in info.get("envErrors", {}):
                self.status[(dev_id, fp)] = {"phase": "failed", "note": "", "error": info["envErrors"][fp], "at": time.time()}
            elif fp not in info.get("building", []):
                self.status[(dev_id, fp)] = {"phase": "failed", "note": "", "error": f"{d.name} restarted before it finished", "at": time.time()}
            else:
                continue
            self._touch_jobs(fp)
            self._fail_if_hopeless(fp)

    def _ready_notes(self, fp: str, d) -> None:
        """A running job whose note still says its environment is being prepared now says where it's ready."""
        for j in self.e.db.all("SELECT id, spec, note FROM jobs WHERE status = 'running'"):
            kind = j["spec"]["spec"]["runtime"]["kind"]
            if kind in KINDS and self.fp(j["spec"]) == fp and (j["note"] or "").startswith(("Preparing its Python environment", "Checking its container image")):
                self.e.db.update("jobs", {"id": j["id"]}, {"note": f"Its {what(kind)} is ready on {d.name}.", "updated_at": time.time()})

    def _touch_jobs(self, fp: str) -> None:
        for j in self.e.db.all("SELECT id, spec FROM jobs WHERE status IN ('running', 'needs_approval')"):
            if j["spec"]["spec"]["runtime"]["kind"] in KINDS and self.fp(j["spec"]) == fp:
                self.e._changed(j["id"])

    def _fail_if_hopeless(self, fp: str) -> None:
        """A job that was approved for setup fails once no computer can prepare its environment."""
        asked = [k for k in self.status if k[1] == fp]
        pending = [k for k in asked if self.status[k]["phase"] != "failed"]
        ready = [d for d in self.e.s.devices.values() if fp in (d.engine_info or {}).get("envs", [])]
        if not asked or pending or ready:
            return
        errors = "; ".join(f"{self.e.s.devices[k[0]].name if k[0] in self.e.s.devices else k[0]}: {self.status[k]['error']}" for k in asked)
        for j in self.e.db.all("SELECT id, spec FROM jobs WHERE status = 'running'"):
            doc = j["spec"]
            kind = doc["spec"]["runtime"]["kind"]
            if kind in KINDS and self.fp(doc) == fp:
                done = "checked" if kind == "isolated-oci" else "prepared"
                self.e._stop_job(j["id"], "failed", f"The {what(kind)} couldn't be {done}. {errors}"[:2000])

    def view(self, job: dict) -> list[dict]:
        """How preparing the job's environment goes on each computer."""
        if job["spec"]["spec"]["runtime"]["kind"] not in KINDS:
            return []
        fp = self.fp(job["spec"])
        out, seen = [], set()
        for d in self.e.s.devices.values():
            if fp in (d.engine_info or {}).get("envs", []):
                st = self.status.get((d.id, fp))
                out.append({"device": d.name, "phase": "ready", "note": st["note"] if st else "Ready", "error": None})
                seen.add(d.id)
        for (dev_id, efp), st in self.status.items():
            if efp != fp or dev_id in seen:
                continue
            d = self.e.s.devices.get(dev_id)
            note = st["note"]
            if st["phase"] in ACTIVE and (d is None or d.closed):
                note = f"{note} (the computer is offline; it carries on when it is back)"
            out.append({"device": d.name if d else dev_id, "phase": st["phase"], "note": note, "error": st["error"]})
        return out
