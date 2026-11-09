"""Spot checks for jobs that run your own code: the same partition, run again elsewhere.

For built-in operators the coordinator re-runs a sample of partitions itself (its
reference executor is the job engine's own code). It never runs your code, though:
python.map functions and ml.evaluate models may need packages the coordinator
doesn't have, and could stall or crash it. For those jobs a spot check re-runs each
sampled partition as a "verify" attempt on one of your computers that can run the
job, preferring one that didn't compute the accepted result, and compares the
digest of its state, and of any records it wrote, with what was accepted.

The job stays "verifying", with its result unpublished, until every sampled
partition matched. One mismatch fails the job and nothing is published. A verify
attempt that errors is retried like any attempt; past the job's tries, the job
fails, since its result couldn't be checked. All of this is saved, so a restart of
the coordinator carries on.
"""

from __future__ import annotations

import json
import secrets
import time


def _canon(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Verifier:
    def __init__(self, mgr):
        self.m = mgr

    @property
    def db(self):
        return self.m.db

    # ------------------------------------------------------------------ starting
    def begin(self, job_id: str, pids: list[str], validation: dict) -> None:
        """Every partition of the stage is accepted and combined: check these partitions before using the result."""
        verify = {"sample": pids, "matched": {}, "tries": {}, "sameDevice": []}
        job = self.db.job(job_id)
        current = {k: v for k, v in (job["validation"] or {}).items() if k != "verify"}
        self.db.update("jobs", {"id": job_id}, {"status": "verifying", "validation": {**current, "verify": verify}, "updated_at": time.time(),
                                                 "note": f"Checking {len(pids)} partition{'s' if len(pids) != 1 else ''} by running "
                                                         f"{'them' if len(pids) != 1 else 'it'} again on another computer…"})
        self.db.event(job_id, "verifying", {"partitions": len(pids)})
        self.m._changed(job_id)
        self.m.dispatch()

    def running(self, pid: str) -> dict | None:
        return self.db.one("SELECT * FROM attempts WHERE partition_id = ? AND purpose = 'verify' AND status = 'running'", (pid,))

    # ------------------------------------------------------------------ placing verify attempts
    def dispatch(self, devices: list, free: dict) -> None:
        now = time.time()
        for job in self.db.all("SELECT * FROM jobs WHERE status = 'verifying' ORDER BY created_at"):
            v = job["validation"]["verify"]
            spec = job["spec"]["spec"]
            meta = None
            ref = spec["inputs"].get("dataset", {}).get("ref")
            if ref:
                ds = self.db.dataset(ref.split(":", 1)[1])
                meta = self.m.dataset_meta(ds) if ds else None
            for pid in v["sample"]:
                if pid in v["matched"] or self.running(pid):
                    continue
                p = self.db.partition(pid)
                if p is None:
                    continue
                best, best_score = None, None
                for d in devices:
                    if free.get(d.id, 0) <= 0 or self.m.refused.get((job["id"], d.id), 0) > now:
                        continue
                    if self.m.planner.eligible({"spec": spec, "plan": {}}, meta, d):
                        continue
                    if not self.m.s.resources.fits(d, "engine", cpu=spec["resources"]["cpuSlots"], mem_gb=spec["resources"]["minFreeMemoryGiB"]):
                        continue
                    score = (d.id == p["device_id"], 0 if d.coordinator else 1, (d.load or {}).get("cpu") or 0.0, d.name)
                    if best_score is None or score < best_score:
                        best, best_score = d, score
                if best is None:
                    continue
                self._assign(job, meta, p, best)
                free[best.id] -= 1

    def _assign(self, job: dict, meta: dict | None, p: dict, d) -> None:
        spec = job["spec"]["spec"]
        row = self.db.one("SELECT MAX(n) AS n FROM attempts WHERE partition_id = ?", (p["id"],))
        n = int((row or {}).get("n") or 0) + 1
        secret = secrets.token_urlsafe(24)
        now = time.time()
        with self.db.tx():
            self.db.insert("attempts", {"partition_id": p["id"], "n": n, "device_id": d.id, "device_name": d.name, "purpose": "verify",
                                        "token_hash": self.m.hash_secret(secret), "status": "running", "started_at": now, "outputs": {},
                                        "lease_until": now + spec["retry"]["leaseSeconds"]})
            self.db.event(job["id"], "verify-assigned", {"attempt": n, "device": d.name}, p["id"], d.id)
        self.m._reserve(job, d.id, p["id"], n)
        task = self.m.task_for(job, meta, p, d, resume=False)
        d.send({"type": "eng.start", "job": job["id"], "partition": p["id"], "attempt": n, "token": f"{p['id']}.{n}.{secret}",
                "timeout": spec["retry"]["timeoutSeconds"], "task": task})
        self.m._changed(job["id"])

    # ------------------------------------------------------------------ what the computer reports
    def handle(self, p: dict, a: dict, d, msg: dict) -> None:
        kind = msg.get("type")
        job = self.db.job(p["job_id"])
        live = a["status"] == "running" and job is not None and job["status"] == "verifying"
        if kind == "eng.progress":
            if not live:
                d.send({"type": "eng.cancel", "partition": p["id"], "attempt": a["n"]})
                return
            self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]},
                           {"lease_until": time.time() + job["spec"]["spec"]["retry"]["leaseSeconds"]})
        elif kind == "eng.refuse" and live:
            self.m.refused[(job["id"], d.id)] = time.time() + 60
            self._end(p, a, "failed", str(msg.get("reason") or "refused")[:300])
            self.m.dispatch()
        elif kind == "eng.done" and live:
            self._done(job, p, a, d, msg)

    def _end(self, p: dict, a: dict, status: str, error: str | None = None) -> None:
        self.db.update("attempts", {"partition_id": p["id"], "n": a["n"]}, {"status": status, "ended_at": time.time(), "error": error})
        self.m.s.resources.release(f"eng:{p['id']}:{a['n']}")

    def _done(self, job: dict, p: dict, a: dict, d, msg: dict) -> None:
        if not msg.get("ok"):
            error = str(msg.get("error") or "the check run failed")[:2000]
            self._end(p, a, "failed", error)
            return self._try_failed(job, p, f"{error} (on {d.name})")
        problem = self.m.check_result(job, p, a, msg)
        if problem:
            self._end(p, a, "failed", problem)
            return self._try_failed(job, p, f"{d.name} sent back a result that doesn't check out: {problem}")
        uploaded = self.db.attempt(p["id"], a["n"])["outputs"] or {}
        state = msg["state"] if msg.get("state") is not None else self.m.store.read_json(uploaded[msg["stateOutput"]]["sha"])
        self._end(p, a, "done")
        accepted = p["output"] or {}
        same = _canon(state) == _canon(self.m.state_of(p))
        for name, o in (accepted.get("outputs") or {}).items():
            if name == "state.json":
                continue
            got = (msg.get("outputs") or {}).get(name)
            if not isinstance(got, dict) or got.get("sha") != o.get("sha"):
                same = False
        if not same:
            self.m.stop_job(job["id"], "failed", f"Spot check failed: partition {p['idx'] + 1}, accepted from {p['device_name']}, came out "
                                                 f"differently when {d.name} ran it again. Nothing was published. (Code that isn't "
                                                 "deterministic, such as unseeded random numbers, can't be spot-checked.)")
            return
        v = job["validation"]["verify"]
        v["matched"][p["id"]] = d.name
        if d.id == p["device_id"]:
            v["sameDevice"].append(p["id"])
        self.db.update("jobs", {"id": job["id"]}, {"validation": job["validation"], "updated_at": time.time()})
        self.db.event(job["id"], "verified", {"device": d.name}, p["id"], d.id)
        if all(pid in v["matched"] for pid in v["sample"]):
            self._publish(job)
        else:
            self.m._changed(job["id"])

    def _try_failed(self, job: dict, p: dict, error: str) -> None:
        v = job["validation"]["verify"]
        tries = v["tries"].get(p["id"], 0) + 1
        v["tries"][p["id"]] = tries
        self.db.update("jobs", {"id": job["id"]}, {"validation": job["validation"]})
        self.db.event(job["id"], "verify-failed", {"error": error, "tries": tries}, p["id"])
        if tries >= job["spec"]["spec"]["retry"]["maxAttempts"]:
            self.m.stop_job(job["id"], "failed", f"Partition {p['idx'] + 1} couldn't be checked again: {error}. Nothing was published.")
            return
        self.m._changed(job["id"])
        self.m.dispatch()

    def _publish(self, job: dict) -> None:
        v = job["validation"]["verify"]
        names = sorted(set(v["matched"].values()))
        k = len(v["sample"])
        stage = self.m.current_stage(job["id"])
        total = stage["total"] if stage else sum(self.db.counts(job["id"]).values())
        line = f"{k} of {total} partitions were run again on {', '.join(names)} and matched exactly"
        if v["sameDevice"]:
            line += f" ({len(v['sameDevice'])} on the computer that first ran {'them' if len(v['sameDevice']) != 1 else 'it'}, the only one that could)"
        self.db.event(job["id"], "verified-all", {"partitions": k})
        self.m.verified(job["id"], line + ".")

    # ------------------------------------------------------------------ leases, restarts, cancelling
    def sweep(self, now: float) -> None:
        rows = self.db.all("SELECT a.*, p.job_id AS job_id FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                           "WHERE a.purpose = 'verify' AND a.status = 'running'")
        for a in rows:
            job = self.db.job(a["job_id"])
            p = self.db.partition(a["partition_id"])
            if job is None or job["status"] != "verifying":
                self._cancel(p, a)
                continue
            limit = job["spec"]["spec"]["retry"]["timeoutSeconds"]
            if (a["lease_until"] or 0) < now:
                self._end(p, a, "lost", f"{a['device_name']} stopped reporting progress.")
                self.m._changed(job["id"])
            elif now - (a["started_at"] or now) > limit:
                self._cancel(p, a, "failed")
                self._try_failed(job, p, f"It ran past the {limit} s time limit on {a['device_name']}.")

    def started(self, now: float) -> None:
        """The coordinator restarted: give running verify attempts time for their computers to reconnect."""
        for a in self.db.all("SELECT a.*, p.job_id AS job_id FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                             "WHERE a.purpose = 'verify' AND a.status = 'running'"):
            job = self.db.job(a["job_id"])
            lease = (job["spec"]["spec"]["retry"]["leaseSeconds"] if job else 45) + 30
            self.db.update("attempts", {"partition_id": a["partition_id"], "n": a["n"]}, {"lease_until": now + lease})
            self.m._reserve(job, a["device_id"], a["partition_id"], a["n"])

    def device_connected(self, d, reported: set) -> None:
        for a in self.db.all("SELECT * FROM attempts WHERE purpose = 'verify' AND status = 'running' AND device_id = ?", (d.id,)):
            if (a["partition_id"], a["n"]) not in reported:
                p = self.db.partition(a["partition_id"])
                self._end(p, a, "lost", f"{d.name} came back without it (it may have restarted).")
                self.m._changed(p["job_id"])

    def keeps(self, a: dict | None, d) -> bool:
        """Whether a reported attempt is a verify attempt this computer should carry on with."""
        return a is not None and a.get("purpose") == "verify" and a["status"] == "running" and a["device_id"] == d.id

    def _cancel(self, p: dict, a: dict, status: str = "cancelled") -> None:
        self._end(p, a, status)
        d = self.m.s.devices.get(a["device_id"])
        if d is not None and not d.closed:
            d.send({"type": "eng.cancel", "partition": p["id"], "attempt": a["n"]})

    def cancel_job(self, job_id: str) -> None:
        for a in self.db.all("SELECT a.* FROM attempts a JOIN partitions p ON p.id = a.partition_id "
                             "WHERE p.job_id = ? AND a.purpose = 'verify' AND a.status = 'running'", (job_id,)):
            self._cancel(self.db.partition(a["partition_id"]), a)

    def view(self, job: dict) -> dict | None:
        v = (job["validation"] or {}).get("verify") if job["status"] == "verifying" else None
        if not v:
            return None
        running = []
        for pid in v["sample"]:
            a = self.running(pid)
            if a:
                running.append(a["device_name"])
        return {"sample": len(v["sample"]), "matched": len(v["matched"]), "running": running}
