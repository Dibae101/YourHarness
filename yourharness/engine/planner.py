"""Who may run a job, where it should run, and how big its partitions should be.

eligible() answers, for one computer, whether it may take the job's partitions at
all: connected, sharing, able to run the workload and read the format, trusted for
the data (a friend's computer only gets a dataset you marked shareable, for a job
that allows friends, and never your own code), and holding the runtime the job needs.

estimate() predicts how long the job takes kept on one computer against spread over
every eligible one, counting the time to send the input over the network, and picks
where to run in "automatic" mode. Throughput comes from earlier runs on each
computer when there are any (the perf table), otherwise from conservative defaults.
"""

from __future__ import annotations

import math
import os

from .partition import target_bytes

# Bytes per second one worker slot gets through, before any history exists.
DEFAULT_RATE = {"batch.aggregate": 25e6, "batch.groupby": 15e6, "batch.transform": 12e6, "python.map": 4e6}
DEFAULT_ITEMS_PER_S = 20.0  # python.map over items: calls per second per slot
NETWORK_BPS = float(os.environ.get("HARNESS_NETWORK_BPS", 25e6))  # what the coordinator can send to other computers
PART_OVERHEAD_S = 0.4  # starting a worker process and fetching its input
MIN_DISTRIBUTED_BYTES = 16 * 2**20


def human_time(s: float) -> str:
    if s < 90:
        return f"{max(1, round(s))} s"
    if s < 5400:
        return f"{round(s / 60)} min"
    return f"{s / 3600:.1f} h"


class Planner:
    def __init__(self, mgr):
        self.m = mgr

    # ------------------------------------------------------------------ who may run it
    def eligible(self, job: dict, ds: dict | None, d) -> str | None:
        """None if computer d may run this job's partitions, otherwise why not."""
        spec = job["spec"]
        wl, runtime = spec["workload"], spec["runtime"]
        info = getattr(d, "engine_info", None)
        if d.closed:
            return "isn't connected"
        if d.stale:
            return "isn't responding"
        if not info:
            return "runs a YourHarness without the job engine" if d.kind == "agent" else "is a browser tab, which doesn't run batch jobs yet"
        if not d.sharing or not info.get("slots"):
            return "isn't sharing its processor"
        if wl["type"] not in info.get("ops", []):
            return f"can't run {wl['type']}"
        if ds is not None and ds["format"] not in info.get("formats", []):
            return f"can't read {ds['format']} files"
        if not d.owned:
            if spec["security"]["allowedTrust"] == "owned-only":
                return "is a friend's computer, and this job runs on your own computers only"
            if ds is not None and ds.get("classification") != "shareable":
                return "is a friend's computer, and the dataset isn't marked shareable"
            if runtime["kind"] != "builtin":
                return "is a friend's computer, and your own code runs only on your own computers"
        if runtime["kind"] not in info.get("runtimes", []):
            return f"doesn't have the {runtime['kind']} runtime"
        if runtime["kind"] == "managed-python":
            why = self.m.runtime_why_not(job, d)
            if why:
                return why
        pin = (job.get("plan") or {}).get("pin")
        if pin and d.id != pin:
            return "isn't the computer this job is kept on"
        return None

    # ------------------------------------------------------------------ how fast
    def rate(self, device_id: str, wl_type: str, items: bool) -> tuple[float, bool]:
        """(units per second per slot, measured?) for a computer and workload."""
        rows = self.m.db.all("SELECT units, seconds FROM perf WHERE device_id = ? AND workload = ? ORDER BY ts DESC LIMIT 30",
                             (device_id, wl_type + (":items" if items else "")))
        secs = sum(r["seconds"] for r in rows)
        if rows and secs > 0.5:
            return sum(r["units"] for r in rows) / secs, True
        return (DEFAULT_ITEMS_PER_S if items else DEFAULT_RATE.get(wl_type, 5e6)), False

    def estimate(self, job: dict, ds: dict | None, devices: list) -> dict:
        spec = job["spec"]
        wl = spec["workload"]
        items = "items" in spec["inputs"]
        units = len(spec["inputs"]["items"]) if items else max(0, ds["size"] - ds["dataStart"])
        rows, ok = [], []
        for d in devices:
            why = self.eligible({**job, "plan": {}}, ds, d)
            slots = max(1, min(int((getattr(d, "engine_info", None) or {}).get("slots") or 1), max(1, d.threads)))
            per_slot, measured = self.rate(d.id, wl["type"], items)
            row = {"id": d.id, "name": d.name, "owned": d.owned, "eligible": why is None, "why": why, "slots": slots,
                   "rate": round(per_slot * slots, 1), "measured": measured, "local": bool(d.coordinator)}
            rows.append(row)
            if why is None:
                ok.append((d, row))
        plan: dict = {"units": units, "unit": "items" if items else "bytes", "devices": rows, "pin": None}
        if not ok:
            from .runtime import SETUP_NEEDED

            need_setup = [d for d, r in zip(devices, rows) if r["why"] == SETUP_NEEDED]
            if need_setup:
                plan.update(decision="NEEDS_SETUP_APPROVAL", targetBytes=None, itemsPerPartition=None,
                            setup=self.m.runtimes.setup_plan(job, need_setup),
                            reasons=[f"{', '.join(d.name for d in need_setup)} can run it once its Python environment is prepared. "
                                     "Approve the setup to start."])
                plan["itemsPerPartition"] = self._items_per(spec, units, sum(r["slots"] for r in rows if r["why"] == SETUP_NEEDED)) if items else None
                plan["targetBytes"] = None if items else self._target(spec, ds, max(1, len(need_setup) * 2), DEFAULT_RATE.get(wl["type"], 5e6))
                return plan
            plan.update(decision="WAIT_FOR_ELIGIBLE_WORKER", targetBytes=None,
                        reasons=["No computer can run this job right now."] + [f"{r['name']} {r['why']}." for r in rows])
            return plan
        # the computer to keep it on: the coordinator's own, else the fastest of your own
        own = [x for x in ok if x[0].owned]
        local_d, local_row = max(own or ok, key=lambda x: (x[1]["local"], x[1]["rate"]))
        net = 0 if items else 1  # items arrive with the task; byte ranges cross the network
        t_local = PART_OVERHEAD_S + units / max(local_row["rate"], 1e-9)
        here = sum(r["rate"] for d, r in ok if r["local"])
        away = sum(r["rate"] for d, r in ok if not r["local"])
        away_eff = min(away, NETWORK_BPS) if net else away
        total_rate = max(here + away_eff, 1e-9)
        slots = sum(r["slots"] for _, r in ok)
        t_dist = 1.0 + units / total_rate + PART_OVERHEAD_S * math.ceil(max(1, units / max(1, self._target(spec, ds, slots, total_rate / slots))) / max(1, slots))
        mode = spec["mode"]
        reasons = []
        if mode == "local":
            decision = "RUN_LOCAL"
            reasons.append(f"Kept on {local_row['name']} as asked.")
        elif mode == "distributed":
            decision = "RUN_DISTRIBUTED"
            reasons.append(f"Spread over {len(ok)} computer{'s' if len(ok) != 1 else ''} as asked.")
        elif len(ok) == 1:
            decision = "RUN_LOCAL"
            reasons.append(f"Only {local_row['name']} can run it.")
        elif not items and units < MIN_DISTRIBUTED_BYTES:
            decision = "RUN_LOCAL"
            reasons.append(f"The input is small ({units / 2**20:.1f} MB): sending it to other computers would take longer than the work.")
        elif t_dist < 0.8 * t_local:
            decision = "RUN_DISTRIBUTED"
            reasons.append(f"About {human_time(t_dist)} spread over {len(ok)} computers against {human_time(t_local)} on {local_row['name']}.")
        else:
            decision = "RUN_LOCAL"
            reasons.append(f"About {human_time(t_local)} on {local_row['name']}; spreading it would take about {human_time(t_dist)}, mostly sending data.")
        if decision == "RUN_LOCAL":
            plan["pin"] = local_d.id
            run_slots, run_rate = local_row["slots"], local_row["rate"] / local_row["slots"]
        else:
            run_slots, run_rate = slots, total_rate / slots
        if not all(r["measured"] for _, r in ok):
            reasons.append("Speeds are estimates until each computer has run a job like this once.")
        plan.update(decision=decision, reasons=reasons, localSeconds=round(t_local, 1), distributedSeconds=round(t_dist, 1),
                    localDevice=local_row["name"], targetBytes=None if items else self._target(spec, ds, run_slots, run_rate),
                    itemsPerPartition=self._items_per(spec, units, run_slots) if items else None)
        return plan

    @staticmethod
    def _target(spec: dict, ds: dict | None, slots: int, per_slot_rate: float) -> int:
        p = spec["partitioning"]
        if p["targetBytes"]:
            return p["targetBytes"]
        size = max(1, (ds["size"] - ds["dataStart"]) if ds else 1)
        t = target_bytes(size, slots, per_slot_rate, p["targetDurationSeconds"])
        return max(t, math.ceil(size / p["maxPartitions"]))

    @staticmethod
    def _items_per(spec: dict, n: int, slots: int) -> int:
        p = spec["partitioning"]
        if p["itemsPerPartition"]:
            return p["itemsPerPartition"]
        per = max(1, math.ceil(n / max(1, slots * 4)))  # about four partitions per slot, for balance
        return max(per, math.ceil(n / p["maxPartitions"]))
