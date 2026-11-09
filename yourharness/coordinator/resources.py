"""Processor and memory reservations on each computer, shared by every kind of work.

Three kinds of work can land on one computer: pieces of the original compute jobs,
partitions of batch jobs (the job engine), and agent tasks. Each reserves what it
uses here, so together they don't ask more of a computer than it has:

* processor slots: at most the computer's cores in total, and batch partitions at
  most the threads its owner shares;
* memory: work that needs N GB goes only where at least N GB is free, after what
  running work has reserved but not yet shown up in the free-memory reading.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# Memory a freshly started task reserves until the computer's next memory reading
# (sent every 30 s) can include it.
FRESH_S = 45.0


@dataclass
class Reservation:
    key: str
    kind: str  # "engine" or "agent-task"
    cpu: float
    mem_gb: float
    at: float = field(default_factory=time.monotonic)


class Resources:
    def __init__(self, sched):
        self.s = sched
        self.by_device: dict[str, dict[str, Reservation]] = {}

    # ------------------------------------------------------------------ bookkeeping
    def reserve(self, device_id: str, key: str, kind: str, cpu: float = 1.0, mem_gb: float = 0.0) -> None:
        self.by_device.setdefault(device_id, {})[key] = Reservation(key, kind, float(cpu), float(mem_gb))
        self._sync(device_id)

    def release(self, key: str) -> None:
        for device_id, held in list(self.by_device.items()):
            if held.pop(key, None) is not None:
                if not held:
                    del self.by_device[device_id]
                self._sync(device_id)

    def held(self, device_id: str, kind: str | None = None) -> list[Reservation]:
        return [r for r in self.by_device.get(device_id, {}).values() if kind is None or r.kind == kind]

    def cpu_reserved(self, device_id: str, kind: str | None = None) -> float:
        return sum(r.cpu for r in self.held(device_id, kind))

    def _sync(self, device_id: str) -> None:
        d = self.s.devices.get(device_id)
        if d is not None:
            d.reserved_cpu = self.cpu_reserved(device_id)

    # ------------------------------------------------------------------ decisions
    def why_not(self, d, kind: str, cpu: float = 1.0, mem_gb: float = 0.0) -> str | None:
        """None if the work fits on computer d now, otherwise why it doesn't."""
        cores = max(1, int(d.info.get("cores") or 1))
        used = len(d.inflight) + self.cpu_reserved(d.id)
        if used + cpu > cores:
            return f"its {cores} processor cores are busy with other work from the pool"
        if kind == "engine" and self.cpu_reserved(d.id, "engine") + cpu > max(1, d.threads):
            return f"it shares {d.threads} thread{'s' if d.threads != 1 else ''}, and they're all in use"
        free = d.info.get("memoryFreeGB")
        if mem_gb and free is not None:
            now = time.monotonic()
            pending = sum(r.mem_gb for r in self.held(d.id) if now - r.at < FRESH_S)
            if free - pending < mem_gb:
                return f"it has {max(0.0, free - pending):.1f} GB of memory free and this needs {mem_gb:g} GB"
        return None

    def fits(self, d, kind: str, cpu: float = 1.0, mem_gb: float = 0.0) -> bool:
        return self.why_not(d, kind, cpu, mem_gb) is None

    def view(self, device_id: str) -> dict:
        held = self.held(device_id)
        return {"cpu": round(sum(r.cpu for r in held), 2), "memGB": round(sum(r.mem_gb for r in held), 2),
                "engine": sum(1 for r in held if r.kind == "engine"), "agentTasks": sum(1 for r in held if r.kind == "agent-task")}
