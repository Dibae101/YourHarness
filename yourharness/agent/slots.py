"""A pool of worker processes, one per shared thread.

Processes rather than threads, so pieces run in parallel despite the GIL, and a
piece that is cancelled or stuck can be stopped by ending its process.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import deque
from pathlib import Path
from typing import Callable

log = logging.getLogger("yourharness")
PACKAGE_PARENT = str(Path(__file__).resolve().parent.parent.parent)


class Slot:
    def __init__(self):
        self.proc: asyncio.subprocess.Process | None = None
        self.task: dict | None = None
        self.ready = False
        self.retire = False
        self.stopping = False

    async def start(self, cwd: Path) -> None:
        env = dict(os.environ)
        env["PYTHONPATH"] = PACKAGE_PARENT + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        env.setdefault("PYTHONUNBUFFERED", "1")
        cmd = [sys.executable, "__slot__"] if getattr(sys, "frozen", False) else [sys.executable, "-m", "yourharness.agent.slot"]
        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, cwd=str(cwd), env=env, limit=256 * 1024 * 1024,
        )
        self.stopping = False
        self.ready = True

    async def run(self, task: dict, ctx: dict) -> dict:
        assert self.proc and self.proc.stdin and self.proc.stdout
        self.proc.stdin.write((json.dumps({"task": task, "ctx": ctx}) + "\n").encode("utf-8"))
        await self.proc.stdin.drain()
        raw = await self.proc.stdout.readline()
        if not raw:
            raise ConnectionError("worker process ended")
        return json.loads(raw)

    def kill(self) -> None:
        self.ready = False
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.kill()
            except ProcessLookupError:
                pass


class SlotPool:
    def __init__(self, ctx: dict, on_done: Callable[[dict, dict], None], work_dir: Path):
        self.ctx = ctx
        self.on_done = on_done
        self.work_dir = work_dir
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.queue: deque[dict] = deque()
        self.slots: list[Slot] = []
        self.dropped: set[str] = set()

    @property
    def size(self) -> int:
        return sum(1 for s in self.slots if not s.retire)

    async def resize(self, n: int) -> None:
        active = [s for s in self.slots if not s.retire]
        while len(active) < n:
            s = Slot()
            self.slots.append(s)
            active.append(s)
            await s.start(self.work_dir)
        for s in active[n:]:
            if s.task is None:
                s.kill()
                self.slots.remove(s)
            else:
                s.retire = True  # finish the current piece first
        self._pump()

    def submit(self, task: dict) -> None:
        self.queue.append(task)
        self._pump()

    def take_queued(self) -> list[dict]:
        """Remove and return pieces that haven't started, so they can be handed back."""
        tasks = list(self.queue)
        self.queue.clear()
        return tasks

    def abort(self, job_id: str) -> None:
        self.dropped.add(job_id)
        self.queue = deque(t for t in self.queue if t["jobId"] != job_id)
        for s in self.slots:
            if s.task and s.task["jobId"] == job_id:
                s.stopping = True
                s.kill()

    def _pump(self) -> None:
        for s in self.slots:
            if not self.queue:
                break
            if s.task is None and s.ready and not s.retire:
                s.task = self.queue.popleft()
                asyncio.ensure_future(self._run(s, s.task))

    async def _run(self, s: Slot, task: dict) -> None:
        try:
            res: dict | None = await s.run(task, self.ctx)
        except (ConnectionError, BrokenPipeError, ValueError, OSError) as e:
            res = None if s.stopping else {"ok": False, "error": f"The worker process stopped unexpectedly ({e}).", "ms": 0}
            s.kill()
            if not s.retire:
                try:
                    await s.start(self.work_dir)
                except OSError:
                    log.exception("could not restart a worker process")
                    s.retire = True
        s.task = None
        if s.retire:
            s.kill()
            self.slots.remove(s)
        if res is not None and task["jobId"] not in self.dropped:
            try:
                self.on_done(task, res)
            except Exception:
                log.exception("reporting a result failed")
        self._pump()

    async def close(self) -> None:
        self.queue.clear()
        for s in self.slots:
            s.kill()
        for s in self.slots:
            if s.proc:
                try:
                    await asyncio.wait_for(s.proc.wait(), 2)
                except asyncio.TimeoutError:
                    pass
        self.slots.clear()
