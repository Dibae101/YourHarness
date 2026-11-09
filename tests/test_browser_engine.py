"""Browser tabs taking batch partitions, end to end through the coordinator.

The tab here is simulated in Python for the connection, but every partition is
computed by the real browser operators (web/engine-ops.js, run with node), from the
bytes the coordinator streams with the attempt's token, and the coordinator's spot
check recomputes some of them with the Python operators and compares.
"""

from __future__ import annotations

import asyncio
import base64
import json
import shutil
import subprocess
import time
import urllib.parse
from pathlib import Path

import aiohttp
import pytest

from conftest import start_coordinator
from test_engine_e2e import AGG, Engine, expected, sales

ROOT = Path(__file__).resolve().parent.parent
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="needs node")

PROFILE = {"ok": True, "why": "", "cores": 8, "memoryGB": 8, "battery": {"charging": False, "level": 0.8}, "recordsPerSecond": 50000,
           "bytesPerSecond": 4e6, "workers": True, "blocked": ""}


def js_partition(task: dict, data: bytes) -> str:
    """The partition's state, as the browser's worker computes and words it."""
    case = {"wl": json.dumps(task["workload"]), "format": task["format"], "header": task.get("header"), "data": base64.b64encode(data).decode(),
            "cuts": [], "start": task["input"]["start"], "bad": task.get("onBadRecord", "fail")}
    out = subprocess.run([NODE, str(ROOT / "tests" / "web" / "engine_ops_run.mjs")], input=json.dumps(case), capture_output=True, text=True,
                         timeout=60, check=True).stdout
    return out.strip()


class Tab:
    """A browser tab that shares its processor: the hello and the eng.* messages engine.js sends."""

    def __init__(self, coord, name: str, mode: str = "compute"):
        self.coord, self.name, self.mode = coord, name, mode
        self.msgs: list[dict] = []
        self.done = 0

    async def __aenter__(self):
        self.http = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True))
        r = await self.http.post(f"{self.coord.base}/api/join", json={"token": self.coord.token, "name": self.name})
        assert r.status == 200
        self.ws = await self.http.ws_connect(self.coord.base.replace("http", "ws") + "/ws", headers={"Origin": self.coord.base})
        engine = {"slots": 2, "ops": ["batch.aggregate", "batch.groupby"], "formats": ["jsonl", "csv", "text"], "runtimes": ["builtin"],
                  "stages": ["map"], "unsupported": ["where.regex"], "python": "", "running": [], "profile": PROFILE}
        await self.ws.send_json({"type": "hello", "name": self.name, "threads": 2, "sharing": True, "runtimes": ["js"],
                                 "info": {"platform": "Test", "cores": 2}, "engine": engine})
        self.reader = asyncio.ensure_future(self._read())
        self.beater = asyncio.ensure_future(self._beat())
        return self

    async def __aexit__(self, *exc):
        self.reader.cancel()
        self.beater.cancel()
        await self.ws.close()
        await self.http.close()

    async def _beat(self):
        while True:
            await asyncio.sleep(0.5)
            await self.ws.send_json({"type": "ping", "t": 0})

    async def _read(self):
        async for m in self.ws:
            if m.type != aiohttp.WSMsgType.TEXT:
                continue
            msg = json.loads(m.data)
            self.msgs.append(msg)
            if msg["type"] == "eng.start":
                asyncio.ensure_future(self._work(msg))

    async def _work(self, m: dict):
        assert "task" not in m and "taskJSON" in m, "a browser gets its task as JSON text"
        task = json.loads(m["taskJSON"])
        head = {"type": "eng.done", "partition": m["partition"], "attempt": m["attempt"]}
        if self.mode == "release":  # what engine.js does when the page is hidden: hand back, then offer nothing
            await self.ws.send_json({"type": "eng.release", "partition": m["partition"], "attempt": m["attempt"], "reason": "the page was hidden"})
            await self.ws.send_json({"type": "update", "engine": {"slots": 0, "ops": ["batch.aggregate", "batch.groupby"], "formats": ["jsonl"],
                                                                  "runtimes": ["builtin"], "stages": ["map"], "running": [],
                                                                  "profile": {**PROFILE, "blocked": "the page is hidden"}}})
            return
        if self.mode == "unsupported":
            await self.ws.send_json({**head, "ok": False, "errorKind": "unsupported", "error": "a number written with digits from another script"})
            return
        url = f"{self.coord.base}/api/engine/attempts/{urllib.parse.quote(m['partition'])}/{m['attempt']}/input"
        async with self.http.get(url, headers={"Authorization": f"Bearer {m['token']}"}) as r:
            assert r.status == 200
            data = await r.read()
        res = json.loads(await asyncio.get_running_loop().run_in_executor(None, js_partition, task, data))
        assert res["ok"], res
        text = js_partition(task, data)
        state = text[text.index('"state":') + 8:text.rindex(',"records"')]  # the state's own text, as the worker sends it
        await self.ws.send_str(json.dumps({**head, "ok": True, "records": res["records"], "skipped": res["skipped"], "bad": [], "seconds": 0.01,
                                           "bytes": len(data), "outputs": {}})[:-1] + f',"state":{state}}}')
        self.done += 1


@pytest.fixture(scope="module")
def pool(tmp_path_factory):
    coord = start_coordinator(tmp_path_factory.mktemp("browser"))
    eng = Engine(coord)
    yield eng
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


async def wait_job(eng, job_id, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        job = await asyncio.get_running_loop().run_in_executor(None, eng.get, job_id)
        if job["status"] in ("done", "failed", "cancelled"):
            return job
        await asyncio.sleep(0.2)
    raise AssertionError(f"job stayed {job['status']}")


def test_a_browser_tab_computes_partitions_the_coordinator_checks(pool, tmp_path):
    data = sales(3000)
    ds = pool.upload("browser.jsonl", data, classification="shareable")
    groupby = {"type": "batch.groupby", "by": ["region"], "metrics": [{"op": "count"}, {"op": "sum", "field": "amount"}, {"op": "max", "field": "amount"},
                                                                      {"op": "min", "field": "qty"}]}

    async def go():
        async with Tab(pool.coord, "Phone") as tab:
            await asyncio.sleep(0.5)
            devs = await asyncio.get_running_loop().run_in_executor(None, pool.c.devices)
            phone = next(d for d in devs if d["name"] == "Phone")
            assert phone["kind"] == "browser" and phone["engine"]["profile"]["recordsPerSecond"] == 50000
            out = []
            for wl in (AGG, groupby):
                job = await asyncio.get_running_loop().run_in_executor(
                    None, lambda wl=wl: pool.start(wl, ds, mode="distributed", partitioning={"targetBytes": 16 * 1024}, output={"spotCheck": 3}))
                out.append(await wait_job(pool, job["id"]))
            return out, tab.done

    (agg, grp), computed = run(go())
    assert agg["status"] == "done" and grp["status"] == "done", (agg, grp)
    assert computed >= agg["total"] + grp["total"]
    assert agg["result"] == expected(tmp_path, data, "jsonl", AGG)["result"]
    assert grp["result"] == expected(tmp_path, data, "jsonl", groupby)["result"]
    for job in (agg, grp):
        assert {d["name"] for d in job["perDevice"]} == {"Phone"}
        assert job["validation"]["level"] == "spot-checked", "the coordinator's own run of 3 partitions matched the browser's"


def test_what_a_browser_tab_is_not_given(pool):
    ds = pool.upload("private.jsonl", sales(200))
    shared = pool.upload("shared.jsonl", sales(200), classification="shareable")

    async def go():
        async with Tab(pool.coord, "Tablet"):
            await asyncio.sleep(0.5)
            loop = asyncio.get_running_loop()
            plans = {}
            for name, wl, d in (("regex", {"type": "batch.aggregate", "where": [{"field": "region", "op": "regex", "value": "^n"}]}, shared),
                                ("code", {"type": "python.map", "code": "def process(r):\n    return r\n"}, ds),
                                ("join", {"type": "batch.join", "on": ["id"]}, shared)):
                inputs = {"dataset": {"ref": d["ref"]}, **({"right": {"ref": shared["ref"]}} if name == "join" else {})}
                doc = pool.job_doc(wl, d, inputs=inputs)
                plans[name] = (await loop.run_in_executor(None, lambda doc=doc: pool.c.request("POST", "/api/engine/plan", {"job": doc})))["plan"]
            return plans

    plans = run(go())
    why = {k: next(d["why"] for d in p["devices"] if d["name"] == "Tablet") for k, p in plans.items()}
    assert "regular expressions" in why["regex"]
    assert "can't run python.map" in why["code"] and "browser tabs run totals" in why["code"]
    assert "can't run batch.join" in why["join"]


def test_a_hidden_tab_hands_its_partitions_back(pool, tmp_path):
    data = sales(2000)
    ds = pool.upload("handback.jsonl", data, classification="shareable")
    pool.coord.agent("Laptop", threads=1, extra=("--no-tasks",))
    pool.ready({"Laptop"})

    async def go(mode):
        async with Tab(pool.coord, f"Hidden-{mode}", mode=mode):
            await asyncio.sleep(0.5)
            loop = asyncio.get_running_loop()
            job = await loop.run_in_executor(None, lambda: pool.start(AGG, ds, mode="distributed", partitioning={"targetBytes": 8 * 1024},
                                                                      retry={"maxAttempts": 1}))
            return await wait_job(pool, job["id"])

    for mode in ("release", "unsupported"):
        job = run(go(mode))
        assert job["status"] == "done", job  # with one try per partition: handing back isn't a failed try
        assert job["result"] == expected(tmp_path, data, "jsonl", AGG)["result"]
        assert {d["name"] for d in job["perDevice"]} == {"Laptop"}
        events = [e["type"] for e in pool.get(job["id"])["events"]]
        assert ("lost" if mode == "release" else "refused") in events


def test_the_agent_says_when_it_works_so_the_desktop_app_can_keep_it_awake(tmp_path, monkeypatch):
    """The desktop app holds off sleep only between these lines (desktop/main.js reads them)."""
    coord = start_coordinator(tmp_path)
    try:
        monkeypatch.setenv("YH_BUSY_LINES", "1")
        desk = coord.agent("Desk", threads=1, extra=("--no-tasks",))
        eng = Engine(coord)
        eng.ready({"Desk"})
        job = eng.run({"type": "python.map", "code": "import time\ndef process(x):\n    time.sleep(3)\n    return x\n"}, items=[1, 2], mode="distributed")
        assert job["status"] == "done", job
        busy: list[str] = []
        end = time.monotonic() + 15
        while time.monotonic() < end:
            busy = [line.strip() for line in desk.lines if line.strip().startswith("::yh-busy::")]
            if len(busy) >= 3 and busy[-1] == "::yh-busy::0":
                break
            time.sleep(0.2)
        assert busy[:3] == ["::yh-busy::0", "::yh-busy::1", "::yh-busy::0"], desk.lines[-30:]
    finally:
        for a in coord.agents:
            a.stop()
        coord.proc.stop()
