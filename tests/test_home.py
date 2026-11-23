"""The Home screen's controls: the pool's owner pauses a device or limits the threads it uses.

What a device offers (its own switch and thread count) and what the owner allows meet on the
coordinator, and everything that hands out work reads the result: compute jobs, the job engine
and agent tasks. A phone or tablet takes one thread unless the owner gives it more.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from types import SimpleNamespace

from conftest import FakeBrowser
from test_browser_engine import Tab
from test_engine_e2e import AGG, Engine, sales

JOB = {"code": "function run(chunk, total) { return chunk; }", "chunks": 40, "combine": "sum", "lang": "js"}


def http(method: str, url: str, token: str, body=None, raw: bytes | None = None) -> tuple[int, dict]:
    data = raw if raw is not None else None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, json.loads(res.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def sign_in(coord, join_token: str, name: str) -> str:
    """A REST sign-in made with a join token (the owner's link or the lending link)."""
    req = urllib.request.Request(f"{coord.base}/api/join", data=json.dumps({"token": join_token, "kind": "agent", "name": name}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as res:
        return json.loads(res.read())["deviceToken"]


def set_limits(coord, token: str, device_id: str, **body) -> dict:
    status, out = http("POST", f"{coord.base}/api/devices/{device_id}/limits", token, body)
    assert status == 200, out
    return out


def device(coord, token: str, device_id: str) -> dict:
    return next(d for d in http("GET", f"{coord.base}/api/devices", token)[1]["devices"] if d["id"] == device_id)


async def until(check, timeout: float = 10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if await asyncio.to_thread(check):
            return
        await asyncio.sleep(0.1)
    raise AssertionError("timed out")


def chunks(fb: FakeBrowser) -> int:
    return sum(1 for m in fb.msgs if m["type"] == "chunk")


def test_a_paused_device_takes_no_new_work_until_it_is_resumed(coord):
    owner = sign_in(coord, coord.token, "Home owner")

    async def main():
        async with FakeBrowser(coord, "Desk tab") as desk, FakeBrowser(coord, "Paused tab") as tab:
            me = (await tab.wait_for(lambda m: m["type"] == "welcome"))["id"]
            out = await asyncio.to_thread(set_limits, coord, owner, me, paused=True)
            assert out["limits"]["paused"] is True
            assert out["device"]["sharing"] is False and out["device"]["limits"]["paused"] is True
            note = await tab.wait_for(lambda m: m["type"] == "limits")
            assert note["paused"] is True and note["by"] == "Home owner" and note["wantSharing"] is True, "the tab still offers to share"

            job = await desk.run("custom", JOB)
            assert job["status"] == "done" and job["result"]["value"] == sum(range(40))
            assert {p["name"] for p in job["perDevice"].values()} == {"Desk tab"}
            assert tab.computed == 0

            out = await asyncio.to_thread(set_limits, coord, owner, me, paused=False)
            assert out["device"]["sharing"] is True
            job = await desk.run("custom", JOB)
            assert "Paused tab" in {p["name"] for p in job["perDevice"].values()}

    asyncio.run(main())


def test_a_thread_limit_holds_a_device_to_that_many_pieces_at_once(coord):
    owner = sign_in(coord, coord.token, "Home owner")

    async def main():
        async with FakeBrowser(coord, "Slow tab", threads=4, stall=True) as tab:
            me = (await tab.wait_for(lambda m: m["type"] == "welcome"))["id"]
            out = await asyncio.to_thread(set_limits, coord, owner, me, threads=2)
            assert out["limits"] == {"threads": 2}
            assert out["device"]["threads"] == 2 and out["device"]["limits"]["cap"] == 2 and out["device"]["limits"]["wantThreads"] == 4
            await tab.ws.send_json({"type": "submit", "job": {"kind": "custom", "params": JOB}})
            job = (await tab.wait_for(lambda m: m["type"] == "job" and m["job"]["createdBy"] == "Slow tab"))["job"]
            try:
                await tab.wait_for(lambda m: m["type"] == "chunk")
                await asyncio.sleep(0.8)
                assert chunks(tab) == 2, "two threads allowed: two pieces at once"
                out = await asyncio.to_thread(set_limits, coord, owner, me, threads=None)  # no limit: all four it offers
                assert out["device"]["threads"] == 4 and out["device"]["limits"]["cap"] is None
                await until(lambda: chunks(tab) == 4)
                await asyncio.sleep(0.5)
                assert chunks(tab) == 4
            finally:
                await asyncio.to_thread(http, "POST", f"{coord.base}/api/jobs/{job['id']}/cancel", owner, {})

    asyncio.run(main())


def test_a_phone_or_tablet_takes_one_thread_unless_the_owner_gives_it_more(coord):
    owner = sign_in(coord, coord.token, "Home owner")

    async def main():
        async with FakeBrowser(coord, "Test phone", threads=4, platform="iPhone") as phone, FakeBrowser(coord, "Laptop tab", threads=4) as tab:
            hi = await phone.wait_for(lambda m: m["type"] == "welcome")
            assert hi["limits"] == {"paused": False, "threads": 1, "wantThreads": 4, "wantSharing": True, "cap": None, "mobile": True}
            other = await tab.wait_for(lambda m: m["type"] == "welcome")
            assert other["limits"]["threads"] == 4 and other["limits"]["mobile"] is False
            assert (await asyncio.to_thread(device, coord, owner, hi["id"]))["threads"] == 1
            for asked, got in ((3, 3), (8, 4), (None, 1)):  # never more than the phone itself offers
                out = await asyncio.to_thread(set_limits, coord, owner, hi["id"], threads=asked)
                assert out["device"]["threads"] == got, (asked, out["device"])
            await asyncio.to_thread(set_limits, coord, owner, hi["id"], threads=3)
            await phone.ws.send_json({"type": "update", "threads": 2})  # the phone's own slider still counts
            await until(lambda: device(coord, owner, hi["id"])["threads"] == 2)
            await phone.ws.send_json({"type": "update", "threads": 4})
            await until(lambda: device(coord, owner, hi["id"])["threads"] == 3)

    asyncio.run(main())


def test_limits_stay_with_the_device_when_it_reconnects(coord):
    owner = sign_in(coord, coord.token, "Home owner")

    async def main():
        async with FakeBrowser(coord, "Comes back", threads=3) as tab:
            me = (await tab.wait_for(lambda m: m["type"] == "welcome"))["id"]
            await asyncio.to_thread(set_limits, coord, owner, me, paused=True, threads=2)
            tab.beater.cancel()
            await tab.ws.close()
            ws = await tab.http.ws_connect(coord.base.replace("http", "ws") + "/ws", headers={"Origin": coord.base})
            try:
                await ws.send_json({"type": "hello", "name": "Comes back", "threads": 3, "sharing": True, "runtimes": ["js"],
                                    "info": {"platform": "Test", "cores": 3}})
                hi = await ws.receive_json(timeout=5)
                while hi["type"] != "welcome":
                    hi = await ws.receive_json(timeout=5)
                assert hi["id"] == me
                assert hi["limits"]["paused"] is True and hi["limits"]["threads"] == 2 and hi["limits"]["cap"] == 2
                view = await asyncio.to_thread(device, coord, owner, me)
                assert view["sharing"] is False and view["threads"] == 2
            finally:
                await ws.close()
        return me

    me = asyncio.run(main())
    stored = json.loads((coord.tmp / "data" / "coordinator.json").read_text())["devices"][me]
    assert stored["limits"] == {"paused": True, "threads": 2}, "kept with the device's sign-in, so it holds after a restart too"


def test_only_the_pools_owner_changes_limits_and_every_change_is_audited(coord):
    owner = sign_in(coord, coord.token, "Home owner")
    lender = sign_in(coord, coord.lend_token, "Lent laptop")

    async def main():
        async with FakeBrowser(coord, "Watched tab") as tab:
            me = (await tab.wait_for(lambda m: m["type"] == "welcome"))["id"]
            url = f"{coord.base}/api/devices/{me}/limits"
            status, _ = await asyncio.to_thread(http, "POST", url, lender, {"paused": True})
            assert status == 403, "a computer lending its processor can't pause anyone"
            for bad in ({"paused": "yes"}, {"paused": 1}, {"threads": 0}, {"threads": 257}, {"threads": 1.5}, {"threads": True}, {"threads": "2"},
                        {}, {"other": 1}, []):
                status, out = await asyncio.to_thread(http, "POST", url, owner, bad)
                assert status == 400, (bad, out)
            assert (await asyncio.to_thread(http, "POST", url, owner, None, b"not json"))[0] == 400
            assert (await asyncio.to_thread(http, "POST", f"{coord.base}/api/devices/nosuchdevice/limits", owner, {"paused": True}))[0] == 404
            assert tab.first("limits") is None, "nothing changed yet"
            await asyncio.to_thread(set_limits, coord, owner, me, paused=True, threads=1)
            await tab.wait_for(lambda m: m["type"] == "limits")
        return me

    me = asyncio.run(main())
    trail = [a for a in http("GET", f"{coord.base}/api/audit", owner)[1]["audit"] if a["action"] == "sharing changed" and a.get("device") == me]
    assert len(trail) == 1, trail
    assert trail[0]["by"] == "Home owner" and trail[0]["name"] == "Watched tab" and trail[0]["paused"] is True and trail[0]["threads"] == 1


def test_the_job_engine_plans_with_the_threads_allowed_and_skips_a_paused_device(coord):
    owner = sign_in(coord, coord.token, "Home owner")
    eng = Engine(coord)
    ds = eng.upload("home.jsonl", sales(200), classification="shareable")

    def row():
        plan = eng.c.request("POST", "/api/engine/plan", {"job": eng.job_doc(AGG, ds)})["plan"]
        return next(d for d in plan["devices"] if d["name"] == "Planned tab")

    async def main():
        async with Tab(coord, "Planned tab") as tab:
            await until(lambda: any(m["type"] == "welcome" for m in tab.msgs))
            me = next(m for m in tab.msgs if m["type"] == "welcome")["id"]
            r = await asyncio.to_thread(row)
            assert r["eligible"] and r["slots"] == 2
            await asyncio.to_thread(set_limits, coord, owner, me, threads=1)
            r = await asyncio.to_thread(row)
            assert r["eligible"] and r["slots"] == 1
            await asyncio.to_thread(set_limits, coord, owner, me, paused=True)
            r = await asyncio.to_thread(row)
            assert not r["eligible"] and r["why"] == "is paused on the Home screen"

    asyncio.run(main())


def test_a_computer_says_in_its_terminal_when_it_is_paused_or_limited(coord):
    owner = sign_in(coord, coord.token, "Home owner")
    proc = coord.agent("Home desk", threads=3, extra=("--no-tasks",))
    me = next(d["id"] for d in http("GET", f"{coord.base}/api/devices", owner)[1]["devices"] if d["name"] == "Home desk")
    set_limits(coord, owner, me, threads=2)
    proc.wait_for("The pool uses 2 of this computer's 3 worker processes (set on the Home screen by Home owner).")
    set_limits(coord, owner, me, paused=True)
    proc.wait_for("Paused on the Home screen by Home owner: this computer takes no new work")
    set_limits(coord, owner, me, paused=False, threads=None)
    proc.wait_for("The pool may use all 3 worker processes (set on the Home screen by Home owner).")


def test_agent_tasks_wait_while_a_computer_is_paused():
    from yourharness.coordinator.tasks.manager import TaskManager

    tm = TaskManager.__new__(TaskManager)  # only what choosing a computer reads
    tm.refused = {}
    tm.s = SimpleNamespace(resources=SimpleNamespace(fits=lambda *a, **k: True), devices={})
    tm.db = SimpleNamespace(busy=lambda device_id: 0)
    d = SimpleNamespace(id="d1", name="Desk", kind="agent", closed=False, stale=False, paused=True, owned=True, exec={},
                        tasks_info={"git": True, "agents": [{"id": "command"}], "slots": 1})
    tm.s.devices["d1"] = d
    task = {"id": "t1", "spec": {"adapter": "command", "device": "d1"}}
    assert tm._eligible(task, d) is False
    assert tm._why_waiting(task) == "Waiting for Desk, which is paused on the Home screen."
    d.paused = False
    assert tm._eligible(task, d) is True
