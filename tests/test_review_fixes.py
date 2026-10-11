"""Regression tests for the review of the lend link, the join code, the pool shell and pooled storage."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import aiohttp

from conftest import FakeBrowser, start_coordinator
from test_pool import http
from yourharness.coordinator.execs import Execs
from yourharness.coordinator.pool_store import PoolStore


# ---------------------------------------------------------------------- lend link
def test_a_lender_agent_is_never_handed_owner_work(fresh_coord):
    """A lender's agent can claim anything in its hello; the pool must not believe it."""
    lend = json.loads((fresh_coord.tmp / "data" / "coordinator.json").read_text())["lend_token"]
    status, body = http("POST", f"{fresh_coord.base}/api/join", {"Content-Type": "application/json"},
                        json.dumps({"token": lend, "kind": "agent", "name": "Friend"}).encode())
    assert status == 200
    token = json.loads(body)["deviceToken"]

    async def main():
        async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"}) as s:
            ws = await s.ws_connect(fresh_coord.base.replace("http", "ws") + "/ws")
            await ws.send_json({"type": "hello", "name": "Friend", "threads": 1, "runtimes": ["python"],
                                "workspace": {"shell": "bash", "system": "Ubuntu"}, "files": {},
                                "exec": {"os": "Ubuntu", "linux": True}, "coordinator": True})
            welcome = await ws.receive_json(timeout=10)
            assert welcome["type"] == "welcome"
            assert welcome["procs"] == [] and welcome["files"] == [] and welcome["pool"]["files"] == []
            assert "joinToken" not in welcome and "joinCode" not in welcome
            status, body = http("GET", f"{fresh_coord.base}/api/devices", {"Authorization": f"Bearer {fresh_coord.client().token}"})
            friend = next(d for d in json.loads(body)["devices"] if d["name"] == "Friend")
            assert friend["exec"] is None and friend["workspace"] is None and friend["files"] is False and friend["coordinator"] is False
            await ws.close()

    asyncio.run(main())


# ---------------------------------------------------------------------- join code
def test_join_code_turns_off_after_wrong_guesses_from_many_addresses(tmp_path: Path):
    """Rotating the code doesn't lower the odds; only a pool-wide limit on guesses does."""
    c = start_coordinator(tmp_path, extra_env={"HARNESS_TRUST_PROXY": "1"})
    try:
        url = f"{c.base}/api/join"
        path = c.tmp / "data" / "coordinator.json"
        code = json.loads(path.read_text())["join_code"]
        for i in range(20):
            # the first entry is what the client sent; only the last one, added by the proxy, counts
            hdr = {"Content-Type": "application/json", "X-Forwarded-For": f"203.0.113.9, 10.0.{i}.1"}
            status, _ = http("POST", url, hdr, json.dumps({"token": f"{(int(code) + 1 + i) % 10000:04d}"}).encode())
            assert status == 401, i
        new = json.loads(path.read_text())["join_code"]
        hdr = {"Content-Type": "application/json", "X-Forwarded-For": "10.9.9.9"}
        status, body = http("POST", url, hdr, json.dumps({"token": new}).encode())
        assert status == 429 and "off" in json.loads(body)["error"], "even the right code is refused once codes are off"
        status, _ = http("POST", url, hdr, json.dumps({"token": c.token}).encode())
        assert status == 200, "the join link still works"

        owner = c.client()
        status, body = http("POST", f"{c.base}/api/join-code", {"Authorization": f"Bearer {owner.token}"})
        assert status == 200
        fresh = json.loads(body)["joinCode"]
        status, _ = http("POST", url, {"Content-Type": "application/json", "X-Forwarded-For": "10.8.8.8"}, json.dumps({"token": fresh}).encode())
        assert status == 200, "a new code from an owner turns codes back on"
    finally:
        c.proc.stop()


def test_spoofed_forwarded_for_does_not_dodge_the_address_limit(tmp_path: Path):
    c = start_coordinator(tmp_path, extra_env={"HARNESS_TRUST_PROXY": "1"})
    try:
        url = f"{c.base}/api/join"
        for i in range(10):
            hdr = {"Content-Type": "application/json", "X-Forwarded-For": f"10.0.0.{i}, 192.0.2.1"}
            assert http("POST", url, hdr, json.dumps({"token": "guess"}).encode())[0] == 401
        hdr = {"Content-Type": "application/json", "X-Forwarded-For": "10.0.0.99, 192.0.2.1"}
        assert http("POST", url, hdr, json.dumps({"token": "guess"}).encode())[0] == 429
    finally:
        c.proc.stop()


def test_signed_in_devices_are_not_locked_out_by_strangers(fresh_coord):
    """Behind a proxy everyone shares one address; wrong guesses must not cut off real devices."""
    owner = fresh_coord.client()
    for _ in range(10):
        http("POST", f"{fresh_coord.base}/api/join", {"Content-Type": "application/json"}, json.dumps({"token": "guess"}).encode())
    status, _ = http("GET", f"{fresh_coord.base}/api/devices", {"Authorization": f"Bearer {owner.token}"})
    assert status == 200


def test_join_from_another_website_is_refused(fresh_coord):
    hdr = {"Content-Type": "text/plain", "Origin": "http://evil.example"}
    status, _ = http("POST", f"{fresh_coord.base}/api/join", hdr, json.dumps({"token": "1234"}).encode())
    assert status == 403


def test_lender_gets_no_file_or_command_broadcasts(fresh_coord):
    lend = json.loads((fresh_coord.tmp / "data" / "coordinator.json").read_text())["lend_token"]

    async def main():
        lender = FakeBrowser(fresh_coord, "Lender")
        lender.coord = SimpleNamespace(base=fresh_coord.base, token=lend)
        async with lender, FakeBrowser(fresh_coord, "Owner") as owner:
            status, _ = http("PUT", f"{fresh_coord.base}/api/files/secret-plans.txt",
                             {"Authorization": f"Bearer {fresh_coord.client().token}"}, b"hello")
            assert status in (200, 201)
            await owner.wait_for(lambda m: m["type"] == "files" and any(f["name"] == "secret-plans.txt" for f in m["files"]))
            await asyncio.sleep(0.5)
            assert not [m for m in lender.msgs if m["type"] in ("files", "pool", "procs", "fs.changed")]

    asyncio.run(main())


# ---------------------------------------------------------------------- pool shell queue
def _dev(i: str, **kw):
    sent: list = []
    d = SimpleNamespace(id=i, name=i, kind="agent", trusted=True, closed=False, stale=False, sharing=True, load={},
                        info={"cores": 2}, exec={"os": "Linux", "linux": True}, sent=sent)
    d.send = sent.append
    d.__dict__.update(kw)
    return d


def test_queue_survives_its_only_computer_leaving():
    """Failing one queued command used to re-enter pump() and crash on the next."""
    a = _dev("A")
    sched = SimpleNamespace(devices={"A": a}, broadcast=lambda *x, **k: None)
    ex = Execs(sched)
    procs = [ex.submit(f"echo {i}", "", {}, "me", "A") for i in range(4)]
    assert procs[0].status == "running" and all(p.status == "queued" for p in procs[1:])
    a.closed = True
    ex.device_gone(a)
    assert all(p.status == "failed" for p in procs)
    assert not ex.queue and not ex.procs


def test_cancelled_command_is_reported_as_cancelled():
    a = _dev("A")
    ex = Execs(SimpleNamespace(devices={"A": a}, broadcast=lambda *x, **k: None))
    p = ex.submit("sleep 9", "", {}, "me")
    ex.cancel(p.id)
    ex.handle(a, {"type": "exec.done", "id": p.id, "code": -15, "error": "stopped by signal 15"})
    assert p.status == "cancelled"


# ---------------------------------------------------------------------- pooled storage
def _store(tmp_path, devices):
    sched = SimpleNamespace(devices={d.id: d for d in devices}, broadcast=lambda *x, **k: None,
                            store=SimpleNamespace(data={"devices": {d.id: {} for d in devices}}))
    return PoolStore(sched, tmp_path)


def test_gc_keeps_chunks_an_unfinished_upload_counts_on(tmp_path):
    a = _dev("A")
    ps = _store(tmp_path, [a])
    ps.holders["h"] = ["A"]
    ps.pending["h"] += 1
    ps._gc(["h"])
    assert ps.holders["h"] == ["A"] and not a.sent
    ps._release(["h"])
    ps._gc(["h"])
    assert "h" not in ps.holders and a.sent == [{"type": "store.del", "hash": "h"}]


def test_chunks_deleted_while_a_holder_was_offline_are_deleted_when_it_returns(tmp_path):
    a, b = _dev("A"), _dev("B", closed=True)
    ps = _store(tmp_path, [a, b])
    ps.holders["h"] = ["A", "B"]
    ps._gc(["h"])
    assert ps.dead == {"h": ["B"]}
    b.closed = False
    assert ps._sweep()
    assert {"type": "store.del", "hash": "h"} in b.sent and not ps.dead


def test_a_full_disk_is_not_mistaken_for_unknown_space(tmp_path):
    full, roomy = _dev("Full", info={"diskFreeGB": 0.0}), _dev("Roomy", info={"diskFreeGB": 50})
    ps = _store(tmp_path, [full, roomy])
    assert [d.id for d in ps._targets("h", 1024)] == ["Roomy"]
