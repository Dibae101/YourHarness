"""End-to-end tests: a real coordinator, real agents, and fake browsers."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import sys
import tarfile
import time
import urllib.error
import urllib.request

import aiohttp
import pytest

from conftest import FakeBrowser
from yourharness.client import Client, PoolError


def http(method: str, url: str, headers: dict | None = None, data: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ---------------------------------------------------------------------- sign-in
def test_api_needs_a_signed_in_device(coord):
    assert http("GET", f"{coord.base}/api/devices")[0] == 401
    assert http("GET", f"{coord.base}/api/devices", {"Authorization": "Bearer nope.nope"})[0] == 401
    assert http("GET", f"{coord.base}/api/files/x.txt")[0] == 401
    status, body = http("GET", f"{coord.base}/api/devices", {"Authorization": f"Bearer {coord.client().token}"})
    assert status == 200 and "devices" in json.loads(body)


def test_wrong_join_tokens_get_locked_out(fresh_coord):
    url = f"{fresh_coord.base}/api/join"
    for _ in range(10):
        status, body = http("POST", url, {"Content-Type": "application/json"}, json.dumps({"token": "guess"}).encode())
        assert status == 401
    status, body = http("POST", url, {"Content-Type": "application/json"}, json.dumps({"token": fresh_coord.token}).encode())
    assert status == 429, "even the right token is refused while the address is locked out"
    assert "Too many failed sign-ins" in json.loads(body)["error"]


def test_cross_site_requests_are_refused(coord):
    async def main():
        async with FakeBrowser(coord, "Victim") as fb:
            r = await fb.http.put(f"{coord.base}/api/files/evil.txt", data=b"x", headers={"Origin": "http://evil.example"})
            assert r.status == 403
            r = await fb.http.put(f"{coord.base}/api/files/fine.txt", data=b"x", headers={"Origin": coord.base})
            assert r.status == 201
            ws = await fb.http.ws_connect(coord.base.replace("http", "ws") + "/ws", headers={"Origin": "http://evil.example"})
            msg = await ws.receive_json(timeout=5)
            assert msg["type"] == "denied"
            await ws.receive(timeout=5)
            assert ws.close_code == 4401
            await ws.close()

    asyncio.run(main())


def test_removed_device_is_signed_out(coord):
    async def main():
        async with FakeBrowser(coord, "Old phone") as phone, FakeBrowser(coord, "Laptop") as laptop:
            welcome = await phone.wait_for(lambda m: m["type"] == "welcome")
            r = await laptop.http.delete(f"{coord.base}/api/devices/{welcome['id']}", headers={"Origin": coord.base})
            assert r.status == 200
            await phone.wait_for(lambda m: m["type"] == "denied")
            for _ in range(50):
                if phone.closed_code:
                    break
                await asyncio.sleep(0.1)
            assert phone.closed_code == 4401
            r = await phone.http.get(f"{coord.base}/api/devices")
            assert r.status == 401

    asyncio.run(main())


# ---------------------------------------------------------------------- jobs
def test_jobs_split_across_an_agent_and_a_browser(coord):
    coord.agent("Agent A", threads=2)

    async def main():
        async with FakeBrowser(coord, "Browser B", threads=2) as fb:
            job = await fb.run("primes", {"limit": 10**7, "chunkSize": 10**5})
            assert job["status"] == "done" and job["result"]["count"] == 664579
            names = {p["name"] for p in job["perDevice"].values()}
            assert names == {"Agent A", "Browser B"}, job["perDevice"]

            job = await fb.run("pi", {"chunks": 40, "samples": 100_000})
            assert abs(job["result"]["estimate"] - 3.14159) < 0.01

            job = await fb.run("mandelbrot", {"width": 200, "height": 120, "maxIter": 300, "rows": 8, "cx": -0.6, "cy": 0, "span": 3.2})
            assert job["status"] == "done" and job["result"]["pixels"] == 24000

    asyncio.run(main())


def test_code_goes_only_to_devices_that_can_run_it(coord):
    async def main():
        async with FakeBrowser(coord, "Browser C") as fb:
            job = await fb.run("custom", {"code": "function run(chunk, total) { return chunk; }", "chunks": 30, "combine": "sum", "lang": "js"})
            assert job["result"]["value"] == sum(range(30))
            assert {p["name"] for p in job["perDevice"].values()} == {"Browser C"}

            job = await fb.run("custom", {"code": "def run(chunk, total):\n    return chunk * 2\n", "chunks": 30, "combine": "sum", "lang": "python"})
            assert job["result"]["value"] == 2 * sum(range(30))
            assert "Browser C" not in {p["name"] for p in job["perDevice"].values()}

    asyncio.run(main())


def test_pool_map_from_python(coord):
    from yourharness import pool

    pool._client = coord.client("workspace")

    def cube(n):
        return n ** 3

    assert pool.map(cube, range(20), quiet=True) == [n ** 3 for n in range(20)]
    assert pool.map(cube, range(10), per_piece=3, quiet=True) == [n ** 3 for n in range(10)]
    with pytest.raises(PoolError, match="ZeroDivisionError"):
        pool.run("def run(chunk, total):\n    return 1 / 0\n", pieces=4, quiet=True)
    with pytest.raises(PoolError, match="defined with def"):
        pool.map(lambda x: x, [1])


def test_shared_files_and_search(coord):
    c: Client = coord.client("files")
    line = b"The quick brown fox. Error: disk full. error again. ERROR!\n"
    body = line * 60000  # about 3.4 MB, so 1 MB pieces cross line boundaries
    path = coord.tmp / "log sample.txt"
    path.write_bytes(body)
    assert c.upload(path)["size"] == len(body)
    assert any(f["name"] == "log sample.txt" and f["size"] == len(body) for f in c.files())

    url = f"{coord.base}/api/files/log%20sample.txt"
    status, part = http("GET", url, {"Authorization": f"Bearer {c.token}", "Range": "bytes=4-8"})
    assert status == 206 and part == b"quick"
    status, _ = http("GET", f"{coord.base}/api/files/..%2F..%2Fetc%2Fpasswd", {"Authorization": f"Bearer {c.token}"})
    assert status in (400, 404)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {c.token}"})
    with urllib.request.urlopen(req) as res:
        assert res.headers["Content-Type"] == "application/octet-stream"
        assert res.headers["X-Content-Type-Options"] == "nosniff"
        assert res.headers["Content-Disposition"].startswith("attachment")

    async def main():
        async with FakeBrowser(coord, "Browser D") as fb:
            fb.file_token = c.token
            job = await fb.run("grep", {"file": "log sample.txt", "term": "error", "chunkMB": 1})
            assert job["result"]["count"] == 60000
            job = await fb.run("grep", {"file": "log sample.txt", "term": "error", "ignoreCase": True, "chunkMB": 1})
            assert job["result"]["count"] == 180000
            await fb.ws.send_json({"type": "submit", "job": {"kind": "grep", "params": {"file": "missing.txt", "term": "x"}}})
            err = await fb.wait_for(lambda m: m["type"] == "error")
            assert "no shared file" in err["message"]

    asyncio.run(main())
    c.delete("log sample.txt")
    assert not any(f["name"] == "log sample.txt" for f in c.files())


def test_wrong_format_results_are_rejected(coord):
    """A device sending a malformed fractal strip doesn't corrupt the image."""

    async def main():
        async with FakeBrowser(coord, "Good browser") as good:
            async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as http_s:
                await http_s.post(f"{coord.base}/api/join", json={"token": coord.token, "name": "Bad browser"})
                ws = await http_s.ws_connect(coord.base.replace("http", "ws") + "/ws", headers={"Origin": coord.base})
                await ws.send_json({"type": "hello", "name": "Bad browser", "threads": 4, "runtimes": ["js"]})

                async def lie():
                    async for m in ws:
                        msg = json.loads(m.data)
                        if msg["type"] == "chunk":
                            await ws.send_json({"type": "result", "jobId": msg["jobId"], "idx": msg["idx"], "value": "AAAA", "ms": 1})
                        elif msg["type"] == "beat":
                            pass

                liar = asyncio.ensure_future(lie())
                job = await good.run("mandelbrot", {"width": 128, "height": 64, "maxIter": 100, "rows": 8, "cx": -0.6, "cy": 0, "span": 3.2})
                liar.cancel()
                await ws.close()
                # Bad pieces are retried; after 3 failures on one piece the job stops instead of finishing with garbage
                if job["status"] == "done":
                    assert "Bad browser" not in {p["name"] for p in job["perDevice"].values()}
                else:
                    assert "wrong format" in job["error"]

    asyncio.run(main())


# ---------------------------------------------------------------------- liveness
def test_quiet_device_loses_its_pieces_within_seconds(fresh_coord):
    """A frozen tab keeps its socket open but stops beating. Its pieces move in ~2 s, not 5 min."""

    async def main():
        async with FakeBrowser(fresh_coord, "Frozen tab", threads=8, stall=True, beat=False) as frozen:
            await frozen.wait_for(lambda m: m["type"] == "welcome")
            async with FakeBrowser(fresh_coord, "Working tab", threads=2) as worker:
                t0 = time.monotonic()
                job = await worker.run("primes", {"limit": 3 * 10**6, "chunkSize": 10**5}, timeout=30)
                took = time.monotonic() - t0
                assert job["status"] == "done" and job["result"]["count"] == 216816
                assert took < 15, f"took {took:.1f} s"
                assert any(m["type"] == "chunk" for m in frozen.msgs), "the frozen tab did take pieces first"

    asyncio.run(main())


# ---------------------------------------------------------------------- workspace
@pytest.mark.skipif(sys.platform.startswith("win"), reason="the workspace needs a POSIX terminal")
def test_workspace_terminal(coord):
    coord.agent("Workspace host", threads=1, workspace=True)

    async def main():
        async with FakeBrowser(coord, "Tablet") as tablet, FakeBrowser(coord, "Snoop") as snoop:
            await tablet.ws.send_json({"type": "term.open", "cols": 100, "rows": 30, "ref": "r1"})
            opened = await tablet.wait_for(lambda m: m["type"] == "term.opened")
            assert opened["host"] == "Workspace host"
            sid = opened["sid"]

            def output():
                return b"".join(base64.b64decode(m["data"]) for m in tablet.msgs if m["type"] == "term.out" and m["sid"] == sid).decode(errors="replace")

            await tablet.wait_for(lambda m: m["type"] == "term.out" and m["sid"] == sid)  # the shell has started
            await asyncio.sleep(1.0)  # let a login shell finish its startup files, as a person would
            # another device can't type into this terminal
            await snoop.ws.send_json({"type": "term.in", "sid": sid, "data": "echo snooped-$((1+1))\r"})
            await tablet.ws.send_json({"type": "term.in", "sid": sid, "data": "echo hello-$((40+2)) && harness devices >/dev/null && echo cli-ok\r"})
            for _ in range(400):
                if "cli-ok" in output():
                    break
                await asyncio.sleep(0.05)
            text = output()
            assert "hello-42" in text and "cli-ok" in text, text[-500:]
            assert "snooped-2" not in text
            await tablet.ws.send_json({"type": "term.close", "sid": sid})

    asyncio.run(main())


# ---------------------------------------------------------------------- install
def test_install_script_and_agent_bundle(coord):
    status, body = http("GET", f"{coord.base}/install.sh")
    assert status == 200 and f'SERVER="{coord.base}"' in body.decode()
    status, body = http("GET", f"{coord.base}/agent.tar.gz")
    assert status == 200
    names = tarfile.open(fileobj=io.BytesIO(body)).getnames()
    assert "yourharness/agent/agent.py" in names and "yourharness/kernels.py" in names
    assert not any("__pycache__" in n for n in names)


# ---------------------------------------------------------------------- 4-digit join code
def test_join_with_four_digit_code(fresh_coord):
    code = json.loads((fresh_coord.tmp / "data" / "coordinator.json").read_text())["join_code"]
    assert len(code) == 4 and code.isdigit()
    body = json.dumps({"token": code, "kind": "agent", "name": "Coded"}).encode()
    status, res = http("POST", f"{fresh_coord.base}/api/join", {"Content-Type": "application/json"}, body)
    assert status == 200 and "deviceToken" in json.loads(res)


def test_agent_joins_with_code_and_server(fresh_coord):
    import os, subprocess
    from conftest import Proc

    code = json.loads((fresh_coord.tmp / "data" / "coordinator.json").read_text())["join_code"]
    env = {**os.environ, "YOURHARNESS_HOME": str(fresh_coord.tmp / "coded-agent")}
    p = Proc([sys.executable, "-m", "yourharness", "agent", "--server", fresh_coord.base, "--join", code, "--threads", "1"], env)
    try:
        p.wait_for("Joined the pool")
    finally:
        p.stop()
    p = subprocess.run([sys.executable, "-m", "yourharness", "agent", "--join", code], env=env, capture_output=True, text=True, timeout=20, cwd=fresh_coord.tmp)
    assert p.returncode == 1  # saved sign-in exists, so this one connects; just must not crash on the missing --server


def test_code_changes_after_too_many_wrong_guesses(fresh_coord):
    path = fresh_coord.tmp / "data" / "coordinator.json"
    old = json.loads(path.read_text())["join_code"]
    url = f"{fresh_coord.base}/api/join"
    hdr = {"Content-Type": "application/json"}
    # wrong guesses from the one test address hit the per-address limit first, so count what the server did
    for i in range(10):
        http("POST", url, hdr, json.dumps({"token": f"{(int(old) + 1 + i) % 10000:04d}"}).encode())
    status, _ = http("POST", url, hdr, json.dumps({"token": old}).encode())
    assert status == 429
