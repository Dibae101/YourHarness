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


def test_windows_installer_is_served(coord):
    status, body = http("GET", f"{coord.base}/install.ps1")
    text = body.decode()
    assert status == 200 and coord.base in text and "wsl.exe" in text and "--workspace" in text


# ---------------------------------------------------------------------- pooled storage
def test_pooled_storage_survives_a_computer_leaving(fresh_coord):
    import hashlib, os, time

    agents = [fresh_coord.agent(f"Disk {n}", threads=1) for n in "ABC"]
    c = fresh_coord.client("storage")
    data = os.urandom(10 * 1024 * 1024 + 123)  # three chunks, the last one short
    src = fresh_coord.tmp / "big.bin"
    src.write_bytes(data)
    assert c.pool_put(src)["size"] == len(data)

    info = c.pool_files()
    f = next(x for x in info["files"] if x["name"] == "big.bin")
    assert f["chunks"] == 3 and f["copies"] == 2 and info["agents"] == 3

    out = fresh_coord.tmp / "back.bin"
    c.pool_get("big.bin", out)
    assert hashlib.sha256(out.read_bytes()).hexdigest() == hashlib.sha256(data).hexdigest()

    # find a computer that holds chunks and take it away
    index = json.loads((fresh_coord.tmp / "data" / "pool-index.json").read_text())
    counts: dict[str, int] = {}
    for ids in index["holders"].values():
        for i in ids:
            counts[i] = counts.get(i, 0) + 1
    assert len(counts) >= 2 and all(n >= 1 for n in counts.values())
    victim = max(counts, key=counts.get)
    names = {d["id"]: d["name"] for d in c.devices()}
    agents[["Disk A", "Disk B", "Disk C"].index(names[victim])].stop()

    for _ in range(100):  # the file stays readable from the other copies
        if len(c.devices()) == 3:  # the coordinator itself isn't an agent here; wait for the drop
            time.sleep(0.1)
        else:
            break
    out2 = fresh_coord.tmp / "back2.bin"
    c.pool_get("big.bin", out2)
    assert out2.read_bytes() == data

    # with two computers left, every chunk gets its second copy back
    for _ in range(150):
        f = next(x for x in c.pool_files()["files"] if x["name"] == "big.bin")
        if f["copies"] == 2:
            break
        time.sleep(0.2)
    assert f["copies"] == 2, f

    c.pool_rm("big.bin")
    assert not c.pool_files()["files"]


def test_pooled_storage_needs_a_computer(fresh_coord):
    c = fresh_coord.client("storage")
    src = fresh_coord.tmp / "x.bin"
    src.write_bytes(b"hello")
    with pytest.raises(PoolError, match="No computer is connected"):
        c.pool_put(src)


def test_discovery_finds_a_coordinator():
    import socket
    from yourharness.coordinator import discovery

    async def main():
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        beacon = await discovery.start(5555, port)
        assert beacon is not None
        found = await asyncio.get_running_loop().run_in_executor(None, lambda: discovery.find(1.0, port, ("127.0.0.1",)))
        beacon.close()
        return found

    found = asyncio.run(main())
    assert found and found[0]["port"] == 5555 and found[0]["host"] == "127.0.0.1"


# ---------------------------------------------------------------------- files on each computer
def test_files_on_each_computer(fresh_coord):
    import os, urllib.parse

    fresh_coord.agent("Mac", threads=1, workspace=True)
    fresh_coord.agent("Windows PC", threads=1, workspace=True)
    fresh_coord.agent("Compute only", threads=1)
    c = fresh_coord.client("files")
    devs = {d["name"]: d for d in c.devices()}
    mac, win, plain = devs["Mac"]["id"], devs["Windows PC"]["id"], devs["Compute only"]["id"]
    assert devs["Mac"]["files"] and not devs["Compute only"]["files"]
    auth = {"Authorization": f"Bearer {c.token}"}
    base = fresh_coord.base
    q = urllib.parse.quote

    def api(method, path, body=None, data=None):
        hdr = dict(auth)
        if body is not None:
            data = json.dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        return http(method, base + path, hdr, data)

    status, body = api("GET", f"/api/fs/{mac}?path=")
    assert status == 200 and json.loads(body)["entries"] == []
    assert api("POST", f"/api/fs/{mac}/mkdir", {"path": "Projects"})[0] == 200

    payload = os.urandom(9 * 1024 * 1024 + 7)  # three 4 MB pieces
    status, body = api("PUT", f"/api/fs/{mac}/upload?path={q('Projects/data.bin')}", data=payload)
    assert status == 201, body
    listing = json.loads(api("GET", f"/api/fs/{mac}?path=Projects")[1])
    assert [(e["name"], e["size"]) for e in listing["entries"]] == [("data.bin", len(payload))]
    status, got = api("GET", f"/api/fs/{mac}/download?path={q('Projects/data.bin')}")
    assert status == 200 and got == payload

    # send it to the other computer
    status, body = api("POST", "/api/fs-copy", {"from": {"device": mac, "path": "Projects/data.bin"}, "to": {"device": win, "dir": ""}})
    assert status == 200, body
    assert api("GET", f"/api/fs/{win}/download?path=data.bin")[1] == payload
    assert api("POST", f"/api/fs/{win}/rename", {"path": "data.bin", "to": "renamed.bin"})[0] == 200
    assert [e["name"] for e in json.loads(api("GET", f"/api/fs/{win}?path=")[1])["entries"]] == ["renamed.bin"]

    # limits: no escaping the home folder, no access where it isn't allowed, non-empty folders need recursive
    status, body = api("GET", f"/api/fs/{mac}?path={q('../..')}")
    assert status == 400 and "outside the home folder" in json.loads(body)["error"]
    status, body = api("GET", f"/api/fs/{plain}?path=")
    assert status == 400 and "doesn't allow file access" in json.loads(body)["error"]
    status, body = api("DELETE", f"/api/fs/{mac}?path=Projects")
    assert status == 400 and "isn't empty" in json.loads(body)["error"]
    assert api("DELETE", f"/api/fs/{mac}?path=Projects&recursive=1")[0] == 200
    assert json.loads(api("GET", f"/api/fs/{mac}?path=")[1])["entries"] == []
    # nothing was written outside the test's own folders
    assert not (fresh_coord.tmp / "renamed.bin").exists()


def test_live_load_is_reported(fresh_coord):
    import time

    fresh_coord.agent("Busy", threads=1)
    c = fresh_coord.client("watcher")
    for _ in range(40):
        d = next(x for x in c.devices() if x["name"] == "Busy")
        if d["load"].get("cpu") is not None:
            break
        time.sleep(0.25)
    assert 0 <= d["load"]["cpu"] <= 100 and d["load"]["memUsedGB"] > 0


# ---------------------------------------------------------------------- YourHarness OS: the pool shell
class Shell:
    """Types into the pool shell like a person in the browser would."""

    def __init__(self, fb):
        self.fb, self.sid, self.out = fb, None, b""

    async def open(self):
        await self.fb.ws.send_json({"type": "os.open", "cols": 100, "rows": 30, "ref": "t"})
        m = await self.fb.wait_for(lambda m: m["type"] == "os.opened")
        self.sid = m["sid"]
        await self.wait_prompt()

    def text(self) -> str:
        data = b"".join(base64.b64decode(m["data"]) for m in self.fb.msgs if m["type"] == "os.out" and m["sid"] == self.sid)
        return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", data.decode(errors="replace")).replace("\r", "")

    async def wait_prompt(self, timeout=30):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.text().rstrip().endswith("$"):
                return
            await asyncio.sleep(0.05)
        raise AssertionError("no prompt; screen:\n" + self.text()[-800:])

    async def run(self, line: str, timeout=40) -> str:
        before = len(self.text())
        await self.fb.ws.send_json({"type": "os.in", "sid": self.sid, "data": line + "\r"})
        await asyncio.sleep(0.1)
        await self.wait_prompt(timeout)
        return self.text()[before:]


import re  # noqa: E402


def test_pool_shell_is_one_os_across_computers(fresh_coord):
    fresh_coord.agent("Alpha", threads=1, workspace=True)
    fresh_coord.agent("Beta", threads=1, workspace=True)

    async def main():
        async with FakeBrowser(fresh_coord, "Terminal user") as fb:
            sh = Shell(fb)
            await sh.open()
            assert "Alpha" in sh.text() and "Beta" in sh.text()

            # a file written on one computer is read on the other
            out = await sh.run("on Alpha sh -c 'mkdir -p src && echo made-on-alpha > src/note.txt && echo wrote'")
            assert "wrote" in out and "ran on Alpha" in out and "1 saved in /pool" in out, out
            out = await sh.run("on Beta cat src/note.txt")
            assert "made-on-alpha" in out and "ran on Beta" in out and "fetched" in out, out

            # cd moves the shell; the next command starts there, wherever it runs
            await sh.run("cd src")
            out = await sh.run("pwd; ls")
            assert "/pool/src" not in out or "note.txt" in out
            assert "note.txt" in out, out
            assert "pool:/pool/src$" in sh.text()
            out = await sh.run("cd .. && cd src && ls")
            assert "note.txt" in out

            # deleting a file in one place deletes it everywhere
            await sh.run("on Beta rm note.txt")
            out = await sh.run("on Alpha ls")
            assert "note.txt" not in out, out

            # unknown computer, failing command, builtin
            assert "No computer matches" in await sh.run("on Nowhere echo hi")
            out = await sh.run("sh -c 'exit 3'")
            assert "exit 3" in out, out
            assert "Alpha" in await sh.run("runners")

            # par spreads work over both computers and shows where each landed
            t0 = time.monotonic()
            out = await sh.run("par sh -c 'sleep 1; echo item-{}' ::: 1 2 3 4", timeout=60)
            took = time.monotonic() - t0
            assert all(f"item-{n}" in out for n in "1234"), out
            assert "Alpha" in out and "Beta" in out, out
            assert took < 3.8, f"4 one-second commands on 2 computers took {took:.1f}s"

            # Ctrl-C stops a running command
            await fb.ws.send_json({"type": "os.in", "sid": sh.sid, "data": "sleep 30\r"})
            await asyncio.sleep(1.2)
            await fb.ws.send_json({"type": "os.in", "sid": sh.sid, "data": "\x03"})
            await sh.wait_prompt(15)

            # tab completion and history
            await sh.run("cd /pool")
            await sh.run("sh -c 'echo x > unique-long-name.txt'")
            await fb.ws.send_json({"type": "os.in", "sid": sh.sid, "data": "cat uniq\t"})
            await asyncio.sleep(0.4)
            assert "cat unique-long-name.txt" in sh.text().split("\n")[-1] or "unique-long-name.txt" in sh.text()[-80:]
            await fb.ws.send_json({"type": "os.in", "sid": sh.sid, "data": "\r"})
            await sh.wait_prompt()

    asyncio.run(main())


def test_pool_shell_needs_a_computer(fresh_coord):
    async def main():
        async with FakeBrowser(fresh_coord, "Lonely") as fb:
            sh = Shell(fb)
            await sh.open()
            assert "No computer can run commands yet" in sh.text()
            assert "No computer can run commands right now" in await sh.run("echo hi")

    asyncio.run(main())


def test_os_disk_folders_in_pooled_storage(fresh_coord):
    fresh_coord.agent("Disk", threads=1)
    c = fresh_coord.client("disk")
    auth = {"Authorization": f"Bearer {c.token}"}
    base = fresh_coord.base

    def api(method, path, body=None, data=None):
        hdr = dict(auth)
        if body is not None:
            data, hdr["Content-Type"] = json.dumps(body).encode(), "application/json"
        return http(method, base + path, hdr, data)

    assert api("PUT", "/api/os/file?path=a/b/c.txt", data=b"deep")[0] == 201
    assert api("POST", "/api/os/mkdir", {"path": "empty"})[0] == 200
    names = lambda p: {e["name"]: e["dir"] for e in json.loads(api("GET", f"/api/os/list?path={p}")[1])["entries"]}  # noqa: E731
    assert names("") == {"a": True, "empty": True} and names("a/b") == {"c.txt": False}
    assert api("GET", "/api/os/file?path=a/b/c.txt")[1] == b"deep"
    # a file can't also be a folder, and paths can't climb out
    assert api("PUT", "/api/os/file?path=a/b/c.txt/x", data=b"no")[0] == 400
    assert api("PUT", "/api/os/file?path=a/b", data=b"no")[0] == 400
    assert api("GET", "/api/os/list?path=../..")[0] == 400
    # rename a folder, delete without and with recursive
    assert api("POST", "/api/os/rename", {"path": "a", "to": "z"})[0] == 200
    assert names("") == {"empty": True, "z": True} and names("z/b") == {"c.txt": False}
    assert api("DELETE", "/api/os/file?path=z")[0] == 400
    assert api("DELETE", "/api/os/file?path=z&recursive=1")[0] == 200
    man = json.loads(api("GET", "/api/os/manifest")[1])
    assert man["files"] == {} and man["dirs"] == ["empty"]


def test_on_accepts_computer_names_with_spaces(fresh_coord):
    fresh_coord.agent("Darshan's MacBook Air", threads=1, workspace=True)
    fresh_coord.agent("Second computer", threads=1, workspace=True)

    async def main():
        async with FakeBrowser(fresh_coord, "Spacey") as fb:
            sh = Shell(fb)
            await sh.open()
            out = await sh.run("on Second computer echo one")
            assert "one" in out and "ran on Second computer" in out, out
            out = await sh.run("on Darshan's MacBook Air echo two")
            assert "two" in out and "ran on Darshan's MacBook Air" in out, out
            out = await sh.run('on "Second computer" echo three')
            assert "three" in out and "ran on Second computer" in out, out
            out = await sh.run("on second echo four")  # the start of a name still works
            assert "four" in out and "ran on Second computer" in out, out

    asyncio.run(main())
