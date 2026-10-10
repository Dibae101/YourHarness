"""The `harness` command.

    harness serve                          start the coordinator
    harness agent --join '<link>'          join this computer as an agent
    harness agent --workspace              ...and host the shared Linux workspace
    harness devices | jobs                 see what's in the pool
    harness run primes --below 1e9         run a job and print the result
    harness run python job.py --pieces 64  run your own run(chunk, total) on every agent
    harness files [ls|put|get|rm]          use the shared folder
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
from pathlib import Path

from . import __version__


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def _number(text: str) -> int:
    try:
        return int(float(text))
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text} isn't a number") from None


# ---------------------------------------------------------------------- serve
def cmd_serve(a) -> int:
    import asyncio

    from .coordinator.app import Config, default_home, serve
    from .coordinator.state import Store

    home = default_home()
    cfg = Config(
        host=a.host, port=a.port,
        data_dir=Path(a.data_dir or os.environ.get("HARNESS_DATA_DIR") or home / "coordinator").expanduser(),
        shared_dir=Path(a.shared_dir or os.environ.get("SHARED_DIR") or home / "shared").expanduser(),
        max_upload_mb=int(os.environ.get("MAX_UPLOAD_MB", 4096)),
        stale_s=float(os.environ.get("HARNESS_STALE_S", 20)),
        chunk_timeout_s=float(os.environ.get("CHUNK_TIMEOUT_S", 300)),
    )
    if a.new_join_link:
        Store(cfg.data_dir).rotate_join_token()
        print("Made a new join link. Devices that already joined stay signed in; the old link stops working.")
    _setup_logging(a.verbose)
    try:
        asyncio.run(serve(cfg, public_url=a.public_url, show_qr=not a.no_qr and not os.environ.get("NO_QR"), local_agent=not a.no_local_agent))
    except KeyboardInterrupt:
        print("\nStopped the coordinator.")
    except OSError as e:
        print(f"Can't listen on {cfg.host}:{cfg.port} ({e.strerror}). Is something else, such as the Node prototype, using that port?", file=sys.stderr)
        return 1
    return 0


# ---------------------------------------------------------------------- agent
def _default_agent_name() -> str:
    from .sysinfo import computer_name

    return computer_name()


def cmd_agent(a) -> int:
    import asyncio

    from .agent.agent import Agent, AgentConfig, SignInError, default_home, join

    home = default_home()
    home.mkdir(parents=True, exist_ok=True)
    cfg_path = home / "agent.json"
    saved = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
    if a.join:
        try:
            saved.update(join(a.join, a.name or saved.get("name") or _default_agent_name(), a.server))
        except SignInError as e:
            print(e, file=sys.stderr)
            return 1
    if not saved.get("token"):
        print("This computer hasn't joined a pool yet. Run:\n  harness agent --server http://<coordinator address>:8420 --join <4-digit code>", file=sys.stderr)
        return 1
    if a.name:
        saved["name"] = a.name
    if a.threads:
        saved["threads"] = a.threads
    if a.workspace or a.no_workspace:
        saved["workspace"] = bool(a.workspace)
    cfg_path.write_text(json.dumps(saved, indent=2), encoding="utf-8")
    os.chmod(cfg_path, 0o600)

    threads = int(saved.get("threads") or max(1, (os.cpu_count() or 2) - 1))
    cfg = AgentConfig(server=saved["server"], token=saved["token"], name=saved.get("name") or _default_agent_name(),
                      threads=threads, sharing=not a.no_share, workspace=bool(saved.get("workspace")), home=home)
    _setup_logging(a.verbose)
    print(f"Connecting to {cfg.server} as {cfg.name}...", flush=True)
    try:
        return asyncio.run(Agent(cfg).run())
    except KeyboardInterrupt:
        print("\nStopped the agent.")
        return 0


# ---------------------------------------------------------------------- client commands
def _client():
    from .client import Client

    return Client()


def cmd_discover(a) -> int:
    from .coordinator import discovery

    found = discovery.find(timeout=a.wait)
    if not found:
        print("No coordinator answered on this network.")
        return 1
    for c in found:
        print(f"http://{c['host']}:{c['port']}  ({c['name']})")
    return 0


def cmd_devices(a) -> int:
    devices = _client().devices()
    if not devices:
        print("No devices are connected.")
    for d in devices:
        info = d["info"]
        parts = [d["kind"], info["platform"], f"{d['threads']} of {info['cores']} threads" if d["sharing"] else "not sharing"]
        if info.get("memoryGB"):
            free = f", {info['memoryFreeGB']} GB free" if info.get("memoryFreeGB") else ""
            parts.append(f"{info['memoryGB']} GB memory{free}")
        if info.get("diskFreeGB"):
            parts.append(f"{info['diskFreeGB']} GB disk free")
        if info.get("gpus"):
            parts.append(", ".join(info["gpus"]))
        if d.get("workspace"):
            parts.append("hosts the workspace")
        if d.get("stale"):
            parts.append("NOT RESPONDING")
        print(f"{d['name']}: " + " · ".join(parts))
    return 0


def cmd_jobs(a) -> int:
    for j in reversed(_client().jobs()):
        print(f"{j['id']:>5}  {j['status']:<9}  {j['done']}/{j['total']}  {j['title']}  (by {j['createdBy']})")
    return 0


def _print_result(job: dict) -> None:
    from .client import breakdown

    r, kind = job["result"], job["kind"]
    if kind == "primes":
        print(f"{r['count']:,} primes below {job['params']['limit']:,}")
    elif kind == "pi":
        print(f"π ≈ {r['estimate']:.7f} from {r['samples']:,} points")
    elif kind == "grep":
        print(f"{r['count']:,} matches for “{job['params']['term']}” in {job['params']['file']}")
    else:
        value = r.get("value")
        print(value if isinstance(value, (int, float, str)) else json.dumps(value, indent=2))
    print(breakdown(job), file=sys.stderr)


def cmd_run(a) -> int:
    from .client import PoolError

    if a.what == "primes":
        kind, params = "primes", {"limit": a.below, "chunkSize": 10**7}
    elif a.what == "pi":
        kind, params = "pi", {"chunks": 400, "samples": max(10**4, a.points // 400)}
    elif a.what == "search":
        kind, params = "grep", {"file": a.file, "term": a.text, "ignoreCase": a.ignore_case}
    else:
        code = Path(a.file).read_text(encoding="utf-8")
        kind, params = "custom", {"code": code, "chunks": a.pieces, "combine": a.combine, "lang": "python"}
    try:
        job = _client().run(kind, params)
    except PoolError as e:
        print(e, file=sys.stderr)
        return 1
    _print_result(job)
    return 0


def cmd_cancel(a) -> int:
    job = _client().cancel(a.job_id)
    print(f"{job['id']} is {job['status']}.")
    return 0


def cmd_files(a) -> int:
    c = _client()
    if a.action in (None, "ls"):
        files = c.files()
        if not files:
            print("The shared folder is empty.")
        for f in files:
            by = f" (added by {f['by']})" if f.get("by") else ""
            print(f"{f['size']:>14,}  {f['name']}{by}")
    elif a.action == "put":
        for p in a.args:
            res = c.upload(p)
            print(f"Added {res['name']} ({res['size']:,} bytes).")
    elif a.action == "get":
        if not a.args:
            print("Name the file to get: harness files get NAME [DEST]", file=sys.stderr)
            return 1
        dest = c.download(a.args[0], a.args[1] if len(a.args) > 1 else ".")
        print(f"Saved {dest}")
    elif a.action == "rm":
        for name in a.args:
            c.delete(name)
            print(f"Deleted {name}.")
    return 0


def cmd_pool(a) -> int:
    c = _client()
    if a.action in (None, "ls"):
        info = c.pool_files()
        for f in info["files"]:
            health = f"{f['copies']} of {f['wanted']} copies" if f["available"] else "OFFLINE (a computer holding it is away)"
            print(f"{f['size']:>14,}  {f['name']}  ({health})")
        if not info["files"]:
            print("No pooled files yet.")
        print(f"\n{info['storedBytes']:,} bytes stored on {info['agents']} computer{'s' if info['agents'] != 1 else ''}, about {info['freeGB']} GB free.")
    elif a.action == "put":
        for p in a.args:
            res = c.pool_put(p)
            print(f"Stored {res['name']} ({res['size']:,} bytes).")
    elif a.action == "get":
        if not a.args:
            print("Name the file to get: harness pool get NAME [DEST]", file=sys.stderr)
            return 1
        print(f"Saved {c.pool_get(a.args[0], a.args[1] if len(a.args) > 1 else '.')}")
    elif a.action == "rm":
        for name in a.args:
            c.pool_rm(name)
            print(f"Deleted {name}.")
    return 0


# ---------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="harness", description="Pool the devices you own into one workspace.")
    p.add_argument("--version", action="version", version=f"yourharness {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="start the coordinator")
    s.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    s.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8420)))
    s.add_argument("--data-dir", help="where sign-ins are stored (default ~/.yourharness/coordinator)")
    s.add_argument("--shared-dir", help="the shared folder (default ~/.yourharness/shared)")
    s.add_argument("--public-url", help="the address devices use, if not this machine's LAN address, e.g. https://box.tailnet.ts.net")
    s.add_argument("--new-join-link", action="store_true", help="replace the join link; devices already in stay signed in")
    s.add_argument("--no-qr", action="store_true")
    s.add_argument("--no-local-agent", action="store_true", help="don't share this computer or host a terminal on it")
    s.add_argument("-v", "--verbose", action="store_true")
    s.set_defaults(fn=cmd_serve)

    s = sub.add_parser("agent", help="share this computer with the pool")
    s.add_argument("--join", metavar="CODE_OR_LINK", help="the 4-digit join code or the join link the coordinator printed")
    s.add_argument("--server", help="the coordinator's address, e.g. http://192.168.1.20:8420 (needed with a 4-digit --join code)")
    s.add_argument("--name", help="how this computer shows up in the pool")
    s.add_argument("--threads", type=int, help="worker processes to run (default: cores minus one)")
    s.add_argument("--workspace", action="store_true", help="host the shared Linux workspace terminal")
    s.add_argument("--no-workspace", action="store_true", help="stop hosting the workspace")
    s.add_argument("--no-share", action="store_true", help="connect without taking any pieces")
    s.add_argument("-v", "--verbose", action="store_true")
    s.set_defaults(fn=cmd_agent)

    s = sub.add_parser("discover", help="find coordinators on this network")
    s.add_argument("--wait", type=float, default=1.5)
    s.set_defaults(fn=cmd_discover)
    sub.add_parser("devices", help="list connected devices").set_defaults(fn=cmd_devices)
    sub.add_parser("jobs", help="list recent runs").set_defaults(fn=cmd_jobs)

    s = sub.add_parser("run", help="run a job on the pool and print the result")
    rs = s.add_subparsers(dest="what", required=True)
    r = rs.add_parser("primes", help="count primes below a number")
    r.add_argument("--below", type=_number, default=10**9)
    r = rs.add_parser("pi", help="estimate π from random points")
    r.add_argument("--points", type=_number, default=2 * 10**9)
    r = rs.add_parser("search", help="count matches in a shared file")
    r.add_argument("file")
    r.add_argument("text")
    r.add_argument("--ignore-case", action="store_true")
    r = rs.add_parser("python", help="run a file that defines run(chunk, total) on every agent")
    r.add_argument("file")
    r.add_argument("--pieces", type=int, default=64)
    r.add_argument("--combine", choices=["sum", "max", "min", "list"], default="list")
    s.set_defaults(fn=cmd_run)

    s = sub.add_parser("cancel", help="cancel a run")
    s.add_argument("job_id")
    s.set_defaults(fn=cmd_cancel)

    s = sub.add_parser("files", help="use the shared folder: ls, put PATH..., get NAME [DEST], rm NAME...")
    s.add_argument("action", nargs="?", choices=["ls", "put", "get", "rm"])
    s.add_argument("args", nargs="*")
    s.set_defaults(fn=cmd_files)

    s = sub.add_parser("pool", help="pooled storage across computers: ls, put PATH..., get NAME [DEST], rm NAME...")
    s.add_argument("action", nargs="?", choices=["ls", "put", "get", "rm"])
    s.add_argument("args", nargs="*")
    s.set_defaults(fn=cmd_pool)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd not in ("serve", "agent", "discover"):
        from .client import PoolError

        try:
            return args.fn(args)
        except PoolError as e:
            print(e, file=sys.stderr)
            return 1
    return args.fn(args)
