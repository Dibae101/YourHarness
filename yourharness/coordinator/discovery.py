"""Lets apps find a coordinator on the local network without typing an address.

The coordinator answers a UDP broadcast ("YOURHARNESS?") on port 8421 with its
HTTP port and name. It reveals nothing secret: joining still needs the code.
"""

from __future__ import annotations

import asyncio
import json
import socket

BEACON_PORT = 8421
QUESTION = b"YOURHARNESS?"


class _Beacon(asyncio.DatagramProtocol):
    def __init__(self, http_port: int, name: str):
        self.reply = json.dumps({"app": "yourharness", "port": http_port, "name": name}).encode()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if data.strip() == QUESTION:
            self.transport.sendto(self.reply, addr)


async def start(http_port: int, port: int = BEACON_PORT):
    """Start answering discovery questions. Returns the transport, or None if the port is taken."""
    try:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: _Beacon(http_port, socket.gethostname().split(".")[0][:40]), local_addr=("0.0.0.0", port), reuse_port=hasattr(socket, "SO_REUSEPORT"),
        )
        return transport
    except OSError:
        return None


def find(timeout: float = 1.5, port: int = BEACON_PORT, targets: tuple[str, ...] = ("255.255.255.255", "127.0.0.1")) -> list[dict]:
    """Ask the network for coordinators. Returns [{"host", "port", "name"}]."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.settimeout(0.3)
    found: dict[tuple, dict] = {}
    try:
        for t in targets:
            try:
                s.sendto(QUESTION, (t, port))
            except OSError:
                pass
        import time

        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                data, addr = s.recvfrom(2048)
                info = json.loads(data)
                if info.get("app") == "yourharness":
                    found[(addr[0], info["port"])] = {"host": addr[0], "port": info["port"], "name": info.get("name", "")}
            except socket.timeout:
                continue
            except (ValueError, KeyError, OSError):
                continue
    finally:
        s.close()
    return list(found.values())
