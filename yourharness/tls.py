"""HTTPS for a pool.

The coordinator serves HTTPS when it is given a certificate and its key:

    harness serve --tls-cert cert.pem --tls-key key.pem --public-url https://<a name the certificate covers>:8420

Use a certificate your devices already trust: `tailscale cert <machine>.<tailnet>.ts.net` on a
tailnet, one from Let's Encrypt, or one from a private authority such as mkcert. Computers and
the CLI check it the way a browser does, against the system's authorities, plus the private
authority's certificate in HARNESS_CA_FILE when there is one (`harness agent --ca FILE` keeps
it). The address in the join link must be one the certificate names.

The coordinator's own computer reaches it over a plain-HTTP listener on 127.0.0.1 that only
that computer can connect to, so the certificate doesn't have to name 127.0.0.1.

TLS protects what travels between devices. It doesn't protect data from the computer chosen to
process it; engine/isolation.py decides which computers get what.
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path


def server_context(cert: str, key: str) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert, key)
    return ctx


def ca_file() -> str | None:
    ca = os.environ.get("HARNESS_CA_FILE")
    return ca if ca and Path(ca).is_file() else None


def client_context(ca: str | None = None) -> ssl.SSLContext | None:
    """For connections to the coordinator: the system's authorities plus the pool's private one, if any (None: Python's default)."""
    ca = ca or ca_file()
    if not ca:
        return None
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=ca)
    return ctx
