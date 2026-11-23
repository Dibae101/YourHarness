"""Local text inference. The agent chooses the endpoint and only offers installed models."""

from __future__ import annotations

import http.client
import json
import os
import re

from .ops import OpError

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,160}(?::[A-Za-z0-9][A-Za-z0-9._-]{0,80})?")
DIGEST = re.compile(r"[0-9a-f]{64}")


class OllamaUnavailable(RuntimeError):
    """The worker cannot serve this model right now. Another attempt can take over."""


def valid_model(name, digest) -> bool:
    return (isinstance(name, str) and NAME.fullmatch(name) is not None and "cloud" not in name.lower().split(":")[-1].split("-")
            and isinstance(digest, str) and DIGEST.fullmatch(digest) is not None)


def request(path: str, body: dict | None = None, *, timeout: int = 5) -> dict:
    try:
        port = int(os.environ.get("HARNESS_OLLAMA_PORT", "11434"))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise OllamaUnavailable("HARNESS_OLLAMA_PORT must be a valid local port number.") from None
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("POST" if body is not None else "GET", "/api/" + path,
                     json.dumps(body).encode() if body is not None else None, {"Content-Type": "application/json"})
        resp = conn.getresponse()
        raw = resp.read(4 * 2**20 + 1)
        if len(raw) > 4 * 2**20:
            raise OpError("Ollama returned a response over 4 MiB.")
        if resp.status != 200:
            raise OllamaUnavailable(f"Local Ollama {path} returned HTTP {resp.status}. Check the model on this computer.")
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get("error"):
            raise OpError(f"Local Ollama could not complete {path}.")
        return result
    except (OSError, http.client.HTTPException, ValueError) as exc:
        raise OllamaUnavailable(f"Cannot read local Ollama {path} on this computer ({exc}).") from None
    finally:
        conn.close()


def inventory() -> list[dict]:
    models = request("tags").get("models")
    if not isinstance(models, list):
        raise OpError("Local Ollama returned an invalid model list.")
    out = []
    for m in models[:256]:
        if not isinstance(m, dict):
            continue
        name, digest = m.get("name"), m.get("digest")
        details = m.get("details")
        if (valid_model(name, digest) and not (m.get("remote_model") or m.get("remote_host"))
                and isinstance(details, dict) and details.get("format") in ("gguf", "safetensors")):
            out.append({"name": name, "digest": digest})
    return sorted(out, key=lambda m: (m["name"], m["digest"]))


def local_completion(name: str) -> bool:
    info = request("show", {"model": name})
    return bool(not (info.get("remote_model") or info.get("remote_host"))
                and info.get("model_info") and "completion" in (info.get("capabilities") or []))


def discover(known: dict) -> dict:
    """Reuse capability checks only while the corresponding manifest is still installed."""
    try:
        candidates = inventory()
    except (ValueError, OllamaUnavailable):
        return {}
    available = {}
    for model in candidates:
        key = (model["name"], model["digest"])
        try:
            available[key] = known[key] if key in known else local_completion(model["name"])
        except (ValueError, OllamaUnavailable):
            continue
    return available


class GenerateOp:
    def __init__(self, wl: dict, write):
        self.model = {"name": wl["model"], "digest": wl["digest"]}
        self.limit = wl["maxTokens"]
        self.write = write
        self.count = 0
        self.preview = []
        if self.model not in inventory() or not local_completion(wl["model"]):
            raise OllamaUnavailable(f"Local Ollama needs the selected revision of {wl['model']} with text completion support.")

    def feed(self, prompt: str) -> None:
        out = request("generate", {"model": self.model["name"], "prompt": prompt, "stream": False,
                                   "options": {"num_predict": self.limit}}, timeout=900)
        if out.get("done") is not True or not isinstance(out.get("response"), str):
            raise OpError("Local Ollama returned an incomplete response.")
        if self.model not in inventory():
            raise OllamaUnavailable("The model revision changed while this prompt was running. Choose the installed revision again.")
        self.write({"prompt": prompt, "response": out["response"], "model": self.model["name"], "digest": self.model["digest"],
                    "doneReason": out.get("done_reason")})
        if len(self.preview) < 3:
            self.preview.append({"prompt": prompt[:240], "response": out["response"][:1200]})
        self.count += 1

    def state(self) -> dict:
        return {"completed": self.count, "preview": self.preview}


def merge(states: list[dict]) -> dict:
    preview = []
    for s in states:
        preview.extend(s["preview"][:3 - len(preview)])
    return {"completed": sum(s["completed"] for s in states), "preview": preview}
