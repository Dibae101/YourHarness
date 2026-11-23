"""Regression coverage for AI jobs. The inference fixture is an offline Ollama API stand-in."""

import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from test_engine_e2e import Engine

from yourharness.engine import ollama, spec

DIGEST = "ab" * 32
MODEL = {"name": "fixture:latest", "digest": DIGEST}
GEN = {"type": "llm.generate", "model": MODEL["name"], "digest": DIGEST}


def test_training_trials_and_regression_use_the_registered_dataset(fresh_coord):
    pool = Engine(fresh_coord)
    for name in ("Alpha", "Beta"):
        fresh_coord.agent(name, threads=1, extra=("--no-tasks", "--no-share-models"))
    pool.ready({"Alpha", "Beta"})
    raw = ("x,y\n" + "".join(f"{i},{3 + 2 * i}\n" for i in range(1000))).encode()
    ds = pool.upload("points.csv", raw)
    job = pool.run({"type": "ml.train", "features": ["x"], "target": "y"}, ds, mode="distributed",
                   partitioning={"targetBytes": 1024}, output={"spotCheck": 1})
    assert job["status"] == "done", job
    fit = job["result"]
    assert job["total"] > 1 and fit["rows"] == 1000
    assert fit["coefficients"] == pytest.approx([2]) and fit["intercept"] == pytest.approx(3)
    code = """def train(params, ctx):
    import csv
    with open(ctx["datasetPath"], newline="") as f:
        rows = list(csv.DictReader(f))
    with open(ctx["checkpointPath"], "wb") as f:
        f.write(bytes([params["scale"]]))
    return [len(rows), float(rows[-1]["x"]) * params["scale"], ctx["datasetSha256"]]
"""
    doc = {"spec": {"workload": {"type": "ml.trials", "code": code},
                    "inputs": {"dataset": {"ref": ds["ref"]}, "grid": {"scale": [2, 3]}}, "mode": "distributed"}}
    job = pool.wait(pool.c.start_job(doc)["id"])
    assert job["status"] == "done", job
    assert job["result"]["value"] == [[1000, s * 999, hashlib.sha256(raw).hexdigest()] for s in (2, 3)]
    for checkpoint, scale in zip(job["result"]["checkpoints"], (2, 3), strict=True):
        assert pool.c.request("GET", checkpoint["url"]) == bytes([scale])
        assert checkpoint["sha256"] == hashlib.sha256(bytes([scale])).hexdigest()
    with pytest.raises(spec.SpecError, match="own computers"):
        spec.parse({"spec": {**doc["spec"], "security": {"allowedTrust": "owned-and-friends"}}})


class ModelAPI(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def answer(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        assert self.path == "/api/tags"
        self.answer({"models": [{**MODEL, "details": {"format": "gguf"}}]})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/api/show":
            self.answer({"capabilities": ["completion"], "model_info": {"general.architecture": "fixture"}})
        else:
            assert self.path == "/api/generate"
            assert body["model"] == MODEL["name"] and body["stream"] is False
            self.answer({"done": True, "response": "reply to " + body["prompt"], "done_reason": "stop"})


def test_friend_model_requires_lender_and_requester_consent(fresh_coord, monkeypatch):
    pool = Engine(fresh_coord)
    with ThreadingHTTPServer(("127.0.0.1", 0), ModelAPI) as server:
        monkeypatch.setenv("HARNESS_OLLAMA_PORT", str(server.server_port))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            for name, opt in (("Lender", ("--share-models",)), ("Private", ())):
                code = pool.c.request("POST", "/api/invites", {"label": name})["code"]
                fresh_coord.agent(name, threads=1, link=code, extra=("--server", fresh_coord.base, "--no-tasks", *opt))
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                devices = {d["name"]: d for d in pool.c.devices()}
                if devices.get("Lender", {}).get("engine", {}).get("localModels") == [MODEL]:
                    break
                time.sleep(0.2)
            assert devices["Lender"]["engine"]["localModels"] == [MODEL]
            assert devices["Private"]["engine"]["localModels"] == []
            doc = pool.job_doc(GEN, items=["one", "two"])
            assert pool.c.plan_job(doc)["decision"] == "WAIT_FOR_ELIGIBLE_WORKER"
            doc["spec"]["security"] = {"allowedTrust": "owned-and-friends"}
            job = pool.wait(pool.c.start_job(doc)["id"])
            assert job["status"] == "done", job
            assert [d["name"] for d in job["perDevice"]] == ["Lender"]
            rows = [json.loads(line) for line in pool.result_bytes(job["id"]).splitlines()]
            assert [(r["prompt"], r["response"], r["digest"]) for r in rows] == [
                (p, "reply to " + p, DIGEST) for p in ("one", "two")]
            doc["spec"]["workload"] = {**GEN, "digest": "00" * 32}
            assert pool.c.plan_job(doc)["decision"] == "WAIT_FOR_ELIGIBLE_WORKER"
        finally:
            server.shutdown()


def test_remote_and_embedding_only_models_do_not_enter_inventory(monkeypatch):
    rows = [{**MODEL, "name": tag, "details": {"format": "gguf"}} for tag in ("remote:latest", "embed:latest")]
    def response(path, body=None):
        if path == "tags":
            return {"models": rows}
        if body["model"] == "remote:latest":
            return {"remote_host": "https://example.invalid", "model_info": {"x": 1}, "capabilities": ["completion"]}
        return {"model_info": {"x": 1}, "capabilities": ["embedding"]}
    monkeypatch.setattr(ollama, "request", response)
    assert not any(ollama.discover({}).values())
