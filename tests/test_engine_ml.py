"""ml.evaluate: a model scored on labeled records across computers, exactly and with provenance.

The models here are pure Python (no PyTorch), so the tests need no network and no
GPU. Every distributed result is compared with the reference executor run on the
same bytes in this process.
"""

from __future__ import annotations

import hashlib
import io
import json
import time
import urllib.parse
import zipfile
from pathlib import Path

import pytest

from conftest import start_coordinator
from test_engine_e2e import Engine
from yourharness.engine import ml
from yourharness.engine import spec as jobspec
from yourharness.engine.index import Indexer
from yourharness.engine.local import reference
from yourharness.engine.models import ModelIntegrityError, fetch
from yourharness.engine.partition import plan_ranges

MODEL_PY = """\
import json, os

def load(folder):
    with open(os.path.join(folder, "weights.json")) as f:
        return json.load(f)

def predict(model, batch):
    out = []
    for x, y in batch:
        score = model["wx"] * x + model["wy"] * y
        out.append("a" if score < model["t1"] else "b" if score < model["t2"] else "c")
    return out
"""
WEIGHTS = {"wx": 1.0, "wy": 0.5, "t1": 40.0, "t2": 80.0}


def labeled(n: int = 3000) -> bytes:
    rows = []
    for i in range(n):
        x, y = (i * 37) % 100, (i * 53) % 60
        score = x + 0.5 * y
        truth = "a" if score < 42 else "b" if score < 78 else "c"  # close to the model's rule, but not the same
        if i % 97 == 0:
            truth = "c"
        rows.append(json.dumps({"id": f"s{i}", "x": x, "y": y, "label": truth}))
    return ("\n".join(rows) + "\n").encode()


def model_zip(extra: str = "") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("model.py", MODEL_PY + extra)
        z.writestr("weights.json", json.dumps(WEIGHTS))
    return buf.getvalue()


def model_folder(tmp: Path) -> dict:
    d = tmp / f"model-{time.monotonic_ns()}"
    d.mkdir()
    (d / "model.py").write_text(MODEL_PY)
    (d / "weights.json").write_text(json.dumps(WEIGHTS))
    return {"adapter": "python", "path": str(d)}


def meta_for(tmp: Path, data: bytes, fmt: str = "jsonl") -> tuple[dict, Path, list[int]]:
    path = tmp / f"data-{time.monotonic_ns()}.{fmt}"
    path.write_bytes(data)
    ix = Indexer(fmt, len(data))  # sized like the coordinator does it, so small files get fine split points
    ix.feed(data)
    meta = ix.finish()
    meta["format"] = fmt
    return meta, path, meta.pop("splits")


EVAL = {"type": "ml.evaluate", "label": "label", "features": ["x", "y"], "id": "id", "batch": 50}


def job_doc(wl: dict, **spec) -> dict:
    return jobspec.parse({"spec": {"workload": wl, "inputs": {"dataset": {"ref": "dataset:d00000000"}, "model": {"ref": "model:m00000000"}}, **spec}})


# ---------------------------------------------------------------- the scores themselves
def test_scores_are_exact_and_the_same_however_the_records_are_split(tmp_path):
    data = labeled()
    meta, path, splits = meta_for(tmp_path, data)
    model = model_folder(tmp_path)
    doc = job_doc(EVAL)
    whole = reference(doc, meta, path, model=model)["result"]
    ranges = plan_ranges(splits, meta["dataStart"], meta["size"], 2048, 5000)
    assert len(ranges) > 20
    split = reference(doc, meta, path, ranges=ranges, merge_order=list(reversed(range(len(ranges)))), model=model)["result"]
    assert split == whole
    # the same numbers, counted by hand
    rows = [json.loads(line) for line in data.splitlines()]
    pred = []
    for r in rows:
        s = WEIGHTS["wx"] * r["x"] + WEIGHTS["wy"] * r["y"]
        pred.append("a" if s < 40 else "b" if s < 80 else "c")
    truth = [r["label"] for r in rows]
    assert whole["samples"] == 3000 and whole["correct"] == sum(p == t for p, t in zip(pred, truth))
    assert whole["accuracy"] == whole["correct"] / 3000
    for c in whole["classes"]:
        tp = sum(p == t == c["label"] for p, t in zip(pred, truth))
        assert c["support"] == truth.count(c["label"]) and c["predicted"] == pred.count(c["label"])
        assert c["precision"] == tp / pred.count(c["label"]) and c["recall"] == tp / truth.count(c["label"])
    assert whole["confusion"]["labels"] == ["a", "b", "c"]
    assert sum(map(sum, whole["confusion"]["rows"])) == 3000
    assert [whole["confusion"]["rows"][i][i] for i in range(3)] == [c["correct"] for c in whole["classes"]]


def test_labels_compare_by_their_canonical_text():
    assert ml.label_key("1") == ml.label_key(1) == ml.label_key(1.0) == ml.label_key(" 1 ") == "1"
    assert ml.label_key(True) == "true" and ml.label_key(" cat ") == "cat" and ml.label_key(None) is None
    assert ml.label_key(2.5) == "2.5" and ml.label_key([1, 2]) == "[1,2]"
    np = pytest.importorskip("numpy")
    assert ml.label_key(np.int64(3)) == "3" and ml.plain(np.array([1, 2])) == [1, 2]


def test_csv_labels_are_text_and_still_match_numeric_predictions(tmp_path):
    rows = ["x,label"] + [f"{i},{int(i >= 5)}" for i in range(10)]
    meta, path, _ = meta_for(tmp_path, ("\n".join(rows) + "\n").encode(), "csv")
    d = tmp_path / "m"
    d.mkdir()
    (d / "model.py").write_text("def predict(model, batch):\n    return [1 if x >= 5 else 0 for (x,) in batch]\n")
    got = reference(job_doc({"type": "ml.evaluate", "label": "label", "features": ["x"]}), meta, path,
                    model={"adapter": "python", "path": str(d)})["result"]
    assert got["accuracy"] == 1.0 and [c["label"] for c in got["classes"]] == ["0", "1"]


def test_a_record_without_a_label_fails_the_job_or_is_left_out(tmp_path):
    meta, path, _ = meta_for(tmp_path, b'{"x": 1, "label": "a"}\n{"x": 2}\n{"x": 3, "label": "a"}\n')
    d = tmp_path / "m"
    d.mkdir()
    (d / "model.py").write_text("def predict(model, batch):\n    return ['a' for _ in batch]\n")
    model = {"adapter": "python", "path": str(d)}
    wl = {"type": "ml.evaluate", "label": "label", "features": ["x"]}
    with pytest.raises(Exception) as e:
        reference(job_doc(wl), meta, path, model=model)
    assert "byte 23" in str(e.value) and "label" in str(e.value)
    got = reference(job_doc(wl, output={"onBadRecord": "skip"}), meta, path, model=model)["result"]
    assert got["samples"] == 2 and got["unlabeled"] == 1 and got["accuracy"] == 1.0


# ---------------------------------------------------------------- model files on a worker
def test_a_model_is_checked_before_use_and_cached_by_its_digest(tmp_path):
    blob = model_zip()
    sha = hashlib.sha256(blob).hexdigest()
    calls = []

    def chunks():
        calls.append(1)
        yield blob[:100]
        yield blob[100:]

    folder = fetch(chunks, tmp_path, {"sha": sha, "format": "zip"})
    assert (folder / "model.py").read_text() == MODEL_PY and json.loads((folder / "weights.json").read_text()) == WEIGHTS
    assert fetch(chunks, tmp_path, {"sha": sha, "format": "zip"}) == folder and len(calls) == 1, "the second job uses the cache"
    with pytest.raises(ModelIntegrityError, match="not the job's"):
        fetch(lambda: iter([b"something else"]), tmp_path, {"sha": "0" * 64, "format": "py"})
    evil = io.BytesIO()
    with zipfile.ZipFile(evil, "w") as z:
        z.writestr("model.py", "def predict(m, b):\n    return b\n")
        z.writestr("../outside.py", "x = 1\n")
    raw = evil.getvalue()
    with pytest.raises(ModelIntegrityError, match="leaves its folder"):
        fetch(lambda: iter([raw]), tmp_path, {"sha": hashlib.sha256(raw).hexdigest(), "format": "zip"})
    assert not (tmp_path / "outside.py").exists() and not (tmp_path / "models" / "outside.py").exists()
    assert not [p for p in (tmp_path / "models").iterdir() if p.name.startswith(".")], "nothing half-made is left"


def test_the_job_spec_for_a_model():
    doc = job_doc({"type": "ml.evaluate", "label": "y"})
    wl = doc["spec"]["workload"]
    assert wl == {"type": "ml.evaluate", "label": "y", "features": None, "id": None, "batch": 64, "predictions": False, "labels": None, "device": "cpu"}
    assert doc["spec"]["runtime"]["kind"] == "native-python" and doc["spec"]["inputs"]["model"] == {"ref": "model:m00000000", "sha256": None}
    for bad, where in (({"spec": {"workload": {"type": "ml.evaluate"}, "inputs": {"dataset": {"ref": "dataset:d00000000"}}}}, "spec.inputs.model.ref"),
                       ({"spec": {"workload": {"type": "ml.evaluate"}, "inputs": {"dataset": {"ref": "dataset:d00000000"}, "model": {"ref": "model:m00000000", "sha256": "abc"}}}},
                        "spec.inputs.model.sha256"),
                       ({"spec": {"workload": EVAL, "inputs": {"dataset": {"ref": "dataset:d00000000"}, "model": {"ref": "model:m00000000"}},
                                  "security": {"allowedTrust": "owned-and-friends"}}}, "spec.security.allowedTrust"),
                       ({"spec": {"workload": {"type": "batch.aggregate"}, "inputs": {"dataset": {"ref": "dataset:d00000000"}, "model": {"ref": "model:m00000000"}}}},
                        "spec.inputs.model")):
        with pytest.raises(jobspec.SpecError) as e:
            jobspec.parse(bad)
        assert e.value.path == where


# ---------------------------------------------------------------- across computers
@pytest.fixture(scope="module")
def pool(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ml")
    coord = start_coordinator(tmp)
    coord.agent("Alpha", threads=2, extra=("--no-tasks",))
    coord.agent("Beta", threads=2, extra=("--no-tasks",))
    eng = Engine(coord)
    eng.ready({"Alpha", "Beta"})
    eng.tmp = tmp
    yield eng
    for a in coord.agents:
        a.stop()
    coord.proc.stop()


def upload_model(eng, name: str, data: bytes, adapter: str | None = None) -> dict:
    q = {"name": name, **({"adapter": adapter} if adapter else {})}
    return eng.c.request("PUT", "/api/engine/models?" + urllib.parse.urlencode(q), data=data,
                         headers={"Content-Type": "application/octet-stream", "Content-Length": str(len(data))})["model"]


def evaluate(eng, wl: dict, ds: dict, model: dict, timeout: float = 120, **spec) -> dict:
    inputs = {"dataset": {"ref": ds["ref"]}, "model": {"ref": model["ref"]}}
    job = eng.c.request("POST", "/api/engine/jobs", {"job": eng.job_doc(wl, ds, inputs=inputs, **spec)})["job"]
    return eng.wait(job["id"], timeout=timeout)


def test_a_model_is_scored_across_computers_and_checked_on_another_one(pool, tmp_path):
    data = labeled()
    ds = pool.upload("labeled.jsonl", data)
    model = upload_model(pool, "classifier.zip", model_zip())
    assert model["adapter"] == "python" and model["format"] == "zip" and model["sha"] == hashlib.sha256(model_zip()).hexdigest()
    wl = {**EVAL, "predictions": True}
    job = evaluate(pool, wl, ds, model, mode="distributed", partitioning={"targetBytes": 8 * 1024}, output={"spotCheck": 2})
    assert job["status"] == "done", job
    doc = pool.get(job["id"])["spec"]
    assert doc["spec"]["inputs"]["model"]["sha256"] == model["sha"], "the job is pinned to exactly this model"
    meta, path, _ = meta_for(tmp_path, data)
    want = reference(job_doc(wl), meta, path, model=model_folder(tmp_path))
    got = job["result"]
    assert {k: got[k] for k in ("samples", "correct", "accuracy", "classes", "macro", "weighted", "confusion", "unlabeled")} == \
        {k: want["result"][k] for k in ("samples", "correct", "accuracy", "classes", "macro", "weighted", "confusion", "unlabeled")}
    assert got["provenance"]["model"]["sha256"] == model["sha"] and got["provenance"]["dataset"]["sha256"] == ds["sha"]
    assert got["latency"]["batches"] > 0
    assert {d["name"] for d in job["perDevice"]} == {"Alpha", "Beta"}
    records = [json.loads(line) for line in pool.result_bytes(job["id"]).decode().splitlines()]
    assert records == want["records"] and records[0] == {"id": "s0", "label": records[0]["label"], "prediction": records[0]["prediction"],
                                                         "correct": records[0]["label"] == records[0]["prediction"]}
    # the spot check ran each sampled partition again on the computer that hadn't computed it
    assert job["validation"]["level"] == "spot-checked" and "run again on" in job["validation"]["checks"][-1], job["validation"]
    full = pool.get(job["id"])
    by_partition = {f"{job['id']}-p{p['idx']}": p["device"] for p in full["partitions"]}
    verified = [e for e in full["events"] if e["type"] == "verified"]
    assert len(verified) == 2
    assert all(e["data"]["device"] != by_partition[e["partition"]] for e in verified), (verified, by_partition)


def test_a_model_that_raises_says_which_line(pool):
    ds = pool.upload("few.jsonl", b"".join(json.dumps({"x": i, "label": "a"}).encode() + b"\n" for i in range(50)))
    code = "def predict(model, batch):\n    out = []\n    for (x,) in batch:\n        out.append(1 / (x - 7))\n    return out\n"
    model = upload_model(pool, "broken.py", code.encode())
    job = evaluate(pool, {"type": "ml.evaluate", "label": "label", "features": ["x"], "batch": 10}, ds, model, retry={"maxAttempts": 2})
    assert job["status"] == "failed" and "ZeroDivisionError" in job["error"] and "line 4 of the model's model.py" in job["error"], job
    assert model["format"] == "py"


def test_a_model_changed_on_the_coordinators_disk_is_refused(pool):
    ds = pool.upload("tamper.jsonl", labeled(200))
    model = upload_model(pool, "tamper.zip", model_zip(extra=f"\n# {time.time_ns()}\n"))  # a model no computer has cached yet
    art = Path(pool.tmp) / "data" / "engine" / "artifacts" / model["sha"][:2] / model["sha"]
    assert art.is_file()
    art.write_bytes(model_zip(extra="\n# something else\n"))
    job = evaluate(pool, EVAL, ds, model, retry={"maxAttempts": 2})
    assert job["status"] == "failed" and "it was not used" in job["error"], job


def test_your_python_is_spot_checked_on_another_computer_and_nondeterminism_is_caught(pool):
    steady = pool.run({"type": "python.map", "code": "def process(x):\n    return x * 3\n"}, items=list(range(40)), mode="distributed",
                      partitioning={"itemsPerPartition": 4}, output={"spotCheck": 3})
    assert steady["status"] == "done" and steady["result"]["value"] == [x * 3 for x in range(40)], steady
    assert steady["validation"]["level"] == "spot-checked" and "run again on" in steady["validation"]["checks"][-1]
    shaky = pool.run({"type": "python.map", "code": "import random\ndef process(x):\n    return random.random()\n"}, items=list(range(40)),
                     mode="distributed", partitioning={"itemsPerPartition": 4}, output={"spotCheck": 2})
    assert shaky["status"] == "failed" and "Spot check failed" in shaky["error"] and "Nothing was published" in shaky["error"], shaky
    assert shaky["result"] is None, "a result that failed its check is never shown"


def test_models_from_the_command_line_and_the_python_sdk(pool, tmp_path, monkeypatch):
    import os
    import subprocess
    import sys

    from yourharness import jobs

    f = tmp_path / "rule.py"
    f.write_text("def predict(model, batch):\n    return ['b' if x > 50 else 'a' for (x, _y) in batch]\n")
    env = {**os.environ, "HARNESS_URL": pool.coord.base, "HARNESS_TOKEN": pool.c.token}
    harness = [sys.executable, "-m", "yourharness"]
    added = subprocess.run(harness + ["model", "add", str(f)], env=env, capture_output=True, text=True, check=True).stdout
    ref = added.split()[1].rstrip(":")
    assert ref.startswith("model:m") and "sha256" in added
    assert ref in subprocess.run(harness + ["model", "ls"], env=env, capture_output=True, text=True, check=True).stdout
    monkeypatch.setenv("HARNESS_URL", pool.coord.base)
    monkeypatch.setenv("HARNESS_TOKEN", pool.c.token)
    jobs._client = None
    ds = pool.upload("sdk.jsonl", labeled(400))
    got = jobs.evaluate(ref, ds["ref"], label="label", features=["x", "y"], id="id", predictions=True)
    assert got["samples"] == 400 and 0 < got["accuracy"] < 1 and len(got["predictions"]) == 400
    assert got["provenance"]["model"]["ref"] == ref
    bad = subprocess.run(harness + ["model", "add", str(tmp_path / "nothing.txt")], env=env, capture_output=True, text=True)
    assert bad.returncode != 0
