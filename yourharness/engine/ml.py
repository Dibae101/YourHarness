"""ml.evaluate: a model's predictions on labeled records, scored exactly.

Each partition runs the model on its records and counts how often each true label
met each predicted label (confusion counts). They are whole numbers, so the counts
of any split add up to the counts of the whole dataset, and accuracy, precision,
recall and F1 come out the same however the records were divided. With
"predictions": true it also writes one record per sample: its id, label,
prediction and whether they agree.

The model is a file uploaded once to the coordinator. Every partition fetches it by
its SHA-256 and checks it (models.py), so every prediction comes from exactly that
model. How a model is loaded is its adapter:

  python       model.py defines predict(model, batch) -> one prediction per sample,
               and optionally load(folder) -> model (called once per partition).
               Upload model.py itself, or a .zip with model.py at the top and the
               files it reads (weights, vocabularies) beside it.
  torchscript  a TorchScript file (torch.jit.save). The features must be numeric
               fields; the prediction is the index of the largest output, named by
               the workload's "labels" when given. Needs torch in the job's Python.

A sample is a list of the "features" fields in order (numbers where they read as
numbers, since CSV values are text), or the whole record without its label and id
fields when no features are named. Labels and predictions are compared in a
canonical text form, so CSV text "1" matches a model's 1 and 1.0.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from pathlib import Path

from .ops import OpError
from .records import MISSING, RecordError, get_field, to_number

MAX_PAIRS = 100_000  # distinct (label, prediction) pairs: more means it isn't a classifier
MATRIX_LABELS = 100  # the confusion matrix is in the result up to this many labels; past that, the pairs are
MODEL_CODE = "<model code>"


class ModelCodeError(RuntimeError):
    """The model's own code raised. Retried elsewhere, like any of your code."""


def describe(e: BaseException) -> str:
    line = None
    for frame in traceback.extract_tb(e.__traceback__):
        if frame.filename == MODEL_CODE:
            line = frame.lineno
    text = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    return f"{text} (line {line} of the model's model.py)" if line else text


def plain(v):
    """A model's output as plain JSON values (NumPy and PyTorch scalars and arrays included)."""
    if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):
        try:
            return plain(v.tolist())
        except Exception:  # noqa: BLE001 - fall through to the value itself
            pass
    if hasattr(v, "item") and not isinstance(v, (str, bytes, list, dict, tuple)):
        try:
            return v.item()
        except Exception:  # noqa: BLE001
            pass
    if isinstance(v, tuple):
        return [plain(x) for x in v]
    if isinstance(v, list):
        return [plain(x) for x in v]
    if isinstance(v, dict):
        return {str(k): plain(x) for k, x in v.items()}
    if isinstance(v, float) and v != v:
        return None
    return v


def label_key(v) -> str | None:
    """The canonical text of a label or prediction; None when there isn't one."""
    v = plain(v)
    if v is MISSING or v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v in (float("inf"), float("-inf")):
            return repr(v)
        return str(int(v)) if v.is_integer() else repr(v)
    if isinstance(v, str):
        s = v.strip()
        n = to_number(s)
        return label_key(n) if n is not None else s
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _order_key(label: str):
    n = to_number(label)
    return (0, n, "") if n is not None else (1, 0, label)


# ------------------------------------------------------------------ loading a model
def python_predictor(folder: Path):
    """model.py's predict(model, batch), with load(folder) called once first when it's defined."""
    src = folder / "model.py"
    try:
        code = src.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        raise OpError(f"the model has no readable model.py ({e})") from None
    ns: dict = {"__name__": "__model__", "__file__": str(src)}
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))  # model.py may import helper modules shipped beside it
    try:
        exec(compile(code, MODEL_CODE, "exec"), ns)  # noqa: S102 - running your model's code is the point
    except SyntaxError as e:
        raise OpError(f"the model's model.py has a syntax error: {e.msg} (line {e.lineno})") from None
    except Exception as e:  # noqa: BLE001
        raise ModelCodeError(describe(e)) from None
    predict, load = ns.get("predict"), ns.get("load")
    if not callable(predict):
        raise OpError("the model's model.py doesn't define predict(model, batch)")
    try:
        model = load(str(folder)) if callable(load) else None
    except Exception as e:  # noqa: BLE001
        raise ModelCodeError(describe(e)) from None

    def run(batch: list) -> list:
        try:
            return predict(model, batch)
        except Exception as e:  # noqa: BLE001
            raise ModelCodeError(describe(e)) from None

    return run


def torchscript_predictor(folder: Path, labels: list | None, device: str):
    try:
        import torch
    except ImportError:
        raise ModelCodeError("this job's Python has no torch; run it in an environment with torch in its requirements") from None
    dev = "cpu"
    if device == "auto":
        if torch.cuda.is_available():
            dev = "cuda"
        elif getattr(getattr(torch, "backends", None), "mps", None) is not None and torch.backends.mps.is_available():
            dev = "mps"
    try:
        net = torch.jit.load(str(folder / "model.pt"), map_location=dev)
        net.eval()
    except Exception as e:  # noqa: BLE001
        raise ModelCodeError(f"couldn't load the TorchScript model: {e}") from None

    def run(batch: list) -> list:
        try:
            with torch.no_grad():
                out = net(torch.tensor(batch, dtype=torch.float32, device=dev))
            idx = out.argmax(dim=1).tolist()
        except Exception as e:  # noqa: BLE001
            raise ModelCodeError(f"the model failed on a batch: {type(e).__name__}: {e}") from None
        return [labels[i] if labels and 0 <= i < len(labels) else i for i in idx]

    return run


def load_predictor(model: dict, wl: dict):
    """model: {"adapter", "path"}: the folder models.fetch() prepared."""
    folder = Path(model["path"])
    if model["adapter"] == "python":
        return python_predictor(folder)
    if model["adapter"] == "torchscript":
        if not wl.get("features"):
            raise OpError("a TorchScript model needs the workload's features: the numeric fields it reads, in order")
        return torchscript_predictor(folder, wl.get("labels"), wl.get("device", "cpu"))
    raise OpError(f"there's no {model['adapter']} model adapter")


# ------------------------------------------------------------------ the operator
class EvaluateOp:
    def __init__(self, wl: dict, ctx: dict | None, write, predictor, bad: str = "fail"):
        self.label, self.features, self.id_field = wl["label"], wl["features"], wl["id"]
        self.size = int(wl.get("batch") or 64)
        self.write = write if wl.get("predictions") else None
        self.resumable = not wl.get("predictions")  # written records can't be resumed halfway
        self.predict = predictor
        self.bad = bad
        self.pairs: dict[str, int] = {}
        self.n = 0
        self.unlabeled = 0
        self.buf: list[tuple] = []
        self.seconds = 0.0
        self.calls = 0

    def feed(self, rec) -> None:
        self.feed_at(None, rec)

    def feed_at(self, offset, rec) -> None:
        label = get_field(rec, self.label)
        key = label_key(label)
        if key is None:
            if self.bad == "fail":
                raise RecordError(offset or 0, f"has no {self.label!r} label")
            self.unlabeled += 1
            return
        sid = get_field(rec, self.id_field) if self.id_field else offset
        self.buf.append((None if sid is MISSING else plain(sid), key, plain(label), self._sample(rec)))
        if len(self.buf) >= self.size:
            self.flush()

    def _sample(self, rec):
        if self.features is None:
            if isinstance(rec, dict):
                return {k: v for k, v in rec.items() if k not in (self.label, self.id_field)}
            return rec
        out = []
        for f in self.features:
            v = get_field(rec, f)
            n = to_number(v)
            out.append(n if n is not None else (None if v is MISSING else v))
        return out

    def flush(self) -> None:
        if not self.buf:
            return
        batch, self.buf = self.buf, []
        t0 = time.perf_counter()
        preds = plain(self.predict([b[3] for b in batch]))
        self.seconds += time.perf_counter() - t0
        self.calls += 1
        if not isinstance(preds, list) or len(preds) != len(batch):
            got = len(preds) if isinstance(preds, list) else type(preds).__name__
            raise OpError(f"predict should return one prediction per sample: it got {len(batch)} and returned {got}")
        for (sid, key, label, _sample), p in zip(batch, preds):
            pk = label_key(p)
            pk = "null" if pk is None else pk
            pair = json.dumps([key, pk], ensure_ascii=False, separators=(",", ":"))
            if pair not in self.pairs and len(self.pairs) >= MAX_PAIRS:
                raise OpError(f"there are over {MAX_PAIRS:,} different label/prediction pairs: ml.evaluate scores classifiers")
            self.pairs[pair] = self.pairs.get(pair, 0) + 1
            self.n += 1
            if self.write is not None:
                try:
                    json.dumps(p, allow_nan=False)
                except (TypeError, ValueError):
                    p = pk
                self.write({"id": sid, "label": label, "prediction": p, "correct": key == pk})

    def state(self) -> dict:
        self.flush()
        return {"pairs": dict(sorted(self.pairs.items())), "n": self.n, "unlabeled": self.unlabeled}

    def load(self, st: dict) -> None:
        self.pairs, self.n, self.unlabeled = dict(st["pairs"]), int(st["n"]), int(st["unlabeled"])

    def timings(self) -> dict:
        """Timing, reported beside the state: it differs from run to run, so it's never compared."""
        return {"predictSeconds": round(self.seconds, 6), "predictCalls": self.calls}


def merge(states: list[dict]) -> dict:
    pairs: dict[str, int] = {}
    for s in states:
        for k, v in s["pairs"].items():
            pairs[k] = pairs.get(k, 0) + int(v)
    return {"pairs": dict(sorted(pairs.items())), "n": sum(int(s["n"]) for s in states),
            "unlabeled": sum(int(s["unlabeled"]) for s in states)}


def final(wl: dict, st: dict) -> dict:
    pairs = {tuple(json.loads(k)): v for k, v in st["pairs"].items()}
    seen = {a for a, _ in pairs} | {b for _, b in pairs}
    given = [label_key(x) for x in wl.get("labels") or []]
    order = [x for i, x in enumerate(given) if x not in given[:i]]
    order += sorted(seen - set(order), key=_order_key)
    n = st["n"]
    correct = sum(v for (a, b), v in pairs.items() if a == b)
    support = {c: 0 for c in order}
    predicted = {c: 0 for c in order}
    for (a, b), v in pairs.items():
        support[a] += v
        predicted[b] += v
    classes = []
    for c in order:
        tp = pairs.get((c, c), 0)
        p = tp / predicted[c] if predicted[c] else 0.0
        r = tp / support[c] if support[c] else 0.0
        classes.append({"label": c, "support": support[c], "predicted": predicted[c], "correct": tp,
                        "precision": p, "recall": r, "f1": 2 * p * r / (p + r) if p + r else 0.0})
    k = len(classes)
    macro = {m: sum(c[m] for c in classes) / k if k else 0.0 for m in ("precision", "recall", "f1")}
    weighted = {m: sum(c[m] * c["support"] for c in classes) / n if n else 0.0 for m in ("precision", "recall", "f1")}
    out = {"samples": n, "correct": correct, "accuracy": correct / n if n else None, "unlabeled": st["unlabeled"],
           "classes": classes, "macro": macro, "weighted": weighted}
    if len(order) <= MATRIX_LABELS:
        out["confusion"] = {"labels": order, "rows": [[pairs.get((a, b), 0) for b in order] for a in order],
                            "note": "rows are the true labels, columns the predictions"}
    else:
        out["pairs"] = [[a, b, v] for (a, b), v in sorted(pairs.items())]
    return out
