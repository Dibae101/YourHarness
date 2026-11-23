"""Keyed shuffles: records with the same key meet on one computer.

A group-by with "shuffle" and a join run in two stages. In the first, every
partition sorts its records (or its partial group totals) into B buckets by a stable
hash of the key and uploads one file per bucket. In the second, bucket b's partition
reads bucket b's files from every first-stage partition, so it sees every record of
each of its keys and no other key, and combines them: the totals per group, or the
joined records. The hash is SHA-256 of the key's text, so a key lands in the same
bucket on every computer and with every Python version.

The coordinator runs the same functions to spot-check partitions (they are the job
engine's own code, never yours), and compares file digests: every file is written
the same way, byte for byte, wherever it is made.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable, Iterable

from .ops import GroupByOp, OpError, group_key
from .records import MISSING, get_field, to_number

FLUSH_BYTES = 8 * 2**20
MAX_RIGHT_RECORDS = 5_000_000  # one bucket of the right side is held in memory while joining


def bucket_of(key: str, n: int) -> int:
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big") % n


def canon(v) -> bytes:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def line(v) -> bytes:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"


class BucketFiles:
    """Lines written to one file per bucket, hashed on the way; with folder=None only the hashes are kept."""

    def __init__(self, folder: Path | None, prefix: str):
        self.folder, self.prefix = folder, prefix
        self.buf: dict[int, bytearray] = {}
        self.hash: dict = {}  # bucket -> hashlib.sha256 of everything written to it
        self.size: dict[int, int] = {}
        self.lines: dict[int, int] = {}
        self.buffered = 0

    def name(self, b: int) -> str:
        return f"{self.prefix}{b}.jsonl"

    def write(self, b: int, data: bytes) -> None:
        self.buf.setdefault(b, bytearray()).extend(data)
        self.lines[b] = self.lines.get(b, 0) + 1
        self.buffered += len(data)
        if self.buffered >= FLUSH_BYTES:
            self.flush()

    def flush(self) -> None:
        for b, data in self.buf.items():
            if not data:
                continue
            self.hash.setdefault(b, hashlib.sha256()).update(data)
            self.size[b] = self.size.get(b, 0) + len(data)
            if self.folder is not None:
                with open(self.folder / self.name(b), "ab") as f:
                    f.write(data)
        self.buf = {}
        self.buffered = 0

    def close(self) -> dict[str, dict]:
        """{file name: {"sha", "size", "records", "bucket"}} for every bucket that got a line."""
        self.flush()
        return {self.name(b): {"sha": h.hexdigest(), "size": self.size[b], "records": self.lines[b], "bucket": b}
                for b, h in sorted(self.hash.items())}


# ------------------------------------------------------------------ group-by
def groupby_buckets(state: dict, n: int) -> dict[str, bytes]:
    """A first-stage partition's group totals, split into one file per bucket: {name: bytes}."""
    split: dict[int, dict] = {}
    for k, mss in state["groups"].items():
        split.setdefault(bucket_of(k, n), {})[k] = mss
    return {f"g{b}.json": canon({"groups": dict(sorted(g.items()))}) for b, g in sorted(split.items())}


def groupby_reduce(wl: dict, blobs: Iterable[bytes], write: Callable[[dict], None]) -> dict:
    """Bucket b's partial totals from every partition: combined, one record per group, sorted by key."""
    states = [{"seen": 0, "matched": 0, "groups": json.loads(b)["groups"]} for b in blobs]
    merged = GroupByOp.merge(wl, states)
    rows = GroupByOp.final(wl, merged)["groups"]
    for r in rows:
        write(r)
    return {"groups": len(rows)}


# ------------------------------------------------------------------ join
def _canon_value(v):
    if v is MISSING or v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    n = to_number(v) if isinstance(v, (int, float, str)) else None
    if n is not None:
        if isinstance(n, float):
            return str(int(n)) if n.is_integer() else repr(n)
        return str(n)
    if isinstance(v, str):
        return v
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def join_key(rec, fields: list[str]) -> str | None:
    """The key's text, with numbers and text that reads as a number matched alike ("7" joins 7); None when a key field is missing."""
    vals = [_canon_value(get_field(rec, f)) for f in fields]
    if any(v is None for v in vals):
        return None
    return json.dumps(vals, ensure_ascii=False, separators=(",", ":"))


class JoinMapper:
    """First stage of a join: each record goes to its key's bucket file, tagged with its side."""

    resumable = False

    def __init__(self, wl: dict, side: str, n: int, files: BucketFiles):
        self.fields = wl["on"] if side == "L" else wl["rightOn"]
        self.keep_keyless = side == "L" and wl["how"] == "left"
        self.n, self.files = n, files
        self.records = self.keyless = 0

    def feed(self, rec) -> None:
        self.records += 1
        k = join_key(rec, self.fields)
        if k is None:
            self.keyless += 1
            if not self.keep_keyless:
                return
            self.files.write(0, line({"k": None, "r": rec}))
            return
        self.files.write(bucket_of(k, self.n), line({"k": k, "r": rec}))

    def state(self) -> dict:
        return {"records": self.records, "keyless": self.keyless}


def merge_records(left: dict, right: dict, right_keys: list[str]) -> dict:
    out = dict(left) if isinstance(left, dict) else {"value": left}
    if not isinstance(right, dict):
        right = {"value": right}
    for k, v in right.items():
        if k in right_keys:
            continue
        out[f"right_{k}" if k in out else k] = v
    return out


def join_reduce(wl: dict, right_lines: Iterable[bytes], left_lines: Iterable[bytes], write: Callable[[dict], None]) -> dict:
    """Bucket b of both sides: every left record with each right record of the same key (and, for a left join, alone if none)."""
    table: dict[str, list] = {}
    rights = 0
    for raw in right_lines:
        if not raw.strip():
            continue
        item = json.loads(raw)
        table.setdefault(item["k"], []).append(item["r"])
        rights += 1
        if rights > MAX_RIGHT_RECORDS:
            raise OpError(f"one bucket of the right dataset holds over {MAX_RIGHT_RECORDS:,} records; join with more buckets")
    left_n = matched = out = 0
    for raw in left_lines:
        if not raw.strip():
            continue
        item = json.loads(raw)
        left_n += 1
        rows = table.get(item["k"]) if item["k"] is not None else None
        if rows:
            matched += 1
            for r in rows:
                write(merge_records(item["r"], r, wl["rightOn"]))
                out += 1
        elif wl["how"] == "left":
            write(item["r"] if isinstance(item["r"], dict) else {"value": item["r"]})
            out += 1
    return {"left": left_n, "right": rights, "matched": matched, "out": out}


def lines_of(chunks: Iterable[bytes]) -> Iterable[bytes]:
    """The lines of a stream of byte chunks."""
    rest = b""
    for c in chunks:
        rest += c
        *done, rest = rest.split(b"\n")
        yield from done
    if rest:
        yield rest


def merge_counts(states: list[dict]) -> dict:
    """The totals of a shuffle's stages, summed (they are counts)."""
    out: dict[str, int] = {}
    for s in states:
        for k, v in s.items():
            if isinstance(v, int) and not isinstance(v, bool):
                out[k] = out.get(k, 0) + v
    return out


def groupby_map_state(op: GroupByOp, n: int) -> tuple[dict, dict[str, bytes]]:
    """What a first-stage group-by partition reports (small) and the bucket files it uploads."""
    st = op.state()
    files = groupby_buckets(st, n)
    return {"seen": st["seen"], "matched": st["matched"], "groups": len(st["groups"]), "files": len(files)}, files


__all__ = ["BucketFiles", "JoinMapper", "bucket_of", "group_key", "groupby_buckets", "groupby_map_state", "groupby_reduce",
           "join_key", "join_reduce", "lines_of", "merge_counts", "merge_records"]
