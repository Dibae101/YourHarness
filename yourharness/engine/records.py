"""Reading records out of a stream of bytes, in bounded memory.

A partition is a byte range that starts and ends on record boundaries (found when
the dataset was registered, see ingest.py), so it can be read on its own: no record
is cut in two, and none belongs to two partitions. The reader yields each record
with the byte offset just after it, which is what a checkpoint records.

Formats:
  text   every line is a record: {"line": "..."} (empty lines included)
  jsonl  one JSON value per line; objects are used as they are, anything else
         becomes {"value": ...}; blank lines are skipped
  csv    the header names the fields; values stay text; quoted fields may span
         lines; blank lines are skipped
"""

from __future__ import annotations

import csv
import json
from typing import Callable, Iterable, Iterator

MAX_RECORD = 64 * 1024 * 1024
FORMATS = ("text", "jsonl", "csv")
MISSING = object()


class RecordError(ValueError):
    """A record that can't be read. offset is where it starts in the dataset."""

    def __init__(self, offset: int, reason: str):
        super().__init__(f"the record at byte {offset:,} {reason}")
        self.offset = offset
        self.reason = reason


def iter_lines(chunks: Iterable[bytes], offset: int) -> Iterator[tuple[int, int, bytes]]:
    """(start, end, line) for each line of the stream, end being just past its newline.

    The last line may have no newline. Lines keep no terminator; a trailing CR is removed.
    """
    pending = b""
    pos = offset  # dataset offset of pending[0]
    for chunk in chunks:
        if not chunk:
            continue
        data = pending + chunk if pending else bytes(chunk)
        start = 0
        while True:
            nl = data.find(b"\n", start)
            if nl < 0:
                break
            line = data[start:nl]
            yield pos + start, pos + nl + 1, line[:-1] if line.endswith(b"\r") else line
            start = nl + 1
        pending = data[start:]
        pos += start
        if len(pending) > MAX_RECORD:
            raise RecordError(pos, "is longer than 64 MB")
    if pending:
        yield pos, pos + len(pending), pending[:-1] if pending.endswith(b"\r") else pending


def _text(line: bytes, start: int) -> str:
    try:
        return line.decode("utf-8")
    except UnicodeDecodeError as e:
        raise RecordError(start, f"isn't UTF-8 text ({e.reason} at character {e.start})") from None


def iter_records(fmt: str, chunks: Iterable[bytes], offset: int, header: list[str] | None = None,
                 on_bad: Callable[[RecordError], None] | None = None) -> Iterator[tuple[int, int, object]]:
    """(start, end, record) for each record in the stream, which begins at dataset offset `offset`.

    A record that can't be read raises RecordError, or, with on_bad, is passed to it and skipped.
    """
    def bad(err: RecordError) -> None:
        if on_bad is None:
            raise err
        on_bad(err)

    if fmt == "text":
        for start, end, line in iter_lines(chunks, offset):
            try:
                rec = {"line": _text(line, start)}
            except RecordError as e:
                bad(e)
                continue
            yield start, end, rec
    elif fmt == "jsonl":
        for start, end, line in iter_lines(chunks, offset):
            if not line.strip():
                continue
            try:
                value = json.loads(_text(line, start))
            except RecordError as e:
                bad(e)
                continue
            except ValueError as e:
                bad(RecordError(start, f"isn't valid JSON ({e})"))
                continue
            yield start, end, value if isinstance(value, dict) else {"value": value}
    elif fmt == "csv":
        if not header:
            raise ValueError("a CSV dataset needs its header")
        yield from _iter_csv(iter_lines(chunks, offset), header, bad)
    else:
        raise ValueError(f"unknown format {fmt}")


def _iter_csv(lines: Iterator[tuple[int, int, bytes]], header: list[str], bad) -> Iterator[tuple[int, int, object]]:
    state: dict = {"start": None, "end": None, "decode": None}

    def feed():
        for start, end, line in lines:
            if state["start"] is None:
                state["start"] = start
            state["end"] = end
            try:
                yield _text(line, start) + "\n"
            except RecordError as e:
                state["decode"] = e
                yield "\n"  # keep the reader going; this record is reported as bad below

    reader = csv.reader(feed(), strict=True)
    width = len(header)
    while True:
        state["start"], state["decode"] = None, None
        try:
            row = next(reader)
        except StopIteration:
            return
        except csv.Error as e:
            bad(RecordError(state["start"] or 0, f"isn't valid CSV ({e})"))
            continue
        if state["decode"] is not None:
            bad(state["decode"])
            continue
        if not row:  # a blank line
            continue
        if len(row) != width:
            bad(RecordError(state["start"] or 0, f"has {len(row)} fields, but the header has {width}"))
            continue
        yield state["start"], state["end"], dict(zip(header, row))


def parse_header(line: bytes) -> list[str]:
    try:
        row = next(csv.reader([line.decode("utf-8-sig")]))
    except (UnicodeDecodeError, csv.Error, StopIteration) as e:
        raise ValueError(f"the CSV header can't be read ({e})") from None
    names = [c.strip() for c in row]
    if not names or any(not n for n in names):
        raise ValueError("every column in the CSV header needs a name")
    if len(set(names)) != len(names):
        raise ValueError("two columns in the CSV header have the same name")
    return names


def get_field(record, path: str):
    """A dotted path into a record ("user.address.city"); MISSING when absent."""
    cur = record
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return MISSING
    return cur


def to_number(v):
    """An int or float for numeric work, or None when the value isn't a number.

    Text such as "12" or "3.5e2" (CSV values are text) counts; booleans don't.
    """
    if isinstance(v, bool) or v is None or v is MISSING:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            pass
        try:
            return float(s)
        except ValueError:
            return None
    return None
