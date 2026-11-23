"""Finding record boundaries in one pass over a dataset, while it is being stored.

The indexer sees every byte once, in order, as the dataset is uploaded or copied in.
About every `step` bytes it records the offset just after the next newline that ends
a record. For CSV that means a newline outside quotes, so a quoted field that spans
lines is never cut. Partitions are later cut only at these offsets (partition.py).

It also finds the CSV header, skips a UTF-8 byte-order mark, and counts lines.
"""

from __future__ import annotations

import math

from .records import parse_header

DEFAULT_STEP = 64 * 1024
MAX_SPLITS = 100_000
BOM = b"\xef\xbb\xbf"


def step_for(size: int | None) -> int:
    if not size:
        return DEFAULT_STEP
    return max(4096, math.ceil(size / MAX_SPLITS))


class Indexer:
    def __init__(self, fmt: str, size_hint: int | None = None, step: int | None = None):
        self.fmt = fmt
        self.step = step or step_for(size_hint)
        self.pos = 0  # bytes seen so far
        self.splits: list[int] = []
        self.newlines = 0
        self.quotes = 0  # CSV: quote characters seen so far
        self.has_quotes = False
        self.last = b""
        self.data_start: int | None = None if fmt == "csv" else 0
        self.head = b""  # the first bytes, for the BOM and the CSV header
        self.header: list[str] | None = None
        self.mark: int | None = None  # look for the next boundary after this offset

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        p = self.pos
        self.pos += len(chunk)
        self.newlines += chunk.count(b"\n")
        self.last = chunk[-1:]
        if self.fmt == "csv":
            self._feed_csv(chunk, p)
            return
        if p == 0 and chunk.startswith(BOM):
            self.data_start = 3  # a byte-order mark: the first record starts after it
        if self.mark is None:
            self.mark = (self.data_start or 0) + self.step
        self._marks(chunk, p, lambda i, nl: True)

    def _marks(self, chunk: bytes, p: int, outside) -> None:
        end = p + len(chunk)
        while self.mark is not None and self.mark < end:
            i = max(0, self.mark - p)
            nl = chunk.find(b"\n", i)
            while nl >= 0 and not outside(i, nl):
                i = nl + 1
                nl = chunk.find(b"\n", i)
            if nl < 0:
                self.mark = end  # keep looking at the start of the next chunk
                return
            split = p + nl + 1
            self.splits.append(split)
            self.mark = split + self.step

    def _feed_csv(self, chunk: bytes, p: int) -> None:
        q_before = self.quotes
        n_quotes = chunk.count(b'"')
        if n_quotes:
            self.has_quotes = True
        self.quotes += n_quotes
        state = {"upto": 0, "q": q_before}

        def outside(i: int, nl: int) -> bool:  # is this newline outside quotes?
            state["q"] += chunk.count(b'"', state["upto"], nl)
            state["upto"] = nl
            return state["q"] % 2 == 0

        if self.data_start is None:  # still reading the header
            self.head += chunk[: max(0, 1024 * 1024 - len(self.head))]
            i = 0
            while True:
                nl = chunk.find(b"\n", i)
                if nl < 0:
                    if len(self.head) >= 1024 * 1024:
                        raise ValueError("the CSV header is longer than 1 MB, or the file has no line breaks")
                    return
                if outside(i, nl):
                    self.data_start = p + nl + 1
                    line = self.head[: self.data_start]
                    self.header = parse_header(line.rstrip(b"\r\n"))
                    self.mark = self.data_start + self.step
                    break
                i = nl + 1
        self._marks(chunk, p, outside)

    def finish(self) -> dict:
        size = self.pos
        if self.fmt == "csv" and self.data_start is None:
            if size == 0:
                raise ValueError("the CSV file is empty")
            self.header = parse_header(self.head.rstrip(b"\r\n"))  # a header with no data after it
            self.data_start = size
        start = self.data_start or 0
        splits = [s for s in self.splits if start < s < size]
        if len(splits) > MAX_SPLITS:
            k = math.ceil(len(splits) / MAX_SPLITS)
            splits = splits[k - 1::k]
        lines = self.newlines + (1 if size and self.last != b"\n" else 0)
        if self.fmt == "csv":
            lines -= 1  # the header
        out = {"splits": splits, "dataStart": start, "size": size, "lines": max(0, lines), "step": self.step}
        if self.fmt == "csv":
            out["header"] = self.header
            out["quoted"] = self.has_quotes  # a quoted field might span lines: no operators that read across partitions
        return out
