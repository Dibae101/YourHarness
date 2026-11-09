"""Cutting an input into partitions with fixed identities and exact ownership.

A byte-range partition owns the records that start inside [start, end). Its edges
are record boundaries recorded when the dataset was registered, so every record
belongs to exactly one partition and no partition reads half a record. The same
dataset, split points and target size always give the same partitions, so a
partition's id means the same thing on every computer and after a restart.
"""

from __future__ import annotations

import math


class PartitionError(ValueError):
    pass


def plan_ranges(splits: list[int], data_start: int, size: int, target: int, max_parts: int = 100_000) -> list[tuple[int, int]]:
    """Contiguous [start, end) ranges covering [data_start, size), cut only at split points.

    Each range is as close to `target` bytes as the split points allow without going
    over, except where one record alone is longer than the target.
    """
    if size <= data_start:
        return []
    target = max(1, int(target))
    if (size - data_start) / target > max_parts:
        target = math.ceil((size - data_start) / max_parts)
    points = sorted({p for p in splits if data_start < p < size}) + [size]  # strictly increasing, last is size
    out: list[tuple[int, int]] = []
    cur, i = data_start, 0  # points[i] is the first point after cur
    while cur < size:
        limit = cur + target
        j = i
        while j + 1 < len(points) and points[j + 1] <= limit:  # the furthest point within the target
            j += 1
        out.append((cur, points[j]))  # if even points[i] is past the target, one long record: take it alone
        cur, i = points[j], j + 1
    check_ranges(out, data_start, size)
    return out


def check_ranges(ranges: list[tuple[int, int]], data_start: int, size: int) -> None:
    """Raises unless the ranges cover [data_start, size) exactly, in order, with no gap or overlap."""
    cur = data_start
    for start, end in ranges:
        if start != cur or end <= start:
            raise PartitionError(f"partition [{start}, {end}) doesn't follow {cur}")
        cur = end
    if size > data_start and cur != size:
        raise PartitionError(f"partitions end at {cur}, not at {size}")


def plan_items(n: int, per: int) -> list[tuple[int, int]]:
    """Item index ranges [i, j) of at most `per` items each."""
    per = max(1, int(per))
    return [(i, min(n, i + per)) for i in range(0, n, per)]


def target_bytes(size: int, slots: int, throughput_bps: float, target_s: float, lo: int = 4096, hi: int = 128 * 2**20,
                 min_parts_per_slot: int = 2) -> int:
    """How big a partition should be: about target_s seconds of work, and enough parts to keep every slot busy."""
    by_time = throughput_bps * target_s
    by_spread = size / max(1, slots * min_parts_per_slot)
    return int(max(lo, min(hi, by_time, by_spread if by_spread >= lo else lo)))
