"""Fit a linear or ridge regression from mergeable, centered sums of products."""

from __future__ import annotations

import math

from .ops import OpError
from .records import get_field, to_number


class TrainOp:
    def __init__(self, workload: dict, bad: str = "fail"):
        self.fields = [*workload["features"], workload["target"]]
        self.bad = bad
        self.rows = self.skipped = 0
        self.mean = [0.0] * len(self.fields)
        # Lower triangle of the sum of centered outer products, including the target.
        self.scatter = [[0.0] * (i + 1) for i in range(len(self.fields))]

    def feed_at(self, offset: int, record: dict) -> None:
        values = []
        for field in self.fields:
            value = to_number(get_field(record, field))
            try:
                valid = value is not None and math.isfinite(value)
            except OverflowError:
                valid = False
            if not valid:
                if self.bad == "skip":
                    self.skipped += 1
                    return
                raise OpError(f"Training field {field!r} needs a finite number at byte {offset}.")
            values.append(float(value))
        self.combine(1, values)

    def combine(self, rows: int, mean: list, scatter: list | None = None) -> None:
        if not rows:
            return
        total = self.rows + rows
        delta = [v - m for v, m in zip(mean, self.mean)]
        weight = self.rows * rows / total
        for i, d in enumerate(delta):
            self.mean[i] += d * (rows / total)
            for j in range(i + 1):
                self.scatter[i][j] += (scatter[i][j] if scatter else 0) + (weight * d) * delta[j]
        self.rows = total

    def state(self) -> dict:
        if not all(math.isfinite(x) for row in [self.mean, *self.scatter] for x in row):
            raise OpError("Training values exceed numeric range. Scale the columns and submit again.")
        return {"rows": self.rows, "skipped": self.skipped, "mean": self.mean, "scatter": self.scatter}


def merge(wl: dict, states: list[dict]) -> dict:
    op = TrainOp(wl)
    for s in states:
        if len(s["mean"]) != len(op.fields) or [len(row) for row in s["scatter"]] != list(range(1, len(op.fields) + 1)):
            raise OpError("A training partition returned an unexpected number of columns.")
        op.combine(s["rows"], s["mean"], s["scatter"])
        op.skipped += s["skipped"]
    return op.state()


def fit(wl: dict, state: dict) -> dict:
    import numpy as np

    if state["rows"] < 2:
        raise OpError("Training needs at least two complete numeric rows.")
    means = np.array(state["mean"])
    n = len(wl["features"])
    cross = np.zeros((n, n))
    for i, row in enumerate(state["scatter"][:-1]):
        cross[i, :i + 1] = row
        cross[:i + 1, i] = row
    rhs = np.array(state["scatter"][-1][:-1])
    matrix = cross + np.eye(n) * wl["ridge"]
    # Normalize each column before solving so features can use different units.
    scale = np.sqrt(np.maximum(np.diag(matrix), 0))
    scale[scale == 0] = 1
    coef, _, rank, _ = np.linalg.lstsq((matrix / scale[:, None]) / scale, rhs / scale, rcond=None)
    coef /= scale
    intercept = float(means[-1] - means[:-1] @ coef)
    residual = float(state["scatter"][-1][-1] - 2 * (coef @ rhs) + coef @ cross @ coef)
    if not (math.isfinite(intercept) and math.isfinite(residual) and np.isfinite(coef).all()):
        raise OpError("Training exceeded numeric range. Scale the columns and submit again.")
    return {"format": "yourharness.linear.v1", "target": wl["target"], "features": wl["features"],
            "coefficients": coef.tolist(), "intercept": intercept, "ridge": wl["ridge"],
            "rows": state["rows"], "skipped": state["skipped"], "rank": int(rank),
            "trainingRMSE": math.sqrt(max(0.0, residual) / state["rows"])}
