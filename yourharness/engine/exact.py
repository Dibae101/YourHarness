"""Exact sums that don't depend on how the numbers were split or ordered.

Adding floating-point numbers in a different order gives a slightly different
total, so a sum computed in pieces on several computers would not match the same
sum computed on one. Every finite float is a whole multiple of 2**-1074, so here a
sum is kept exactly as one Python integer counting those units, and integers stay
integers. Combining pieces adds integers, which is exact, and the total is rounded
to a float only at the end: the correctly rounded sum of all the numbers, identical
for any split and any order, with no overflow along the way.

NaN and infinities are counted apart, and every state is plain JSON (big integers
as strings, so JavaScript workers can carry them too).
"""

from __future__ import annotations

import math
from fractions import Fraction

NAN, POS_INF, NEG_INF = "NaN", "Infinity", "-Infinity"
SCALE_BITS = 1074  # 2**-1074 is the smallest positive float
ONE = 1 << SCALE_BITS


def scaled(x: float) -> int:
    """x * 2**1074 exactly, for a finite float x."""
    num, den = x.as_integer_ratio()
    return num << (SCALE_BITS - (den.bit_length() - 1))


def number_out(v):
    """A number as JSON can carry it: NaN and infinities become strings."""
    if isinstance(v, float):
        if v != v:
            return NAN
        if v == math.inf:
            return POS_INF
        if v == -math.inf:
            return NEG_INF
    return v


def number_in(v):
    if v == NAN:
        return math.nan
    if v == POS_INF:
        return math.inf
    if v == NEG_INF:
        return -math.inf
    return v


def _to_float(frac: Fraction) -> float:
    try:
        return float(frac)  # correctly rounded
    except OverflowError:
        return math.inf if frac > 0 else -math.inf


class ExactSum:
    __slots__ = ("ints", "units", "floats", "nan", "pinf", "ninf", "n")

    def __init__(self):
        self.ints = 0  # the integers, as they are
        self.units = 0  # the finite floats, in units of 2**-1074
        self.floats = 0  # values that weren't integers: the total is a float if there was any
        self.nan = self.pinf = self.ninf = 0
        self.n = 0

    def add(self, v) -> None:
        """Add an int or a float (not a bool)."""
        self.n += 1
        if isinstance(v, int):
            self.ints += v
            return
        self.floats += 1
        if v != v:
            self.nan += 1
        elif v == math.inf:
            self.pinf += 1
        elif v == -math.inf:
            self.ninf += 1
        else:
            self.units += scaled(v)

    def merge(self, other: "ExactSum") -> None:
        self.ints += other.ints
        self.units += other.units
        self.floats += other.floats
        self.nan += other.nan
        self.pinf += other.pinf
        self.ninf += other.ninf
        self.n += other.n

    def _special(self):
        if self.nan or (self.pinf and self.ninf):
            return NAN
        if self.pinf:
            return POS_INF
        if self.ninf:
            return NEG_INF
        return None

    def exact(self) -> Fraction:
        return Fraction((self.ints << SCALE_BITS) + self.units, ONE)

    def value(self):
        """The correctly rounded total: an int if every value was an int, else a float (NaN/inf as strings)."""
        special = self._special()
        if special:
            return special
        if not self.floats:
            return self.ints
        return number_out(_to_float(self.exact()))

    def mean(self):
        """The correctly rounded mean of the values, or None if there were none."""
        if self.n == 0:
            return None
        special = self._special()
        if special:
            return special
        return number_out(_to_float(self.exact() / self.n))

    def float_value(self) -> float:
        v = self.value()
        return number_in(v) if isinstance(v, str) else float(v)

    def to_state(self) -> dict:
        return {"i": str(self.ints), "u": str(self.units), "f": self.floats, "x": [self.nan, self.pinf, self.ninf], "n": self.n}

    @classmethod
    def from_state(cls, st: dict) -> "ExactSum":
        s = cls()
        s.ints = int(st.get("i", "0"))
        s.units = int(st.get("u", "0"))
        s.floats = int(st.get("f", 0))
        s.nan, s.pinf, s.ninf = (list(st.get("x", [0, 0, 0])) + [0, 0, 0])[:3]
        s.n = int(st.get("n", 0))
        return s
