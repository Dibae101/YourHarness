"""Set the engine's version, so a release reports the version it actually is.

The release workflow already renames the desktop app after the tag with ``npm
version``. Nothing did that for the Python engine, so ``__version__`` kept whatever
was committed and every build announced that instead: v0.7.0 shipped an engine that
told ``/api/me`` it was 0.2.0.

Run it with the version only, no leading "v":

    python desktop/set_version.py 0.7.0

Paths are resolved from this file, not the working directory, so it can be called
from anywhere. It rewrites nothing when the version already matches, and it fails
loudly rather than silently leaving a file untouched -- a release that quietly keeps
the old number is the bug this exists to prevent.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (file, pattern, replacement template). Each pattern must match exactly once.
TARGETS = [
    (ROOT / "yourharness" / "__init__.py", re.compile(r'^__version__ = "[^"]*"', re.M), '__version__ = "{v}"'),
    (ROOT / "pyproject.toml", re.compile(r'^version = "[^"]*"', re.M), 'version = "{v}"'),
]

VALID = re.compile(r"^\d+\.\d+\.\d+([-.+][0-9A-Za-z.-]+)?$")


def set_version(version: str) -> list[str]:
    if not VALID.match(version):
        raise SystemExit(f"'{version}' isn't a version like 1.2.3 (pass it without a leading 'v')")
    notes = []
    for path, pattern, template in TARGETS:
        text = path.read_text(encoding="utf-8")
        new, count = pattern.subn(template.format(v=version), text)
        if count != 1:
            raise SystemExit(f"{path.name}: expected one version line, found {count} -- not guessing")
        rel = path.relative_to(ROOT).as_posix()
        if new == text:
            notes.append(f"{rel} already {version}")
            continue
        path.write_text(new, encoding="utf-8")
        notes.append(f"{rel} -> {version}")
    return notes


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit(__doc__.strip().splitlines()[0] + "\n\nusage: set_version.py 1.2.3")
    for note in set_version(argv[1]):
        print(note)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
