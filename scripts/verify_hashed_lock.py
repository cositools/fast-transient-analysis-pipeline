#!/usr/bin/env python3
"""Fail unless every installable entry in a pip lock has a SHA-256 hash."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


HASH_RE = re.compile(r"--hash=sha256:[0-9a-f]{64}(?:\s|$)")


def logical_lines(text: str) -> list[str]:
    result = []
    current = ""
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        current = f"{current} {stripped}".strip()
        if current.endswith("\\"):
            current = current[:-1].rstrip()
        else:
            result.append(current)
            current = ""
    if current:
        result.append(current)
    return result


def verify(path: Path) -> list[str]:
    failures = []
    for entry in logical_lines(path.read_text(encoding="utf-8")):
        if entry.startswith("-"):
            continue
        if "git+" in entry:
            failures.append(f"mutable installer input: {entry}")
        elif not HASH_RE.search(entry):
            failures.append(f"missing SHA-256: {entry}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("locks", nargs="+", type=Path)
    args = parser.parse_args()
    failures = [f"{path}: {failure}" for path in args.locks for failure in verify(path)]
    if failures:
        raise SystemExit("\n".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
