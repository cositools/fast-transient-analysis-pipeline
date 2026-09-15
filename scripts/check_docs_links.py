#!/usr/bin/env python3
"""Check local Markdown targets and, optionally, external HTTP links."""

from __future__ import annotations

import argparse
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


LINK_RE = re.compile(r"\[[^]]+\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^#{1,6}\s+(.+?)\s*$")


def markdown_files(inputs: list[Path]) -> list[Path]:
    files: set[Path] = set()
    for value in inputs:
        if value.is_dir():
            files.update(value.rglob("*.md"))
        elif value.suffix.lower() == ".md":
            files.add(value)
        else:
            raise ValueError(f"unsupported link-check input: {value}")
    return sorted(files)


def slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text).strip().lower()
    text = re.sub(r"[`*{}\[\]()]", "", text)
    text = re.sub(r"[^\w\- ]", "", text)
    return re.sub(r"[\s-]+", "-", text).strip("-")


def anchors(path: Path) -> set[str]:
    found: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING_RE.match(line)
        if match:
            found.add(slug(match.group(1)))
    return found


def external_ok(url: str) -> tuple[bool, str]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "FasTP-doc-link-check/1.0"},
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return 200 <= response.status < 400, str(response.status)
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403, 429}:
            return True, str(exc.code)
        if exc.code not in {405, 501}:
            return False, str(exc.code)
    except urllib.error.URLError as exc:
        return False, str(exc.reason)

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "FasTP-doc-link-check/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return 200 <= response.status < 400, str(response.status)
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        return False, str(exc)


def ignored_urls(path: Path | None) -> set[str]:
    if path is None:
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def check(
    files: list[Path], check_external: bool, external_ignores: set[str]
) -> list[str]:
    errors: list[str] = []
    external: set[str] = set()
    for source in files:
        text = source.read_text(encoding="utf-8")
        for raw_target in LINK_RE.findall(text):
            target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
            parsed = urllib.parse.urlsplit(target)
            if parsed.scheme in {"http", "https"}:
                if parsed.hostname not in {"localhost", "127.0.0.1"}:
                    external.add(target)
                continue
            if parsed.scheme or target.startswith("//"):
                continue
            target_path = source if not parsed.path else (source.parent / urllib.parse.unquote(parsed.path))
            target_path = target_path.resolve()
            if not target_path.exists():
                errors.append(f"{source}: missing target {target}")
                continue
            if parsed.fragment and target_path.suffix.lower() == ".md":
                if urllib.parse.unquote(parsed.fragment) not in anchors(target_path):
                    errors.append(f"{source}: missing anchor {target}")

    if check_external:
        for target in sorted(external - external_ignores):
            ok, detail = external_ok(target)
            if not ok:
                errors.append(f"external link failed ({detail}): {target}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external", action="store_true", help="check HTTP(S) targets")
    parser.add_argument(
        "--ignore-file",
        type=Path,
        help="file of exact external URLs to skip, one per line",
    )
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        files = markdown_files(args.paths)
    except ValueError as exc:
        parser.error(str(exc))
    external_ignores = ignored_urls(args.ignore_file)
    errors = check(files, args.external, external_ignores)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Checked {len(files)} Markdown files successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
