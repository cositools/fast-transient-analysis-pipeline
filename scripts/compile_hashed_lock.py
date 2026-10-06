#!/usr/bin/env python3
"""Replace reviewed VCS inputs with release wheels and compile a hashed lock."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse


VCS_REQUIREMENT_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*@\s*git\+https://\S+$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def load_artifacts(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise SystemExit("unsupported artifact manifest")
    result = {}
    for artifact in data.get("artifacts", []):
        name = canonical_name(artifact.get("distribution", ""))
        filename = artifact.get("filename", "")
        digest = artifact.get("sha256", "")
        if not name or Path(filename).name != filename or not SHA256_RE.fullmatch(digest):
            raise SystemExit("invalid artifact manifest entry")
        result[name] = artifact
    return result


def render_input(source: str, artifacts: dict[str, dict], asset_base_url: str) -> str:
    rendered = []
    used = set()
    for line in source.splitlines():
        match = VCS_REQUIREMENT_RE.fullmatch(line.strip())
        if not match:
            rendered.append(line)
            continue
        name = canonical_name(match.group("name"))
        artifact = artifacts.get(name)
        if artifact is None:
            raise SystemExit(f"no release wheel recorded for VCS dependency {name}")
        rendered.append(
            f"{match.group('name')} @ {asset_base_url.rstrip('/')}/"
            f"{artifact['filename']}#sha256={artifact['sha256']}"
        )
        used.add(name)
    if not used:
        raise SystemExit("direct requirements contain no VCS dependency to replace")
    return "\n".join(rendered) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--artifact-manifest", required=True, type=Path)
    parser.add_argument("--asset-base-url", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args()
    parsed_url = urlparse(args.asset_base_url)
    if parsed_url.scheme != "https" or not parsed_url.netloc or parsed_url.query:
        raise SystemExit("asset base URL must be an HTTPS URL without a query")
    artifacts = load_artifacts(args.artifact_manifest)
    rendered = render_input(
        args.input.read_text(encoding="utf-8"), artifacts, args.asset_base_url
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.render_only:
        args.output.write_text(rendered, encoding="utf-8")
        return 0
    with tempfile.TemporaryDirectory() as directory:
        release_input = Path(directory) / "release.in"
        release_input.write_text(rendered, encoding="utf-8")
        subprocess.run(
            [
                args.python,
                "-m",
                "piptools",
                "compile",
                "--generate-hashes",
                "--allow-unsafe",
                "--resolver=backtracking",
                "--output-file",
                str(args.output),
                str(release_input),
            ],
            check=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
