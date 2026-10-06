#!/usr/bin/env python3
"""Build deterministic wheel candidates from the reviewed VCS commits.

The output directory is suitable for upload to the GitHub Release named by
``release/vcs-artifacts.json``. The script never updates dependencies and
records a SHA-256 for every wheel it creates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
from pathlib import Path


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
PYTHON_RE = re.compile(r"^[0-9]+\.[0-9]+$")
RELEASE_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_config(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or data.get("module_version") != "1.0.0":
        raise SystemExit("artifact config must describe schema 1 / module 1.0.0")
    packages = data.get("packages")
    if not isinstance(packages, list) or not packages:
        raise SystemExit("artifact config must contain packages")
    for package in packages:
        if not NAME_RE.fullmatch(package.get("distribution", "")):
            raise SystemExit("invalid distribution name in artifact config")
        if not PYTHON_RE.fullmatch(package.get("python", "")):
            raise SystemExit("invalid Python version in artifact config")
        if not package.get("source", "").startswith("git+https://"):
            raise SystemExit("VCS sources must use git+https")
        if not SHA_RE.fullmatch(package.get("revision", "")):
            raise SystemExit("VCS revisions must be full 40-character SHAs")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("release/vcs-artifacts.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--release-tag",
        help="override the recorded tag, for example for a platform PoC release",
    )
    parser.add_argument(
        "--python",
        action="append",
        default=[],
        metavar="VERSION=EXECUTABLE",
        help="interpreter used for a runtime, for example 3.12=python3.12",
    )
    args = parser.parse_args()
    interpreters = {}
    for value in args.python:
        if value.count("=") != 1:
            raise SystemExit("--python must use VERSION=EXECUTABLE")
        version, executable = value.split("=", 1)
        if not PYTHON_RE.fullmatch(version) or not executable:
            raise SystemExit("invalid --python mapping")
        interpreters[version] = executable

    config = load_config(args.config)
    release_tag = args.release_tag or config["release_tag"]
    if not RELEASE_TAG_RE.fullmatch(release_tag):
        raise SystemExit("invalid release tag")
    args.output.mkdir(parents=True, exist_ok=True)
    records = []
    build_env = dict(os.environ)
    build_env.update({"PYTHONHASHSEED": "0", "SOURCE_DATE_EPOCH": "315532800"})
    for package in config["packages"]:
        version = package["python"]
        executable = interpreters.get(version, f"python{version}")
        before = {path.name for path in args.output.glob("*.whl")}
        source = f"{package['source']}@{package['revision']}"
        subprocess.run(
            [executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", args.output, source],
            check=True,
            env=build_env,
        )
        created = sorted(
            (path for path in args.output.glob("*.whl") if path.name not in before),
            key=lambda path: path.name,
        )
        if len(created) != 1:
            raise SystemExit(
                f"expected one new wheel for {package['distribution']}, found {len(created)}"
            )
        wheel = created[0]
        records.append(
            {
                "distribution": package["distribution"],
                "python": version,
                "source": package["source"],
                "revision": package["revision"],
                "filename": wheel.name,
                "sha256": sha256(wheel),
            }
        )

    manifest = {
        "schema_version": 1,
        "module_version": config["module_version"],
        "release_tag": release_tag,
        "target": config["target"],
        "build_host": {
            "system": platform.system().lower(),
            "machine": platform.machine().lower(),
            "release": platform.release(),
        },
        "artifacts": sorted(records, key=lambda item: item["distribution"].casefold()),
    }
    manifest_path = args.output / "artifact-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    checksum_paths = sorted(args.output.glob("*.whl"), key=lambda path: path.name)
    checksum_paths.append(manifest_path)
    (args.output / "SHA256SUMS").write_text(
        "".join(f"{sha256(path)}  {path.name}\n" for path in checksum_paths),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
