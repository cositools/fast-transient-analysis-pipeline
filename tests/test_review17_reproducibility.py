import hashlib
import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path

import yaml


REPO = Path(__file__).resolve().parents[1]


class ReproducibilityContractTests(unittest.TestCase):
    def setUp(self):
        self.manifest = yaml.safe_load((REPO / "Manifest.yaml").read_text(encoding="utf-8"))

    def test_canonical_manifest_and_runtime_matrix(self):
        self.assertEqual(self.manifest["schema_version"], 1)
        self.assertEqual(self.manifest["kind"], "cosiflow-module")
        self.assertEqual(self.manifest["module"]["id"], "fast-transient-analysis-pipeline")
        self.assertEqual(self.manifest["module"]["version"], "1.0.0")
        self.assertEqual(self.manifest["compatibility"]["platforms"], ["linux/arm64"])
        self.assertEqual(
            self.manifest["compatibility"]["deployment_hosts"], ["ubuntu/26.04"]
        )
        versions = {
            name: environment["python"]
            for name, environment in self.manifest["environments"].items()
        }
        self.assertEqual(versions, {"cosipy": "3.12", "bct": "3.11", "nimcosipy": "3.12"})

    def test_direct_dependencies_and_lock_checksums(self):
        required = {"pyyaml", "astropy", "healpy", "mhealpy"}
        for name, environment in self.manifest["environments"].items():
            direct = (REPO / environment["direct_requirements"]).read_text(encoding="utf-8").lower()
            self.assertTrue(required.issubset(set(re.findall(r"(?m)^([a-z0-9_.-]+)", direct))), name)
            lock = REPO / environment["lock"]
            self.assertEqual(hashlib.sha256(lock.read_bytes()).hexdigest(), environment["lock_sha256"])

    def test_production_inputs_have_no_moving_vcs_refs(self):
        inputs = list((REPO / "env").glob("requirements*.in")) + list((REPO / "env/locks").glob("*.lock"))
        moving = re.compile(r"git\+\S+@(develop|main|master|HEAD)(?:#|$)")
        for path in inputs:
            self.assertIsNone(moving.search(path.read_text(encoding="utf-8")), path)

    def test_optional_image_base_is_digest_pinned(self):
        dockerfile = (REPO / "env/Dockerfile").read_text(encoding="utf-8")
        base = next(
            line
            for line in dockerfile.splitlines()
            if line.startswith("FROM ")
        )
        self.assertRegex(base, r"^FROM python:3\.12-slim-bookworm@sha256:[0-9a-f]{64}$")
        self.assertIn("locks/cosipy-py312.lock", dockerfile)
        self.assertIn("python -m pip check", dockerfile)
        self.assertIn("snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}", dockerfile)

    def test_release_artifact_contract_and_hashed_lock_tools(self):
        artifact_config = json.loads(
            (REPO / "release/vcs-artifacts.json").read_text(encoding="utf-8")
        )
        self.assertEqual(artifact_config["module_version"], "1.0.0")
        self.assertEqual(artifact_config["target"]["platform"], "linux/arm64")
        self.assertEqual(
            artifact_config["target"]["deployment_host"], "ubuntu/26.04"
        )
        self.assertEqual(
            {item["distribution"] for item in artifact_config["packages"]},
            {"cosipy", "astro-gdt", "bctools", "cosipy.nonimaging"},
        )
        for item in artifact_config["packages"]:
            self.assertRegex(item["revision"], r"^[0-9a-f]{40}$")

        def load(name, path):
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(module)
            return module

        compiler = load("review17_compile_lock", REPO / "scripts/compile_hashed_lock.py")
        verifier = load("review17_verify_lock", REPO / "scripts/verify_hashed_lock.py")
        digest = "a" * 64
        rendered = compiler.render_input(
            "demo @ git+https://example.invalid/demo.git@" + "b" * 40 + "\n",
            {"demo": {"filename": "demo-1.0-py3-none-any.whl", "sha256": digest}},
            "https://github.com/example/project/releases/download/deps-v1",
        )
        self.assertIn("demo-1.0-py3-none-any.whl#sha256=" + digest, rendered)
        with tempfile.TemporaryDirectory() as directory:
            good = Path(directory) / "good.lock"
            bad = Path(directory) / "bad.lock"
            good.write_text("demo==1.0 --hash=sha256:" + digest + "\n", encoding="utf-8")
            bad.write_text("demo==1.0\n", encoding="utf-8")
            self.assertEqual(verifier.verify(good), [])
            self.assertTrue(verifier.verify(bad))


if __name__ == "__main__":
    unittest.main()
