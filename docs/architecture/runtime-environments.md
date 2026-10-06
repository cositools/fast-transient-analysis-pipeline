# Runtime environments

## Reproducible module contract

The root `Manifest.yaml` is the authoritative COSIflow module contract.
`env/fta-pipe.config.yaml` remains only for old loader versions. The scientific
pipeline YAML files are run configuration and are not installation manifests.

The supported matrix is COSIpy on Python 3.12, BCT on Python 3.11, and
nimcosipy on Python 3.12. Release 1.0.0 targets the OCI platform
`linux/arm64` and is validated on an Ubuntu 26.04 LTS deployment host. These
are separate properties: the former selects container architecture, while the
latter identifies the supported host operating system. Each environment has:

- `env/requirements_<name>.in`, the reviewed direct dependencies;
- `env/locks/<name>-py<version>.lock`, the exact captured package inventory;
- a SHA-256 recorded in `Manifest.yaml`.

VCS packages use full commit SHAs. The loader verifies lock checksums, rebuilds
the managed environment from scratch, runs `pip check` and the COSIflow-owned
`fastp-v1` smoke profile, then emits a version report and CycloneDX SBOM.

To refresh a lock, first validate an unchanged working environment. Run
`scripts/capture_runtime_lock.py` with that environment's interpreter, review
the diff and VCS revisions, update the manifest checksum, and repeat two clean
rebuilds. A lock or module version is promoted by changing the controlled
COSIflow registry record only after both inventories match. Rollback selects
the previous approved `module-id@version`; do not edit a released lock in
place.

## Release wheels and hashed locks

The checked-in locks are captured inventories while release 1.0.0 remains a
candidate. They are not yet safe to install with pip's `--require-hashes`.
Promotion requires the following one-time release procedure on an Ubuntu
26.04 ARM64 runner:

1. Build the four VCS dependencies (COSIpy, astro-gdt, bc-tools and
   cosipy.nonimaging) at their reviewed commits:

   ```bash
   python3 scripts/build_vcs_wheels.py \
     --config release/vcs-artifacts.json \
     --output dist/reproducibility-deps-v1.0.0 \
     --python 3.11=python3.11 \
     --python 3.12=python3.12
   ```

2. Create the draft GitHub Release `reproducibility-deps-v1.0.0` in the FasTP
   repository. Upload every wheel, `artifact-manifest.json`, and
   `SHA256SUMS`. Do not replace assets in a published release; create a new
   module patch version instead.
3. For each runtime, convert its direct input to immutable Release asset URLs
   and compile the transitive lock. For example:

   ```bash
   python3 scripts/compile_hashed_lock.py \
     --python python3.12 \
     --input env/requirements_cosipy.in \
     --artifact-manifest dist/reproducibility-deps-v1.0.0/artifact-manifest.json \
     --asset-base-url https://github.com/cositools/fast-transient-analysis-pipeline/releases/download/reproducibility-deps-v1.0.0 \
     --output env/locks/cosipy-py312.lock
   ```

   Repeat with Python 3.11 for BCT and Python 3.12 for nimcosipy.
4. Verify that every resolved artifact is hashed:

   ```bash
   python3 scripts/verify_hashed_lock.py env/locks/*.lock
   ```

5. Recalculate the lock SHA-256 values in `Manifest.yaml`, perform two clean
   installs with `pip install --require-hashes`, run `pip check` and the
   scientific smoke suite, and compare the two version reports.

GitHub Releases is used for generic wheel assets; GHCR is used for OCI images.
The checksum in the lock is the trust anchor even if a Release asset is later
deleted. A future institutional artifact service can replace GitHub without
changing module contract v1.

Before the target runner is available, the same procedure may be exercised on
an Apple Silicon macOS host using a distinct `-macos-arm64-poc` Release tag.
That prototype validates artifact publication, authenticated download, hashes
and clean installation only. It is not evidence for the declared
`linux/arm64` and Ubuntu 26.04 compatibility contract and must never be used to
promote the production registry record.

## System inputs

The optional FasTP image fixes both its base-image digest and the Debian APT
snapshot (`20261001T000000Z`). Changing either is a reviewed release input.
The deployment host remains Ubuntu 26.04 LTS even though individual containers
may use a different distribution internally.

The module configuration currently selects `install_mode: environment` and
enables three managed Python environments:

| Environment | Python | Requirements file | Current use |
| --- | --- | --- | --- |
| `cosipy` | 3.12 | `env/requirements_cosipy.txt` | staging, GeD background cut, GeD, ARM selection, BGO preprocessing/significance |
| `bct` | 3.11 | `env/requirements_bct.txt` | BGO duration/background/light curves and GeD duration |
| `nimcosipy` | 3.12 | `env/requirements_nimcosipy.txt` | BGO non-imaging localization |

Staging and the GeD background cut are `BashOperator` tasks. They explicitly
invoke `/home/gamma/envs/cosipy/bin/python` by default through the
`COSIPY_PYTHON` environment variable. They do **not** use the optional module
Docker image. This is the current runtime contract in
[`cosipipe_initpipeline.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/cosipipe_initpipeline.py).

The repository also contains a Python 3.12 Dockerfile that installs the COSIpy
requirements. The loader builds it only for `container` or `both` installation
modes, and none of the current FasTP DAG tasks launch that image.

The environment split isolates incompatible scientific dependency trees. The
VCS revisions and image base are pinned, but promotion still requires Release
wheel publication, per-artifact hashed locks, and the two clean rebuilds above.
