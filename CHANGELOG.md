# Changelog

This document records the significant changes to the Fast Transient Analysis
Pipeline (FasTP) based on the tags published in the GitHub repository.

The dates shown are the dates of the annotated tags. The history before
`v0.1.0-ged.1`, which began in February 2026 but did not contain version tags,
is consolidated into the first tagged baseline. The `-ged.1` tags form a
GeD-specific prerelease line and are kept separate from the general `v0.2.x`
releases. The **Unreleased** section describes commits currently available on
the `dev` branch after `v0.2.1`; these changes are not part of a release yet.

## [Unreleased]

Changes after `v0.2.1`, up to commit `979ae6b` dated July 20, 2026.

### Changed

- Refactored the all-in-one installer for the current COSIflow Compose layout:

  - added `-r`/`--cosiflow-ref` alongside `-c`/`--cosi-path` and made `dev` the
    default COSIflow ref for new checkouts;
  - added interactive configuration for the GCN MySQL port and credentials and
    for the GCN Kafka client credentials;
  - updated host-IP handling to edit the Compose YAML anchor introduced after
    COSIflow `v0.2.0`;
  - hid secret values during input and in installer logs;
  - improved macOS/Linux compatibility, path quoting, replacement validation,
    logging, option handling, and error reporting.

- Switched `cosipy.nonimaging` to the official `cositools` repository, pinned it
  to commit `3f62bcb03d60dbecfa01b05040bdbf413840ffe6`, and installed it as a
  non-editable dependency for reproducible virtual-environment creation.
- Removed the separate editable COSIpy checkout from the `nimcosipy`
  requirements because the non-imaging package now provides the required
  dependency path.
- Renamed `cosipipe_simdata.py` to `cosipipe_initpipeline.py` to match its
  `init_pipelines` DAG purpose; the DAG ID and staging workflow remain
  unchanged.
- Relaxed the GeD date filter from “today only” to dates up to and including the
  current day, allowing older unprocessed product directories to be selected.
- Expanded inline documentation for the BGO and GeD DAG configuration, task
  wiring, monitoring rules, and concurrency settings.

### Removed

- Removed the superseded Light Curve, TS Map, and standalone Fast GRB COSIDAGs,
  leaving `init_pipelines`, `cosidag_BGO`, and `cosidag_GeD` as the active DAG
  set.
- Removed the matching legacy Light Curve, TS Map, Fast GRB, and DC3 binning
  helper scripts and configuration files. Their maintained functionality is now
  integrated into the BGO and GeD fast-transient branches.

## [v0.2.1] - 2026-07-15

_Annotated tag: “Bug fix for cosipy.nonimaging”._

Patch release correcting installation of the BGO non-imaging localization
runtime.

### Fixed

- Replaced the editable `file://` dependency on a locally mounted
  `cosipy.nonimaging` checkout with the remote
  `NicoloParmiggiani/cosipy.nonimaging` `bgo-localization` branch. The
  `nimcosipy` environment can therefore be created without requiring a sibling
  repository at a workstation-specific path.

## [v0.2.0] - 2026-07-14

_Annotated tag: “Updated BGO and GeD branches with new GCN tasks”._

This release connects the BGO and GeD scientific branches to the COSIflow GCN
inbox and outbox, expands the BGO implementation, and introduces a dedicated
runtime for non-imaging localization.

### Added

- Added GCN inbox queries for time-relevant long- and short-GRB notices stored
  in COSIflow's `gcn_inbound_notices` table. Queries derive a search interval
  from the pipeline trigger and science windows, normalize notice metadata, and
  record matching results without failing the pipeline when the database is
  unavailable.
- Added reusable GCN outbox support for building COSI initial-alert JSON
  payloads and queueing them in `gcn_outbound_notices`. Outbound rows include
  source DAG metadata, checksums, idempotency keys, priorities, retry limits,
  product locations, and event metadata for processing by the COSIflow GCN
  client.
- Activated the `GCN_BGO` and `GCN_GeD` DAG tasks. Each task writes a local
  notice artifact and queues a test-topic outbox record rather than publishing
  directly to the GCN network.
- Added BGO and GeD product mapping for GCN notices, including duration,
  significance, localization, classification, SNR, HEALPix, and spectral fields
  when those products are available.
- Added the Python 3.12 `nimcosipy` environment for BGO non-imaging
  localization, combining COSIpy, `bc-tools`, and `cosipy.nonimaging`.
- Added detailed BGO and GeD data-flow documentation covering runtime
  environments, task dependencies, inputs, outputs, generated products,
  placeholders, GCN integration, and the shared `pipeline_config.yaml` state.

### Changed

- Refactored the BGO analysis around centralized defaults and structured YAML
  state. Preprocessing now merges configuration consistently, validates required
  files and keys, prepares task directories, and records task-level inputs,
  outputs, status, and timing.
- Expanded the active BGO workflow for duration estimation, background
  extraction, light-curve generation, significance analysis, and
  `bc-tools`/non-imaging localization. `Localization_Chi2`, `Localization_DL`,
  and `Classification_BGO` remain explicit placeholders.
- Enhanced BGO localization with attitude extraction, detector-count ordering,
  configurable map resolution, and structured localization products.
- Updated the GeD branch so unbinned skymap and final localization stages can
  correlate their science windows with notices from the GCN inbox.
- Extended module configuration from two to three managed environments:
  `cosipy` for the main GeD/COSIpy work, `bct` for Bayesian-Blocks tasks, and
  `nimcosipy` for BGO localization.
- Updated scientific dependency constraints for the new BGO runtime and added
  `PyMySQL` to the COSIpy environment for GCN database access.
- Pinned the installer to the matching COSIflow `v0.2.0` tag.

## [v0.1.1-ged.1] - 2026-07-01

_Annotated tag: “Bug fix for Docker Image of FasTP”._

Corrective GeD prerelease focused on making the module image and the combined
container/virtual-environment installation path usable.

### Changed

- Updated the FasTP runtime image from Python 3.10 Slim to Python 3.12 Slim to
  match the primary COSIpy environment.
- Changed the module installation mode from `environment` to `both`, so the
  hot-loader creates the configured Python environments and builds the FasTP
  Docker image.
- Stopped the bootstrap installer from overriding the module configuration back
  to environment-only mode.
- Updated the installer to check out the tagged COSIflow `v0.1.0` baseline
  instead of the development `container_arch` branch.
- Disabled the obsolete `requirements_no_deps` entry for the BCT environment so
  the active dependency definition is used directly.

### Fixed

- Corrected the Docker build to copy and install
  `env/requirements_cosipy.txt` instead of the missing generic
  `env/requirements.txt` file.

## [v0.1.0-ged.1] - 2026-06-30

_Annotated tag: “v0.1.0-ged.1: GeD branch smoke-test baseline”._

The first tagged FasTP baseline, created specifically for the simulator-to-GeD
Airflow smoke-test path. It consolidates all earlier untagged development into
a reproducible COSIflow module with staging, scientific DAGs, helper libraries,
and managed runtimes.

### Added

- Added the `init_pipelines` staging DAG for DC3 and DC4 datasets. It resolves
  Wasabi inputs, downloads and validates source, background, orientation, and
  response files, creates date-indexed run directories, links staged products,
  and extracts a source-specific background window.
- Added destination handoff for `lcurve`, `tsmap`, `fast`, and `tdrss` product
  roots. COSIDAG workflows monitor the resulting `products/` directories rather
  than being triggered directly by the initializer.
- Added the initial GeD fast-transient branch with preprocessing, unbinned
  light-curve generation, data binning, light-curve analysis, unbinned skymap
  production, duration and localization results, TS maps over multiple
  timescales, binned light curves, and Bayesian-Blocks duration estimation.
  Spectral analysis, classification, and GCN publication were represented as
  explicit placeholder tasks in this baseline.
- Added the initial BGO branch with preprocessing, duration estimation,
  background preparation, light-curve generation, significance analysis, and
  `bc-tools` localization. Alternative localization, classification, and GCN
  nodes were retained as placeholders for later implementation.
- Added Docker and ExternalPythonOperator variants of the Light Curve and TS Map
  workflows, together with standalone fast-GRB localization and time-series
  COSIDAGs.
- Added reusable pipeline utilities for FITS data handling, background-window
  extraction, Wasabi staging, file readiness checks, binning, light curves,
  TS maps, HEALPix products, and fast transient analysis.
- Added two isolated scientific environments:

  - Python 3.12 `cosipy` for GeD/COSIpy analysis;
  - Python 3.11 `bct` for Bayesian-Blocks duration and BGO localization.

- Added `fta-pipe.config.yaml` for module paths, managed environments, and
  hot-loader behavior, plus a Debian-based FasTP Docker runtime definition.
- Added an interactive installation script that clones COSIflow, applies host
  UID/GID values, configures Airflow and PostgreSQL, pre-creates bind-mounted
  directories, builds and starts the Compose stack, and hot-loads FasTP.
- Added cross-platform `sed` handling for macOS and Linux and explicit error
  reporting during installation.
- Added repository, DAG-catalog, pipeline-layout, installation, and GeD
  smoke-test documentation, including the `tdrss` handoff from
  `init_pipelines` to `cosidag_GeD`.

### Changed

- Split COSIpy and BCT dependencies into separate requirement files to isolate
  incompatible scientific dependency trees and allow each Airflow task to run
  with its intended external interpreter.
- Standardized COSIDAG file matching with regex support for background,
  orientation, and response products and improved Wasabi key normalization and
  staging error handling.
- Made host-workspace paths configurable through `HOST_WORKSPACE_PATH`, avoiding
  machine-specific paths in DockerOperator mounts.

### Fixed

- Replaced in-process gzip extraction with a subprocess-based implementation to
  avoid corrupted FITS reads caused by incomplete decompression.
- Improved file-resource handling, background-file readiness checks, symlink
  creation, and staged-file validation across the initialization workflow.

[Unreleased]: https://github.com/cositools/fast-transient-analysis-pipeline/compare/v0.2.1...dev
[v0.2.1]: https://github.com/cositools/fast-transient-analysis-pipeline/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/cositools/fast-transient-analysis-pipeline/compare/v0.1.1-ged.1...v0.2.0
[v0.1.1-ged.1]: https://github.com/cositools/fast-transient-analysis-pipeline/compare/v0.1.0-ged.1...v0.1.1-ged.1
[v0.1.0-ged.1]: https://github.com/cositools/fast-transient-analysis-pipeline/tree/v0.1.0-ged.1
