# Fast Transient Analysis Pipeline

The Fast Transient Analysis Pipeline (FasTP) is a COSIflow module for rapid
analysis of transient events observed by the
[COSI](https://cosi.ssl.berkeley.edu/) mission. It supplies an input-staging
DAG and three filesystem-driven scientific branches for BGO, binned GeD, and
unbinned ARM-selection analysis.

## Current DAGs

| DAG ID | Runtime | Role |
| --- | --- | --- |
| `init_pipelines` | Airflow Python + module Docker image | Resolve and stage GeD or BGO inputs |
| `cosidag_BGO` | `cosipy`, `bct`, `nimcosipy` | BGO duration, background, light curve, significance, and localization |
| `cosidag_GeD` | `cosipy`, `bct` | Binned GeD data, TS maps, light curve, and duration |
| `cosidag_ARMselection` | `cosipy` | Unbinned ARM-gated light curve and HEALPix localization |

The exact graph, monitoring contract, and smoke-test procedure are in
[src/dags/README.md](src/dags/README.md).

## Repository layout

```text
fast-transient-analysis-pipeline/
├── env/
│   ├── Dockerfile
│   ├── fta-pipe.config.yaml
│   ├── requirements_cosipy.txt
│   ├── requirements_bct.txt
│   ├── requirements_nimcosipy.txt
│   └── bin/
│       ├── install
│       └── README.md
├── src/
│   ├── dags/
│   │   ├── cosipipe_initpipeline.py
│   │   ├── init_pipeline_config.py
│   │   ├── cosidag_BGO.py
│   │   ├── cosidag_GeD.py
│   │   ├── cosidag_ARMselection.py
│   │   └── README.md
│   └── pipeline/
│       ├── stage_files.py
│       ├── bkg_cut.py
│       ├── download_data.py
│       ├── README.md
│       └── fast_transient_pipeline/
│           ├── bgo_functions.py
│           ├── ged_functions.py
│           ├── armsel_functions.py
│           ├── fast_helper_functions.py
│           ├── gcn/
│           ├── README_BGO.md
│           ├── README_GeD.md
│           └── README_ARMselection.md
├── tests/
└── CHANGELOG.md
```

## Runtime environments

The checked-in module configuration uses `install_mode: both`: it builds the
module image and recreates every enabled external Python environment.

| Environment | Python | Requirements | Used by |
| --- | --- | --- | --- |
| `cosipy` | 3.12 | `env/requirements_cosipy.txt` | GeD, ARM selection, shared BGO helpers |
| `bct` | 3.11 | `env/requirements_bct.txt` | Bayesian-Blocks duration and BGO time-series work |
| `nimcosipy` | 3.12 | `env/requirements_nimcosipy.txt` | BGO non-imaging localization |

The Docker image is based on Python 3.12 and installs the COSIpy requirement
set. It is required by `init_pipelines` staging/background-cut tasks.

The runtimes isolate incompatible stacks, but complete build reproducibility
also requires pinning every VCS dependency and base-image digest. The current
COSIpy and bc-tools requirements still follow development branches; review and
pin them before producing a release image.

## Installation

### Prerequisites

Install Git, Bash, Docker Engine or Docker Desktop, and Docker Compose v2.

Clone this repository into a workspace:

```bash
mkdir -p ~/cosi
cd ~/cosi
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
```

### All-in-one bootstrap

The bundled installer clones COSIflow when necessary, configures it, starts the
Compose stack, and hot-loads this module:

```bash
cd ~/cosi/fast-transient-analysis-pipeline/env/bin
./install
```

Use another workspace or COSIflow ref with:

```bash
./install --cosi-path /absolute/path/to/cosi \
  --cosiflow-ref <branch-tag-or-commit>
```

The selected ref is checked out only when the installer creates a new COSIflow
clone. For an existing checkout, review its working tree and select the desired
ref yourself before running the installer.

The installer:

1. clones COSIflow if `<COSI_PATH>/cosiflow` does not exist;
2. aligns COSIflow and module-image UID/GID values with the host user;
3. writes non-sensitive host, user, port, and database-name choices into
   `docker-compose.yaml`;
4. writes passwords and optional GCN credentials into ignored
   `cosiflow/env/.env` with mode `0600`;
5. pre-creates bind-mount directories;
6. builds and starts the COSIflow stack;
7. runs `hot_load_module.sh fast-transient-analysis-pipeline install`.

Do not commit `.env`. GCN client credentials are optional: without them the
receiver stays idle, while local inbox injection and dry-run outbox processing
continue to work.

The detailed bootstrap and credential procedure is in
[env/bin/README.md](env/bin/README.md).

### Install into an existing COSIflow checkout

If COSIflow is already running and both repositories are siblings:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh fast-transient-analysis-pipeline install
```

Review [env/fta-pipe.config.yaml](env/fta-pipe.config.yaml) first. Change
`install_mode` to `environment`, `container`, or `none` only when the DAGs you
intend to run do not require the omitted runtime.

General module lifecycle documentation is in
[`../cosiflow/env/README.md`](../cosiflow/env/README.md).

## Pipeline handoff

`init_pipelines` accepts `pipeline_branch=GeD` or `pipeline_branch=BGO`.

### GeD staging

The initializer stages source, background, orientation, and response files,
creates a background-window FITS file, and writes a new:

```text
/home/gamma/workspace/data/<destination>/YYYY_MM/YYMMDDXXX/products
```

GeD supports the `lcurve`, `tsmap`, `fast`, and `tdrss` destinations, but the
current `cosidag_GeD` and `cosidag_ARMselection` branches monitor `tdrss`.

Both scientific DAGs resolve the same staged files independently:

```text
products
├── cosidag_GeD          -> binned branch, pipeline_config.yaml
└── cosidag_ARMselection -> unbinned branch, armsel_pipeline_config.yaml
```

Separate YAML state files prevent the former parallel-write race between the
binned and unbinned branches. Each DAG also has its own
`COSIDAG_PROCESSED::<dag_id>` history.

### BGO staging

BGO always stages into `tdrss`. It resolves a multi-panel light curve, three
localization lookup tables, and orientation; it does not run the GeD background
cut.

```text
init_pipelines(BGO)
  -> tdrss/.../products
  -> cosidag_BGO
```

The initializer creates products but does not directly trigger any scientific
DAG. Unpause the desired COSIDAGs before staging; their sensors discover the new
`products/` directory.

## Scientific data flows

- [BGO branch](src/pipeline/fast_transient_pipeline/README_BGO.md)
- [Binned GeD branch](src/pipeline/fast_transient_pipeline/README_GeD.md)
- [Unbinned ARM-selection branch](src/pipeline/fast_transient_pipeline/README_ARMselection.md)

These guides are authoritative for task inputs, state files, plots, and
placeholder nodes.

## GCN integration

Scientific tasks do not connect to Kafka. They use the COSIflow GCN client's
MySQL boundary:

```text
GCN Kafka -> receiver -> gcn_inbound_notices -> FasTP queries
FasTP task -> gcn_outbound_notices -> outbox worker -> GCN Kafka
```

`gcn.query.query_relevant_grb_notices` searches the inbox around a trigger or
science interval. Database errors produce a structured `unavailable` result
instead of failing the scientific task.

The final BGO, GeD, and ARM-selection tasks call
`gcn.outbox.queue_cosi_alert`. The helper:

1. creates a COSI test alert;
2. writes the JSON artifact beside the source products;
3. builds an idempotency key from DAG/task/run identity;
4. inserts or updates the durable outbox row.

With the checked-in safety defaults:

```text
GCN_PRODUCER_ENABLED=false
GCN_DRY_RUN=true
GCN_REQUIRE_TEST_TOPICS=true
```

the outbox worker records `dry_run_published` without contacting the external
broker. Do not enable real publication until topic, schema, allowlist, and
mission-producer credentials have been approved.

Full GCN client architecture and SQL examples are documented in
[`../cosiflow/gcn-client/README.md`](../cosiflow/gcn-client/README.md).

## After installation

With default endpoint values:

- Airflow: `http://localhost:8080/home`
- MailHog: `http://localhost:8025`

Custom host ports configured by the installer are applied to the Docker
published ports and to the displayed URLs.

If DAGs do not appear:

```bash
cd ~/cosi/cosiflow/env
docker compose exec airflow airflow dags list
```

or use **Develop Tools → Refresh DAGs List** in Airflow.

Update or remove the module with:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh fast-transient-analysis-pipeline update
./hot_load_module.sh fast-transient-analysis-pipeline remove
```

Removal deletes the module links, configured external environments, and module
image; it does not delete this repository or generated scientific data.

## Tests

Run the repository unit tests from the module root:

```bash
python3 -m unittest discover -s tests -v
```

Tests that import a live Airflow DAG are skipped when Airflow is not installed
in the current Python environment.
