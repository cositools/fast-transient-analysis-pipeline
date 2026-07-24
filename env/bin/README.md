# Installing the Fast Transient Analysis Pipeline in COSIflow

This directory contains the all-in-one [`install`](install) script. It can
bootstrap a COSIflow checkout, configure and start its Docker Compose stack,
and hot-load the Fast Transient Analysis Pipeline.

## Prerequisites

Before running the script, install:

- Git;
- Docker Engine or Docker Desktop with Docker Compose v2;
- Bash;
- standard Unix tools (`sed`, `grep`, `id`).

Create a workspace and clone the pipeline as a sibling of the future
`cosiflow` checkout:

```bash
mkdir -p ~/cosi
cd ~/cosi
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
```

The script clones COSIflow automatically if `~/cosi/cosiflow` does not yet
exist.

## Install this pipeline

Run:

```bash
cd ~/cosi/fast-transient-analysis-pipeline/env/bin
./install
```

For a different workspace:

```bash
./install --cosi-path /absolute/path/to/cosi
```

The installer:

1. clones COSIflow and checks out the selected ref;
2. updates UID/GID values so bind-mounted files remain owned by the host user;
3. prompts for Airflow, PostgreSQL, GCN MySQL, and GCN Kafka settings;
4. builds and starts the COSIflow Docker Compose stack;
5. runs `hot_load_module.sh fast-transient-analysis-pipeline install`.

The module layout expected by COSIflow is:

```text
fast-transient-analysis-pipeline/
├── env/
│   ├── Dockerfile
│   ├── fta-pipe.config.yaml
│   └── requirements_*.txt
└── src/
    ├── dags/
    └── pipeline/
```

`fta-pipe.config.yaml` selects the installation mode, source paths, and Python
environments. Review it before installation, especially:

- `install_mode`: `container`, `environment`, `both`, or `none`;
- `paths.dags`, `paths.pipeline`, and `paths.images`;
- each environment's requirements file, Python version, and container
  `venv_path`.

To install another pipeline of your own, give it the same basic structure,
create a `*.config.yaml`, place its directory beside `cosiflow`, and run:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh <your-module-directory> install
```

Use `update` after changing its requirements or runtime configuration and
`remove` to unload it. See
[`cosiflow/env/README.md`](../../../cosiflow/env/README.md) for the complete
module format and lifecycle.

## Select a COSIflow version

The installer defaults to the value assigned near line 118:

```bash
COSIFLOW_REF="${COSIFLOW_REF:-dev}"
```

`COSIFLOW_REF` may be a branch, release tag, or commit that Git can check out.
The preferred way to select it without modifying the script is:

```bash
./install --cosiflow-ref <branch-tag-or-commit>
```

The short option and environment-variable forms are equivalent:

```bash
./install -r <branch-tag-or-commit>
COSIFLOW_REF=<branch-tag-or-commit> ./install
```

If this installer is being distributed as a version-specific bootstrap
script, change the default on that line, for example:

```bash
COSIFLOW_REF="${COSIFLOW_REF:-vX.Y.Z}"
```

Use a real COSIflow tag, branch, or full commit hash in place of `vX.Y.Z`. A
release tag or commit is more reproducible than a moving branch.

The script performs the checkout only when it creates a new
`<COSI_PATH>/cosiflow` clone. If that directory already exists, select the
required ref in that checkout before running the installer:

```bash
cd ~/cosi/cosiflow
git fetch --tags origin
git checkout <branch-tag-or-commit>
```

Review local changes before switching refs; do not overwrite an existing dirty
working tree.

## GCN Kafka credentials for integration tests

The Fast Transient Analysis Pipeline reads received alerts from the prototype
MySQL inbox and queues COSI alerts in its outbox. To test the live Kafka
receiver, create this file in the COSIflow Compose directory:

```text
cosiflow/env/.env
```

Its complete credential-only content is:

```dotenv
GCN_CLIENT_ID=<your-gcn-client-id>
GCN_CLIENT_SECRET=<your-gcn-client-secret>
```

Replace the placeholders with credentials issued to you. Never copy someone
else's credentials, commit `.env`, paste its contents into issues, or include
the secret in terminal screenshots. Restrict local access if needed:

```bash
chmod 600 ~/cosi/cosiflow/env/.env
```

To obtain credentials:

1. open [gcn.nasa.gov](https://gcn.nasa.gov/);
2. find the **GCN Kafka** card and click **Get Started**;
3. sign in or create an account;
4. create or select a client credential; use the public-consumer scope for
   receiving public GCN notices;
5. select the notice formats/topics needed by the test and generate the sample
   code;
6. copy only the generated client ID and secret into the local `.env` file.

The official [Kafka client setup guide](https://gcn.nasa.gov/docs/client)
explains the generated consumer configuration. Credentials allowed to publish
mission notices require separate coordination with the GCN team and the
mission-producer scope; public-consumer credentials are not sufficient for
real COSI publication.

After creating or changing `.env`, recreate the GCN client so Docker Compose
loads the new values:

```bash
cd ~/cosi/cosiflow/env
docker compose up -d --force-recreate gcn-client
docker compose logs -f gcn-client
```

Keep the prototype producer defaults in place for ordinary integration tests:
`GCN_PRODUCER_ENABLED=false`, `GCN_DRY_RUN=true`, and test topics only. The
outbox will be exercised without publishing an external notice.

For inbox/outbox queries and a sample COSI alert, see the
[GCN integration section](../../README.md#gcn-client-integration) and
[`cosiflow/gcn-client/README.md`](../../../cosiflow/gcn-client/README.md).
