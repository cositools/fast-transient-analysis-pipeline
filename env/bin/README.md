# Installing FasTP in COSIflow

The [`install`](install) script bootstraps a COSIflow checkout, configures its
Docker Compose stack, and hot-loads the Fast Transient Analysis Pipeline.

## Prerequisites

- Git;
- Docker Engine or Docker Desktop with Docker Compose v2;
- Bash and standard Unix tools (`awk`, `grep`, `id`, `mktemp`, `sed`).

Clone FasTP into a workspace. COSIflow will be cloned beside it if missing:

```bash
mkdir -p ~/cosi
cd ~/cosi
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
cd fast-transient-analysis-pipeline/env/bin
./install
```

For another workspace or COSIflow ref:

```bash
./install --cosi-path /absolute/path/to/cosi --cosiflow-ref dev
```

Short options are also available:

```bash
./install -c /absolute/path/to/cosi -r <branch-tag-or-commit>
```

Run `./install --help` for the complete option list.

## What the installer changes

The script:

1. clones COSIflow when `<COSI_PATH>/cosiflow` is absent and checks out the
   selected ref;
2. updates non-secret Compose settings, including UID/GID and loopback ports;
3. creates or updates `cosiflow/env/.env` with generated Airflow keys,
   generated database passwords, and required GCN Kafka credentials, then sets
   its mode to `0600`;
4. creates the required bind-mount directories;
5. builds and starts COSIflow;
6. runs `hot_load_module.sh fast-transient-analysis-pipeline install`.

Personal credentials are deliberately kept out of the tracked
`docker-compose.yaml`. Existing `.env` values are shown only as masked values
and secret prompts do not echo their input.

The installer updates an existing COSIflow checkout in place; it does not
switch that checkout to `--cosiflow-ref`. If a specific ref is required,
review local changes and switch it yourself before running the installer:

```bash
cd ~/cosi/cosiflow
git fetch --tags origin
git checkout <branch-tag-or-commit>
```

A release tag or commit is more reproducible than the default moving `dev`
branch.

## Module configuration

Review [`fta-pipe.config.yaml`](../fta-pipe.config.yaml) before installation.
It controls:

- `install_mode`: `container`, `environment`, `both`, or `none`;
- source paths for DAGs, pipeline code, and images;
- Python versions, requirements files, and container virtual-environment
  paths.

The current module uses `install_mode: environment`: the loader creates the
`cosipy`, `bct`, and `nimcosipy` environments without giving Airflow access to
a container runtime.

Use the COSIflow module loader after subsequent changes:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh fast-transient-analysis-pipeline update
./hot_load_module.sh fast-transient-analysis-pipeline remove
```

See the complete
[`cosiflow/env` module guide](../../../cosiflow/env/README.md) for configuration
discovery, install modes, lifecycle behavior, and troubleshooting.

## Credentials and GCN safety

The installer manages these values in `cosiflow/env/.env`:

```dotenv
AIRFLOW_ADMIN_PASSWORD=<local-password>
AIRFLOW__WEBSERVER__SECRET_KEY=<random-key>
AIRFLOW__CORE__INTERNAL_API_SECRET_KEY=<different-random-key>
AIRFLOW__CORE__FERNET_KEY=<fernet-key>
POSTGRES_PASSWORD=<local-password>
GCN_DB_PASSWORD=<local-password>
GCN_MYSQL_ROOT_PASSWORD=<local-password>
GCN_CLIENT_ID=<your-gcn-client-id>
GCN_CLIENT_SECRET=<your-gcn-client-secret>
```

You may edit that ignored file manually instead. Never commit it, paste it into
issues, or share terminal output containing secrets. The installer rejects
empty required values and replaces short legacy defaults. After changing GCN
credentials, revoke the previous client at GCN and recreate the client through
the audited orchestrator wrapper:

```bash
cd ~/cosi/cosiflow/env
./gcn-lifecycle.sh restart
docker compose logs -f gcn-client
```

Credentials can be created through the official
[GCN Kafka client guide](https://gcn.nasa.gov/docs/client). Public-consumer
credentials can receive public notices; publishing mission notices requires
separate producer authorization.

For normal integration tests, keep:

```dotenv
GCN_PRODUCER_ENABLED=false
GCN_DRY_RUN=true
```

This exercises the local inbox/outbox without sending an external notice. See
the repository [GCN integration guide](../../README.md#gcn-integration)
and COSIflow's
[`gcn-client` guide](../../../cosiflow/gcn-client/README.md).
