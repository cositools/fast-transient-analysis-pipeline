# Installation

## Prerequisites

- Git;
- Bash and standard Unix tools (`awk`, `grep`, `id`, `mktemp`, `sed`);
- Docker Engine or Docker Desktop with Docker Compose v2;
- Python 3 for installer secret generation.

Clone FasTP into a workspace and run its interactive installer:

```bash
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
cd fast-transient-analysis-pipeline/env/bin
./install
```

Use `./install --help` for supported arguments. A different workspace or
COSIflow ref can be selected when needed:

```bash
./install --cosi-path /path/to/workspace --cosiflow-ref <branch-tag-or-commit>
```

The selected ref is checked out only when the installer creates a new COSIflow
clone. It does not switch an existing checkout.

## What the installer changes

The current [installer source](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/env/bin/install):

1. clones COSIflow when the selected workspace has no `cosiflow` checkout;
2. aligns Compose and image UID/GID values with the host user;
3. updates non-secret Compose choices such as ports, usernames, and database names;
4. writes generated passwords, separate Airflow keys, and required GCN client
   credentials to the ignored `cosiflow/env/.env` file with mode `0600`;
5. pre-creates bind-mount directories;
6. builds and starts the COSIflow Compose stack;
7. invokes `hot_load_module.sh fast-transient-analysis-pipeline install`.

The installer is interactive and updates both checkouts in place. Review local
changes before running it. Never commit `.env` or paste its contents into logs
or issues.

## Module configuration

The checked-in [module configuration](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/env/fta-pipe.config.yaml)
uses `install_mode: environment`; the loader creates the `cosipy`, `bct`, and
`nimcosipy` environments. See [runtime environments](../architecture/runtime-environments.md)
before selecting a different install mode.

COSIflow owns the module-loader lifecycle. Its documentation is available in
the [COSIflow repository documentation](https://github.com/cositools/cosiflow).
