# Fast Transient Analysis Pipeline

The **Fast Transient Analysis Pipeline** (`fast-transient-analysis-pipeline`) is a
**Cosiflow module** that provides a set of Airflow DAGs and pipeline scripts for the
fast analysis of transient events (e.g. Gamma-Ray Bursts) observed by the
[COSI](https://cosi.ssl.berkeley.edu/) mission.

This README explains:

1. what a Cosiflow module is and how this repository is organized;
2. how to install Cosiflow **and** the Fast Transient Analysis Pipeline module on a
   new machine, using the bundled `install` script.

> If you are looking for the general Cosiflow documentation about modules
> (creating, updating, removing, writing Dockerfiles, etc.), refer to
> [`cosiflow/env/README.md`](../cosiflow/env/README.md).

---

## Table of Contents

1. [What is a Cosiflow module?](#what-is-a-cosiflow-module)
2. [Repository structure](#repository-structure)
3. [Installation](#installation)
   1. [Step 1 — Create the workspace folder](#step-1--create-the-workspace-folder)
   2. [Step 2 — Clone the repository](#step-2--clone-the-repository)
   3. [Step 3 — Customize the runtime environment](#step-3--customize-the-runtime-environment)
      - [3.1 Docker container path (`env/Dockerfile`)](#31-docker-container-path-envdockerfile)
      - [3.2 External Python virtual environment path (`env/requirements.txt`)](#32-external-python-virtual-environment-path-envrequirementstxt)
   4. [Step 4 — Review the module configuration file](#step-4--review-the-module-configuration-file)
   5. [Step 5 — Run the install script](#step-5--run-the-install-script)
   6. [Step 6 — Fill in the prompts and wait](#step-6--fill-in-the-prompts-and-wait)
4. [After the installation](#after-the-installation)

---

## What is a Cosiflow module?

[Cosiflow](https://github.com/cositools/cosiflow) is the Airflow-based orchestration
environment used by the COSI collaboration to run scientific pipelines.

A **Cosiflow module** is a self-contained plug-in that adds new DAGs and pipeline
scripts to a running Cosiflow instance. Modules are *hot-loaded* into the Airflow
container by the `hot_load_module.sh` helper script, which:

- creates symbolic links from the Airflow `dags/` and `pipeline/` directories to the
  module sources (so Airflow discovers the new DAGs automatically);
- optionally creates one or more Python virtual environments inside the Airflow
  container (used by `ExternalPythonOperator` tasks);
- optionally builds a dedicated Docker image for the module (used by
  `DockerOperator` tasks).

The behaviour of `hot_load_module.sh` is driven by a YAML configuration file that
lives in the module root. For this module that file is
[`env/fta-pipe.config.yaml`](env/fta-pipe.config.yaml).

A standard Cosiflow module follows this directory layout (taken from
`cosiflow/env/README.md`):

```
your_module_name/
├── env/
│   ├── Dockerfile          # Container image definition
│   └── requirements.txt    # Python dependencies (optional)
├── src/
│   ├── dags/               # Airflow DAG definitions
│   │   └── *.py
│   └── pipeline/           # Pipeline scripts executed by tasks
│       └── *.py
└── <module>.config.yaml    # (recommended) configuration file for hot_load_module.sh
```

For the full description of the module structure and of the configuration file,
see the *Module Structure* and *Using Configuration Files* sections in
[`cosiflow/env/README.md`](../cosiflow/env/README.md).

---

## Repository structure

This module is organized as follows:

```
fast-transient-analysis-pipeline/
├── env/
│   ├── Dockerfile              # Module Docker image (Python 3.10 slim + cosipy)
│   ├── requirements.txt        # Python dependencies (regular pip install)
│   ├── requirements_nodeps.txt # Python dependencies installed with --no-deps
│   ├── fta-pipe.config.yaml    # hot_load_module.sh configuration file
│   └── bin/
│       └── install             # All-in-one bootstrap script (Cosiflow + this module)
└── src/
    ├── dags/                   # Airflow DAGs (cosidag_*.py, cosipipe_*.py)
    └── pipeline/               # Pipeline scripts called by the DAGs
```

The DAGs in `src/dags/` use the `COSIDAG` framework provided by Cosiflow and cover,
among others, light-curve generation, TS-map computation, GRB localization, T90
estimation and fast GRB time-series analysis (both as `DockerOperator` and
`ExternalPythonOperator` variants).

---

## Installation

The recommended way to install the pipeline on a fresh machine is to use the
bootstrap script [`env/bin/install`](env/bin/install). The script performs the
**full installation, including Cosiflow itself**: you do not need to clone or
configure Cosiflow separately.

The high-level workflow is:

1. create a `cosi/` workspace folder;
2. clone this repository inside it;
3. (optionally) tune the runtime environment of the module (Dockerfile and/or
   `requirements.txt`);
4. (optionally) tune the module configuration file `fta-pipe.config.yaml`;
5. run the `install` script;
6. answer the interactive prompts and wait for the build to complete.

### Step 1 — Create the workspace folder

By default the `install` script expects everything to live under `~/cosi/`. Create
the folder if it does not already exist:

```bash
mkdir -p ~/cosi
```

> If you want to use a different location, you can pass it later to the install
> script via the `-c <path>` (or `--cosi-path <path>`) option.

### Step 2 — Clone the repository

Clone the Fast Transient Analysis Pipeline repository **inside** the workspace
folder:

```bash
cd ~/cosi
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
```

After this step you should have:

```
~/cosi/
└── fast-transient-analysis-pipeline/
    ├── env/
    └── src/
```

The Cosiflow repository will be cloned automatically by the `install` script in
the next steps; you do **not** need to clone it manually.

### Step 3 — Customize the runtime environment

The pipeline can be executed in two complementary ways, depending on which
operator is used by the DAGs:

| Mode | Operator | Pros | Cons |
| --- | --- | --- | --- |
| **Docker container** | `DockerOperator` | Strong isolation, fully reproducible runtime | Heavier image, slower startup, more disk/RAM |
| **External Python venv** | `ExternalPythonOperator` | Lightweight, fast startup, simpler debugging | Less isolated (shares the Airflow container) |

You only need to customize the path(s) you actually plan to use. By default this
module is shipped configured to install the **external Python virtual environment**
only (this is the **currently recommended** configuration; see Step 4).

#### 3.1 Docker container path (`env/Dockerfile`)

Edit [`env/Dockerfile`](env/Dockerfile) only if you intend to use the
`DockerOperator`-based DAGs (e.g. `cosidag_lcurve_dock.py`, `cosidag_tsmap_dock.py`).

Things you may want to adjust:

- the **base image** (`python:3.10-slim` by default);
- the list of **system packages** installed with `apt-get` (e.g. add libraries
  required by extra dependencies);
- the **`UID` / `GID`** ARGs at the top of the file. They are normally rewritten
  automatically by the `install` script to match your host user, so you usually
  do not need to touch them by hand;
- the Python dependencies installed inside the image (see Step 3.2 — they are
  read from `requirements.txt`).

This path is heavier on disk and CPU than the virtualenv path, but provides a
fully isolated runtime that is easier to reproduce on different machines.

#### 3.2 External Python virtual environment path (`env/requirements.txt`)

Edit [`env/requirements.txt`](env/requirements.txt) if you intend to use the
external Python virtual environment (the default and currently recommended
configuration). This file is used both by:

- the **virtualenv** created inside the Airflow container by `hot_load_module.sh`
  (used by `ExternalPythonOperator` tasks);
- the **Docker image** built from `env/Dockerfile` (which `pip install`s the same
  list at build time).

Pin or change the dependencies you need (e.g. switch the `cosipy` branch from
`develop` to a stable release, add new scientific packages, etc.).

> ##### Footnote — Resolving dependency conflicts with `requirements_nodeps.txt`
>
> Some scientific packages used by this pipeline have **incompatible transitive
> dependencies** that `pip` cannot resolve in a single `pip install` call. A
> typical example is the conflict between `gbm-data-tools` (which pins
> `matplotlib<=3.2.1`) and `bc-tools` (which requires `matplotlib>=3.7.1`).
>
> To work around this, the module ships an additional file
> [`env/requirements_nodeps.txt`](env/requirements_nodeps.txt) that lists the
> packages that must be installed **without** their own dependency tree, i.e.
> with `pip install --no-deps`. The idea is:
>
> 1. install everything in `requirements.txt` normally — this resolves a
>    consistent set of common dependencies (NumPy, SciPy, matplotlib, …);
> 2. install the packages in `requirements_nodeps.txt` **on top**, with
>    `--no-deps`, so that pip does not try to downgrade or re-resolve packages
>    that have already been installed.
>
> The hot-loader honours both files automatically when the module configuration
> declares `requirements_no_deps:` (as in
> [`env/fta-pipe.config.yaml`](env/fta-pipe.config.yaml)). If you hit a new
> dependency conflict while customizing the requirements, the recommended fix
> is to **move the offending package** from `requirements.txt` to
> `requirements_nodeps.txt` and re-run the installer.
>
> Note: installing with `--no-deps` means **you take responsibility** for the
> compatibility of that package with the rest of the environment — only do this
> for packages that are known to work with the already-installed versions.

### Step 4 — Review the module configuration file

The module ships with [`env/fta-pipe.config.yaml`](env/fta-pipe.config.yaml).
This file tells `hot_load_module.sh`:

- where to find the DAGs, the pipeline scripts and the Docker context;
- which Python virtual environments to create inside the Airflow container;
- whether to build the Docker image, the virtualenv, both, or none.

The default content is:

```yaml
install_mode: environment        # only create the Python virtual environment

paths:
  dags: src/dags
  pipeline: src/pipeline
  images: env

environments:
  cosipy:
    requirements: env/requirements.txt
    requirements_no_deps: env/requirements_nodeps.txt
    venv_path: /home/gamma/envs/cosipy
    enabled: true
    description: "Stable cosipy environment"
    python_version: "3.12"

default_environment: cosipy
```

The most relevant key is `install_mode`:

- `environment` — only create the Python virtualenv inside the Airflow container
  (**default and currently recommended**);
- `container` — only build the module Docker image;
- `both` — create the virtualenv **and** build the Docker image;
- `none` — only link DAGs and pipeline scripts.

You can leave this file unchanged for the default flow. The `install` script
will, in any case, force `install_mode: environment` before invoking the
hot-loader, so the recommended setup works out of the box.

If you want to use a different mode (e.g. `both`), edit the file **after**
running the script, or change the script to skip that step.

For a complete description of every field, see the *Using Configuration Files*
section in [`cosiflow/env/README.md`](../cosiflow/env/README.md).

### Step 5 — Run the install script

From the module root, simply run:

```bash
cd ~/cosi/fast-transient-analysis-pipeline/env/bin
./install
```

If your workspace is **not** under `~/cosi`, pass the actual path with `-c`:

```bash
./install -c /absolute/path/to/cosi
```

The script automates the full bootstrap of Cosiflow **and** the installation of
this module. In order, it will:

1. **Clone Cosiflow** (`https://github.com/cositools/cosiflow.git`) into
   `<COSI_PATH>/cosiflow` and check out the `container_arch` branch.
2. **Patch UID/GID** in the Cosiflow `docker-compose.yaml` and in
   `Dockerfile.airflow` so that the Airflow container user (`gamma`) matches
   your host user; this avoids permission issues on bind-mounted volumes.
3. **Configure the Airflow stack** by interactively prompting for the main
   `docker-compose.yaml` variables (admin password, host IP, admin username and
   email, MailHog and Airflow Web UI ports, Postgres user/database/password).
   Pressing *Enter* on a prompt accepts the default value shown in parentheses.
4. **Patch UID/GID** in this module's `env/Dockerfile` as well, again to match
   your host user.
5. **Pre-create the host directories** that will be bind-mounted as Docker
   volumes (`<COSI_PATH>/cosiflow/pipeline`, `<COSI_PATH>/cosiflow/data`,
   `<COSI_PATH>/envs`) so that they are owned by your user and not by `root`.
6. **Build the Cosiflow image stack** with `docker compose build` from
   `<COSI_PATH>/cosiflow/env`.
7. **Start the Cosiflow stack** in detached mode with `docker compose up -d`
   (Airflow webserver, scheduler, Postgres, MailHog, …).
8. **Force `install_mode: environment`** in
   `fast-transient-analysis-pipeline/env/fta-pipe.config.yaml` (i.e. the
   recommended virtualenv-only setup).
9. **Hot-load this module** by running
   `./hot_load_module.sh fast-transient-analysis-pipeline install` from
   `<COSI_PATH>/cosiflow/env`. This creates the symlinks for DAGs and pipeline
   scripts and builds the `cosipy` Python virtualenv inside the Airflow
   container, using `env/requirements.txt` (and `env/requirements_nodeps.txt`
   for the no-deps section).

### Step 6 — Fill in the prompts and wait

The script is interactive: at several points it will:

- ask you to type a value (with a sensible default in parentheses) — just press
  *Enter* to accept the default, or type your own value and press *Enter*;
- ask you to press *Enter* to continue between major steps — this gives you a
  chance to inspect the modified files and the build logs before proceeding.

The longest steps are the Cosiflow image build (step 6) and the creation of the
`cosipy` virtualenv inside the Airflow container (step 9), which compiles
several scientific packages from source. On a typical workstation the full run
takes **tens of minutes**.

When the script completes, you should see a final green log message and the
`hot_load_module.sh` summary; at that point Airflow will already be running
with the new DAGs being indexed.

---

## After the installation

Once the script has finished:

- The Airflow Web UI is reachable at
  `http://<HOST_IP>:<AIRFLOW_WEBUI_PORT>` (defaults: `http://localhost:8080`)
  with the admin credentials you provided during step 5.3 of the installer.
- The MailHog Web UI is reachable at
  `http://<HOST_IP>:<MAILHOG_WEBUI_PORT>` (default: `http://localhost:8025`).
- Airflow may need **a few minutes** to scan the new DAGs before they appear in
  the UI. If they do not show up, see the *Troubleshooting* section in
  [`cosiflow/env/README.md`](../cosiflow/env/README.md) (in particular the
  `airflow dags list` diagnostic command).

To **update** the module after editing DAGs, pipeline scripts, the Dockerfile or
the requirements:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh fast-transient-analysis-pipeline update
```

To **remove** the module from the running Cosiflow instance:

```bash
cd ~/cosi/cosiflow/env
./hot_load_module.sh fast-transient-analysis-pipeline remove
```

For everything else — managing multiple modules, switching to the
`DockerOperator` path, writing your own DAGs on top of `COSIDAG`, troubleshooting
DAG discovery, etc. — refer to the main Cosiflow documentation in
[`cosiflow/env/README.md`](../cosiflow/env/README.md).
