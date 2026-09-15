# Fast Transient Analysis Pipeline

The Fast Transient Analysis Pipeline (FasTP) is a COSIflow module for rapid
analysis of transient events observed by the
[COSI mission](https://cosi.ssl.berkeley.edu/). It provides input staging and
filesystem-driven BGO, binned GeD, and unbinned ARM-selection DAGs.

## Quick start

Install Git, Bash, Docker Engine or Docker Desktop, and Docker Compose v2, then
run the bundled installer from a workspace directory:

```bash
git clone https://github.com/cositools/fast-transient-analysis-pipeline.git
cd fast-transient-analysis-pipeline/env/bin
./install
```

The installer is interactive. Review the credential prompts and the current
module configuration before starting a shared or production-like environment.

## Documentation

- [Installation](docs/getting-started/installation.md)
- [First run](docs/getting-started/first-run.md)
- [Architecture](docs/architecture/overview.md)
- [DAG reference](docs/reference/dags.md)
- [Pipeline configuration](docs/reference/configuration.md)
- [Scientific branches](docs/pipelines/bgo.md)
- [Using FasTP as a DAG/module example](docs/development/using-fastp-as-example.md)
- [Testing and documentation checks](docs/development/testing.md)

Build the complete local documentation with:

```bash
python3 -m pip install -r requirements-docs.txt
python3 -m mkdocs build --strict
python3 scripts/check_docs_links.py docs README.md
```

See [CHANGELOG.md](CHANGELOG.md) for release history. FasTP is under active
development; placeholder tasks and current runtime limitations are identified
explicitly in the documentation.
