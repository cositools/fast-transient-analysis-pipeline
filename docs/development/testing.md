# Testing and documentation checks

## Unit and smoke tests

Run the repository tests from the module root:

```bash
python3 -m unittest discover -s tests -v
```

Tests that require a live Airflow installation may be skipped when Airflow is
not installed. A successful unit-test run does not replace a COSIflow smoke run
for DAG import, managed environments, data staging, or scientific products.

Compile changed Python modules and run a targeted unused-import check as part
of code cleanup:

```bash
python3 -m compileall src tests
ruff check --select F401 <changed-python-files>
```

`ruff` is a development tool, not a documentation dependency.

## Documentation

Documentation dependencies are isolated from the application environments and
exactly pinned in `requirements-docs.txt`:

```bash
python3 -m pip install -r requirements-docs.txt
python3 -m mkdocs build --strict
python3 scripts/check_docs_links.py docs README.md
```

Use `--external` for the network-dependent link check:

```bash
python3 scripts/check_docs_links.py --external docs README.md
```

The documentation workflow runs the strict build and external link check on
relevant pull requests and `dev` pushes. It validates documentation changes but
does not publish a website. Public hosting requires separate project approval.
The generated `site` directory is ignored and must not be committed.
