# Using FasTP as a COSIflow module example

FasTP is a worked example for developers creating a new COSIflow module or
filesystem-driven DAG. Reuse its repository boundaries and test strategy; do
not copy its scientific parameters, filenames, state schema, or task graph as
generic COSIflow requirements.

## Reusable repository pattern

| Area | FasTP example | What a new module owns |
| --- | --- | --- |
| Loader contract | `env/fta-pipe.config.yaml` | DAG and pipeline paths, install mode, environments, requirements |
| DAG definitions | `src/dags/` | DAG IDs, parameters, monitoring policy, operator wiring |
| Task code | `src/pipeline/` | Callable inputs, outputs, validation, state and errors |
| Scientific runtimes | managed `cosipy`, `bct`, and `nimcosipy` environments | The smallest compatible environment for each task |
| Tests | `tests/` | Parameter validation, graph shape, runtime choice and stable outputs |
| Documentation | `docs/` and `mkdocs.yml` | The module's supported installation and runtime contract |

Start with the [COSIflow repository documentation](https://github.com/cositools/cosiflow)
for the loader interface, then compare FasTP's
[configuration](../reference/configuration.md),
[DAG definitions](../reference/dags.md), and
[pipeline layout](../reference/pipeline-code.md).

## Suggested development sequence

1. Create `src/dags/`, `src/pipeline/`, `env/`, `docs/`, and `tests/`.
2. Declare module paths and isolated Python environments in a checked-in
   `*.config.yaml` file.
3. Import the shared DAG class with `from cosidag import COSIDAG`.
4. Keep orchestration in the DAG file and testable implementation logic in the
   pipeline package.
5. Select `PythonOperator` or `ExternalPythonOperator` from the actual runtime
   dependency boundary. Do not assume that Airflow has Docker daemon access.
6. Define monitoring folders, glob or `regex:` input patterns, selection policy,
   and mutable state ownership explicitly.
7. Add characterization tests before changing messages, task wiring, state, or
   scientific outputs.
8. Document only behavior supported by code, configuration, schemas, and tests;
   mark placeholders and open limitations directly.
9. Build the module documentation with `mkdocs build --strict` and run its link
   checker before review.

## Patterns demonstrated by FasTP

FasTP demonstrates a staging DAG that creates a filesystem handoff rather than
directly triggering downstream DAGs. Its three COSIDAGs then monitor the handoff
directory and resolve branch-specific inputs. It also demonstrates:

- isolated external-Python environments selected per task;
- distinct state files for workflows that may observe the same input folder;
- branch-specific parameter validation before staging;
- explicit placeholder tasks that are not presented as implemented science;
- database-mediated GCN integration rather than Kafka access from scientific
  tasks;
- compact XCom values while large products remain on disk.

These are examples, not mandatory choices for every module. A new DAG should
adopt only the patterns justified by its own execution and data contracts.

## Review checklist

Before handing a new module to another developer, verify that:

- every DAG imports in the supported Airflow environment;
- defaults and validation agree with the trigger form;
- task runtimes match the declared environments;
- the module owns its mutable state and cannot collide with another DAG;
- placeholders and known races are clearly labelled;
- no credential, workstation path, or sibling-repository documentation link is
  committed;
- tests cover any behavior changed during cleanup;
- `mkdocs build --strict` and the link checker pass independently of COSIflow's
  documentation build.
