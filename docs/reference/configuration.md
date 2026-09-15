# Configuration reference

## Module loader configuration

[`env/fta-pipe.config.yaml`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/env/fta-pipe.config.yaml)
is the source read by the COSIflow loader. It defines:

- `install_mode`: `container`, `environment`, `both`, or `none`;
- relative paths for DAGs, pipeline code, and the Docker build context;
- the enabled `cosipy`, `bct`, and `nimcosipy` environments;
- each environment's Python version, requirements file, and target path;
- `cosipy` as the default environment.

The dormant `requirements_no_deps` line for BCT is commented out. The file
`env/requirements_bct_nodeps.txt` is retained in the repository but is not read
by the current configuration.

## `init_pipelines` trigger parameters

The trigger form and runtime validation are defined by
[`cosipipe_initpipeline.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/cosipipe_initpipeline.py)
and the pure helper module
[`init_pipeline_config.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/init_pipeline_config.py).

| Parameter | Default | Validation or behavior |
| --- | --- | --- |
| `pipeline_branch` | `GeD` | `GeD` or `BGO`; missing legacy values resolve to `GeD` |
| `destination` | `tdrss` | one of `lcurve`, `tsmap`, `fast`, `tdrss`; ignored by BGO |
| `data_challenge` | `DC4` | `DC3` or `DC4`; GeD only |
| `source_path`, `background_path`, `orientation_path`, `response_path` | `__default__` | Wasabi key or local path; GeD only |
| `eps_time` | `50` | non-negative number; GeD background-cut margin in seconds |
| `lightcurve_path`, three LUT paths, `bgo_orientation_path` | `__default__` | Wasabi key or local path; BGO only |

Only parameters belonging to the selected branch are resolved. BGO forces the
destination to `tdrss` and bypasses `background_cut`.

## Runtime overrides

The scientific DAGs accept environment overrides for interpreter and library
paths: `EXTERNAL_PYTHON_COSIPY`, `EXTERNAL_PYTHON_BCT`,
`EXTERNAL_PYTHON_BGO`, `EXTERNAL_PYTHON_NIMCOSIPY`, and
`LIB_DIR_FAST_TRANSIENT_PIPELINE`. `COSIPY_PYTHON` overrides the interpreter
used by staging and background cut.

GCN-specific settings are listed in the [GCN integration guide](../integration/gcn.md).
