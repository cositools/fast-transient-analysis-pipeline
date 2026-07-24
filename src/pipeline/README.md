# Pipeline Scripts

This directory contains the Python code executed by the Airflow DAGs in `../dags/`.

The DAG catalog and operational entry points are documented in:

```text
../dags/README.md
```

## Current Workflow

For the Light Curve and TS Map workflows, the usual flow is:

1. Enable the target COSIDAG in the Airflow UI:
   * `cosidag_lcurve_extpy` or `cosidag_lcurve_dock`
   * `cosidag_tsmap_extpy` or `cosidag_tsmap_dock`
2. Enable and trigger `init_pipelines` with `pipeline_branch=GeD` (the default).
3. Set the `destination` parameter:
   * `lcurve` writes staged products under `/home/gamma/workspace/data/lcurve`
   * `tsmap` writes staged products under `/home/gamma/workspace/data/tsmap`
   * `fast` writes staged products under `/home/gamma/workspace/data/fast_localize_grb`
   * `tdrss` writes staged products under `/home/gamma/workspace/data/tdrss`
4. The enabled COSIDAG discovers new `products/` folders through `monitoring_folders`.

For the fast-transient BGO path, enable `cosidag_BGO` and trigger the same
initializer with `pipeline_branch=BGO`. BGO stages its NPZ light curve, soft,
medium, and hard LUTs, plus orientation under the `tdrss` destination. It does
not run the GeD background cut.

The Airflow form groups parameters into `General`, `GeD inputs`, and
`BGO inputs`. `pipeline_branch` determines which input section is resolved and
staged at runtime; the UI does not dynamically hide the other section.

`init_pipelines` creates the run `products/` directory and symlinks for either
branch. GeD additionally runs the background cut. It does not directly trigger
the scientific COSIDAG with a `TriggerDagRunOperator`.

## Pipeline Areas

| Directory or file | Used by | Purpose |
| --- | --- | --- |
| `stage_files.py` | `init_pipelines` | Generically stages the selected GeD or BGO input mapping |
| `bkg_cut.py` | GeD path of `init_pipelines` | Creates the GeD background time-window product |
| `lcurve/` | `cosidag_lcurve_extpy`, `cosidag_lcurve_dock` | Light curve tasks |
| `ts_map/` | `cosidag_tsmap_extpy`, `cosidag_tsmap_dock` | TS map tasks |
| `fast_grb/` | `cosidag_fast_localize_grb`, `cosidag_fast_grb_timeseries` | Fast GRB localization and time-series tasks |
| `fast_transient_pipeline/` | `cosidag_BGO`, `cosidag_GeD` | Fast transient BGO and GeD branch helpers |
| `binning_script/` | Light Curve and TS Map tasks | Binning scripts and YAML inputs |
| `download_data.py` | Utility script | Data download helper |

## Runtime Styles

The module includes both external-Python and Docker-based task implementations.

| Style | DAG suffix | Runtime |
| --- | --- | --- |
| External Python | `_extpy` or no suffix for fast/BGO/GeD branches | `/home/gamma/envs/cosipy/bin/python` |
| Docker | `_dock` and `init_pipelines` Docker tasks | `fast-transient-analysis-pipeline:latest` |

The default module configuration installs the external Python environment. Docker-based
DAGs require the module Docker image and the host workspace mounts expected by the DAGs.
