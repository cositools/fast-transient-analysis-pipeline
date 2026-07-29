# FasTP pipeline code

This directory contains the code executed by the DAGs in `../dags`.

## Entry points

| File or directory | Called by | Purpose |
| --- | --- | --- |
| `stage_files.py` | `init_pipelines.stage_all_files` | Download/copy and validate selected GeD or BGO inputs |
| `bkg_cut.py` | `init_pipelines.background_cut` | Build the GeD background-window FITS product |
| `download_data.py` | utility use | Wasabi data download helper |
| `fast_transient_pipeline/bgo_functions.py` | `cosidag_BGO` | BGO scientific functions |
| `fast_transient_pipeline/ged_functions.py` | `cosidag_GeD` and ARM preprocessing | Shared GeD preprocessing and binned analysis |
| `fast_transient_pipeline/armsel_functions.py` | `cosidag_ARMselection` | Unbinned ARM gating and localization |
| `fast_transient_pipeline/fast_helper_functions.py` | all scientific branches | Shared configuration, plotting, and bookkeeping helpers |
| `fast_transient_pipeline/gcn/query.py` | BGO/ARM science tasks | Fail-open GCN inbox queries |
| `fast_transient_pipeline/gcn/outbox.py` | final GCN tasks | Build notice JSON and queue durable outbox rows |

The removed `lcurve/`, `ts_map/`, `fast_grb/`, and `binning_script/` directories
are not current runtime entry points.

## Current handoff

`init_pipelines` creates a new `products` directory. It does not trigger a
scientific DAG directly.

```text
init_pipelines(GeD)
  -> tdrss/.../products
       |- cosidag_GeD          -> pipeline_config.yaml
       `- cosidag_ARMselection -> armsel_pipeline_config.yaml

init_pipelines(BGO)
  -> tdrss/.../products
       `- cosidag_BGO          -> pipeline_config.yaml
```

The two GeD-family graphs resolve the same staged input files but keep separate
mutable state.

## Runtime mapping

| Work | Runtime |
| --- | --- |
| staging and GeD background cut | `fast-transient-analysis-pipeline:latest` |
| binned GeD and ARM selection | `/home/gamma/envs/cosipy/bin/python` |
| duration/Bayesian Blocks | `/home/gamma/envs/bct/bin/python` |
| BGO non-imaging localization | `/home/gamma/envs/nimcosipy/bin/python` |
| GCN outbox insertion | Airflow Python environment |

## Detailed documentation

- [DAG catalog and smoke tests](../dags/README.md)
- [BGO data flow](fast_transient_pipeline/README_BGO.md)
- [Binned GeD data flow](fast_transient_pipeline/README_GeD.md)
- [ARM-selection data flow](fast_transient_pipeline/README_ARMselection.md)
