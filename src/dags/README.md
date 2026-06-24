# DAG Catalog

This directory contains the Airflow DAGs shipped by the Fast Transient Analysis Pipeline module.
The DAGs are installed into Cosiflow with `cosiflow/env/hot_load_module.sh`.

## DAG Summary

| DAG ID | File | Type | Runtime | Purpose |
| --- | --- | --- | --- | --- |
| `init_pipelines` | `cosipipe_simdata.py` | Standard DAG | `PythonOperator`, `DockerOperator` | Stage input files and create a run `products/` folder |
| `cosidag_lcurve_extpy` | `cosidag_lcurve_extpy.py` | COSIDAG | `ExternalPythonOperator` | Light curve products from staged GRB/background inputs |
| `cosidag_lcurve_dock` | `cosidag_lcurve_dock.py` | COSIDAG | `DockerOperator` | Docker-based Light Curve variant |
| `cosidag_tsmap_extpy` | `cosidag_tsmap_extpy.py` | COSIDAG | `ExternalPythonOperator` | TS map products from staged GRB/background inputs |
| `cosidag_tsmap_dock` | `cosidag_tsmap_dock.py` | COSIDAG | `DockerOperator` | Docker-based TS Map variant |
| `cosidag_fast_localize_grb` | `cosidag_fast_localize_grb.py` | COSIDAG | `ExternalPythonOperator` | Fast GRB localization |
| `cosidag_fast_grb_timeseries` | `cosidag_fast_grb_timeseries.py` | COSIDAG | `ExternalPythonOperator` | Fast GRB time-series/light-curve extraction |
| `cosidag_BGO` | `cosidag_BGO.py` | COSIDAG | `ExternalPythonOperator`, `EmptyOperator` | BGO branch of the fast transient pipeline |
| `cosidag_GeD` | `cosidag_GeD.py` | COSIDAG | `ExternalPythonOperator`, `EmptyOperator` | GeD branch of the fast transient pipeline |

## `init_pipelines`

`init_pipelines` is the staging and initialization DAG.

It accepts Airflow trigger parameters:

| Parameter | Purpose |
| --- | --- |
| `response_path` | Response file or remote key |
| `orientation_path` | Orientation file or remote key |
| `source_path` | Source/GRB file or remote key |
| `background_path` | Background file or remote key |
| `destination` | One of `lcurve`, `tsmap`, `fast` |
| `eps_time` | Background-cut time tolerance |

Its task chain is:

```text
prepare_raw_dirs -> resolve_config -> stage_all_files -> create_products_dir -> create_symlinks -> background_cut
```

The DAG writes products under:

| `destination` | Output root |
| --- | --- |
| `lcurve` | `/home/gamma/workspace/data/lcurve` |
| `tsmap` | `/home/gamma/workspace/data/tsmap` |
| `fast` | `/home/gamma/workspace/data/fast_localize_grb` |

`init_pipelines` does not directly trigger downstream COSIDAGs. The downstream COSIDAGs monitor these output roots and pick up new `products/` folders.

## Light Curve COSIDAGs

| DAG ID | Monitored folder | File patterns | Custom tasks |
| --- | --- | --- | --- |
| `cosidag_lcurve_extpy` | `/home/gamma/workspace/data/lcurve` | `grb_file`, `background_file`, `orientation_file`, `response_file` | `bin_grb_source`, `bin_background`, `plot_lightcurve` |
| `cosidag_lcurve_dock` | `/home/gamma/workspace/data/lcurve` | `grb_file`, `background_file`, `orientation_file`, `response_file` | `bin_grb_source`, `bin_background`, `plot_lightcurve` |

The external-Python variant runs in `/home/gamma/envs/cosipy/bin/python`.
The Docker variant runs in the `fast-transient-analysis-pipeline:latest` image.

## TS Map COSIDAGs

| DAG ID | Monitored folder | File patterns | Custom tasks |
| --- | --- | --- | --- |
| `cosidag_tsmap_extpy` | `/home/gamma/workspace/data/tsmap` | `grb_file`, `background_file`, `orientation_file`, `response_file` | `bin_grb_source`, `bin_background`, `ts_map_computation`, `ts_map_mulres_computation` |
| `cosidag_tsmap_dock` | `/home/gamma/workspace/data/tsmap` | `grb_file`, `background_file`, `orientation_file`, `response_file` | `bin_grb_source`, `bin_background`, `ts_map_computation`, `ts_map_mulres_computation` |

The two variants expose the same workflow shape and differ by runtime isolation.

## Fast GRB COSIDAGs

| DAG ID | Monitored folder | File patterns | Custom task |
| --- | --- | --- | --- |
| `cosidag_fast_localize_grb` | `/home/gamma/workspace/data/fast_localize_grb` | `grb_file`, `background_file` | `fast_localize_grb` |
| `cosidag_fast_grb_timeseries` | `/home/gamma/workspace/data/fast_localize_grb` | `grb_file` | `fast_grb_timeseries` |

These DAGs use `ExternalPythonOperator` and the module's fast GRB helper scripts.

## Fast Transient Branch COSIDAGs

| DAG ID | Monitored folder | File patterns | Purpose |
| --- | --- | --- | --- |
| `cosidag_BGO` | `/home/gamma/workspace/data/tdrss` | `lightcurve_file`, LUT files, `orientation_file` | BGO pre-processing, duration, background preparation, localization, significance, classification, GCN placeholders |
| `cosidag_GeD` | `/home/gamma/workspace/data/tdrss` | `grb_file`, `background_file`, `orientation_file`, `response_file` | GeD pre-processing, binning, light curve, duration, TS map, localization summary, classification, GCN placeholders |

Both branches use `ExternalPythonOperator` for implemented scientific steps and `EmptyOperator` for workflow placeholders.

## Shared COSIDAG Behavior

Most COSIDAGs in this module use:

| Setting | Value |
| --- | --- |
| `only_basename` | `products` |
| `select_policy` | `latest_mtime` |
| `auto_retrig` | `True` for continuously watching pipelines where configured |
| `level` | `3` |
| `idle_seconds` | `5` |

The exact values are defined in each DAG file and can be adjusted there when the workflow changes.
