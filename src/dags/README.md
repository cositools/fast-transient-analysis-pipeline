# FasTP DAG catalog

This directory contains the four Airflow DAGs currently shipped by the Fast
Transient Analysis Pipeline module.

## Summary

| DAG ID | File | Type | Main runtime | Purpose |
| --- | --- | --- | --- | --- |
| `init_pipelines` | `cosipipe_initpipeline.py` | standard DAG | Airflow + module image | Resolve and stage GeD or BGO inputs |
| `cosidag_BGO` | `cosidag_BGO.py` | COSIDAG | `cosipy`, `bct`, `nimcosipy` | BGO transient analysis |
| `cosidag_GeD` | `cosidag_GeD.py` | COSIDAG | `cosipy`, `bct` | Binned GeD analysis |
| `cosidag_ARMselection` | `cosidag_ARMselection.py` | COSIDAG | `cosipy` | Unbinned ARM selection and localization |

The module no longer ships the former standalone Light Curve, TS Map, or Fast
GRB COSIDAG files. Their maintained functionality is part of the GeD and
ARM-selection branches.

## `init_pipelines`

`init_pipelines` is the common input resolver and staging DAG. Legacy triggers
without `pipeline_branch` default to `GeD`.

### Trigger form

Airflow 2.10 groups parameters into:

| Section | Parameters |
| --- | --- |
| General | `pipeline_branch`, `destination` |
| GeD inputs | `data_challenge`, `source_path`, `background_path`, `orientation_path`, `response_path`, `eps_time` |
| BGO inputs | `lightcurve_path`, `soft_lut_path`, `medium_lut_path`, `hard_lut_path`, `bgo_orientation_path` |

Every file parameter accepts:

- `__default__`;
- a complete Wasabi key;
- a local path visible inside the Airflow container.

Only fields belonging to the selected branch are resolved and validated.

### Default BGO inputs

| Input | Default Wasabi key |
| --- | --- |
| Light curve | `COSI-SMEX/develop/Data/Responses/BGO/bn240810880light_curve.npz` |
| Soft LUT | `COSI-SMEX/develop/Data/Responses/BGO/soft_local_loc_table_run17.pkl` |
| Medium LUT | `COSI-SMEX/develop/Data/Responses/BGO/medium_local_loc_table_run17.pkl` |
| Hard LUT | `COSI-SMEX/develop/Data/Responses/BGO/hard_local_loc_table_run17.pkl` |
| Orientation | `COSI-SMEX/DC4/Data/Orientation/DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits` |

The DC3 `.ori` orientation is also accepted.

### Task graph

```text
prepare_raw_dirs
  -> resolve_config
  -> stage_all_files
  -> create_products_dir
  -> create_symlinks
  -> select_postprocessing
       |- background_cut        (GeD)
       `- bgo_staging_complete  (BGO)
  -> initialization_complete
```

GeD stages source, background, orientation, and response, then creates the
background-window FITS product. BGO stages its light curve, LUTs, and
orientation and skips `background_cut`.

### Destination roots

| `destination` | Root |
| --- | --- |
| `lcurve` | `/home/gamma/workspace/data/lcurve` |
| `tsmap` | `/home/gamma/workspace/data/tsmap` |
| `fast` | `/home/gamma/workspace/data/fast_localize_grb` |
| `tdrss` | `/home/gamma/workspace/data/tdrss` |

BGO always resolves to `tdrss`. The three active scientific COSIDAGs currently
monitor `tdrss`.

`init_pipelines` does not invoke `TriggerDagRunOperator`. It creates a new
`YYYY_MM/YYMMDDXXX/products` directory that an unpaused COSIDAG sensor discovers.

## Shared COSIDAG input contract

All scientific branches use:

| Setting | Value |
| --- | --- |
| `monitoring_folders` | `/home/gamma/workspace/data/tdrss` |
| `policy` | `folder-driven` |
| `only_basename` | `products` |
| `level` | `3` |
| `idle_seconds` | `5` |
| `min_files` | `1` |
| `select_policy` | `latest_mtime` |
| `auto_retrig` | `True` |

Because BGO and GeD products share the same monitored root and file matching
happens after folder selection, keep unrelated scientific DAGs paused during a
single-branch smoke test.

## `cosidag_BGO`

### Resolved files

| XCom key | Pattern |
| --- | --- |
| `lightcurve_file` | `*.npz` |
| `soft_lut_file` | `soft_*.pkl` |
| `medium_lut_file` | `medium_*.pkl` |
| `hard_lut_file` | `hard_*.pkl` |
| `orientation_file` | regex excluding GRB/BG and accepting `.fits` or `.ori` |

### Graph

```text
PreProcessing_BGO
  -> Duration_on_different_binning
       |- Light_Curve_generation
       `- Background_extraction_and_data_preparation
            |- Localization_Chi2           (placeholder)
            |- Localization_bc_tools       (active)
            |- Localization_DL             (placeholder)
            `- Significance_Analysis
  -> Localization_Results                  (join placeholder)
       |- Classification_BGO               (placeholder)
       `- GCN_BGO                          (active outbox task)
```

Detailed products and state are documented in
[`../pipeline/fast_transient_pipeline/README_BGO.md`](../pipeline/fast_transient_pipeline/README_BGO.md).

## `cosidag_GeD`

This DAG owns only the binned COSIpy branch.

### Resolved files

| XCom key | Pattern |
| --- | --- |
| `grb_file` | `*[Gg][Rr][Bb]*.fits*` |
| `background_file` | `*[Bb][Gg]*_window.fits*` |
| `orientation_file` | regex excluding GRB/BG and accepting `.fits` or `.ori` |
| `response_file` | regex excluding GRB/BG and accepting `.h5` |

### Graph

```text
PreProcessing_GeD
  -> Data_Binning
  -> TS_Map_on_different_timescales
  -> Light_Curve
  -> Duration
  -> Spectral_Analysis       (placeholder)
  -> Classification_GeD      (placeholder)
  -> GCN_GeD                 (active outbox task)
```

The mutable state is `pipeline_config.yaml`. Detailed products are documented
in
[`../pipeline/fast_transient_pipeline/README_GeD.md`](../pipeline/fast_transient_pipeline/README_GeD.md).

## `cosidag_ARMselection`

This independent DAG owns the unbinned analysis previously embedded in the GeD
graph. It resolves the same four input files as `cosidag_GeD`.

```text
PreProcessing_ARMselection
  -> Unbinned_Light_Curve_Generation
  -> Light_Curve_Analysis
  -> Skymap_unbinned
  -> Duration_and_Localization_Results
  -> GCN_ARMselection
```

Its mutable state is `armsel_pipeline_config.yaml`, so it can run alongside
`cosidag_GeD` without both graphs writing the same YAML file.

Detailed products are documented in
[`../pipeline/fast_transient_pipeline/README_ARMselection.md`](../pipeline/fast_transient_pipeline/README_ARMselection.md).

## Smoke tests

### GeD and ARM selection

1. Pause `cosidag_BGO`.
2. Unpause either or both of `cosidag_GeD` and `cosidag_ARMselection`.
3. Trigger `init_pipelines` with:

   | Parameter | Value |
   | --- | --- |
   | `pipeline_branch` | `GeD` |
   | `data_challenge` | `DC4` |
   | `source_path` | `__default__` |
   | `background_path` | `__default__` |
   | `orientation_path` | `__default__` |
   | `response_path` | `__default__` |
   | `destination` | `tdrss` |
   | `eps_time` | `1` |

4. Wait for `background_cut` and `initialization_complete`.
5. Confirm the new `tdrss/YYYY_MM/YYMMDDXXX/products` directory contains the
   GRB file, background-window file, orientation, and response.
6. Each unpaused GeD/ARM COSIDAG should discover the directory independently.

Required runtimes are:

| Runtime | Path or image |
| --- | --- |
| Module image | `fast-transient-analysis-pipeline:latest` |
| COSIpy | `/home/gamma/envs/cosipy/bin/python` |
| BCT | `/home/gamma/envs/bct/bin/python` |

### BGO

1. Pause `cosidag_GeD` and `cosidag_ARMselection`.
2. Unpause `cosidag_BGO`.
3. Trigger `init_pipelines` with `pipeline_branch=BGO`.
4. Leave all five BGO file inputs at `__default__` for the standard run17 data.
5. Confirm `background_cut` is skipped and the new `products` directory contains
   the NPZ light curve, three LUTs, and orientation.
6. `cosidag_BGO` should discover the directory.

Required external interpreters are:

```text
/home/gamma/envs/cosipy/bin/python
/home/gamma/envs/bct/bin/python
/home/gamma/envs/nimcosipy/bin/python
```
