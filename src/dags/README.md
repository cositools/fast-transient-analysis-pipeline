# DAG Catalog

This directory contains the Airflow DAGs shipped by the Fast Transient Analysis Pipeline module.
The DAGs are installed into Cosiflow with `cosiflow/env/hot_load_module.sh`.

## DAG Summary

| DAG ID | File | Type | Runtime | Purpose |
| --- | --- | --- | --- | --- |
| `init_pipelines` | `cosipipe_initpipeline.py` | Standard DAG | `PythonOperator`, `DockerOperator` | Stage GeD or BGO inputs and create a run `products/` folder |
| `cosidag_lcurve_extpy` | `cosidag_lcurve_extpy.py` | COSIDAG | `ExternalPythonOperator` | Light curve products from staged GRB/background inputs |
| `cosidag_lcurve_dock` | `cosidag_lcurve_dock.py` | COSIDAG | `DockerOperator` | Docker-based Light Curve variant |
| `cosidag_tsmap_extpy` | `cosidag_tsmap_extpy.py` | COSIDAG | `ExternalPythonOperator` | TS map products from staged GRB/background inputs |
| `cosidag_tsmap_dock` | `cosidag_tsmap_dock.py` | COSIDAG | `DockerOperator` | Docker-based TS Map variant |
| `cosidag_fast_localize_grb` | `cosidag_fast_localize_grb.py` | COSIDAG | `ExternalPythonOperator` | Fast GRB localization |
| `cosidag_fast_grb_timeseries` | `cosidag_fast_grb_timeseries.py` | COSIDAG | `ExternalPythonOperator` | Fast GRB time-series/light-curve extraction |
| `cosidag_BGO` | `cosidag_BGO.py` | COSIDAG | `ExternalPythonOperator`, `EmptyOperator` | BGO branch of the fast transient pipeline |
| `cosidag_GeD` | `cosidag_GeD.py` | COSIDAG | `ExternalPythonOperator`, `EmptyOperator` | GeD branch of the fast transient pipeline |

## `init_pipelines`

`init_pipelines` is the shared staging and initialization DAG for the GeD and
BGO data families. Existing triggers that omit `pipeline_branch` continue to
use GeD.

Airflow 2.10 renders the parameters in three visual sections:

| Section | Parameters | Purpose |
| --- | --- | --- |
| General | `pipeline_branch` | `GeD` (default) or `BGO` |
| General | `destination` | GeD destination: `lcurve`, `tsmap`, `fast`, or `tdrss`; BGO always resolves to `tdrss` |
| GeD inputs | `data_challenge` | Preset input set, `DC3` or `DC4` |
| GeD inputs | `source_path`, `background_path`, `orientation_path`, `response_path` | GeD Wasabi keys; `__default__` uses the selected Data Challenge preset |
| GeD inputs | `eps_time` | GeD background-cut time tolerance |
| BGO inputs | `lightcurve_path` | BGO multi-panel light-curve NPZ |
| BGO inputs | `soft_lut_path`, `medium_lut_path`, `hard_lut_path` | BGO localization lookup tables |
| BGO inputs | `bgo_orientation_path` | BGO orientation (`.fits` or legacy `.ori`) |

Every file parameter accepts `__default__`. The staging script also accepts a
complete Wasabi key or a local path. Runtime resolution reads and validates
only the inputs belonging to `pipeline_branch`; fields in the other visual
section are not used.

The BGO defaults are:

| Input | Default Wasabi key |
| --- | --- |
| Light curve | `COSI-SMEX/develop/Data/Responses/BGO/bn240810880light_curve.npz` |
| Soft LUT | `COSI-SMEX/develop/Data/Responses/BGO/soft_local_loc_table_run17.pkl` |
| Medium LUT | `COSI-SMEX/develop/Data/Responses/BGO/medium_local_loc_table_run17.pkl` |
| Hard LUT | `COSI-SMEX/develop/Data/Responses/BGO/hard_local_loc_table_run17.pkl` |
| Orientation | `COSI-SMEX/DC4/Data/Orientation/DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits` |

The BGO orientation choices also include the DC3 legacy orientation
`COSI-SMEX/DC3/Data/Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori`.

The shared prefix and branch-specific tails are:

```text
prepare_raw_dirs -> resolve_config -> stage_all_files -> create_products_dir -> create_symlinks
  -> select_postprocessing -> background_cut        (GeD)
                           `-> bgo_staging_complete (BGO)
  -> initialization_complete
```

GeD stages source, background, orientation, and response, then creates the
background-window FITS product. BGO stages the light curve, three LUTs, and
orientation, creates five symlinks in `products/`, and never runs the GeD
background cut. The final join accepts one successful branch and the skipped
other branch, so both paths finish successfully.

The DAG writes products under:

| `destination` | Output root |
| --- | --- |
| `lcurve` | `/home/gamma/workspace/data/lcurve` |
| `tsmap` | `/home/gamma/workspace/data/tsmap` |
| `fast` | `/home/gamma/workspace/data/fast_localize_grb` |
| `tdrss` | `/home/gamma/workspace/data/tdrss` |

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

## Testing the GeD Branch

The GeD test flow is:

```text
init_pipelines -> /home/gamma/workspace/data/tdrss/.../products -> cosidag_GeD
```

`init_pipelines` stages the simulator inputs and creates the `products/` folder.
`cosidag_GeD` does not receive a direct Airflow trigger; it watches the `tdrss`
output tree and starts when it finds a new `products/` folder with the expected
files.

Before testing, make sure the runtime pieces used by both DAGs are available:

| Runtime piece | Used by | Expected path or image |
| --- | --- | --- |
| Module Docker image | `init_pipelines` Docker tasks | `fast-transient-analysis-pipeline:latest` |
| Cosipy virtualenv | GeD COSIpy analysis tasks | `/home/gamma/envs/cosipy/bin/python` |
| BCT virtualenv | GeD duration task | `/home/gamma/envs/bct/bin/python` |
| Host workspace mount | Docker tasks | `HOST_WORKSPACE_PATH` in `cosiflow/env/docker-compose.yaml` |

The default module installation may create only the Python environments,
depending on `env/fta-pipe.config.yaml`. If the image
`fast-transient-analysis-pipeline:latest` does not exist, build the module in
container mode or `both` mode before running `init_pipelines`.

To run the GeD branch from Airflow:

1. In the Airflow UI, unpause `cosidag_GeD`.
2. Trigger `init_pipelines`.
3. Use these trigger parameters for a default GeD smoke test:

   | Parameter | Value |
   | --- | --- |
   | `pipeline_branch` | `GeD` |
   | `data_challenge` | `DC4` |
   | `response_path` | `__default__` |
   | `orientation_path` | `__default__` |
   | `source_path` | `__default__` |
   | `background_path` | `__default__` |
   | `destination` | `tdrss` |
   | `eps_time` | `1` |

4. Wait for the full `init_pipelines` chain to succeed, especially
   `background_cut`.
5. Check that a new folder exists under:

   ```text
   /home/gamma/workspace/data/tdrss/YYYY_MM/YYMMDDXXX/products
   ```

6. `cosidag_GeD` should pick up that folder automatically and start from
   `PreProcessing_GeD`.

The `products/` folder must contain the staged GRB/source file, the background
window file created by `background_cut`, the orientation file, and the response
file. These are matched by `cosidag_GeD` as `grb_file`, `background_file`,
`orientation_file`, and `response_file`.

## Testing the BGO Branch

1. Unpause `cosidag_BGO`.
2. Trigger `init_pipelines` with `pipeline_branch=BGO`; leave the five BGO
   input parameters at `__default__` for the standard run17 data.
3. The initializer creates a new
   `/home/gamma/workspace/data/tdrss/YYYY_MM/YYMMDDXXX/products` folder with
   the NPZ light curve, three LUTs, and orientation symlinks.
4. `background_cut` is skipped, `initialization_complete` succeeds, and
   `cosidag_BGO` discovers the new folder.

The basenames are intentionally preserved so the files match
`lightcurve_file`, `soft_lut_file`, `medium_lut_file`, `hard_lut_file`, and
`orientation_file` in `cosidag_BGO.py`.

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
