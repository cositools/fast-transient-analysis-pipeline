# DAG reference

## Catalog

| DAG ID | Type | Runtime summary | Purpose |
| --- | --- | --- | --- |
| `init_pipelines` | standard Airflow DAG | Airflow plus managed COSIpy commands | Resolve and stage GeD, BGO or both input families |
| `cosidag_BGO` | COSIDAG | `cosipy`, `bct`, `nimcosipy`, Airflow | BGO transient analysis |
| `cosidag_GeD` | COSIDAG | `cosipy`, `bct`, Airflow | Binned GeD analysis |

All three DAGs have no time schedule. The two scientific DAGs use COSIDAG
folder monitoring with `level=3`, `only_basename=products`, `idle_seconds=5`,
`min_files=1`, `select_policy=latest_mtime`, and `auto_retrig=True`. They
monitor `/home/gamma/workspace/data/tdrss` and maintain independent processed
folder history under their DAG IDs.

## `init_pipelines`

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

The DAG creates a products directory for a single-family trigger, or two
independent products directories when `pipeline_branch=both`. In combined mode,
`select_postprocessing` selects both downstream paths: GeD background cutting
and BGO staging completion. The join waits for both selected paths and fails
if either fails. Existing task IDs and single-family output shapes remain
available. The DAG does not use `TriggerDagRunOperator`. Parameters and defaults are documented in
[configuration](configuration.md).

The initializer resolves `stage_files.py` and `bkg_cut.py` from the module's
own `src/pipeline` directory using the resolved location of
`init_pipeline_config.py`. This also works when Airflow imports the DAG through
the installed `.cfmodule` symlink. No scripts or compatibility symlinks are
required directly under `/home/gamma/airflow/pipeline`.

## `cosidag_BGO`

```text
PreProcessing_BGO
  -> Duration_on_different_binning
       |- Light_Curve_generation
       `- Background_extraction_and_data_preparation
            |- Significance_Analysis
            |- Localization_bc_tools
            |- Localization_Chi2       (placeholder)
            `- Localization_DL         (placeholder)
  -> Localization_Results              (placeholder join)
       |- Classification_BGO            (placeholder)
       `- GCN_BGO
```

`Localization_bc_tools` is the implemented localization path. The marked
`EmptyOperator` tasks are not scientific products.

## `cosidag_GeD`

```text
PreProcessing_GeD
  -> Data_Binning
  -> TS_Map_on_different_timescales
  -> Light_Curve
  -> Duration
  -> Spectral_Analysis       (placeholder)
  -> Classification_GeD      (placeholder)
  -> GCN_GeD
```

Exact input patterns and operator wiring remain authoritative in the
[`src/dags` sources](https://github.com/cositools/fast-transient-analysis-pipeline/tree/dev/src/dags).
