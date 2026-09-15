# DAG reference

## Catalog

| DAG ID | Type | Runtime summary | Purpose |
| --- | --- | --- | --- |
| `init_pipelines` | standard Airflow DAG | Airflow plus managed COSIpy commands | Resolve and stage GeD or BGO inputs |
| `cosidag_BGO` | COSIDAG | `cosipy`, `bct`, `nimcosipy`, Airflow | BGO transient analysis |
| `cosidag_GeD` | COSIDAG | `cosipy`, `bct`, Airflow | Binned GeD analysis |
| `cosidag_ARMselection` | COSIDAG | `cosipy`, Airflow | Unbinned ARM selection and localization |

All four DAGs have no time schedule. The three scientific DAGs use COSIDAG
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

The DAG creates a products directory; it does not use
`TriggerDagRunOperator`. Parameters and defaults are documented in
[configuration](configuration.md).

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

## `cosidag_ARMselection`

```text
PreProcessing_ARMselection
  -> Unbinned_Light_Curve_Generation
  -> Light_Curve_Analysis
  -> Skymap_unbinned
  -> Duration_and_Localization_Results
  -> GCN_ARMselection
```

Exact input patterns and operator wiring remain authoritative in the
[`src/dags` sources](https://github.com/cositools/fast-transient-analysis-pipeline/tree/dev/src/dags).
