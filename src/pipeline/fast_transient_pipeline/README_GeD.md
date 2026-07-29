# GeD binned-analysis branch

`cosidag_GeD` is the Germanium-detector branch for binned COSIpy analysis.
The unbinned ARM-selection and fast-localization workflow lives in the
separate [`cosidag_ARMselection`](README_ARMselection.md) branch.

## Inputs

`COSIDAG` resolves four files from a selected `products` directory:

| XCom key | Required product |
|---|---|
| `grb_file` | GRB event FITS file |
| `background_file` | background event FITS file |
| `orientation_file` | orientation/attitude file |
| `response_file` | detector response file |

The exact filename patterns are declared in
[`cosidag_GeD.py`](../../dags/cosidag_GeD.py). Preprocessing validates all four
paths before creating the run state.

## Task graph and runtimes

```text
PreProcessing_GeD                 [cosipy, Python 3.12]
  └─ Data_Binning                [cosipy, Python 3.12]
      └─ TS_Map_on_different_timescales
                                 [cosipy, Python 3.12]
          └─ Light_Curve         [cosipy, Python 3.12]
              └─ Duration        [bct, Python 3.11]
                  └─ Spectral_Analysis
                                 [placeholder]
                      └─ Classification_GeD
                                 [placeholder]
                          └─ GCN_GeD
                                 [Airflow runtime]
```

`Spectral_Analysis` and `Classification_GeD` are currently
`EmptyOperator` placeholders. They must not be interpreted as completed
scientific products.

## State and generated products

The branch writes mutable state to `pipeline_config.yaml` beside the selected
source file. It records resolved inputs, promoted analysis parameters, task
status, timings, GCN query results, and paths to generated artifacts.

Important products include:

| Stage | Products |
|---|---|
| Preprocessing | validated state in `pipeline_config.yaml` |
| Data binning | `inputs_grb.yaml`, `inputs_bg.yaml`, dynamically named source/background HDF5 files, `ged_preprocessed_data.hdf5`, `ged_preprocessed_background_model.hdf5` |
| TS map | `ged_fast_likelihood_tsmap.png`, `ged_multiorder_likelihood_tsmap.png`, `tsmap_results.yaml` |
| Light curve | `ged_point_source_response_selected_lightcurve.png`, `lightcurve_results.yaml` |
| Duration | `ged_bayesian_blocks_lightcurve_analysis.png`, `duration_results.yaml` |

Plots are written to the sibling `plots/` directory; YAML and HDF5 products
are written beside the input data. Binning filenames containing time bounds
are derived at runtime, so consumers should read their paths from
`pipeline_config.yaml` instead of constructing them.

## Analysis behavior

- Preprocessing infers the available FITS time range and merges the
  `GED_ANALYSIS_CONFIG` values supplied by the DAG.
- This branch sets `prepare_event_data=false`: it does not create the
  `ged_preprocessed_events.hdf5` product used by ARMselection.
- Data binning prepares source and background datasets for the likelihood
  stages.
- The TS-map stage performs both the fast and multi-order scans and stores the
  selected coordinates in the shared state.
- The light-curve stage uses the selected point-source response.
- The duration stage applies Bayesian Blocks and records both the burst window
  and T90 interval.

## GCN integration

Scientific stages query the local GCN inbox for notices relevant to their time
windows. `GCN_GeD` then creates an outbox record using the duration result and
the configured GeD topic. It does not publish directly to Kafka.

Keep the default safe settings for development:

```dotenv
GCN_PRODUCER_ENABLED=false
GCN_DRY_RUN=true
```

See the repository [GCN integration guide](../../../README.md#gcn-integration)
and the COSIflow
[`gcn-client` guide](../../../../cosiflow/gcn-client/README.md).

## Running and troubleshooting

The DAG is manually scheduled. Trigger it after a matching `products`
directory has been staged under the configured HEASARC root.

If input resolution fails, inspect the `resolve_inputs` XCom values and compare
the directory contents with the patterns in `cosidag_GeD.py`. If a stage is
rerun, use the `config_path` returned by `PreProcessing_GeD`; do not substitute
the ARMselection state file.
