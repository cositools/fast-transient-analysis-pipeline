# ARMselection unbinned-analysis branch

`cosidag_ARMselection` performs the unbinned GeD event selection and fast
HEALPix localization. It was split from `cosidag_GeD`, which now contains only
the binned COSIpy likelihood workflow.

## Inputs

The branch resolves the same four input classes as GeD:

| XCom key | Required product |
|---|---|
| `grb_file` | GRB event FITS file |
| `background_file` | background event FITS file |
| `orientation_file` | orientation/attitude file |
| `response_file` | detector response file |

The concrete filename patterns are defined in
[`cosidag_ARMselection.py`](../../dags/cosidag_ARMselection.py).

## Task graph and runtime

All scientific tasks run in the `cosipy` Python 3.12 environment:

```text
PreProcessing_ARMselection
  └─ Unbinned_Light_Curve_Generation
      └─ Light_Curve_Analysis
          └─ Skymap_unbinned
              └─ Duration_and_Localization_Results
                  └─ GCN_ARMselection  [Airflow runtime]
```

## Isolated state

The branch writes `armsel_pipeline_config.yaml` beside the source data. This is
intentionally separate from the GeD branch's `pipeline_config.yaml`, so the two
DAGs do not update the same YAML file when they run concurrently.

The raw input and output directories are shared, but each DAG must continue
using the `config_path` returned by its own preprocessing task.

## Generated products

| Stage | Products |
|---|---|
| Preprocessing | `armsel_pipeline_config.yaml`, `ged_preprocessed_events.hdf5` |
| Unbinned light curve | `lc_lc.csv`, `lc_lc.png`, `lc_diagnostics.png`, `unbinned_lightcurve_results.yaml` |
| Light-curve analysis | `grb_fast_localize_prep.npz`, `grb_fast_localize_prep.yaml` |
| Unbinned skymap | one `grb_sigmap_nside<N>.npz` per configured NSIDE, `grb_fast_localize_skymap.yaml` |
| Results | `grb_localization_top50.csv`, `grb_significance_map.png`, `grb_timing.csv`, `grb_fast_localize_results.yaml` |
| Optional NSIDE sweep | `grb_nside_summary.csv`, `grb_nside_summary.png` |

The prefixes, top-K value, plot format, and NSIDE list are configurable in the
DAG's `arm_analysis_config`; the table shows the current defaults. Plots are written
to the sibling `plots/` directory and data products beside the selected input
files.

## Analysis behavior

1. Preprocessing validates all inputs and prepares ON/OFF event data.
2. The unbinned light curve applies the configured ARM gate around the selected
   Galactic coordinates and writes diagnostic plots.
3. Light-curve analysis prepares ON/OFF cone vectors and the exposure ratio
   `alpha = Ton / Toff`.
4. `Skymap_unbinned` creates Li & Ma significance maps at the configured
   HEALPix resolution or resolutions.
5. The results task selects the maximum-significance location, writes the
   top-K pixels and timing summary, and records the reconstructed coordinates.

The default pre-burst OFF window is controlled by `off_pre` and `off_gap`. If
that window contains no background events, the current default
`off_fallback_strategy=on_background` uses background events from the ON
window; change this setting if a strict failure is preferable.

## GCN integration

The skymap and final-results stages query the local GCN inbox. The final
`GCN_ARMselection` task queues an outbox alert containing the localization
result. Its topic is selected in this order:

1. `GCN_ARMSELECTION_OUTBOUND_TOPIC`;
2. `GCN_GED_OUTBOUND_TOPIC`;
3. `GCN_OUTBOUND_TOPIC_DEFAULT`;
4. the built-in test topic.

The task queues the alert locally; external publication remains controlled by
COSIflow's GCN client and should stay disabled/dry-run for development.

## Running and troubleshooting

The DAG is manually scheduled and monitors `products` directories under the
configured HEASARC root. Because GeD and ARMselection use the same monitored
root and input family, pause the branch you do not intend to exercise during a
focused smoke test.

If a stage reports missing prepared data, verify that it received
`armsel_pipeline_config.yaml` from `PreProcessing_ARMselection` and that
`ged_preprocessed_events.hdf5` exists.
