# ARM-selection branch

`cosidag_ARMselection` performs unbinned GeD event selection, an ARM-gated
light curve, and Li & Ma HEALPix localization. It uses the same staged input
classes as GeD but writes independent mutable state to
`armsel_pipeline_config.yaml`.

## Products

| Stage | Products |
| --- | --- |
| Preprocessing | state YAML and `ged_preprocessed_events.hdf5` |
| Unbinned light curve | `lc_lc.csv`, `lc_lc.png`, `lc_diagnostics.png`, `unbinned_lightcurve_results.yaml` |
| Light-curve analysis | `grb_fast_localize_prep.npz`, `grb_fast_localize_prep.yaml` |
| Skymap | `grb_sigmap_nside<N>.npz`, `grb_fast_localize_skymap.yaml` |
| Results | `grb_localization_top50.csv`, `grb_significance_map.png`, `grb_timing.csv`, `grb_fast_localize_results.yaml` |
| Optional NSIDE sweep | `grb_nside_summary.csv`, `grb_nside_summary.png` |

Prefixes, top-K, plot format, and NSIDE selections are configurable in the
DAG's `arm_analysis_config`; the table shows defaults. Plots are written to the
adjacent `plots` directory and data products beside the selected inputs.

The default pre-burst OFF window is controlled by `off_pre` and `off_gap`. If
it has no background events, the current default
`off_fallback_strategy=on_background` uses background events from the ON
window. This is current behavior, not a guarantee that the fallback is
scientifically suitable for every run.

See
[`cosidag_ARMselection.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/cosidag_ARMselection.py)
and the [DAG graph](../reference/dags.md#cosidag_armselection).
