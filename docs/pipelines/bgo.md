# BGO branch

`cosidag_BGO` analyzes the BGO shield light curve, estimates burst duration,
prepares panel counts, evaluates significance, and runs the implemented
non-imaging localization.

## Inputs

| XCom key | Required product |
| --- | --- |
| `lightcurve_file` | BGO light-curve NumPy file |
| `soft_lut_file` | soft-band lookup table |
| `medium_lut_file` | medium-band lookup table |
| `hard_lut_file` | hard-band lookup table |
| `orientation_file` | orientation/attitude file |

The exact patterns are declared in
[`cosidag_BGO.py`](https://github.com/cositools/fast-transient-analysis-pipeline/blob/dev/src/dags/cosidag_BGO.py).

## State and products

The branch stores mutable state in `pipeline_config.yaml` beside the input
light curve. It records resolved inputs, analysis configuration, task records,
duration/background/significance/localization results, GCN queries, and paths.

Current plot products include:

- `bgo_acs_panel_<panel>_count_lightcurve.png`;
- `bgo_acs_panel_<panel>_bayesian_blocks_lightcurve_analysis.png`;
- `bgo_acs_grb_localization_tsmap_90pct_confidence_region.png`;
- `bgo_localization_tsmap_no_containment.png`.

Plots are written below the adjacent `plots` directory. Numerical results are
stored in `pipeline_config.yaml` and compact result mappings are passed through
XCom. The active localization combines panel counts, the three energy-band
lookup tables, and attitude data in `nimcosipy`.

`Localization_Chi2`, `Localization_DL`, `Localization_Results`, and
`Classification_BGO` are placeholders. See the [DAG graph](../reference/dags.md#cosidag_bgo).
