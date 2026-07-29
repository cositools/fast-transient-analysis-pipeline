# BGO fast-transient branch

`cosidag_BGO` analyzes the BGO shield light curve, estimates the burst
duration, prepares panel counts, evaluates significance, and runs the
non-imaging localization.

## Inputs

`COSIDAG` resolves five files from the selected `products` directory:

| XCom key | Required product |
|---|---|
| `lightcurve_file` | BGO light-curve NumPy file |
| `soft_lut_file` | soft-band lookup table |
| `medium_lut_file` | medium-band lookup table |
| `hard_lut_file` | hard-band lookup table |
| `orientation_file` | orientation/attitude file |

The exact patterns are declared in
[`cosidag_BGO.py`](../../dags/cosidag_BGO.py). Preprocessing validates all five
paths and the required light-curve structure.

## Task graph and runtimes

```text
PreProcessing_BGO                    [cosipy, Python 3.12]
  └─ Duration_on_different_binning   [bct, Python 3.11]
      ├─ Light_Curve_generation      [bct, Python 3.11]
      └─ Background_extraction_and_data_preparation
          ├─ Significance_Analysis   [cosipy, Python 3.12]
          ├─ Localization_bc_tools   [nimcosipy, Python 3.12]
          ├─ Localization_Chi2       [placeholder]
          └─ Localization_DL         [placeholder]

all localization branches
  └─ Localization_Results            [placeholder join]
      ├─ Classification_BGO          [placeholder]
      └─ GCN_BGO                     [Airflow runtime]
```

`Localization_Chi2`, `Localization_DL`, `Localization_Results`, and
`Classification_BGO` are currently `EmptyOperator` placeholders. The
implemented localization is `Localization_bc_tools`.

## State and products

The branch stores its mutable run state in `pipeline_config.yaml` beside the
input light curve. It contains the resolved inputs, analysis configuration,
task records, duration/background/significance/localization results, GCN
queries, and generated paths.

The current user-facing plot products are:

- `bgo_acs_panel_<panel>_count_lightcurve.png`;
- `bgo_acs_panel_<panel>_bayesian_blocks_lightcurve_analysis.png`;
- `bgo_acs_grb_localization_tsmap_90pct_confidence_region.png`;
- `bgo_localization_tsmap_no_containment.png`.

`<panel>` is the configured panel or the panel selected by the analysis. Plots
are written to the `plots/` directory adjacent to the input data. Numerical
duration, background, panel-count, significance, and localization metadata are
recorded in `pipeline_config.yaml` and passed between parallel tasks through
XCom.

## Analysis behavior

- Duration estimation runs Bayesian Blocks on the configured light-curve key
  and records the burst bounds, T90, and uncertainty.
- Background extraction fits the off-burst regions and returns signal,
  background, and net counts for the detector panels.
- Light-curve generation plots counts and the Bayesian-Blocks analysis.
- Significance analysis computes per-panel significance using the duration and
  count products.
- `Localization_bc_tools` combines the panel counts, three energy-band lookup
  tables, and attitude information in the `nimcosipy` runtime.

The localization and significance branches receive upstream XCom payloads
directly. This avoids depending on whichever parallel task most recently wrote
`pipeline_config.yaml`; the YAML remains the persistent audit record.

## GCN integration

The duration stage queries the local GCN inbox for notices in the science
window. `GCN_BGO` queues an outbox alert containing the duration, significance,
and implemented localization results. It does not publish directly to Kafka.

Use test topics and keep these development defaults unless publication has
been explicitly authorized:

```dotenv
GCN_PRODUCER_ENABLED=false
GCN_DRY_RUN=true
```

## Running and troubleshooting

The DAG is manually scheduled. Trigger it after a matching BGO product
directory has been staged.

For resolution failures, inspect the `resolve_inputs` XCom values and compare
the files with the patterns in `cosidag_BGO.py`. For runtime failures, check
that the `bct` and `nimcosipy` environments were created by the module loader
and that `EXTERNAL_PYTHON_BGO` and `EXTERNAL_PYTHON_NIMCOSIPY` point to their
interpreters.
