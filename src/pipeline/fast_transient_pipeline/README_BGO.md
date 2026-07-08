# BGO Pipeline Data Flow

This document describes the BGO branch of the fast transient analysis pipeline.

The orchestration lives in `src/dags/cosidag_BGO.py`. The task logic lives in
`src/pipeline/fast_transient_pipeline/bgo_functions.py`.

The DAG keeps large products on disk. Airflow/XCom mainly carries the path to
`pipeline_config.yaml` and small result dictionaries. Each task reads the shared YAML file, writes
its own products, and updates the YAML with task metadata.

## Runtime Contract

The BGO branch uses three Python environments:

```text
cosipy      -> preprocessing and lightweight shared helpers
bct         -> duration, background, and light-curve tasks
nimcosipy   -> BGO localization with cosipy.nonimaging and bc-tools
```

The shared YAML state follows the same structure used by GeD:

```yaml
BGO:
  name: cosidag_BGO
  trigger_time: ...
  input_resolved: ...
  PreProcessing_BGO: ...
  Duration_on_different_binning: ...
```

The final notice is queued through `gcn.outbox.queue_cosi_alert`, using the YAML config and the
task result payloads as the source products.

## High-Level DAG Flow

```text
PreProcessing_BGO
`- Duration_on_different_binning
   |- Background_extraction_and_data_preparation
   |  |- Localization_Chi2
   |  |- Localization_bc_tools
   |  |- Localization_DL
   |  `- Significance_Analysis
   `- Light_Curve_generation

Localization_Chi2 + Localization_bc_tools + Localization_DL + Significance_Analysis
`- Localization_Results
   |- Classification_BGO
   `- GCN_BGO

Classification_BGO -> GCN_BGO
```

`Localization_Chi2`, `Localization_DL`, and `Classification_BGO` are currently placeholders.
`Localization_bc_tools` is the active localization branch.

## Task 1: PreProcessing_BGO

Function: `preprocess_data`

### Input

From the DAG:

```text
lightcurve_file   -> BGO multi-panel NPZ light curve
soft_lut_file     -> soft-spectrum BGO localization LUT
medium_lut_file   -> medium-spectrum BGO localization LUT
hard_lut_file     -> hard-spectrum BGO localization LUT
orientation_file  -> spacecraft orientation FITS/ORI file
analysis_config   -> BGO analysis parameters
trigger_time      -> Airflow timestamp
```

### Processing

The preprocessing task:

- checks that all required input files exist;
- merges `BGO_ANALYSIS_DEFAULTS` with the analysis configuration provided by the DAG;
- expands analysis sections as top-level config keys, such as `duration`, `background`,
  `light_curve`, `significance`, and `localization_bctools`;
- defines output directories:
  - `data_dir`: usually the directory containing the NPZ light curve;
  - `products_dir`: same as `data_dir`;
  - `plots_dir`: `data_dir/../plots`;
- initializes the structured pipeline bookkeeping under the `BGO` key;
- writes the shared YAML state.

### Output

```text
pipeline_config.yaml
```

## Task 2: Duration_on_different_binning

Function: `get_duration`

### Input

```text
pipeline_config.yaml
lightcurve_path
duration configuration
```

The NPZ array is read from the configured key, currently `light_curve`.

### Processing

The task:

- reads the multi-panel light curve array;
- converts rates to counts if configured;
- reconstructs bin edges from bin centers;
- builds one `TimeBins` object per panel;
- selects the panel with the highest peak counts;
- runs Bayesian Blocks on that selected panel;
- extracts signal start/stop, `T90`, and `T90` uncertainty.

If Bayesian Blocks fails, the task returns sentinel values while preserving the expected tuple
shape for downstream tasks.

### Output

The YAML config is updated with:

```yaml
duration_result:
  lc_timebins: ...
  bb_lc_timebins: ...
  tstart: ...
  tstop: ...
  t90: ...
  t90_err_low: ...
  t90_err_high: ...
```

## Task 3: Background_extraction_and_data_preparation

Function: `background_extraction_and_data_preparation`

### Input

```text
pipeline_config.yaml
duration_result.lc_timebins
duration_result.tstart
duration_result.tstop
background configuration
```

### Processing

The task:

- rebuilds `TimeBins` for each BGO panel;
- excludes the Bayesian-Blocks signal window plus optional buffer;
- fits a polynomial background model per panel;
- integrates observed signal counts inside the signal window;
- integrates fitted background counts over the same window;
- computes net counts per panel.

The panel order produced by this branch is:

```text
z0, z1, x0, x1, y0, y1
```

### Output

```yaml
background_result:
  signal_counts: [...]
  background_counts: [...]
  net_counts: [...]
  tstart: ...
  tstop: ...
```

## Task 4: Light_Curve_generation

Function: `light_curve_generation`

### Input

```text
pipeline_config.yaml
duration_result
light_curve configuration
```

### Processing

The task:

- selects the configured panel, or the panel with the largest peak count;
- plots the raw counts light curve;
- fits and plots the polynomial background model;
- overlays the Bayesian-Blocks light curve and signal boundaries.

### Output

```text
light_curve_<panel>.png
light_curve_background_t90_bblocks_<panel>.png
```

## Task 5: Significance_Analysis

Function: `significance_analysis`

### Input

```text
background_result.signal_counts
background_result.background_counts
duration_result.t90
```

### Processing

The task computes a Li and Ma significance per panel. Panels with fewer than the configured minimum
signal or background counts are assigned significance zero.

### Output

```yaml
significance_result:
  sigmas: [...]
```

## Task 7: Localization_bc_tools

Function: `localize_bctools`

### Input

```text
pipeline_config.yaml
soft_lut_path
medium_lut_path
hard_lut_path
orientation_path
background_result.signal_counts
background_result.background_counts
duration_result.tstart
```

### Processing

The task:

- reads the spacecraft attitude nearest to `duration_result.tstart`;
- supports current FITS orientation tables and legacy `.ori` text files;
- loads the soft, medium, and hard BGO LUTs through `ACSLocalizerBCT`;
- handles both supported LUT formats:
  - local tables with `to_skyloctable()`;
  - already projected `HealpixLocTable` pickles;
- reorders the count arrays from BGO panel order to the LUT detector-label order;
- computes the TS map and selects the spectrum hypothesis with maximum TS;
- writes the localization plot.

The BGO task output order is:

```text
z0, z1, x0, x1, y0, y1
```

The LUT label order is normally:

```text
BGO_X0, BGO_X1, BGO_Y0, BGO_Y1, BGO_Z0, BGO_Z1
```

The code reorders by label before passing counts to BC-tools.

### Output

```text
bgo_localization.png
```

The YAML config is updated with:

```yaml
localization_result:
  localization_plots_dir: ...
  soft_lut_path: ...
  medium_lut_path: ...
  hard_lut_path: ...
```

## Task 11: GCN_BGO

Function: `_queue_gcn_outbox_notice` in the DAG

### Input

```text
pipeline_config.yaml
duration_result
significance_result
localization_result
```

### Processing

The task queues a COSI BGO alert notice in the local GCN outbox. It does not publish directly to an
external broker from the DAG task.

### Output

An outbox record containing:

```text
pipeline = BGO
instrument = BGO Shields
source_config_path = pipeline_config.yaml
duration_result
significance_result
localization_result
```

## Main Products Summary

### Shared State

```text
pipeline_config.yaml
```

### Products

```text
light_curve_<panel>.png
light_curve_background_t90_bblocks_<panel>.png
bgo_localization.png
```

### Notice Queue

```text
GCN outbox record
```

## Important Design Notes

The shared `pipeline_config.yaml` is both task input and mutable pipeline state. The BGO DAG passes
the `background_result` and `duration_result` XCom payloads directly into localization and
significance to avoid relying on whichever parallel task wrote the YAML last.

The localization branch currently relies on `nimcosipy` because the active BC-tools implementation
needs both `cosipy` and `cosipy.nonimaging`.
