# GeD Pipeline Data Flow

This document describes the data flow implemented by the GeD branch of the fast transient analysis pipeline.

The orchestration lives in `src/dags/cosidag_GeD.py`. The task logic lives in
`src/pipeline/fast_transient_pipeline/ged_functions.py`.

The DAG passes large data products through files on disk. Airflow/XCom mainly carries the path to
`pipeline_config.yaml`; each task reads that YAML file, writes its own outputs, and updates the YAML
with the new metadata and product paths.

## Runtime Contract

The GeD branch uses two Python environments:

```text
cosipy  -> preprocessing, unbinned localization, binning, TS maps, light curves
bct     -> Bayesian-Blocks duration task
```

The shared YAML state is structured under a top-level `GeD` run section:

```yaml
GeD:
  name: cosidag_GeD
  trigger_time: ...
  input_resolved: ...
  PreProcessing_GeD: ...
  Data_Binning: ...
```

The final notice is queued through `gcn.outbox.queue_cosi_alert`, using the YAML config and the
task result payloads as the source products.

## High-Level DAG Flow

```text
PreProcessing_GeD
|- Unbinned_Light_Curve_Generation
|  `- Light_Curve_Analysis
|     `- Skymap_unbinned
|        `- Duration_and_Localization_Results
`- Data_Binning
   `- TS_Map_on_different_timescales
      `- Light_Curve
         `- Duration
            `- Spectral_Analysis
               `- Classification_GeD

Duration_and_Localization_Results + Classification_GeD -> GCN_GeD
```

`Spectral_Analysis` and `Classification_GeD` are currently represented by `EmptyOperator` nodes in
the DAG. `GCN_GeD` is active and queues a notice through the local outbox.

## Task 1: PreProcessing_GeD

Function: `preprocess_data`

### Input

From the DAG:

```text
source_file       -> GRB FITS event file
background_file   -> background FITS event file
orientation_file  -> spacecraft orientation FITS/ORI file
response_file     -> detector response H5 file
analysis_config   -> GeD analysis parameters
trigger_time      -> Airflow timestamp
```

### Processing

The preprocessing task:

- checks that all required input files exist;
- merges `GED_ANALYSIS_DEFAULTS` with the analysis configuration provided by the DAG;
- expands each analysis section as a top-level config key, such as `tsmap`, `binning_data`,
  `light_curve`, `duration`, and `fast_localize`;
- defines the output directories:
  - `data_dir`: usually the directory containing the GRB file;
  - `products_dir`: same as `data_dir`;
  - `plots_dir`: `data_dir/../plots`;
- reads the source FITS file and infers timing information from the `TimeTags` column:
  - `tstart`;
  - `tstop`;
  - `grb_duration`;
- creates the unbinned prepared event product used by the fast-localization branch:
  - original source event columns;
  - original background event columns;
  - aggregated ON event sample, built as GRB ON events plus background ON events;
  - OFF background event sample;
  - ON/OFF timing metadata and `alpha`;
- initializes the structured pipeline bookkeeping under the `GeD` key.

### Output

```text
pipeline_config.yaml
ged_preprocessed_events.hdf5
```

The config contains input paths, analysis parameters, output directories, inferred GRB timing, and
the task status under `GeD.PreProcessing_GeD`.

It also initializes:

```yaml
prepared_data:
  event_data_path: ...
  event_format: hdf5
  contains_background: true
  source_path_original: ...
  background_path_original: ...
```

## Task 2: Unbinned_Light_Curve_Generation

Function: `unbinned_light_curve_generation`

### Input

```text
pipeline_config.yaml
source_path = GRB FITS event file
unbinned_light_curve configuration
```

The task reads these FITS columns:

```text
TimeTags
Chi galactic
Psi galactic
Phi
```

### Processing

The task:

- loads the event times, reconstructed event directions, and Compton angle `Phi`;
- converts `Phi` from radians to degrees;
- selects events in the configured time window;
- chooses a source position `(l, b)`;
- computes the angular resolution measure for each event:

```text
ARM = angular_separation(event_direction, source_position) - Phi
```

- keeps only events whose ARM lies inside the configured ARM gate;
- histograms the surviving event times into a light curve.

### Output

```text
lc_lc.csv
lc_lc.png
lc_diagnostics.png
unbinned_lightcurve_results.yaml
```

The YAML config is updated with:

- gated and ungated counts;
- time bin centers;
- selected localization;
- ARM gate parameters;
- output paths;
- event statistics.

If `source_path` is not a FITS file, this task is marked as skipped.

## Task 3: Data_Binning

Wrapper in DAG: `_run_data_binning`

Function: `bin_data`

### Input

```text
pipeline_config.yaml
source_path = GRB FITS or HDF5
background_path = background FITS or HDF5
tstart
tstop
binning_data configuration
```

`tstart` and `tstop` are the burst bounds inferred from the source FITS. The binned products are
created over the wider analysis window:

```text
[tstart - eps_bkg_preburst, tstop + eps_bkg_postburst]
```

### Processing

The DAG wrapper first checks whether both source and background are already `.hdf5` files.

If both inputs are already binned:

- binning is skipped;
- `source_binned_file_path` is set to `source_path`;
- `background_binned_file_path` is set to `background_path`.
- the canonical binned prepared product is still built.

If the inputs are unbinned FITS files, `bin_data`:

- builds a COSIpy binning configuration for the source file using the wider analysis window;
- builds a COSIpy binning configuration for the background file using the same wider window;
- writes:

```text
inputs_grb.yaml
inputs_bg.yaml
```

- runs `cosipy.BinnedData`;
- creates binned HDF5 histograms for source and background.
- builds the canonical prepared binned data product:
  - `ged_preprocessed_data.hdf5`, containing `source + normalized_background_model`;
  - `ged_preprocessed_background_model.hdf5`, containing the background model used by the analyses.

### Output

```text
source_binned_file_path
background_binned_file_path
inputs_grb.yaml
inputs_bg.yaml
ged_preprocessed_data.hdf5
ged_preprocessed_background_model.hdf5
```

The shared config is updated with:

```yaml
source_binned_file_path: ...
background_binned_file_path: ...
binned_data:
  source_binned_file_path: ...
  background_binned_file_path: ...
prepared_data:
  data_path: ...
  background_model_path: ...
  format: hdf5
  contains_background: true
  source_path_original: ...
  background_path_original: ...
  source_data_path: ...
  background_data_path: ...
```

## Task 4: Light_Curve_Analysis

Function: `light_curve_analysis`

This task belongs to the unbinned localization branch.

### Input

```text
pipeline_config.yaml
prepared_data.event_data_path
tstart
tstop
fast_localize configuration
```

### Processing

The task defines two time windows:

```text
ON  = [tstart, tstop]
OFF = [tstart - off_pre, tstart - off_gap]
```

It then computes:

```text
alpha = duration(ON) / duration(OFF)
```

The task preferably loads the prepared event HDF5 created by preprocessing. From that file it reads
the aggregated ON sample and OFF background sample. If the prepared file is missing, it falls back to
reading the GRB and background FITS files directly.

The ON sample is:

```text
GRB events in ON + background events in ON
```

The OFF sample is taken from background events in the pre-burst OFF window. If no pre-burst OFF
events are found and `off_fallback_strategy` is set to `on_background`, the task uses background
events from the ON window as the OFF sample.

The event directions `(l, b)` are converted into 3D unit vectors. The Compton angles are kept in
degrees.

### Output

```text
grb_fast_localize_prep.npz
grb_fast_localize_prep.yaml
```

The NPZ file contains:

```text
on_cvec
off_cvec
on_phi_deg
off_phi_deg
```

The config is updated with:

- ON/OFF window bounds;
- `Ton_s`;
- `Toff_s`;
- `alpha`;
- ARM gate;
- number of ON/OFF events;
- timing metadata;
- path to the preparation NPZ.

## Task 4.1: Skymap_unbinned

Function: `skymap_unbinned`

### Input

```text
pipeline_config.yaml
grb_fast_localize_prep.npz
fast_localize configuration
```

### Processing

The task builds an ARM-gated HEALPix Li and Ma significance map.

For each configured `nside`, it:

- builds the HEALPix pixel grid;
- treats each pixel as a candidate source position;
- computes the angular separation between every event and each candidate pixel;
- computes the ARM for each event-pixel pair:

```text
ARM = angular_separation(event_direction, candidate_pixel) - Phi
```

- counts events passing the ARM gate:
  - `Non`: ON events passing the gate;
  - `Noff`: OFF events passing the gate;
- computes the Li and Ma significance:

```text
S_li_ma(pixel) = f(Non, Noff, alpha)
```

The computation is chunked by pixels and events to limit memory use.

### Output

For each `nside`:

```text
grb_sigmap_nside{nside}.npz
```

The NPZ file contains:

```text
sig   = significance map
non   = ON counts per pixel
noff  = OFF counts per pixel
nside
```

The task also writes:

```text
grb_fast_localize_skymap.yaml
```

The config is updated with:

- `main_nside`;
- list of `nsides_run`;
- one result row per `nside`;
- best pixel per `nside`;
- best `(l, b)` per `nside`;
- maximum significance;
- map runtime.

## Task 5: Duration_and_Localization_Results

Function: `duration_and_localization_results`

This task finalizes the unbinned localization products.

### Input

```text
pipeline_config.yaml
grb_sigmap_nside{nside}.npz
fast_localize metadata
```

### Processing

The task:

- loads the significance map for the main `nside`;
- finds the pixel with maximum significance:

```text
ip_best = argmax(sig_map)
```

- converts the best HEALPix pixel to galactic coordinates:

```text
best_l_deg
best_b_deg
```

- writes a top-K localization table;
- renders a Mollweide significance map;
- writes a timing table;
- optionally writes an NSIDE summary if multiple NSIDEs were run.

It also copies the reconstructed localization back into the config:

```yaml
unbinned_light_curve:
  l: best_l_deg
  b: best_b_deg

tsmap:
  fast_localize_coordinates:
    l_deg: best_l_deg
    b_deg: best_b_deg
```

### Output

```text
grb_localization_top50.csv
grb_significance_map.png
grb_timing.csv
grb_fast_localize_results.yaml
```

Optional outputs when multiple NSIDEs are run:

```text
grb_nside_summary.csv
grb_nside_summary.png
```

## Task 6: TS_Map_on_different_timescales

Function: `compute_ts_map`

This task belongs to the binned analysis branch.

### Input

```text
pipeline_config.yaml
prepared_data.data_path
prepared_data.background_model_path
orientation_path
response_path
default_spectrum configuration
tsmap configuration
```

### Processing

The task:

- opens the canonical prepared aggregate histogram as `data`;
- opens the canonical prepared background model as `bkg_model`;
- falls back to aggregating `source_binned_file_path` and `background_binned_file_path` only if
  `prepared_data` is missing;
- uses the already prepared relationship:

```text
data = signal + normalized_background_model
```

- defines a power-law source spectrum;
- opens the spacecraft history;
- selects orientation data over the wider `prepared_data` time interval;
- runs `FastTSMap.fit`;
- finds the maximum FastTSMap pixel and coordinates;
- runs `MOCTSMap.fit`;
- finds the maximum MOCTSMap pixel and coordinates;
- saves both TS map plots.

Although both FastTSMap and MOCTSMap results are stored, the selected coordinates are currently
taken from the MOC result.

### Output

```text
tsmap_fast.png
tsmap_moc.png
tsmap_results.yaml
```

The config is updated with:

```yaml
tsmap:
  fast:
    max_ts: ...
    l_deg: ...
    b_deg: ...
    plot_path: ...
  moc:
    max_ts: ...
    l_deg: ...
    b_deg: ...
    plot_path: ...
    uniq: ...
    nside: ...
    pix: ...
  selected_coordinates:
    l_deg: moc_l
    b_deg: moc_b
```

These selected coordinates are used by the next `Light_Curve` task.

## Task 7: Light_Curve

Function: `light_curve`

### Input

```text
pipeline_config.yaml
prepared_data.data_path
orientation_path
response_path
tsmap.selected_coordinates
light_curve configuration
binning_data configuration
```

### Processing

The task:

- opens the canonical prepared aggregate histogram;
- takes the source coordinates selected by the TS map;
- opens the spacecraft orientation;
- opens the detector response;
- creates a point source response map for the selected coordinates;
- projects the response onto:

```text
Em
Phi
PsiChi
```

- builds a containment mask from the point source response map;
- loops over time bins from `tstart - eps_bkg_preburst` to `tstop + eps_bkg_postburst`;
- for each time bin:
  - slices the prepared aggregate histogram;
  - projects it onto `Em`, `Phi`, and `PsiChi`;
  - multiplies the data map by the response mask;
  - sums the masked counts.

If `prepared_data.data_path` is missing, the task falls back to the previous behavior and combines
source and background histograms locally.

### Output

```text
lightcurve.png
lightcurve_results.yaml
```

The config is updated with:

```yaml
light_curve:
  plot_path: ...
  bin_size: ...
  nside: ...
  time_bins: [...]
  time_centers: [...]
  counts: [...]
  used_coordinates:
    l_deg: ...
    b_deg: ...
  containment: ...
  source_data_path: ...
  background_data_path: ...
  using_prepared_aggregate: true
```

These `time_centers` and `counts` are the default input for the `Duration` task.

## Task 8: Duration

Function: `duration`

### Input

Default input from the previous task:

```text
pipeline_config.yaml
light_curve.time_centers
light_curve.counts
duration configuration
```

Optional external input:

```text
duration_lightcurve_path
```

### Processing

If `duration_lightcurve_path` is configured, the task loads the light curve from that file.

Otherwise, it reconstructs a BGO-like light curve array from:

```text
light_curve.time_centers
light_curve.counts
```

Then `get_duration`:

- converts the light curve into `TimeBins`;
- chooses the panel with the largest maximum count;
- runs Bayesian Blocks;
- extracts the signal start and stop times;
- computes `T90`;
- estimates the uncertainty on `T90`.

If Bayesian Blocks fails, the task returns the configured sentinel value, by default `-9999.0`.

### Output

```text
duration_plot.png
duration_results.yaml
```

The config is updated with:

```yaml
duration:
  input_lightcurve_path: ...
  p0: ...
  is_rate: ...
  panels: ...
  bayes_quantile: ...
  bayes_error_nsamples: ...
  lc_timebins: ...
  bb_lc_timebins: ...
  tstart_t90: ...
  tend_t90: ...
  tstart: ...
  tstop: ...
  t90: ...
  t90_err_low: ...
  t90_err_high: ...

tstart_t90: ...
tend_t90: ...
t90: ...
t90_err_low: ...
t90_err_high: ...
```

Important: this task does not overwrite the top-level `tstart` and `tstop`. Those remain the burst
bounds inferred during preprocessing. The Bayesian-Blocks signal range is stored separately as
`tstart_t90` and `tend_t90`.

## Task 9: Spectral_Analysis

Current DAG node: `EmptyOperator`

Function placeholder: `spectral_analysis`

### Expected Input

Conceptually, this task would likely need:

```text
source_binned_file_path
background_binned_file_path
duration results
localization results
orientation_path
response_path
```

### Current Processing

No processing is currently performed by the DAG node.

### Current Output

No real output is currently produced.

## Task 10: Classification_GeD

Current DAG node: `EmptyOperator`

Function placeholder: `classification_ged`

### Expected Input

Conceptually, this task would likely need:

```text
duration results
spectral results
localization results
light curve features
```

### Current Processing

No processing is currently performed by the DAG node.

### Current Output

No real output is currently produced.

## Task 11: GCN_GeD

Function: `_queue_gcn_outbox_notice` in the DAG

### Input

```text
pipeline_config.yaml
duration_result
localization_result
```

### Processing

The task queues a COSI GeD alert notice in the local GCN outbox. It does not publish directly to an
external broker from the DAG task.

### Output

An outbox record containing:

```text
pipeline = GeD
instrument = Germanium Detectors
source_config_path = pipeline_config.yaml
duration_result
localization_result
```

## Main Products Summary

### Shared State

```text
pipeline_config.yaml
```

### Unbinned Branch

```text
ged_preprocessed_events.hdf5
unbinned_lightcurve_results.yaml
lc_lc.csv
lc_lc.png
lc_diagnostics.png
grb_fast_localize_prep.npz
grb_fast_localize_prep.yaml
grb_sigmap_nside{nside}.npz
grb_fast_localize_skymap.yaml
grb_localization_top50.csv
grb_significance_map.png
grb_timing.csv
grb_fast_localize_results.yaml
```

### Binned Branch

```text
inputs_grb.yaml
inputs_bg.yaml
source_binned_file_path.hdf5
background_binned_file_path.hdf5
ged_preprocessed_data.hdf5
ged_preprocessed_background_model.hdf5
tsmap_fast.png
tsmap_moc.png
tsmap_results.yaml
lightcurve.png
lightcurve_results.yaml
duration_plot.png
duration_results.yaml
```

### Notice Queue

```text
GCN outbox record
```

## Important Design Note

The shared `pipeline_config.yaml` is both the task input and the mutable pipeline state. This makes
the flow easy to inspect, but it also means that parallel tasks can race when they read and write the
same config file.

Immediately after `PreProcessing_GeD`, the DAG starts both `Unbinned_Light_Curve_Generation` and
`Data_Binning`. If both tasks read the same initial config and then save it, the last writer can
overwrite metadata produced by the other task. This is the main data-flow risk in the current design.
