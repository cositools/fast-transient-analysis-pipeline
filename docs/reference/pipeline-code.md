# Pipeline code

The DAGs call the following implementation modules:

| File or directory | Called by | Responsibility |
| --- | --- | --- |
| `stage_files.py` | `init_pipelines.stage_all_files` | Download or copy and validate selected inputs |
| `bkg_cut.py` | `init_pipelines.background_cut` | Create the GeD background-window FITS product |
| `download_data.py` | utility callers | Wasabi download helper |
| `fast_transient_pipeline/bgo_functions.py` | `cosidag_BGO` | BGO scientific functions and plot metadata |
| `fast_transient_pipeline/ged_functions.py` | `cosidag_GeD`, ARM preprocessing | Shared GeD preprocessing and binned analysis |
| `fast_transient_pipeline/armsel_functions.py` | `cosidag_ARMselection` | Unbinned ARM gating and localization |
| `fast_transient_pipeline/fast_helper_functions.py` | all scientific branches | YAML state I/O plus FITS loading, background extraction/slicing, background-model creation, and data aggregation |
| `fast_transient_pipeline/gcn/query.py` | scientific tasks | Fail-open GCN inbox queries |
| `fast_transient_pipeline/gcn/outbox.py` | final GCN tasks | Alert JSON construction and durable outbox insertion |

The `fast_helper_functions.py` description follows its currently defined
functions and all three current call sites; plotting and plot-metadata helpers
live in the branch-specific modules. The source is available in the
[`src/pipeline` tree](https://github.com/cositools/fast-transient-analysis-pipeline/tree/dev/src/pipeline).

The removed legacy `lcurve`, `ts_map`, `fast_grb`, and `binning_script`
directories are not runtime entry points. Current state and output details are
owned by the [BGO](../pipelines/bgo.md), [GeD](../pipelines/ged.md), and
[ARM-selection](../pipelines/arm-selection.md) pages.
