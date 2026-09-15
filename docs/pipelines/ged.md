# GeD branch

`cosidag_GeD` owns the binned COSIpy likelihood branch. Unbinned event
selection and localization are isolated in [ARM selection](arm-selection.md).

## Inputs

| XCom key | Required product |
| --- | --- |
| `grb_file` | GRB event FITS file |
| `background_file` | background-window FITS file |
| `orientation_file` | orientation/attitude file |
| `response_file` | detector response file |

The branch writes `pipeline_config.yaml` beside the selected source. Important
products include:

| Stage | Products |
| --- | --- |
| Data binning | `inputs_grb.yaml`, `inputs_bg.yaml`, source/background HDF5 files, `ged_preprocessed_data.hdf5`, `ged_preprocessed_background_model.hdf5` |
| TS map | `ged_fast_likelihood_tsmap.png`, `ged_multiorder_likelihood_tsmap.png`, `tsmap_results.yaml` |
| Light curve | `ged_point_source_response_selected_lightcurve.png`, `lightcurve_results.yaml` |
| Duration | `ged_bayesian_blocks_lightcurve_analysis.png`, `duration_results.yaml` |

Binning filenames with time bounds are derived at runtime; consumers should
read their recorded paths rather than construct them. This branch sets
`prepare_event_data=False`, so it does not create the event HDF5 product used
by ARM selection.

`Spectral_Analysis` and `Classification_GeD` are `EmptyOperator` placeholders,
not implemented scientific products. The exact graph and runtime split are in
the [DAG reference](../reference/dags.md#cosidag_ged).
