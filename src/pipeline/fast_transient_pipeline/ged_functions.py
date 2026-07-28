from __future__ import annotations


# Shared preprocessing defaults for the binned GeD and unbinned ARM-selection
# branches. User-provided YAML values are deep-merged into this structure.
GED_ANALYSIS_DEFAULTS: dict[str, Any] = {
    "unbinned_light_curve": {
        "arm-min": -15.0,
        "arm-max": 15.0,
        "bin": 1.0,
        "out-prefix": "lc",
        "arm-hist-bins": 30,
        "format": "png",
        "diagnostics": True,
        "col-time": "TimeTags",
        "col-l": "Chi galactic",
        "col-b": "Psi galactic",
        "col-phi": "Phi",
        "plot_figsize": [8, 4],
        "diagnostics_figsize": [14, 10],
        "plot_dpi": 200,
    },
    "default_spectrum": {
        "index": -2.2,
        "K": 10.0,
        "K_unit": "1 / (cm2 keV s)",
        "piv": 100.0,
        "piv_unit": "keV",
    },
    "tsmap": {
        "nside": 16,
        "energy_channel": [2, 3],
        "cpu_cores": 8,
        "selected_method": "moc",
        "selected_coordinates": {
            "l_deg": 0,
            "b_deg": 0,
        },
        "cds_frame": "local",
        "map_scheme": "nested",
        "coordsys": "galactic",
        "fast_plot_name": "tsmap_fast.png",
        "moc_plot_name": "ged_multiorder_likelihood_tsmap.png",
        "plot_dpi": 300,
    },
    "binning_data": {
        "bin_size": 1,
        "eps_bkg_preburst": 20,
        "eps_bkg_postburst": 20,
        "nside": 16,
        "cosipy_time_bins": 1,
        "cosipy_energy_bins": [
            100.0,
            158.489,
            251.189,
            398.107,
            630.957,
            1000.0,
            1584.89,
            2511.89,
            3981.07,
            6309.57,
            10000.0,
        ],
        "cosipy_phi_pix_size": 6,
        "cosipy_nside": 8,
        "cosipy_scheme": "ring",
        "cosipy_psichi_binning": "local",
        "cosipy_unbinned_output": "fits",
        "cosipy_ori_file": "NA",
    },
    "light_curve": {
        "bin_size": 1,
        "nside": 16,
        "containment": 0.5,
        "plot_figsize": [10, 4],
        "plot_dpi": 150,
        "used_coordinates": {
            "l_deg": 0,
            "b_deg": 0,
        },
    },
    "duration": {
        "lightcurve_path": "",
        "p0": 0.05,
        "is_rate": False,
        "panels": ["ged"],
        "bayes_quantile": 0.9,
        "bayes_error_nsamples": 100,
        "sentinel_value": -9999.0,
        "plot_figsize": [10, 4],
        "plot_dpi": 150,
    },
    "fast_localize": {
        "off_pre": 20.0,
        "off_gap": 5.0,
        "off_fallback_strategy": "on_background",
        "arm_min": -13.0,
        "arm_max": 13.0,
        "nside": 32,
        "nsides": None,
        "pix_chunk": 256,
        "event_chunk": 200000,
        "topk": 50,
        "out_prefix": "grb",
        "true_l_deg": None,
        "true_b_deg": None,
        "suppress_mmap_warning": True,
        "make_lc": False,
        "lc_bin": 1.0,
        "lc_arm_min": None,
        "lc_arm_max": None,
        "lc_diagnostics": False,
        "lc_script": "make_timeseries_all.py",
        "col_time": "TimeTags",
        "col_l": "Chi galactic",
        "col_b": "Psi galactic",
        "col_phi": "Phi",
        "map_plot_figsize": [10, 6],
        "map_plot_dpi": 220,
        "nside_table_figsize_width": 14,
        "nside_table_figsize_base_height": 2.4,
        "nside_table_figsize_row_height": 0.38,
        "nside_table_dpi": 240,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    from copy import deepcopy

    # Keep defaults immutable for callers by always building a fresh tree.
    merged = deepcopy(base)
    if not override:
        return merged
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _to_yaml_safe(value: Any) -> Any:
    from pathlib import Path

    import numpy as np

    # YAML cannot serialize numpy scalars, arrays, or Path objects directly.
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _to_yaml_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_yaml_safe(v) for v in value]
    return value


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _plot_identity(
    instrument: str,
    caption: str,
    trigger_time: Any = None,
    plot_name: str | None = None,
    preview_title: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
) -> tuple[str, dict[str, str]]:
    """Return a uniform on-figure title and searchable PNG text metadata."""
    import os
    from datetime import datetime, timezone

    def _context_value(value: Any, environment_key: str) -> str:
        resolved = value
        if resolved in (None, ""):
            resolved = os.environ.get(environment_key)
        return str(resolved) if resolved not in (None, "") else "N/A"

    generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    trigger_label = (
        str(trigger_time)
        if trigger_time not in (None, "")
        else "* (TODO: provide trigger time)"
    )
    title = (
        f"COSI {instrument} - Trigger Time: {trigger_label} - "
        f"Generated: {generated_utc}"
    )
    if plot_name:
        title = f"{title}\n{plot_name}"

    metadata = {
        "Title": title.replace("\n", " — "),
        "Description": caption,
        "Caption": caption,
        "Instrument": f"COSI {instrument}",
        "PreviewTitle": preview_title or plot_name or f"COSI {instrument} Plot",
        "TriggerTime": trigger_label,
        "GeneratedUTC": generated_utc,
        "DAGID": _context_value(dag_id, "AIRFLOW_CTX_DAG_ID"),
        "DAGRunID": _context_value(dag_run_id, "AIRFLOW_CTX_DAG_RUN_ID"),
        "DAGTaskID": _context_value(task_id, "AIRFLOW_CTX_TASK_ID"),
    }
    return title, metadata


def _config_trigger_time(config: dict[str, Any], instrument: str) -> Any:
    """Resolve trigger time from either the flat or structured run config."""
    trigger_time = config.get("trigger_time")
    if trigger_time not in (None, ""):
        return trigger_time

    pipeline_name = str(config.get("pipeline_name", instrument))
    run_config = config.get(pipeline_name, {})
    if isinstance(run_config, dict):
        return run_config.get("trigger_time")
    return None


def _ensure_structured_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    # Airflow/DAG consumers expect a top-level run section named after the
    # pipeline. This helper creates that section and records resolved inputs.
    pipeline_name = str(config.get("pipeline_name", "GeD"))
    run_cfg = config.setdefault(pipeline_name, {})
    run_cfg["name"] = config.get("cosidag_id", "cosidag_GeD")
    run_cfg.setdefault("trigger_time", config.get("trigger_time", _now_iso()))
    run_cfg["input_resolved"] = _to_yaml_safe(
        config.get(
            "input_resolved",
            {
                "source_path": config.get("source_path"),
                "background_path": config.get("background_path"),
                "orientation_path": config.get("orientation_path"),
                "response_path": config.get("response_path"),
            },
        )
    )
    return run_cfg


def _record_pipeline_task(
    config: dict[str, Any],
    task_id: str,
    task_name: str,
    input_data: dict[str, Any] | None = None,
    output_data: dict[str, Any] | None = None,
    status: str = "success",
    start_time: str | None = None,
    end_time: str | None = None,
) -> None:
    # All task records share the same schema so the final YAML can be consumed
    # by monitoring/reporting code without task-specific parsing.
    run_cfg = _ensure_structured_pipeline_config(config)
    existing = run_cfg.get(task_id, {})
    run_cfg[task_id] = {
        "name": task_name,
        "start-time": start_time or existing.get("start-time") or _now_iso(),
        "input-data": _to_yaml_safe(input_data or existing.get("input-data", {})),
        "status": status,
        "end-time": end_time or _now_iso(),
        "output-data": _to_yaml_safe(output_data or {}),
    }


def _ensure_pipeline_dirs(data_dir: str) -> tuple[Path, Path]:
    from pathlib import Path

    base_dir = Path(data_dir)
    # Products stay beside the data while plots are grouped in a sibling folder.
    plots_dir = base_dir / ".." / "plots"
    products_dir = base_dir
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    return plots_dir, products_dir


def _is_fits_path(path: str) -> bool:
    return str(path).lower().endswith((".fits", ".fits.gz", ".fit", ".fit.gz"))


def _write_histogram(hist: Any, path: str) -> str:
    from pathlib import Path

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    try:
        hist.write(str(out), overwrite=True)
    except TypeError:
        hist.write(str(out))
    return str(out)


def _prepared_time_window(config: dict[str, Any]) -> tuple[float, float]:
    # Binned products include background padding around the burst interval.
    bin_cfg = dict(config.get("binning_data", {}))
    eps_pre = float(bin_cfg.get("eps_bkg_preburst", 0.0))
    eps_post = float(bin_cfg.get("eps_bkg_postburst", 0.0))
    return float(config["tstart"]) - eps_pre, float(config["tstop"]) + eps_post



def _build_prepared_binned_data(
    config: dict[str, Any],
    source_data_path: str,
    background_data_path: str,
) -> dict[str, Any]:
    """
    Persist the canonical binned prepared product: aggregated data and background model.
    """
    from pathlib import Path

    try:
        from . import fast_helper_functions as fhf
    except ImportError:
        import fast_helper_functions as fhf

    data_dir = config["data_dir"]
    _, products_dir = _ensure_pipeline_dirs(data_dir)
    products_dir = Path(products_dir)
    prepared_data_path = products_dir / "ged_preprocessed_data.hdf5"
    background_model_path = products_dir / "ged_preprocessed_background_model.hdf5"

    prepared_tstart, prepared_tstop = _prepared_time_window(config)
    # Aggregate source and background histograms over the padded time window.
    data, bkg_model = fhf.aggregate_data(
        source_data_path,
        background_data_path,
        prepared_tstart,
        prepared_tstop,
    )
    _write_histogram(data, str(prepared_data_path))
    _write_histogram(bkg_model, str(background_model_path))

    payload = {
        "data_path": str(prepared_data_path),
        "background_model_path": str(background_model_path),
        "format": "hdf5",
        "contains_background": True,
        "source_path_original": config.get("source_path"),
        "background_path_original": config.get("background_path"),
        "source_data_path": source_data_path,
        "background_data_path": background_data_path,
        "tstart": prepared_tstart,
        "tstop": prepared_tstop,
        "burst_tstart": float(config["tstart"]),
        "burst_tstop": float(config["tstop"]),
    }
    config["prepared_data"] = {**dict(config.get("prepared_data", {})), **payload}
    config["aggregated_data_path"] = str(prepared_data_path)
    config["background_model_path"] = str(background_model_path)
    return payload

#########################################################
# TASK 1: Preprocessing
#########################################################
def preprocess_data(
    lightcurve,
    eps_time: float = 0.000000001):
    """
    Preprocess the pipeline inputs and persist them in a YAML config.

    The function accepts either:
    - a dict containing input keys, or
    - a string path to an existing YAML file to enrich/normalize.
    """
    from pathlib import Path

    import fast_helper_functions as fhf

    # Accept both a config dict from Python callers and an existing YAML path
    # from DAG/operator callers.
    if isinstance(lightcurve, str):
        config = fhf._load_yaml(lightcurve)
    elif isinstance(lightcurve, dict):
        config = dict(lightcurve)
    else:
        raise TypeError("preprocess_data expects a dict or a YAML path string")
    # Promote nested analysis defaults to top-level sections for older tasks
    # that still read config["tsmap"], config["duration"], etc.
    analysis_config = _deep_merge(GED_ANALYSIS_DEFAULTS, config.get("analysis_config", {}))
    config["analysis_config"] = analysis_config
    for section, defaults in analysis_config.items():
        if isinstance(defaults, dict):
            config[section] = _deep_merge(defaults, config.get(section, {}))
        else:
            config.setdefault(section, defaults)
    # These inputs define the minimum reproducible GeD run.
    required_keys = [
        "source_path",
        "background_path",
        "orientation_path",
        "response_path",
    ]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(f"Missing required preprocessing fields: {missing}")
    # Put products beside the source file unless the YAML overrides data_dir.
    default_data_dir = str(Path(config["source_path"]).parent)
    data_dir = config.get("data_dir", default_data_dir)
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    config["data_dir"] = str(Path(data_dir))
    config["plots_dir"] = str(plots_dir)
    config["products_dir"] = str(products_dir)
    # Be explicit even though _ensure_pipeline_dirs already creates them; this
    # keeps the behavior obvious to future maintainers.
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    nside = int(config["tsmap"].get("nside", 16))
    # Backward-compatible top-level aliases used by downstream tasks.
    config.setdefault("tsmap_nside", config["tsmap"].get("nside", nside))
    config.setdefault("tsmap_energy_channel", config["tsmap"].get("energy_channel", [2, 3]))
    config.setdefault("tsmap_cpu_cores", config["tsmap"].get("cpu_cores", 8))
    config.setdefault("lightcurve_nside", config["light_curve"].get("nside", nside))
    config.setdefault(
        "lightcurve_bin_size",
        config["light_curve"].get("lightcurve_bin_size", 1),
    )
    config.setdefault(
        "duration_lightcurve_path",
        config["duration"].get("lightcurve_path", ""),
    )
    config.setdefault("duration_p0", config["duration"].get("p0", 0.05))
    config.setdefault("duration_is_rate", config["duration"].get("is_rate", False))
    config.setdefault(
        "duration_panels",
        config["duration"].get("panels", ["ged"]),
    )
    # Ensure top-level time bounds exist for downstream binning.
    
    # The source FITS time range defines the burst interval; eps_time keeps
    # edge events from being lost to strict half-open comparisons downstream.
    grb_tstart, grb_tstop, grb_duration = fhf._infer_time_bounds_from_fits(str(config["source_path"]))
    print(
        "[preprocess_data] Inferred tstart/tstop/duration from source FITS: "
        f"{grb_tstart} -> {grb_tstop} -> {grb_duration}"
    )
    config["tstart"] = grb_tstart - eps_time
    config["tstop"] = grb_tstop + eps_time
    config["grb_duration"] = grb_duration
    # The ARM-selection DAG requests this unbinned prepared product. The
    # binned-only GeD DAG disables it so the two branches do not duplicate the
    # expensive FITS preparation step.
    if bool(config.get("prepare_event_data", True)):
        try:
            from .armsel_functions import _build_prepared_event_data
        except ImportError:
            from armsel_functions import _build_prepared_event_data

        config["prepared_data"] = {
            **dict(config.get("prepared_data", {})),
            **_build_prepared_event_data(config),
        }

    _ensure_structured_pipeline_config(config)
    preprocessing_task_id = str(
        config.get("preprocessing_task_id", "PreProcessing_GeD")
    )
    preprocessing_task_name = str(
        config.get("preprocessing_task_name", "PreProcessing GeD")
    )
    _record_pipeline_task(
        config,
        task_id=preprocessing_task_id,
        task_name=preprocessing_task_name,
        input_data={
            "source_path": config["source_path"],
            "background_path": config["background_path"],
            "orientation_path": config["orientation_path"],
            "response_path": config["response_path"],
            "eps_time": eps_time,
            "analysis_config": analysis_config,
        },
        output_data={
            "data_dir": config["data_dir"],
            "plots_dir": config["plots_dir"],
            "products_dir": config["products_dir"],
            "tstart": config["tstart"],
            "tstop": config["tstop"],
            "grb_duration": config["grb_duration"],
            "prepared_data": config.get("prepared_data", {}),
        },
    )

    config_path = config.get("config_path", str(products_dir / "pipeline_config.yaml"))
    fhf._save_yaml(config_path, config)
    return config_path



#########################################################
# TASK 3: Binning
#########################################################
def bin_data(config_path: str) -> tuple[str, str]:
    """
    Bin source and background data with a single entrypoint.
    Returns (source_binned_file_path, background_binned_file_path).

    Args:
        config_path: Path to the yaml configuration file.
    """
    import os

    import yaml

    import fast_helper_functions as fhf

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)

    source_path = config["source_path"]
    background_path = config["background_path"]
    burst_tstart = float(config["tstart"])
    burst_tstop = float(config["tstop"])
    tstart, tstop = _prepared_time_window(config)
    bin_cfg = dict(config.get("binning_data", {}))

    from cosipy import BinnedData

    def _bin_single_data(
        unbinned_file_path: str,
        label: str,
        inputs_filename: str,
        tstart: float,
        tstop: float,
    ) -> str:
        """
        Bin a single data file.
        Returns the path to the binned file.

        Args:
            unbinned_file_path: Path to the unbinned fits file.
            label: Label of the data file.
            inputs_filename: Name of the inputs file.
            tstart: Start time of the data.
            tstop: Stop time of the data.
        """
        import math

        data_folder = os.path.dirname(unbinned_file_path)

        print(f"[bin_data:{label}] Found unbinned fits file: {unbinned_file_path}")

        extension = unbinned_file_path.split(".")[-1]
        window_tag = f"t{int(math.floor(float(tstart)))}_{int(math.ceil(float(tstop)))}"
        binned_file_name = (
            unbinned_file_path.replace("_unbinned_", "_binned_")
            .replace(f".{extension}", "")
            + f"_{window_tag}"
        )
        binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")

        print(f"[bin_data:{label}] Expected output file: {binned_file_path}")

        if os.path.exists(binned_file_path):
            print(f"[bin_data:{label}] Binned file already exists: {binned_file_path}")
            print(f"[bin_data:{label}] Skipping binning step.")
            return binned_file_path

        print(f"[bin_data:{label}] Binned file not found. Proceeding with binning process...")
        print(f"[bin_data:{label}] Creating binning configuration...")

        # COSIpy expects a small YAML file describing how to transform the
        # unbinned event list into a binned histogram product.
        binning_config = {
            "data_file": unbinned_file_path,
            "ori_file": bin_cfg.get("cosipy_ori_file", "NA"),
            "unbinned_output": bin_cfg.get("cosipy_unbinned_output", "fits"),
            "time_bins": bin_cfg.get("cosipy_time_bins", 1),
            "energy_bins": bin_cfg.get(
                "cosipy_energy_bins",
                GED_ANALYSIS_DEFAULTS["binning_data"]["cosipy_energy_bins"],
            ),
            "phi_pix_size": bin_cfg.get("cosipy_phi_pix_size", 6),
            "nside": bin_cfg.get("cosipy_nside", 8),
            "scheme": bin_cfg.get("cosipy_scheme", "ring"),
            "tmin": tstart,
            "tmax": tstop,
        }

        inputs_path = os.path.join(data_folder, inputs_filename)
        with open(inputs_path, "w") as f:
            yaml.dump(binning_config, f, default_flow_style=False)

        print(f"[bin_data:{label}] Created binning configuration: {inputs_path}")
        print(f"[bin_data:{label}] Initializing COSIpy BinnedData analysis...")
        analysis = BinnedData(inputs_path)
        print(f"[bin_data:{label}] BinnedData analysis object created successfully")

        print(f"[bin_data:{label}] Starting binning process for output: {binned_file_name}")
        analysis.get_binned_data(
            unbinned_data=unbinned_file_path,
            output_name=binned_file_name,
            psichi_binning=bin_cfg.get("cosipy_psichi_binning", "local"),
        )

        print(f"[bin_data:{label}] Binning completed successfully")
        print(f"[bin_data:{label}] Output file created: {binned_file_path}")

        if os.path.exists(binned_file_path):
            file_size = os.path.getsize(binned_file_path) / (1024 * 1024)
            print(f"[bin_data:{label}] Output file verified: {file_size:.2f} MB")
        else:
            print(f"[bin_data:{label}] Warning: Expected output file not found: {binned_file_path}")

        return binned_file_path

    # Bin source data
    source_binned_file_path = _bin_single_data(
        unbinned_file_path=source_path,
        label="source",
        inputs_filename="inputs_grb.yaml",
        tstart=tstart,
        tstop=tstop,
    )
    # Bin background data
    background_binned_file_path = _bin_single_data(
        unbinned_file_path=background_path,
        label="background",
        inputs_filename="inputs_bg.yaml",
        tstart=tstart,
        tstop=tstop,
    )
    # Update the config with the binned file paths and record the task
    current_config = fhf._load_yaml(config_path)
    current_config["source_binned_file_path"] = source_binned_file_path
    current_config["background_binned_file_path"] = background_binned_file_path
    current_config["binned_data"] = {
        "source_binned_file_path": source_binned_file_path,
        "background_binned_file_path": background_binned_file_path,
    }
    # Persist a canonical aggregate/background pair consumed by TS map and
    # light-curve stages.
    prepared_data = _build_prepared_binned_data(
        current_config,
        source_binned_file_path,
        background_binned_file_path,
    )
    # Record the binning task in the pipeline config
    _record_pipeline_task(
        current_config,
        task_id="Data_Binning",
        task_name="Data Binning",
        input_data={
            "config_path": config_path,
            "source_path": source_path,
            "background_path": background_path,
            "tstart": tstart,
            "tstop": tstop,
            "burst_tstart": burst_tstart,
            "burst_tstop": burst_tstop,
            **bin_cfg,
        },
        output_data={
            "source_binned_file_path": source_binned_file_path,
            "background_binned_file_path": background_binned_file_path,
            "prepared_data": prepared_data,
        },
        start_time=task_start,
    )
    fhf._save_yaml(config_path, current_config)
    return source_binned_file_path, background_binned_file_path



#########################################################
# TASK 6: TS_Map_on_different_timescales
#########################################################
def compute_ts_map(
    config_path: str,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str = "TS_Map_on_different_timescales",
) -> str:
    """
    Compute TS map for the source and background data.
    Returns the path to the TS map file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    from pathlib import Path

    import numpy as np

    try:
        from . import fast_helper_functions as fhf
    except ImportError:
        import fast_helper_functions as fhf

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")


    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    import logging
    logging.basicConfig(level = logging.INFO)
    import gc
    import astropy.units as u
    from astropy.time import Time
    from cosipy import FastTSMap, MOCTSMap, SpacecraftHistory
    from histpy import Histogram
    from mhealpy import HealpixMap
    import healpy as hp
    import matplotlib.pyplot as plt
    from threeML import Powerlaw

    # Define the assumed source spectrum used by FastTSMap and MOCTSMap.
    spectrum_cfg = config.get("default_spectrum", {})
    spectrum = Powerlaw()
    spectrum.index.value = float(spectrum_cfg.get("index", -2.2))
    spectrum.K.value = float(spectrum_cfg.get("K", 10.0))
    spectrum.piv.value = float(spectrum_cfg.get("piv", 100.0))
    spectrum.K.unit = u.Unit(spectrum_cfg.get("K_unit", "1 / (cm2 keV s)"))
    spectrum.piv.unit = u.Unit(spectrum_cfg.get("piv_unit", "keV"))
    # Prefer the canonical preprocessed aggregate; fall back to aggregating
    # binned source/background products on demand for older configs.
    prepared_data = dict(config.get("prepared_data", {}))
    source_data_path = prepared_data.get("data_path")
    background_data_path = prepared_data.get("background_model_path")
    map_tstart = float(prepared_data.get("tstart", _prepared_time_window(config)[0]))
    map_tstop = float(prepared_data.get("tstop", _prepared_time_window(config)[1]))
    if (
        source_data_path
        and background_data_path
        and Path(source_data_path).is_file()
        and Path(background_data_path).is_file()
    ):
        data = Histogram.open(source_data_path)
        bkg_model = Histogram.open(background_data_path)
    else:
        source_data_path = config.get("source_binned_file_path", config["source_path"])
        background_data_path = config.get("background_binned_file_path", config["background_path"])
        data, bkg_model = fhf.aggregate_data(source_data_path, background_data_path, map_tstart, map_tstop)
    # Get the tsmap configuration
    tsmap_cfg = config.get("tsmap", {})
    cds_frame = str(tsmap_cfg.get("cds_frame", "local"))
    map_scheme = str(tsmap_cfg.get("map_scheme", "nested"))
    coordsys = str(tsmap_cfg.get("coordsys", "galactic"))
    plot_dpi = int(tsmap_cfg.get("plot_dpi", 300))
    fast_plot_name = str(tsmap_cfg.get("fast_plot_name", "tsmap_fast.png"))
    moc_plot_name = str(
        tsmap_cfg.get("moc_plot_name", "ged_multiorder_likelihood_tsmap.png")
    )
    # Get the nside
    nside = int(config.get("tsmap_nside", tsmap_cfg.get("nside", 16)))
    # Get the energy channel
    energy_channel = config.get("tsmap_energy_channel", tsmap_cfg.get("energy_channel", [2, 3]))
    # Get the cpu cores
    cpu_cores = int(config.get("tsmap_cpu_cores", tsmap_cfg.get("cpu_cores", 8)))

    # Restrict spacecraft history to the same padded interval used by the map data.
    ori_full = SpacecraftHistory.open(config["orientation_path"])
    grb_ori = ori_full.select_interval(
        Time(map_tstart, format = "unix"), 
        Time(map_tstop, format = "unix"))

    # FastTSMap evaluates a fixed-resolution HEALPix grid.
    fast = FastTSMap(
        data=data,
        bkg_model=bkg_model,
        orientation=grb_ori,
        response_path=config["response_path"],
        cds_frame=cds_frame,
    )
    fast_ts = fast.fit(
        nside=nside,
        energy_channel=energy_channel,
        spectrum=spectrum,
        cpu_cores=cpu_cores,
    )
    fast_idx = int(np.argmax(fast_ts))
    fast_max_ts = float(np.max(fast_ts))
    fast_map = HealpixMap(nside=nside, scheme=map_scheme, coordsys=coordsys)
    fast_coo = fast_map.pix2skycoord(fast_idx)
    fast_l = float(fast_coo.l.value)
    fast_b = float(fast_coo.b.value)
    fast_plot_path = plots_dir / "ged_fast_likelihood_tsmap.png"
    trigger_time = _config_trigger_time(config, "GeD")
    fast_caption = (
        "Mollweide projection of the fixed-resolution likelihood-ratio test "
        "statistic in Galactic coordinates. At each HEALPix position, the "
        "source-plus-background model is evaluated using the detector response, "
        "spacecraft orientation, background model, selected energy channels, "
        "and assumed source spectrum. Pixel colors encode TS values, and the "
        "fuchsia cross marks the maximum-TS direction. TS values are not "
        "displayed as Gaussian significances."
    )
    fast_title, fast_metadata = _plot_identity(
        "GeD",
        fast_caption,
        trigger_time,
        f"Fixed-resolution likelihood-ratio TS map (NSIDE={nside})",
        preview_title="COSI GeD fixed-resolution TS map",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
    )
    fig_fast = plt.figure(figsize=(10, 6), dpi=plot_dpi)
    hp.mollview(
        fast_ts,
        nest=map_scheme.startswith("nest"),
        coord="G",
        title=fast_title,
        unit="TS",
        fig=fig_fast.number,
    )
    hp.graticule(color="grey", alpha=0.45)
    hp.projscatter(
        fast_l,
        fast_b,
        marker="x",
        linewidths=1.2,
        lonlat=True,
        coord="G",
        color="fuchsia",
        label="Maximum TS",
    )
    fast_ax = plt.gca()
    fast_ax.set_xlabel("Galactic longitude l [deg]")
    fast_ax.set_ylabel("Galactic latitude b [deg]")
    fast_ax.legend(loc="lower left", framealpha=0.9)
    fig_fast.savefig(
        fast_plot_path,
        dpi=plot_dpi,
        bbox_inches="tight",
        metadata=fast_metadata,
    )
    plt.close(fig_fast)

    # MOCTSMap uses a multi-order map and reports the best UNIQ cell.
    moc = MOCTSMap(
        data=data,
        bkg_model=bkg_model,
        response_path=config["response_path"],
        orientation=grb_ori,
        cds_frame=cds_frame,
    )
    moc_ts, moc_uniq = moc.fit(
        max_nside=nside,
        energy_channel=energy_channel,
        spectrum=spectrum,
    )
    moc_idx = int(np.argmax(moc_ts))
    moc_max_ts = float(moc_ts[moc_idx])
    max_uniq = int(moc_uniq[moc_idx])
    # Convert NUNIQ encoding back to the NSIDE/pixel pair used for coordinates.
    order = int(np.floor(np.log2(max_uniq / 4) / 2))
    moc_nside = 2 ** order
    moc_pix = int(max_uniq - 4 * moc_nside * moc_nside)
    moc_map = HealpixMap(nside=moc_nside, scheme=map_scheme, coordsys=coordsys)
    moc_coo = moc_map.pix2skycoord(moc_pix)
    moc_l = float(moc_coo.l.value)
    moc_b = float(moc_coo.b.value)
    moc_plot_path = plots_dir / moc_plot_name
    moc_caption = (
        "Multi-order likelihood-ratio test-statistic map in Galactic "
        "coordinates. Pixel colors encode TS values, while the grey boundaries "
        "show the HEALPix cells at their respective spatial orders. The map is "
        "computed using the detector response, spacecraft orientation, "
        "background model, selected energy channels, and assumed source "
        "spectrum. The red cross marks the maximum-TS cell."
    )
    moc_title, moc_metadata = _plot_identity(
        "GeD",
        moc_caption,
        trigger_time,
        (
            "Multi-order likelihood-ratio TS map "
            f"(maximum-TS cell NSIDE={moc_nside})"
        ),
        preview_title="COSI GeD multi-order TS map",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
    )
    moc_plot_map = HealpixMap(data=moc_ts, uniq=moc_uniq)
    fig_moc = plt.figure(figsize=(10, 6), dpi=plot_dpi)
    moc_ax = fig_moc.add_subplot(1, 1, 1, projection="mollview")
    moc_plot_map.plot(ax=moc_ax)
    moc_plot_map.plot_grid(ax=moc_ax, color="grey", linewidth=0.1)
    moc_ax.text(
        moc_l,
        moc_b,
        "x",
        size=8,
        horizontalalignment="center",
        verticalalignment="center",
        transform=moc_ax.get_transform("world"),
        color="red",
    )
    moc_ax.set_title(moc_title)
    moc_ax.set_xlabel("Galactic longitude l [deg]")
    moc_ax.set_ylabel("Galactic latitude b [deg]")
    fig_moc.savefig(
        moc_plot_path,
        dpi=plot_dpi,
        bbox_inches="tight",
        metadata=moc_metadata,
    )
    plt.close(fig_moc)

    tsmap_payload = {
        "fast": {
            "max_ts": fast_max_ts,
            "l_deg": fast_l,
            "b_deg": fast_b,
            "plot_path": str(fast_plot_path),
        },
        "moc": {
            "max_ts": moc_max_ts,
            "l_deg": moc_l,
            "b_deg": moc_b,
            "plot_path": str(moc_plot_path),
            "uniq": max_uniq,
            "nside": moc_nside,
            "pix": moc_pix,
        },
        "selected_method": tsmap_cfg.get("selected_method", "moc"),
        "selected_coordinates": {"l_deg": moc_l, "b_deg": moc_b},
        "parameters": {
            "source_data_path": source_data_path,
            "background_data_path": background_data_path,
            "tstart": map_tstart,
            "tstop": map_tstop,
            "burst_tstart": config["tstart"],
            "burst_tstop": config["tstop"],
            "nside": nside,
            "energy_channel": energy_channel,
            "cpu_cores": cpu_cores,
            "cds_frame": cds_frame,
            "map_scheme": map_scheme,
            "coordsys": coordsys,
            "plot_dpi": plot_dpi,
        },
    }
    config["tsmap"] = tsmap_payload
    config["tsmap_yaml"] = str(products_dir / "tsmap_results.yaml")

    _record_pipeline_task(
        config,
        task_id="TS_Map_on_different_timescales",
        task_name="TS Map on different timescales",
        input_data={"config_path": config_path, **tsmap_cfg},
        output_data=tsmap_payload,
        start_time=task_start,
    )
    fhf._save_yaml(config["tsmap_yaml"], tsmap_payload)
    fhf._save_yaml(config_path, config)
    return config["tsmap_yaml"]

#########################################################
# TASK 7: Light_Curve
#########################################################
def light_curve(
    config_path: str,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str = "Light_Curve",
) -> str:
    """
    Generate the light curve of the source and background data.
    Returns the path to the light curve file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    from pathlib import Path

    import matplotlib.pyplot as plt
    import numpy as np

    try:
        from . import fast_helper_functions as fhf
    except ImportError:
        import fast_helper_functions as fhf

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)
    prepared_data = dict(config.get("prepared_data", {}))
    source_data_path = prepared_data.get("data_path")
    background_data_path = None
    using_prepared_aggregate = bool(source_data_path and Path(source_data_path).is_file())
    if not using_prepared_aggregate:
        # Legacy mode: add source and background histograms at each time bin.
        source_data_path = config.get("source_binned_file_path", config["source_path"])
        background_data_path = config.get("background_binned_file_path", config["background_path"])

    tsmap_cfg = config.get("tsmap", {})
    selected = tsmap_cfg.get("selected_coordinates", {})
    lon = selected.get("l_deg")
    lat = selected.get("b_deg")
    if lon is None or lat is None:
        raise ValueError("TSMap coordinates are missing in config['tsmap']['selected_coordinates']")

    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from cosipy import SpacecraftHistory
    from cosipy.response import FullDetectorResponse
    from histpy import Histogram
    
    lightcurve_cfg = dict(config["light_curve"])
    lightcurve_nside = int(lightcurve_cfg["nside"])
    containment = float(lightcurve_cfg.get("containment", 0.5))

    def _create_psr(l_deg: float, b_deg: float, ori_file: str, rsp_file: str, nside: int) -> np.ndarray:
        """
        Create the PSR map.
        Args:
            l_deg: Longitude in degrees.
            b_deg: Latitude in degrees.
            ori_file: Path to the orientation file.
            rsp_file: Path to the response file.
            nside: Nside of the PSR map.
        Returns:
            The PSR map.
        """
        grb_ori = SpacecraftHistory.open(ori_file)
        coord = SkyCoord(l=l_deg * u.deg, b=b_deg * u.deg, frame="galactic")
        scatt_map = grb_ori.get_scatt_map(nside=nside)
        with FullDetectorResponse.open(rsp_file) as response:
            return response.get_point_source_response(coord=coord, scatt_map=scatt_map)

    def _mask_from_cumdist_vectorized(psr_map: np.ndarray, containment: float = 0.5) -> np.ndarray:
        """
        Create the mask from the PSR map.
        Args:
            psr_map: PSR map.
            containment: Containment fraction.
        Returns:
            The mask.
        """
        # Normalize each response slice and keep the highest-probability bins
        # until the requested containment is reached.
        psr_sum = np.sum(psr_map, axis=-1, keepdims=True)
        psr_norm = np.divide(psr_map, psr_sum, out=np.zeros_like(psr_map), where=psr_sum != 0)
        sort_idx = np.argsort(psr_norm, axis=-1)[..., ::-1]
        sorted_vals = np.take_along_axis(psr_norm, sort_idx, axis=-1)
        cumsum_vals = np.cumsum(sorted_vals, axis=-1)
        mask_sorted = (cumsum_vals < containment).astype(float)
        mask = np.empty_like(mask_sorted)
        np.put_along_axis(mask, sort_idx, mask_sorted, axis=-1)
        return mask

    def _load_data(signal_full: Histogram, bkg_full: Histogram | None, window_start: float, window_stop: float) -> np.ndarray:
        """
        Load the data from the signal and background files.
        Args:
            signal_full: Signal full.
            bkg_full: Background full.
            window_start: Window start.
            window_stop: Window stop.
        """
        def _project_window(hist: Histogram, window_start: float, window_stop: float) -> np.ndarray:
            edges = hist.axes["Time"].edges.value
            if window_stop <= edges[0] or window_start >= edges[-1]:
                return np.zeros_like(mask_map, dtype=float)

            # Translate physical time bounds into histogram bin indices.
            start_idx = int(np.searchsorted(edges, window_start, side="right") - 1)
            stop_idx = int(np.searchsorted(edges, window_stop, side="left"))
            start_idx = max(start_idx, 0)
            stop_idx = min(stop_idx, len(edges) - 1)

            if stop_idx <= start_idx:
                return np.zeros_like(mask_map, dtype=float)

            return hist.slice[start_idx:stop_idx, :].project(["Em", "Phi", "PsiChi"]).contents

        signal = _project_window(signal_full, window_start, window_stop)
        if bkg_full is None:
            return signal
        bkg = _project_window(bkg_full, window_start, window_stop)
        return signal + bkg
    
    signal_full = Histogram.open(source_data_path)
    bkg_full = None if using_prepared_aggregate else Histogram.open(background_data_path)

    # Build the point-source response and a containment mask around the selected
    # TS-map location.
    psr_map = _create_psr(
        lon,
        lat,
        ori_file=config["orientation_path"],
        rsp_file=config["response_path"],
        nside=lightcurve_nside,
    )
    # Project the PSR map and the mask map
    input_psr = psr_map.project(["Em", "Phi", "PsiChi"]).contents
    # Create the mask map
    mask_map = _mask_from_cumdist_vectorized(
        input_psr,
        containment=containment,
    )
    
    counts = []
    time_centers = []
    bin_size = lightcurve_cfg["bin_size"]
    eps_preburst = config["binning_data"]["eps_bkg_preburst"]
    eps_postburst = config["binning_data"]["eps_bkg_postburst"]
    tstart = config["tstart"]
    tstop = config["tstop"]
    
    i = tstart - eps_preburst
    tend = tstop - bin_size + eps_postburst
    while i < tend:
        # Sum masked counts in each time bin to form the GeD light curve.
        j = i + bin_size
        data_map = _load_data(signal_full, bkg_full, i, j)
        masked_data = mask_map * data_map
        counts.append(float(masked_data.sum()))
        time_centers.append(float((i + j) / 2.0))
        i = j

    # Build bin edges aligned with the configured pre/post burst windows.
    bins = np.arange(tstart - eps_preburst, tstop + eps_postburst + bin_size, bin_size)

    lightcurve_caption = (
        f"Blue step histogram of the counts in {bin_size:g} s bins after "
        "selecting Compton-data-space bins from the point-source response "
        "evaluated at the selected TS-map direction, using a nominal "
        f"{containment:.0%} response-containment threshold. No background "
        "subtraction is applied. The orange dashed line marks the configured "
        "start of the burst interval."
    )
    lightcurve_title, lightcurve_metadata = _plot_identity(
        "GeD",
        lightcurve_caption,
        _config_trigger_time(config, "GeD"),
        "Point-source-response-selected count light curve",
        preview_title="COSI GeD response-selected light curve",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
    )
    fig_lc, ax_lc = plt.subplots(figsize=tuple(lightcurve_cfg.get("plot_figsize", [10, 4])))
    ax_lc.step(
        time_centers,
        counts,
        where="mid",
        color="tab:blue",
        label="Selected counts",
    )
    ax_lc.set_xlabel("Time [s]")
    ax_lc.set_ylabel(f"Counts / {bin_size:g} s")
    ax_lc.set_title(lightcurve_title)
    ax_lc.axvline(
        x=config["tstart"],
        color="tab:orange",
        linestyle="--",
        linewidth=1.5,
        label="Trigger-window start",
    )
    ax_lc.legend()
    ax_lc.grid(True, alpha=0.3)

    lightcurve_plot_path = (
        plots_dir / "ged_point_source_response_selected_lightcurve.png"
    )
    fig_lc.tight_layout()
    fig_lc.savefig(
        lightcurve_plot_path,
        dpi=int(lightcurve_cfg.get("plot_dpi", 150)),
        metadata=lightcurve_metadata,
    )
    plt.close(fig_lc)

    lightcurve_payload = {
        "plot_path": str(lightcurve_plot_path),
        "bin_size": bin_size,
        "nside": lightcurve_nside,
        "time_bins": bins.tolist(),
        "time_centers": time_centers,
        "counts": counts,
        "used_coordinates": {"l_deg": lon, "b_deg": lat},
        "containment": containment,
        "source_data_path": source_data_path,
        "background_data_path": background_data_path,
        "using_prepared_aggregate": using_prepared_aggregate,
    }
    config["light_curve"] = lightcurve_payload
    config["lightcurve_yaml"] = str(products_dir / "lightcurve_results.yaml")

    _record_pipeline_task(
        config,
        task_id="Light_Curve",
        task_name="Light Curve",
        input_data={"config_path": config_path, **lightcurve_cfg},
        output_data=lightcurve_payload,
        start_time=task_start,
    )
    fhf._save_yaml(config["lightcurve_yaml"], lightcurve_payload)
    fhf._save_yaml(config_path, config)
    return config["lightcurve_yaml"]

#########################################################
# TASK 8: Duration  
#########################################################
def duration(
    config_path: str,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str = "Duration",
) -> str:
    """
    Compute the duration of the source and background data.
    Returns the path to the duration file.

    Args:
        config_path: Path to the yaml configuration file.
    
    Returns:
        Path to the duration yaml file.
    """
    import os

    import numpy as np
    import fast_helper_functions as fhf

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    duration_cfg = config.get("duration", {})
    lightcurve_path = config.get(
        "duration_lightcurve_path",
        duration_cfg.get("lightcurve_path"),
    )

    p0 = float(config.get("duration_p0", duration_cfg.get("p0", 0.05)))
    is_rate = bool(config.get("duration_is_rate", duration_cfg.get("is_rate", False)))
    bayes_quantile = float(duration_cfg.get("bayes_quantile", 0.9))
    bayes_error_nsamples = int(duration_cfg.get("bayes_error_nsamples", 100))
    panels = list(
        config.get(
            "duration_panels",
            duration_cfg.get("panels", ["ged"]),
        )
    )

    if lightcurve_path:
        # External duration inputs are usually stored as .npy/.npz arrays.
        if not os.path.exists(lightcurve_path):
            raise FileNotFoundError(f"Duration input lightcurve not found: {lightcurve_path}")
        loaded = np.load(lightcurve_path, allow_pickle=True)
        if isinstance(loaded, np.ndarray):
            lightcurve = loaded
        else:
            if "light_curve" in loaded.files:
                lightcurve = loaded["light_curve"]
            elif len(loaded.files) == 1:
                lightcurve = loaded[loaded.files[0]]
            else:
                raise ValueError(
                    f"Cannot infer lightcurve dataset from {lightcurve_path}. "
                    f"Available arrays: {loaded.files}"
                )
    else:
        # GeD fallback: reconstruct BGO-like lightcurve array from Light_Curve outputs.
        lc_cfg = config.get("light_curve", {})
        time_centers = lc_cfg.get("time_centers", [])
        counts = lc_cfg.get("counts", [])
        if not time_centers or not counts or len(time_centers) != len(counts):
            raise ValueError(
                "Missing duration lightcurve input. Set 'duration_lightcurve_path' "
                "or run Light_Curve with the updated code to populate "
                "'light_curve.time_centers' and 'light_curve.counts'."
            )
        n_bins = len(time_centers)
        n_panels = max(1, len(panels))
        lightcurve = np.zeros((n_bins, n_panels, 2), dtype=float)
        times_arr = np.asarray(time_centers, dtype=float)
        counts_arr = np.asarray(counts, dtype=float)
        for i in range(n_panels):
            lightcurve[:, i, 0] = times_arr
            lightcurve[:, i, 1] = counts_arr
        lightcurve_path = "generated_from_light_curve_payload"

    (
        lc_sel,
        bb_lc,
        lc_timebins,
        bb_lc_timebins,
        tstart,
        tstop,
        t90,
        t90_err_low,
        t90_err_high,
    ) = get_duration(
        lightcurve=lightcurve,
        p0=p0,
        isRate=is_rate,
        panels=panels,
        quantile=bayes_quantile,
        error_nsamples=bayes_error_nsamples,
        sentinel_value=float(duration_cfg.get("sentinel_value", -9999.0)),
    )

    if bb_lc is not None and lc_sel is not None:
        plot_duration(
            lc_sel,
            bb_lc,
            tstart,
            tstop,
            save_path=str(
                plots_dir / "ged_bayesian_blocks_lightcurve_analysis.png"
            ),
            figsize=duration_cfg.get("plot_figsize", [10, 4]),
            dpi=int(duration_cfg.get("plot_dpi", 150)),
            trigger_time=_config_trigger_time(config, "GeD"),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    duration_payload = {
        "input_lightcurve_path": lightcurve_path,
        "p0": p0,
        "is_rate": is_rate,
        "panels": panels,
        "bayes_quantile": bayes_quantile,
        "bayes_error_nsamples": bayes_error_nsamples,
        "lc_timebins": lc_timebins,
        "bb_lc_timebins": bb_lc_timebins,
        "tstart_t90": float(tstart),
        "tend_t90": float(tstop),
        # Backward-compatible aliases scoped to the duration payload only.
        "tstart": float(tstart),
        "tstop": float(tstop),
        "t90": float(t90),
        "t90_err_low": float(t90_err_low),
        "t90_err_high": float(t90_err_high),
        "plot_path": (
            str(plots_dir / "ged_bayesian_blocks_lightcurve_analysis.png")
            if bb_lc is not None and lc_sel is not None
            else None
        ),
    }

    config["duration"] = duration_payload
    config["duration_yaml"] = str(products_dir / "duration_results.yaml")
    # aliases for downstream operators expecting top-level keys
    config["duration_lightcurve_path"] = lightcurve_path
    config["duration_p0"] = p0
    config["duration_is_rate"] = is_rate
    config["duration_panels"] = panels
    config["tstart_t90"] = float(tstart)
    config["tend_t90"] = float(tstop)
    config["t90"] = float(t90)
    config["t90_err_low"] = float(t90_err_low)
    config["t90_err_high"] = float(t90_err_high)

    _record_pipeline_task(
        config,
        task_id="Duration",
        task_name="Duration",
        input_data={"config_path": config_path, **duration_cfg},
        output_data=duration_payload,
        start_time=task_start,
    )
    fhf._save_yaml(config["duration_yaml"], duration_payload)
    fhf._save_yaml(config_path, config)
    return config["duration_yaml"]


def get_duration(
    lightcurve: np.ndarray,
    p0: float = 0.05,
    isRate: bool = False,
    panels: list[str] = ["z0", "z1", "x0", "x1", "y0", "y1"],
    quantile: float = 0.9,
    error_nsamples: int = 100,
    sentinel_value: float = -9999.0,
) -> tuple[
    Any, 
    Any, 
    dict[str, dict[str, list[float]]], 
    dict[str, list[float]], 
    float, 
    float, 
    float, 
    float, 
    float]:
    """
    Analyze a multi-detector light curve and estimate T90 via Bayesian Blocks.

    Args:
        lightcurve: Light curve data.
        p0: False alarm probability for Bayesian Blocks.
        isRate: If True, the input signal is a rate (counts/s) and is converted to counts/bin.
        panels: List of panels to process.
        quantile: Duration quantile passed to Bayesian Blocks.
        error_nsamples: Number of samples for the Bayesian Blocks duration error.
        sentinel_value: Value returned for scalar outputs when Bayesian Blocks fails.

    Returns:
        A tuple with the following elements:
        - lc_sel: Light curve selection.
        - bb_lc: Bayesian blocks light curve.
        - lc_timebins: Light curve time bins.
        - bb_lc_timebins: Bayesian blocks light curve time bins.
        - tstart: Start time of the signal.
        - tstop: Stop time of the signal.
        - t90: T90 duration.
        - t90_error_low: Lower uncertainty estimate on T90.
        - t90_error_high: Upper uncertainty estimate on T90.
    """
    import numpy as np

    from gdt.core.data_primitives import TimeBins
    from bctools.analysis import BayesianBlocksLightcurve

    data = lightcurve
    time = data[:, 0, 0]
    bin_width = time[1] - time[0]

    signal: dict[str, np.ndarray] = {}
    for i, panel in enumerate(panels):
        y = data[:, i, 1]
        if isRate:
            # BayesianBlocksLightcurve expects counts per bin, not rates.
            y = y * bin_width
        signal[panel] = y

    # Reconstruct bin edges from bin centers for the GDT TimeBins object.
    bin_edges = np.zeros(len(time) + 1)
    bin_edges[1:-1] = (time[:-1] + time[1:]) / 2
    bin_edges[0] = time[0] - (time[1] - time[0]) / 2
    bin_edges[-1] = time[-1] + (time[-1] - time[-2]) / 2
    lo_edges, hi_edges = bin_edges[:-1], bin_edges[1:]
    exposure = np.full(len(time), bin_width)

    lc = {}
    lc_timebins: dict[str, dict[str, list[float]]] = {}
    for panel in panels:
        signal_panel = signal[panel]
        lc[panel] = TimeBins(signal_panel, lo_edges, hi_edges, exposure)
        lc_timebins[panel] = {
            "signal": signal_panel.tolist(),
            "lo_edges": lo_edges.tolist(),
            "hi_edges": hi_edges.tolist(),
            "exposure": exposure.tolist(),
        }

    # Use the panel with the strongest bin as the duration estimator input.
    best_panel = max(panels, key=lambda p: max(lc[p].counts))
    lc_sel = lc[best_panel]

    
    try:
        bb_lc = BayesianBlocksLightcurve(lc_sel)
        bb_lc.compute_bayesian_blocks(p0=p0)

        signal_range = bb_lc.signal_range
        t90 = bb_lc.duration(quantile=quantile)

        bb_lc_timebins = {
            "lo_edges": bb_lc.bb_lightcurve.lo_edges.tolist(),
            "hi_edges": bb_lc.bb_lightcurve.hi_edges.tolist(),
            "rates": bb_lc.bb_lightcurve.rates.tolist(),
        }

    except Exception as exc:
        import traceback

        traceback.print_exc()
        print(
            "WARNING: Main Bayesian Blocks calculation failed: "
            f"{type(exc).__name__}: {exc}"
        )

        return (
            None,
            None,
            lc_timebins,
            {},
            sentinel_value,
            sentinel_value,
            sentinel_value,
            sentinel_value,
            sentinel_value,
        )

    try:
        t90_error = bb_lc.duration_error(
            quantile,
            nsamples=error_nsamples,
        )

    except Exception as exc:
        import traceback

        traceback.print_exc()
        print(
            "WARNING: T90 calculated successfully, but "
            "duration_error failed: "
            f"{type(exc).__name__}: {exc}"
        )

        t90_error = np.asarray(
            [sentinel_value, sentinel_value],
            dtype=float,
        )

    # try:
    #     bb_lc = BayesianBlocksLightcurve(lc_sel)
    #     bb_lc.compute_bayesian_blocks(p0=p0)
    #     signal_range = bb_lc.signal_range
    #     t90 = bb_lc.duration(quantile=quantile)
    #     t90_error = bb_lc.duration_error(quantile, nsamples=error_nsamples)
    #     bb_lc_timebins = {
    #         "lo_edges": bb_lc.bb_lightcurve.lo_edges.tolist(),
    #         "hi_edges": bb_lc.bb_lightcurve.hi_edges.tolist(),
    #         "rates": bb_lc.bb_lightcurve.rates.tolist(),
    #     }
    # except Exception as e:
    #     print(e)
    #     print("WARNING: Bayesian Blocks failed; returning sentinel values")
    #     return (
    #         None,
    #         None,
    #         lc_timebins,
    #         {},
    #         sentinel_value,
    #         sentinel_value,
    #         sentinel_value,
    #         sentinel_value,
    #         sentinel_value,
    #     )

    return (
        lc_sel,
        bb_lc,
        lc_timebins,
        bb_lc_timebins,
        float(signal_range.tstart),
        float(signal_range.tstop),
        float(t90),
        float(t90_error[0]),
        float(t90_error[1]),
    )


def plot_duration(
    lc_sel, 
    bb_lc, 
    tstart, 
    tstop, 
    save_path = None,
    figsize: list[float] | tuple[float, float] = (10, 4),
    dpi: int = 150,
    trigger_time: Any = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str = "Duration",
) -> None:
    """
    Plot the duration of the source and background data.

    Args:
        lc_sel: Light curve selection.
        bb_lc: Bayesian blocks light curve.
        tstart: Start time of the signal.
        tstop: Stop time of the signal.
        save_path: Path to save the plot.

    Returns:
        None
    """
    import matplotlib.pyplot as plt
    import numpy as np

    caption = (
        "Grey points with horizontal and vertical error bars show the observed "
        "count rate, the red dotted curve the fitted background rate, and the "
        "blue step curve the Bayesian-block model. Olive dashed vertical lines "
        "delimit the signal interval identified by the Bayesian-block analysis."
    )
    title, metadata = _plot_identity(
        "GeD",
        caption,
        trigger_time,
        "Bayesian-block analysis of the GeD light curve",
        preview_title="COSI GeD Bayesian-block light-curve analysis",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
    )
    fig, ax = plt.subplots(figsize=tuple(figsize))
    ax.plot(
        lc_sel.centroids,
        bb_lc.bkg_counts / lc_sel.exposure,
        color="red",
        ls=":",
        label="Fitted background",
    )
    ax.errorbar(
        lc_sel.centroids,
        lc_sel.rates,
        xerr=[
            lc_sel.centroids - lc_sel.lo_edges,
            lc_sel.hi_edges - lc_sel.centroids,
        ],
        yerr=lc_sel.rate_uncertainty,
        ls="none",
        color=".7",
        label="Raw data",
    )
    # Signal light curve
    lc_bayes = bb_lc.bb_lightcurve
    # Bayesian blocks light curve
    ax.plot(
        np.append(lc_bayes.lo_edges, lc_bayes.hi_edges[-1]),
        np.append(lc_bayes.rates, lc_bayes.rates[-1]),
        drawstyle="steps-post",
        color="tab:blue",
        label="Bayesian blocks",
    )
    # Vertical lines showing the start and stop of the identified signal
    ax.axvline(
        bb_lc.signal_range.tstart,
        ls="--",
        color="olive",
        label="Signal start/stop",
    )
    ax.axvline(bb_lc.signal_range.tstop, ls="--", color="olive")
    # Add legend and grid
    ax.set_xlabel("Time [s]")
    ax.set_ylabel("Rate [counts/s]")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    # Save the plot
    if save_path:
        fig.savefig(save_path, dpi=dpi, metadata=metadata)
    plt.close(fig)

#########################################################
# TASK 9: Spectral_Analysis
# TODO: Implement the spectral analysis
#########################################################
def spectral_analysis(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Analyze the spectral properties of the source and background data.
    Returns the path to the spectral analysis file.
    """
    raise NotImplementedError("Not implemented yet")

#########################################################
# TASK 10: Classification_GeD
# TODO: Implement the classification
#########################################################
def classification_ged(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Classify the source and background data.
    Returns the path to the classification file.
    """
    raise NotImplementedError("Not implemented yet")

#########################################################   
# TASK 11: GCN_GeD
# TODO: Implement the GCN message generation
#########################################################
def gcn_ged(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Generate the GCN message for the source and background data.
    Returns the path to the GCN message file.
    """
    raise NotImplementedError("Not implemented yet")
