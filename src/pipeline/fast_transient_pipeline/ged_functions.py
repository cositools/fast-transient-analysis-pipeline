from __future__ import annotations


# Centralized defaults for every GeD analysis stage. User-provided YAML values
# are deep-merged into this structure during preprocessing.
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
        "moc_plot_name": "tsmap_moc.png",
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
) -> tuple[str, dict[str, str]]:
    """Return a uniform on-figure title and searchable PNG text metadata."""
    from datetime import datetime, timezone

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


def _build_prepared_event_data(config: dict[str, Any]) -> dict[str, Any]:
    """
    Persist the unbinned prepared event product used by the fast localization branch.

    The file keeps the original source/background event columns and the prepared
    aggregate ON sample plus OFF background sample. Downstream unbinned tasks can
    use this product without re-reading and re-combining the raw FITS files.
    """
    from pathlib import Path
    import time

    import h5py
    import numpy as np

    source_path = str(config["source_path"])
    background_path = str(config["background_path"])
    if not (_is_fits_path(source_path) and _is_fits_path(background_path)):
        return {
            "event_data_status": "skipped",
            "event_data_reason": "source/background inputs are not unbinned FITS files",
            "source_path_original": source_path,
            "background_path_original": background_path,
        }

    fl = dict(config.get("fast_localize", {}))
    data_dir = config["data_dir"]
    _, products_dir = _ensure_pipeline_dirs(data_dir)
    event_data_path = Path(products_dir) / "ged_preprocessed_events.hdf5"

    tstart = float(config["tstart"])
    tstop = float(config["tstop"])
    off_pre = float(fl.get("off_pre", 20.0))
    off_gap = float(fl.get("off_gap", 5.0))
    off_fallback_strategy = str(fl.get("off_fallback_strategy", "on_background"))
    if tstart - off_gap <= tstart - off_pre:
        raise ValueError(
            "OFF window invalid while building prepared event data: need off_pre > off_gap. "
            f"Got off_pre={off_pre}, off_gap={off_gap}."
        )

    col_time = str(fl.get("col_time", "TimeTags"))
    col_l = str(fl.get("col_l", "Chi galactic"))
    col_b = str(fl.get("col_b", "Psi galactic"))
    col_phi = str(fl.get("col_phi", "Phi"))

    t0 = time.perf_counter()
    # Load source and background events once, then persist the prepared subsets
    # so later unbinned tasks can skip the expensive FITS reads.
    t_grb, l_grb, b_grb, phi_grb = _fl_load_events_simple(
        source_path, col_time, col_l, col_b, col_phi, memmap=True
    )
    t_bkg, l_bkg, b_bkg, phi_bkg = _fl_load_events_simple(
        background_path, col_time, col_l, col_b, col_phi, memmap=True
    )

    on_start, on_stop = tstart, tstop
    off_start = tstart - off_pre
    off_stop = tstart - off_gap
    # ON contains burst-window source events plus background events in the same
    # interval. OFF uses a pre-burst background-only interval by default.
    m_on_grb = (t_grb >= on_start) & (t_grb < on_stop)
    m_on_bkg = (t_bkg >= on_start) & (t_bkg < on_stop)
    m_off_bkg = (t_bkg >= off_start) & (t_bkg < off_stop)

    on_t = np.concatenate([t_grb[m_on_grb], t_bkg[m_on_bkg]])
    on_l = np.concatenate([l_grb[m_on_grb], l_bkg[m_on_bkg]])
    on_b = np.concatenate([b_grb[m_on_grb], b_bkg[m_on_bkg]])
    on_phi = np.concatenate([phi_grb[m_on_grb], phi_bkg[m_on_bkg]])
    off_t = t_bkg[m_off_bkg]
    off_l = l_bkg[m_off_bkg]
    off_b = b_bkg[m_off_bkg]
    off_phi = phi_bkg[m_off_bkg]
    off_strategy_used = "preburst"

    if on_l.size == 0:
        raise RuntimeError("No ON events found while building prepared event data.")
    if off_l.size == 0:
        # Simulated or clipped files may not contain pre-burst background. In
        # that case, optionally fall back to background events during the burst.
        if off_fallback_strategy == "on_background" and np.any(m_on_bkg):
            off_start, off_stop = on_start, on_stop
            off_t = t_bkg[m_on_bkg]
            off_l = l_bkg[m_on_bkg]
            off_b = b_bkg[m_on_bkg]
            off_phi = phi_bkg[m_on_bkg]
            off_strategy_used = "on_background_fallback"
        else:
            raise RuntimeError(
                "No OFF background events found while building prepared event data. "
                f"Requested OFF=[{off_start}, {off_stop}], "
                f"off_fallback_strategy={off_fallback_strategy!r}."
            )

    ton = on_stop - on_start
    toff = off_stop - off_start
    alpha = ton / toff
    event_data_path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(event_data_path, "w") as h5:
        # Store window metadata as attributes and event columns as compressed
        # datasets for fast random access by downstream stages.
        h5.attrs["format"] = "hdf5"
        h5.attrs["contains_background"] = True
        h5.attrs["source_path_original"] = source_path
        h5.attrs["background_path_original"] = background_path
        h5.attrs["on_start"] = on_start
        h5.attrs["on_stop"] = on_stop
        h5.attrs["off_start"] = off_start
        h5.attrs["off_stop"] = off_stop
        h5.attrs["Ton_s"] = ton
        h5.attrs["Toff_s"] = toff
        h5.attrs["alpha"] = alpha
        h5.attrs["off_strategy"] = off_strategy_used

        def _write_event_group(group_name: str, t: np.ndarray, l: np.ndarray, b: np.ndarray, phi: np.ndarray) -> None:
            group = h5.create_group(group_name)
            group.create_dataset("time", data=np.asarray(t, dtype=np.float64), compression="gzip")
            group.create_dataset("l_deg", data=np.asarray(l, dtype=np.float64), compression="gzip")
            group.create_dataset("b_deg", data=np.asarray(b, dtype=np.float64), compression="gzip")
            group.create_dataset("phi_deg", data=np.asarray(phi, dtype=np.float64), compression="gzip")

        _write_event_group("source", t_grb, l_grb, b_grb, phi_grb)
        _write_event_group("background", t_bkg, l_bkg, b_bkg, phi_bkg)
        _write_event_group("aggregated_on", on_t, on_l, on_b, on_phi)
        _write_event_group("background_off", off_t, off_l, off_b, off_phi)

    return {
        "event_data_status": "ok",
        "event_data_path": str(event_data_path),
        "event_format": "hdf5",
        "contains_background": True,
        "source_path_original": source_path,
        "background_path_original": background_path,
        "n_source_events": int(t_grb.size),
        "n_background_events": int(t_bkg.size),
        "n_on_events": int(on_l.size),
        "n_off_events": int(off_l.size),
        "windows": {
            "on_start": on_start,
            "on_stop": on_stop,
            "off_start": off_start,
            "off_stop": off_stop,
            "Ton_s": ton,
            "Toff_s": toff,
            "alpha": alpha,
            "off_strategy": off_strategy_used,
        },
        "timings_s": {"t_event_prepare": float(time.perf_counter() - t0)},
    }


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
    # Build the unbinned prepared product during preprocessing so localization
    # can reuse it even if later tasks are run independently.
    config["prepared_data"] = {
        **dict(config.get("prepared_data", {})),
        **_build_prepared_event_data(config),
    }

    _ensure_structured_pipeline_config(config)
    _record_pipeline_task(
        config,
        task_id="PreProcessing_GeD",
        task_name="PreProcessing GeD",
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
# TASK 2: Unbinned_Light_Curve_Generation
#########################################################
def unbinned_light_curve_generation(config_path: str) -> str:
    """
    Generate an ARM-gated unbinned light curve from event FITS input.
    """
    import os

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

    source_path = config["source_path"]
    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    ulc_cfg = config.get("unbinned_light_curve", {})
    arm_min = float(ulc_cfg.get("arm-min", -15.0))
    arm_max = float(ulc_cfg.get("arm-max", 15.0))
    bin_size = float(ulc_cfg.get("bin", 1.0))
    out_prefix = str(ulc_cfg.get("out-prefix", "lc"))
    arm_hist_bins = int(ulc_cfg.get("arm-hist-bins", 30))

    # Optional parameters (with backward-compatible defaults).
    fmt = str(ulc_cfg.get("format", "png"))
    diagnostics = bool(ulc_cfg.get("diagnostics", True))
    col_time = str(ulc_cfg.get("col-time", "TimeTags"))
    col_l = str(ulc_cfg.get("col-l", "Chi galactic"))
    col_b = str(ulc_cfg.get("col-b", "Psi galactic"))
    col_phi = str(ulc_cfg.get("col-phi", "Phi"))

    # Use explicit localization if present, otherwise fallback to TSMap selected coordinates.
    tsmap_sel = config.get("tsmap", {}).get("selected_coordinates", {})
    l_deg = float(ulc_cfg.get("l", tsmap_sel.get("l_deg", 0.0)))
    b_deg = float(ulc_cfg.get("b", tsmap_sel.get("b_deg", 0.0)))

    if not source_path.lower().endswith((".fits", ".fits.gz", ".fit", ".fit.gz")):
        payload = {
            "status": "skipped",
            "reason": "source_path is not an unbinned FITS file",
            "source_path": source_path,
        }
        config["unbinned_light_curve"] = {**ulc_cfg, **payload}
        config["unbinned_lightcurve_yaml"] = str(products_dir / "unbinned_lightcurve_results.yaml")
        _record_pipeline_task(
            config,
            task_id="Unbinned_Light_Curve_Generation",
            task_name="Unbinned Light Curve Generation",
            input_data={"config_path": config_path, **ulc_cfg},
            output_data=payload,
            status="skipped",
            start_time=task_start,
        )
        fhf._save_yaml(config["unbinned_lightcurve_yaml"], payload)
        fhf._save_yaml(config_path, config)
        return config["unbinned_lightcurve_yaml"]

    from astropy.io import fits
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    with fits.open(source_path, memmap=True) as hdul:
        # Event directions are stored in Galactic coordinates; Phi is converted
        # to degrees to match the ARM calculation below.
        data = hdul[1].data
        t_all = data[col_time].astype(np.float64)
        l_all = data[col_l].astype(np.float64)
        b_all = data[col_b].astype(np.float64)
        phi_deg_all = np.rad2deg(data[col_phi].astype(np.float64))

    tstart = float(ulc_cfg.get("tstart", float(np.min(t_all))))
    tstop = float(ulc_cfg.get("tstop", float(np.max(t_all))))

    m_time = (t_all >= tstart) & (t_all < tstop)
    if not np.any(m_time):
        raise RuntimeError("No events in the requested time window.")

    t = t_all[m_time]
    l_evt = l_all[m_time]
    b_evt = b_all[m_time]
    phi_deg = phi_deg_all[m_time]
    n_total = int(t_all.size)
    n_time = int(t.size)

    # ARM is the difference between the reconstructed event-source separation
    # and the measured Compton scatter angle. The gate selects likely events.
    src = SkyCoord(l=l_deg * u.deg, b=b_deg * u.deg, frame="galactic")
    reco = SkyCoord(l=np.asarray(l_evt) * u.deg, b=np.asarray(b_evt) * u.deg, frame="galactic")
    arm = reco.separation(src).deg - np.asarray(phi_deg)
    m_arm = (arm >= arm_min) & (arm <= arm_max)
    t_g = t[m_arm]
    n_gated = int(t_g.size)

    # Histogram both all events and ARM-gated events for diagnostics.
    edges = np.arange(tstart, tstop + bin_size, bin_size, dtype=np.float64)
    centers = 0.5 * (edges[:-1] + edges[1:])
    counts_all, _ = np.histogram(t, bins=edges)
    counts_gated, _ = np.histogram(t_g, bins=edges)

    out_csv = os.path.join(products_dir, f"{out_prefix}_lc.csv")
    np.savetxt(
        out_csv,
        np.column_stack([centers, counts_gated]),
        delimiter=",",
        header="t_center_s,counts",
        comments="",
    )

    trigger_time = _config_trigger_time(config, "GeD")
    lc_caption = (
        "COSI GeD ARM-gated light curve. The black step histogram shows the "
        f"number of events per {bin_size:.3f} s bin after selecting events with "
        f"ARM between {arm_min:.1f} and {arm_max:.1f} deg around the Galactic "
        f"position (l, b)=({l_deg:.3f}, {b_deg:.3f}) deg."
    )
    lc_title, lc_metadata = _plot_identity(
        "GeD",
        lc_caption,
        trigger_time,
        "ARM-gated light curve",
        preview_title="GeD Light Curve",
    )
    out_lc = os.path.join(plots_dir, f"{out_prefix}_lc.{fmt}")
    fig_lc, ax_lc = plt.subplots(figsize=tuple(ulc_cfg.get("plot_figsize", [8, 4])))
    ax_lc.step(centers, counts_gated, where="mid", color="black")
    ax_lc.set_xlabel("Time [s]")
    ax_lc.set_ylabel(f"Counts / {bin_size:.3f} s")
    ax_lc.set_title(lc_title)
    ax_lc.grid(True, ls="--", alpha=0.5)
    plot_dpi = int(ulc_cfg.get("plot_dpi", 200))
    save_metadata = lc_metadata if fmt.lower() == "png" else None
    fig_lc.savefig(
        out_lc,
        dpi=plot_dpi,
        bbox_inches="tight",
        metadata=save_metadata,
    )
    plt.close(fig_lc)

    out_diag = None
    if diagnostics:
        out_diag = os.path.join(plots_dir, f"{out_prefix}_diagnostics.{fmt}")
        fig, ax = plt.subplots(
            2,
            2,
            figsize=tuple(ulc_cfg.get("diagnostics_figsize", [14, 10])),
            constrained_layout=True,
        )
        ax[0, 0].step(centers, counts_all, where="mid", color="black")
        ax[0, 0].set_title("Light curve (all events, time-windowed)")
        ax[0, 0].set_xlabel("Time [s]")
        ax[0, 0].set_ylabel(f"Counts / {bin_size:.3f} s")
        ax[0, 0].grid(True, ls="--", alpha=0.4)

        ax[0, 1].step(centers, counts_gated, where="mid", color="tab:blue")
        ax[0, 1].set_title(f"Light curve (ARM-gated [{arm_min:.1f},{arm_max:.1f}] deg)")
        ax[0, 1].set_xlabel("Time [s]")
        ax[0, 1].set_ylabel(f"Counts / {bin_size:.3f} s")
        ax[0, 1].grid(True, ls="--", alpha=0.4)

        ax[1, 0].hist(arm, bins=arm_hist_bins, color="tab:orange", edgecolor="black", alpha=0.7)
        ax[1, 0].axvline(arm_min, color="red", ls="--")
        ax[1, 0].axvline(arm_max, color="red", ls="--")
        ax[1, 0].set_title("ARM distribution (time-windowed events)")
        ax[1, 0].set_xlabel("ARM [deg]")
        ax[1, 0].set_ylabel("Counts")
        ax[1, 0].grid(True, ls="--", alpha=0.4)

        ax[1, 1].axis("off")
        frac_time = (n_time / n_total) if n_total > 0 else 0.0
        frac_gated = (n_gated / n_time) if n_time > 0 else 0.0
        text = (
            "Inputs\n"
            f"  l,b (deg): {l_deg:.6f}, {b_deg:.6f}\n"
            f"  Time window (s): [{tstart:.3f}, {tstop:.3f}]\n"
            f"  Bin size (s): {bin_size:.3f}\n"
            f"  ARM gate (deg): [{arm_min:.1f}, {arm_max:.1f}]\n\n"
            "Event statistics\n"
            f"  Total events in file: {n_total}\n"
            f"  In time window: {n_time}  (fraction {frac_time:.3f})\n"
            f"  ARM-gated: {n_gated}  (fraction {frac_gated:.3f})\n"
        )
        ax[1, 1].text(0.02, 0.98, text, va="top", ha="left", fontsize=10, family="monospace")
        diagnostics_caption = (
            "COSI GeD event-selection diagnostics. The upper-left black step "
            "histogram is the light curve for all events in the selected time "
            "window; the upper-right blue histogram contains only ARM-gated "
            "events. The lower-left orange histogram is the ARM distribution, "
            "with red dashed lines marking the accepted interval. The lower-right "
            "panel reports the selection parameters and event totals."
        )
        diagnostics_title, diagnostics_metadata = _plot_identity(
            "GeD",
            diagnostics_caption,
            trigger_time,
            f"Event-selection diagnostics ({out_prefix})",
            preview_title="GeD Light Curve",
        )
        fig.suptitle(diagnostics_title, fontsize=14)
        fig.savefig(
            out_diag,
            dpi=plot_dpi,
            bbox_inches="tight",
            metadata=diagnostics_metadata if fmt.lower() == "png" else None,
        )
        plt.close(fig)

    payload = {
        "status": "ok",
        "source_path": source_path,
        "time_window": {"tstart": tstart, "tstop": tstop},
        "localization": {"l_deg": l_deg, "b_deg": b_deg},
        "arm_gate": {"min_deg": arm_min, "max_deg": arm_max},
        "bin_size": bin_size,
        "counts": counts_gated.astype(float).tolist(),
        "counts_all": counts_all.astype(float).tolist(),
        "time_centers": centers.astype(float).tolist(),
        "csv_path": out_csv,
        "plot_path": out_lc,
        "diagnostics_path": out_diag,
        "n_total": n_total,
        "n_time_window": n_time,
        "n_gated": n_gated,
    }

    config["unbinned_light_curve"] = {**ulc_cfg, **payload}
    config["unbinned_lightcurve_yaml"] = str(products_dir / "unbinned_lightcurve_results.yaml")
    _record_pipeline_task(
        config,
        task_id="Unbinned_Light_Curve_Generation",
        task_name="Unbinned Light Curve Generation",
        input_data={"config_path": config_path, **ulc_cfg},
        output_data=payload,
        start_time=task_start,
    )
    fhf._save_yaml(config["unbinned_lightcurve_yaml"], payload)
    fhf._save_yaml(config_path, config)
    return config["unbinned_lightcurve_yaml"]


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


# =============================================================================
# Fast GRB localization (Li & Ma HEALPix map) - helpers (notebook logic)
# =============================================================================
def _fl_load_events_simple(
    fits_path: str,
    col_time: str = "TimeTags",
    col_l: str = "Chi galactic",
    col_b: str = "Psi galactic",
    col_phi: str = "Phi",
    memmap: bool = True) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    import numpy as np
    from astropy.io import fits

    with fits.open(fits_path, memmap=memmap) as hdul:
        data = hdul[1].data
        t = np.asarray(data[col_time], dtype=np.float64)
        lon = np.asarray(data[col_l], dtype=np.float64)
        lat = np.asarray(data[col_b], dtype=np.float64)
        phi_deg = np.degrees(np.asarray(data[col_phi], dtype=np.float64))
    return t, lon, lat, phi_deg


def _fl_lb_to_unitvec(l_deg: np.ndarray, b_deg: np.ndarray) -> np.ndarray:
    import numpy as np

    # Convert Galactic longitude/latitude into Cartesian unit vectors. This
    # makes angular separations fast via dot products.
    ll = np.radians(np.asarray(l_deg, dtype=np.float64))
    bb = np.radians(np.asarray(b_deg, dtype=np.float64))
    cb = np.cos(bb)
    x = cb * np.cos(ll)
    y = cb * np.sin(ll)
    z = np.sin(bb)
    return np.vstack([x, y, z]).T


def _fl_healpix_pixel_area_sr(nside: int) -> float:
    import numpy as np

    return float(4.0 * np.pi / (12.0 * nside**2))


def _fl_healpix_mean_spacing_deg(nside: int) -> float:
    import numpy as np

    a_pix_sr = _fl_healpix_pixel_area_sr(nside)
    a_pix_deg2 = a_pix_sr * (180.0 / np.pi) ** 2
    return float(np.sqrt(a_pix_deg2))


def _fl_healpix_res_from_nside(nside: int) -> int:
    import numpy as np

    return int(np.round(np.log2(int(nside))))


def _fl_wrap_lon_deg(l_deg: float) -> float:
    return ((l_deg + 180.0) % 360.0) - 180.0


def _fl_angsep_deg(l1: float, b1: float, l2: float, b2: float) -> float:
    import numpy as np

    v1 = _fl_lb_to_unitvec(np.array([l1]), np.array([b1]))[0]
    v2 = _fl_lb_to_unitvec(np.array([l2]), np.array([b2]))[0]
    c = np.clip(np.dot(v1, v2), -1.0, 1.0)
    return float(np.degrees(np.arccos(c)))


def _fl_li_ma(Non: np.ndarray, Noff: np.ndarray, alpha: float) -> np.ndarray:
    import numpy as np

    # Li & Ma significance for ON/OFF counting statistics. Empty or invalid
    # bins remain at zero to avoid log/division edge cases.
    Non = np.asarray(Non, dtype=np.float64)
    Noff = np.asarray(Noff, dtype=np.float64)
    alpha = float(alpha)
    s = np.zeros_like(Non, dtype=np.float64)
    m = (Non > 0) & (Noff > 0) & (alpha > 0)
    if not np.any(m):
        return s
    non_m = Non[m]
    noff_m = Noff[m]
    tot = non_m + noff_m
    term1 = non_m * np.log(((1 + alpha) / alpha) * (non_m / tot))
    term2 = noff_m * np.log((1 + alpha) * (noff_m / tot))
    s2 = 2.0 * (term1 + term2)
    s[m] = np.sqrt(np.maximum(s2, 0.0))
    return s


def _fl_count_arm_pass_for_pixels_chunked(
    event_cvec: np.ndarray,
    event_phi_deg: np.ndarray,
    svecs: np.ndarray,
    arm_min: float,
    arm_max: float,
    event_chunk: int = 200_000) -> np.ndarray:
    import numpy as np

    n = event_cvec.shape[0]
    m = svecs.shape[0]
    counts = np.zeros(m, dtype=np.int64)
    for j0 in range(0, n, event_chunk):
        # Work in event chunks to limit the temporary event-by-pixel matrix.
        j1 = min(j0 + event_chunk, n)
        c = event_cvec[j0:j1]
        phi = event_phi_deg[j0:j1]
        # Dot products give cos(theta) between each event direction and trial
        # sky pixel; the ARM cut is applied for every trial pixel.
        dots = c @ svecs.T
        np.clip(dots, -1.0, 1.0, out=dots)
        alpha_deg = np.degrees(np.arccos(dots))
        arm = alpha_deg - phi[:, None]
        mask = (arm >= arm_min) & (arm <= arm_max)
        counts += np.count_nonzero(mask, axis=0).astype(np.int64)
        del dots, alpha_deg, arm, mask
    return counts


def _fl_build_significance_map(
    nside: int,
    on_cvec: np.ndarray,
    on_phi_deg: np.ndarray,
    off_cvec: np.ndarray,
    off_phi_deg: np.ndarray,
    arm_min: float,
    arm_max: float,
    alpha: float,
    pix_chunk: int = 256,
    event_chunk: int = 200_000) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    import healpy as hp
    import numpy as np

    npix = hp.nside2npix(nside)
    sig = np.zeros(npix, dtype=np.float64)
    non = np.zeros(npix, dtype=np.int64)
    noff = np.zeros(npix, dtype=np.int64)
    svec_all = np.array(hp.pix2vec(nside, np.arange(npix))).T
    for i0 in range(0, npix, pix_chunk):
        # Pixel chunks keep memory bounded while still vectorizing over events.
        i1 = min(i0 + pix_chunk, npix)
        svecs = svec_all[i0:i1]
        non_chunk = _fl_count_arm_pass_for_pixels_chunked(
            on_cvec, on_phi_deg, svecs, arm_min, arm_max, event_chunk=event_chunk
        )
        noff_chunk = _fl_count_arm_pass_for_pixels_chunked(
            off_cvec, off_phi_deg, svecs, arm_min, arm_max, event_chunk=event_chunk
        )
        non[i0:i1] = non_chunk
        noff[i0:i1] = noff_chunk
        sig[i0:i1] = _fl_li_ma(non_chunk, noff_chunk, alpha)
    return sig, non, noff


def _fl_parse_nsides(nsides_val: Any, default_nside: int) -> list[int]:
    if nsides_val is None or nsides_val == "":
        return [int(default_nside)]
    if isinstance(nsides_val, (list, tuple)):
        return [int(x) for x in nsides_val]
    return [int(x.strip()) for x in str(nsides_val).split(",") if str(x).strip()]


def _fl_save_localization_csv(out_path: str, nside: int, sig_map: np.ndarray, topk: int = 50) -> str:
    import healpy as hp
    import numpy as np

    # Keep only the highest-significance pixels for compact downstream reports.
    idx = np.argsort(sig_map)[::-1][:topk]
    rows = []
    for ipix in idx:
        theta, phi = hp.pix2ang(nside, int(ipix))
        ll = float(np.degrees(phi))
        bb = float(90.0 - np.degrees(theta))
        rows.append((int(ipix), ll, bb, float(sig_map[ipix])))
    header = "ipix,l_deg,b_deg,S_li_ma"
    np.savetxt(out_path, np.array(rows, dtype=float), delimiter=",", header=header, comments="")
    return out_path


def _fl_save_nside_summary_csv(out_path: str, rows: list[list[float]]) -> str:
    import numpy as np

    header = (
        "Res,NSIDE,Npix,Mean_spacing_deg,Area_sr,Best_l_deg,Best_b_deg,Best_S_sigma,"
        "Offset_deg,Map_time_s"
    )
    arr = np.array(rows, dtype=float)
    np.savetxt(out_path, arr, delimiter=",", header=header, comments="")
    return out_path


def _fl_save_timing_csv(out_path: str, nside: int, t_io: float, t_prep: float, t_map: float, t_total: float) -> str:
    import numpy as np

    header = "nside,t_io_s,t_prep_s,t_map_s,t_total_s"
    arr = np.array([[float(nside), float(t_io), float(t_prep), float(t_map), float(t_total)]], dtype=float)
    np.savetxt(out_path, arr, delimiter=",", header=header, comments="")
    return out_path


def _fl_save_pretty_significance_map(
    out_path: str,
    nside: int,
    sig_map: np.ndarray,
    on_start: float,
    on_stop: float,
    off_start: float,
    off_stop: float,
    alpha: float,
    arm_min: float,
    arm_max: float,
    best_l: float,
    best_b: float,
    best_s: float,
    true_l: float | None = None,
    true_b: float | None = None,
    trigger_time: Any = None,
    figsize: list[float] | tuple[float, float] = (10, 6),
    dpi: int = 220) -> str:
    import healpy as hp
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.lines import Line2D

    mean_spacing = _fl_healpix_mean_spacing_deg(nside)
    offset = None
    if true_l is not None and true_b is not None:
        offset = _fl_angsep_deg(true_l, true_b, best_l, best_b)
    details = (
        f"ARM-gated HEALPix significance map (NSIDE={nside}, mean spacing~{mean_spacing:.2f} deg)\n"
        f"ON=[{on_start:.3f},{on_stop:.3f}]  OFF=[{off_start:.3f},{off_stop:.3f}]  "
        f"alpha={alpha:.3f}  ARM=[{arm_min:.1f},{arm_max:.1f}] deg\n"
        f"Max pixel significance: {best_s:.2f} sigma"
        + (f" | Offset = {offset:.2f} deg" if offset is not None else "")
    )
    caption = (
        "COSI GeD all-sky localization significance map in Galactic "
        "coordinates using a Mollweide projection. Pixel colors follow the "
        "inferno scale and encode Li & Ma significance in Gaussian sigma. "
        "The cyan star marks the reconstructed transient position"
        + (
            ", while the magenta star marks the supplied reference position."
            if true_l is not None and true_b is not None
            else "."
        )
    )
    title, metadata = _plot_identity(
        "GeD",
        caption,
        trigger_time,
        details,
        preview_title="GeD TS Map",
    )
    plt.figure(figsize=tuple(figsize), dpi=int(dpi))
    hp.mollview(sig_map, title=title, unit="sigma", cmap="inferno")
    hp.graticule(color="white", alpha=0.35)
    best_lw = _fl_wrap_lon_deg(best_l)
    hp.projplot(best_lw, best_b, lonlat=True, marker="*", markersize=20, color="cyan", mec="black", mew=1.5)
    legend_handles = [
        Line2D(
            [],
            [],
            marker="*",
            linestyle="None",
            markersize=12,
            color="cyan",
            mec="black",
            label="Reconstructed GRB",
        ),
    ]
    if true_l is not None and true_b is not None:
        true_lw = _fl_wrap_lon_deg(true_l)
        hp.projplot(
            true_lw,
            true_b,
            lonlat=True,
            marker="*",
            markersize=20,
            color="magenta",
            mec="black",
            mew=1.5,
        )
        legend_handles.insert(
            0,
            Line2D(
                [],
                [],
                marker="*",
                linestyle="None",
                markersize=12,
                color="magenta",
                mec="black",
                label="True GRB (reference)",
            ),
        )
        info = (
            f"True:  (l,b)=({true_lw:.2f} deg, {true_b:.2f} deg)\n"
            f"Reco: (l,b)=({best_lw:.2f} deg, {best_b:.2f} deg)\n"
            f"S_max = {best_s:.2f} sigma\n"
            f"Offset = {offset:.2f} deg"
        )
    else:
        info = f"Reco: (l,b)=({best_lw:.2f} deg, {best_b:.2f} deg)\n" f"S_max = {best_s:.2f} sigma"
    ax = plt.gca()
    ax.set_xlabel("Galactic longitude l [deg]")
    ax.set_ylabel("Galactic latitude b [deg]")
    ax.legend(handles=legend_handles, loc="lower left", bbox_to_anchor=(0.02, -0.05), framealpha=0.92)
    ax.text(
        0.02,
        0.98,
        info,
        transform=ax.transAxes,
        fontsize=9,
        color="white",
        ha="left",
        va="top",
        bbox=dict(facecolor="black", alpha=0.60, boxstyle="round,pad=0.35"),
    )
    plt.savefig(out_path, bbox_inches="tight", metadata=metadata)
    plt.close()
    return out_path


def _fl_save_nside_table_png(
    out_path: str,
    rows: list[list[float]],
    trigger_time: Any = None,
    figsize_width: float = 14,
    figsize_base_height: float = 2.4,
    figsize_row_height: float = 0.38,
    dpi: int = 240,
) -> str:
    import matplotlib.pyplot as plt
    import numpy as np

    col_labels = [
        "Res",
        "NSIDE",
        "NPixels",
        "Mean Spacing (deg)",
        "Area (sterad)",
        "Best l (deg)",
        "Best b (deg)",
        "Smax (sigma)",
        "Offset (deg)",
        "Map time (s)",
    ]
    disp = []
    for r in rows:
        res, ns, npix, ms, area_sr, bl, bb, bs, off, tm = r
        disp.append(
            [
                f"{int(res)}",
                f"{int(ns)}",
                f"{int(npix)}",
                f"{ms:.4f}",
                f"{area_sr:.7e}",
                f"{bl:.2f}",
                f"{bb:.2f}",
                f"{bs:.2f}",
                f"{off:.2f}" if np.isfinite(off) else "-",
                f"{tm:.2f}",
            ]
        )
    fig, ax = plt.subplots(
        figsize=(figsize_width, figsize_base_height + figsize_row_height * max(len(disp), 1)),
        dpi=dpi,
    )
    ax.axis("off")
    tbl = ax.table(cellText=disp, colLabels=col_labels, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.25)
    for (row, col), cell in tbl.get_celld().items():
        cell.set_linewidth(1.0)
        cell.set_edgecolor("black")
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#f0f0f0")
    caption = (
        "COSI GeD tabular summary of the HEALPix localization runs. Each row "
        "reports one map resolution and lists its NSIDE, pixel count, angular "
        "spacing, solid angle, best-fit Galactic coordinates, peak significance, "
        "reference offset when available, and map computation time."
    )
    title, metadata = _plot_identity(
        "GeD",
        caption,
        trigger_time,
        "HEALPix localization run summary",
        preview_title="GeD TS Map",
    )
    ax.set_title(title, fontsize=14, weight="bold", pad=14)
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight", metadata=metadata)
    plt.close(fig)
    return out_path


#########################################################
# TASK 4: Light_Curve_Analysis
# ON/OFF temporal windows, alpha = Ton/Toff, ARM-gated event prep for Li & Ma map
#########################################################
def light_curve_analysis(config_path: str) -> str:
    """
    Load GRB and background FITS, define ON ([tmin,tmax]) and OFF (pre-burst background) windows,
    compute alpha = Ton/Toff, and persist ARM-gated event cone vectors for the skymap step.

    Returns the path to ``fast_localize_prep.yaml`` under products.
    """
    from pathlib import Path
    import time
    import warnings

    import numpy as np
    from astropy.utils.exceptions import AstropyWarning

    import fast_helper_functions as fhf

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    fl = dict(config.get("fast_localize", {}))
    source_path = str(config["source_path"])
    background_path = str(config["background_path"])
    data_dir = config["data_dir"]
    _, products_dir = _ensure_pipeline_dirs(data_dir)
    products_dir = Path(products_dir)

    out_prefix = str(fl.get("out_prefix", "grb"))
    prep_yaml = products_dir / f"{out_prefix}_fast_localize_prep.yaml"
    prep_npz = products_dir / f"{out_prefix}_fast_localize_prep.npz"

    if not source_path.lower().endswith((".fits", ".fits.gz", ".fit", ".fit.gz")):
        payload = {
            "status": "skipped",
            "reason": "source_path is not an unbinned FITS file",
            "source_path": source_path,
        }
        config["fast_localize"] = {**fl, **payload}
        _record_pipeline_task(
            config,
            task_id="Light_Curve_Analysis",
            task_name="Light Curve Analysis",
            input_data={"config_path": config_path, **fl},
            output_data=payload,
            status="skipped",
            start_time=task_start,
        )
        fhf._save_yaml(str(prep_yaml), payload)
        fhf._save_yaml(config_path, config)
        return str(prep_yaml)

    if bool(fl.get("suppress_mmap_warning", True)):
        warnings.filterwarnings(
            "ignore", message="Could not memory map array", category=AstropyWarning
        )

    tstart = float(config["tstart"])
    tstop = float(config["tstop"])
    off_pre = float(fl.get("off_pre", 20.0))
    off_gap = float(fl.get("off_gap", 5.0))
    off_fallback_strategy = str(fl.get("off_fallback_strategy", "on_background"))
    arm_min = float(fl.get("arm_min", -13.0))
    arm_max = float(fl.get("arm_max", 13.0))

    on_start, on_stop = tstart, tstop
    off_start = tstart - off_pre
    off_stop = tstart - off_gap
    if off_stop <= off_start:
        raise ValueError(
            "OFF window invalid: need off_pre > off_gap. "
            f"Got off_pre={off_pre}, off_gap={off_gap}."
        )
    ton = on_stop - on_start

    col_time = str(fl.get("col_time", "TimeTags"))
    col_l = str(fl.get("col_l", "Chi galactic"))
    col_b = str(fl.get("col_b", "Psi galactic"))
    col_phi = str(fl.get("col_phi", "Phi"))

    t0_io = time.perf_counter()
    t0_prep = time.perf_counter()
    prepared_event_path = dict(config.get("prepared_data", {})).get("event_data_path")
    if prepared_event_path and Path(prepared_event_path).is_file():
        # Prefer the preprocessed HDF5 product when available. It preserves the
        # exact ON/OFF windows used during preprocessing.
        import h5py

        with h5py.File(prepared_event_path, "r") as h5:
            on_l = np.asarray(h5["aggregated_on/l_deg"], dtype=np.float64)
            on_b = np.asarray(h5["aggregated_on/b_deg"], dtype=np.float64)
            on_phi = np.asarray(h5["aggregated_on/phi_deg"], dtype=np.float64)
            off_l = np.asarray(h5["background_off/l_deg"], dtype=np.float64)
            off_bb = np.asarray(h5["background_off/b_deg"], dtype=np.float64)
            off_phi = np.asarray(h5["background_off/phi_deg"], dtype=np.float64)
            on_start = float(h5.attrs.get("on_start", on_start))
            on_stop = float(h5.attrs.get("on_stop", on_stop))
            off_start = float(h5.attrs.get("off_start", off_start))
            off_stop = float(h5.attrs.get("off_stop", off_stop))
            ton = float(h5.attrs.get("Ton_s", on_stop - on_start))
            toff = float(h5.attrs.get("Toff_s", off_stop - off_start))
            alpha = float(h5.attrs.get("alpha", ton / toff))
            off_strategy_used = str(h5.attrs.get("off_strategy", "preburst"))
        t_io = time.perf_counter() - t0_io
    else:
        # Fallback path for legacy configs that do not have prepared event data.
        t_grb, l_grb, b_grb, phi_grb = _fl_load_events_simple(
            source_path, col_time, col_l, col_b, col_phi, memmap=True
        )
        t_bkg, l_bkg, b_bkg, phi_bkg = _fl_load_events_simple(
            background_path, col_time, col_l, col_b, col_phi, memmap=True
        )
        t_io = time.perf_counter() - t0_io

        # Build ON from source+background in the burst window and OFF from
        # background-only events before the burst.
        m_on_grb = (t_grb >= on_start) & (t_grb < on_stop)
        m_on_bkg = (t_bkg >= on_start) & (t_bkg < on_stop)
        on_l = np.concatenate([l_grb[m_on_grb], l_bkg[m_on_bkg]])
        on_b = np.concatenate([b_grb[m_on_grb], b_bkg[m_on_bkg]])
        on_phi = np.concatenate([phi_grb[m_on_grb], phi_bkg[m_on_bkg]])

        m_off_bkg = (t_bkg >= off_start) & (t_bkg < off_stop)
        off_l = l_bkg[m_off_bkg]
        off_bb = b_bkg[m_off_bkg]
        off_phi = phi_bkg[m_off_bkg]
        off_strategy_used = "preburst"

        if off_l.size == 0:
            if off_fallback_strategy == "on_background" and np.any(m_on_bkg):
                off_start, off_stop = on_start, on_stop
                off_l = l_bkg[m_on_bkg]
                off_bb = b_bkg[m_on_bkg]
                off_phi = phi_bkg[m_on_bkg]
                off_strategy_used = "on_background_fallback"
                print(
                    "[light_curve_analysis] No pre-burst OFF background events found. "
                    "Using background events in the ON window as OFF sample "
                    "(off_fallback_strategy='on_background')."
                )
            else:
                bkg_min = float(np.min(t_bkg)) if t_bkg.size else None
                bkg_max = float(np.max(t_bkg)) if t_bkg.size else None
                raise RuntimeError(
                    "No OFF background events found in [tmin-off_pre, tmin-off_gap]. "
                    f"Requested OFF=[{off_start}, {off_stop}], "
                    f"background time range=[{bkg_min}, {bkg_max}], "
                    f"off_fallback_strategy={off_fallback_strategy!r}."
                )

        toff = off_stop - off_start
        alpha = ton / toff

    if on_l.size == 0:
        raise RuntimeError("No ON events found in [tmin,tmax].")
    if off_l.size == 0:
        raise RuntimeError("No OFF events found for fast localization.")

    # Store unit vectors instead of coordinates; skymap construction then only
    # needs matrix products and avoids repeated coordinate conversion.
    on_cvec = _fl_lb_to_unitvec(on_l, on_b)
    off_cvec = _fl_lb_to_unitvec(off_l, off_bb)
    on_phi_deg = np.asarray(on_phi, dtype=np.float64)
    off_phi_deg = np.asarray(off_phi, dtype=np.float64)
    t_prep = time.perf_counter() - t0_prep

    np.savez_compressed(
        prep_npz,
        on_cvec=on_cvec,
        off_cvec=off_cvec,
        on_phi_deg=on_phi_deg,
        off_phi_deg=off_phi_deg,
    )

    wall_t0 = time.time()
    payload = {
        "status": "ok",
        "prep_npz": str(prep_npz),
        "windows": {
            "on_start": on_start,
            "on_stop": on_stop,
            "off_start": off_start,
            "off_stop": off_stop,
            "Ton_s": ton,
            "Toff_s": toff,
            "alpha": alpha,
            "off_strategy": off_strategy_used,
        },
        "arm_gate_deg": {"min": arm_min, "max": arm_max},
        "n_on_events": int(on_l.size),
        "n_off_events": int(off_l.size),
        "timings_s": {"t_io": float(t_io), "t_prep": float(t_prep)},
        "wall_t0_unix": float(wall_t0),
        "source_path": source_path,
        "background_path": background_path,
        "prepared_event_data_path": prepared_event_path,
    }
    config["fast_localize"] = {**fl, **payload}
    config["fast_localize_prep_yaml"] = str(prep_yaml)
    _record_pipeline_task(
        config,
        task_id="Light_Curve_Analysis",
        task_name="Light Curve Analysis",
        input_data={"config_path": config_path, **fl},
        output_data=payload,
        start_time=task_start,
    )
    fhf._save_yaml(str(prep_yaml), payload)
    fhf._save_yaml(config_path, config)
    return str(prep_yaml)


#########################################################
# TASK 4.1: Skymap_unbinned
#########################################################
def skymap_unbinned(config_path: str) -> str:
    """
    Build the ARM-gated HEALPix Li & Ma significance map(s) from prep vectors.

    Returns the path to ``fast_localize_skymap.yaml`` under products.
    """
    from pathlib import Path
    import time

    import healpy as hp
    import numpy as np

    try:
        from . import fast_helper_functions as fhf
    except ImportError:
        import fast_helper_functions as fhf
    from gcn.query import query_relevant_grb_notices

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    fl = dict(config.get("fast_localize", {}))
    data_dir = config["data_dir"]
    _, products_dir = _ensure_pipeline_dirs(data_dir)
    products_dir = Path(products_dir)

    if fl.get("status") == "skipped":
        out = products_dir / f"{fl.get('out_prefix', 'grb')}_fast_localize_skymap.yaml"
        gcn_query_result = query_relevant_grb_notices(
            config=config,
            task_id="Skymap_unbinned",
            tstart=config.get("tstart"),
            tstop=config.get("tstop"),
        )
        p = {
            "status": "skipped",
            "reason": "fast_localize prep was skipped",
            "gcn_notice_query": gcn_query_result,
        }
        config["fast_localize_skymap_yaml"] = str(out)
        config.setdefault("gcn_notice_queries", {})["Skymap_unbinned"] = gcn_query_result
        _record_pipeline_task(
            config,
            task_id="Skymap_unbinned",
            task_name="Skymap unbinned",
            input_data={"config_path": config_path, **fl},
            output_data=p,
            status="skipped",
            start_time=task_start,
        )
        fhf._save_yaml(str(out), p)
        fhf._save_yaml(config_path, config)
        return str(out)
    out_prefix = str(fl.get("out_prefix", "grb"))
    skymap_yaml = products_dir / f"{out_prefix}_fast_localize_skymap.yaml"

    prep_npz = fl.get("prep_npz")
    if not prep_npz:
        # Recover the prep file path from either the stage YAML or the standard
        # output filename so this task can be rerun from a partially updated config.
        prep_yaml = config.get("fast_localize_prep_yaml")
        if prep_yaml and Path(prep_yaml).is_file():
            prep_payload = fhf._load_yaml(str(prep_yaml))
            prep_npz = prep_payload.get("prep_npz")
            if prep_npz:
                fl["prep_npz"] = prep_npz

    if not prep_npz:
        candidate_npz = products_dir / f"{out_prefix}_fast_localize_prep.npz"
        if candidate_npz.is_file():
            prep_npz = str(candidate_npz)
            fl["prep_npz"] = prep_npz

    if not prep_npz or not Path(prep_npz).is_file():
        raise FileNotFoundError(
            "Missing fast_localize prep .npz. Run light_curve_analysis first. "
            f"Looked for config fast_localize.prep_npz and {products_dir / f'{out_prefix}_fast_localize_prep.npz'}."
        )

    z = np.load(prep_npz, allow_pickle=False)
    on_cvec = z["on_cvec"]
    off_cvec = z["off_cvec"]
    on_phi_deg = z["on_phi_deg"]
    off_phi_deg = z["off_phi_deg"]

    win = fl.get("windows", {})
    alpha = float(win.get("alpha", 1.0))
    arm_min = float(fl.get("arm_min", -13.0))
    arm_max = float(fl.get("arm_max", 13.0))
    pix_chunk = int(fl.get("pix_chunk", 256))
    event_chunk = int(fl.get("event_chunk", 200_000))
    main_nside = int(fl.get("nside", 32))
    nsides = _fl_parse_nsides(fl.get("nsides"), main_nside)

    summary_rows: list[list[float]] = []
    per_nside: list[dict[str, Any]] = []
    best_for_main: tuple[int, np.ndarray, float, float, float] | None = None

    for nside in nsides:
        # Each NSIDE produces a full Li & Ma map plus a compact metadata row.
        t0_map = time.perf_counter()
        sig_map, non_map, noff_map = _fl_build_significance_map(
            nside=nside,
            on_cvec=on_cvec,
            on_phi_deg=on_phi_deg,
            off_cvec=off_cvec,
            off_phi_deg=off_phi_deg,
            arm_min=arm_min,
            arm_max=arm_max,
            alpha=alpha,
            pix_chunk=pix_chunk,
            event_chunk=event_chunk,
        )
        t_map = time.perf_counter() - t0_map

        ip_best = int(np.argmax(sig_map))
        th, ph = hp.pix2ang(nside, ip_best)
        best_l = float(np.degrees(ph))
        best_b = float(90.0 - np.degrees(th))
        best_s = float(sig_map[ip_best])

        mean_spacing = _fl_healpix_mean_spacing_deg(nside)
        area_sr = _fl_healpix_pixel_area_sr(nside)
        res = _fl_healpix_res_from_nside(nside)
        true_l = fl.get("true_l_deg")
        true_b = fl.get("true_b_deg")
        offset = np.nan
        if true_l is not None and true_b is not None:
            offset = _fl_angsep_deg(float(true_l), float(true_b), best_l, best_b)

        summary_rows.append(
            [
                float(res),
                float(nside),
                float(hp.nside2npix(nside)),
                float(mean_spacing),
                float(area_sr),
                float(best_l),
                float(best_b),
                float(best_s),
                float(offset),
                float(t_map),
            ]
        )

        # Persist the full map arrays separately from YAML to keep metadata files small.
        sig_npz = products_dir / f"{out_prefix}_sigmap_nside{nside}.npz"
        np.savez_compressed(sig_npz, sig=sig_map, non=non_map, noff=noff_map, nside=nside)
        per_nside.append(
            {
                "nside": nside,
                "sigmap_npz": str(sig_npz),
                "best_ipix": ip_best,
                "best_l_deg": best_l,
                "best_b_deg": best_b,
                "best_S_sigma": best_s,
                "map_time_s": float(t_map),
            }
        )

        if (len(nsides) == 1) or (nside == main_nside):
            best_for_main = (nside, np.array(sig_map), best_l, best_b, best_s)

    if best_for_main is None:
        raise RuntimeError("Internal error: no significance map stored for main NSIDE.")

    main_nside_r, _, _, _, _ = best_for_main
    t_map_main = next((r[-1] for r in summary_rows if int(r[1]) == int(main_nside)), summary_rows[0][-1])

    payload = {
        "status": "ok",
        "main_nside": int(main_nside_r),
        "nsides_run": nsides,
        "per_nside": per_nside,
        "summary_rows": summary_rows,
        "t_map_main_s": float(t_map_main),
    }
    gcn_query_result = query_relevant_grb_notices(
        config=config,
        task_id="Skymap_unbinned",
        tstart=config.get("tstart"),
        tstop=config.get("tstop"),
    )
    payload["gcn_notice_query"] = gcn_query_result
    config["fast_localize"] = {**fl, **payload}
    config["fast_localize_skymap_yaml"] = str(skymap_yaml)
    config.setdefault("gcn_notice_queries", {})["Skymap_unbinned"] = gcn_query_result
    _record_pipeline_task(
        config,
        task_id="Skymap_unbinned",
        task_name="Skymap unbinned",
        input_data={"config_path": config_path, **fl},
        output_data=payload,
        start_time=task_start,
    )
    fhf._save_yaml(str(skymap_yaml), payload)
    fhf._save_yaml(config_path, config)
    return str(skymap_yaml)


#########################################################
# TASK 5: Duration_and_Localization_Results
#########################################################
def duration_and_localization_results(config_path: str) -> str:
    """
    Read the significance map for the configured NSIDE, write top-K CSV, timing CSV,
    Mollweide PNG, optional NSIDE sweep summary, update ``unbinned_light_curve`` with
    the reconstructed (l, b), and optionally run the external time-series script.

    Returns the path to ``fast_localize_results.yaml`` under products.
    """
    from pathlib import Path
    import subprocess
    import time

    import healpy as hp
    import numpy as np

    try:
        from . import fast_helper_functions as fhf
    except ImportError:
        import fast_helper_functions as fhf
    from gcn.query import query_relevant_grb_notices

    task_start = _now_iso()
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    fl = dict(config.get("fast_localize", {}))
    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)
    products_dir = Path(products_dir)
    plots_dir = Path(plots_dir)
    out_prefix = str(fl.get("out_prefix", "grb"))
    results_yaml = products_dir / f"{out_prefix}_fast_localize_results.yaml"

    if fl.get("status") == "skipped":
        gcn_query_result = query_relevant_grb_notices(
            config=config,
            task_id="Duration_and_Localization_Results",
            tstart=config.get("tstart"),
            tstop=config.get("tstop"),
        )
        p = {
            "status": "skipped",
            "gcn_notice_query": gcn_query_result,
        }
        config["fast_localize_results_yaml"] = str(results_yaml)
        config.setdefault("gcn_notice_queries", {})[
            "Duration_and_Localization_Results"
        ] = gcn_query_result
        _record_pipeline_task(
            config,
            task_id="Duration_and_Localization_Results",
            task_name="Duration and Localization Results",
            input_data={"config_path": config_path, **fl},
            output_data=p,
            status="skipped",
            start_time=task_start,
        )
        fhf._save_yaml(str(results_yaml), p)
        fhf._save_yaml(config_path, config)
        return str(results_yaml)

    per_nside = fl.get("per_nside")
    main_nside = int(fl.get("main_nside", fl.get("nside", 32)))
    if not per_nside:
        raise RuntimeError("Missing fast_localize skymap metadata. Run skymap_unbinned first.")

    sig_path = None
    for row in per_nside:
        if int(row.get("nside", -1)) == main_nside:
            sig_path = row.get("sigmap_npz")
            break
    if not sig_path or not Path(sig_path).is_file():
        raise FileNotFoundError(f"Significance map npz for NSIDE={main_nside} not found.")

    # The results stage reads the selected map and turns it into user-facing
    # artifacts: top-K CSV, plots, timing table, and config coordinates.
    z = np.load(sig_path, allow_pickle=False)
    sig_map = z["sig"]

    win = fl.get("windows", {})
    on_start = float(win["on_start"])
    on_stop = float(win["on_stop"])
    off_start = float(win["off_start"])
    off_stop = float(win["off_stop"])
    alpha = float(win["alpha"])
    arm_min = float(fl.get("arm_min", -13.0))
    arm_max = float(fl.get("arm_max", 13.0))
    topk = int(fl.get("topk", 50))

    ip_best = int(np.argmax(sig_map))
    th, ph = hp.pix2ang(main_nside, ip_best)
    best_l = float(np.degrees(ph))
    best_b = float(90.0 - np.degrees(th))
    best_s = float(sig_map[ip_best])

    true_l = fl.get("true_l_deg")
    true_b = fl.get("true_b_deg")
    true_lf = float(true_l) if true_l is not None else None
    true_bf = float(true_b) if true_b is not None else None

    out_csv = str(products_dir / f"{out_prefix}_localization_top{topk}.csv")
    _fl_save_localization_csv(out_csv, main_nside, sig_map, topk=topk)

    out_png = str(plots_dir / f"{out_prefix}_significance_map.png")
    _fl_save_pretty_significance_map(
        out_png,
        main_nside,
        sig_map,
        on_start,
        on_stop,
        off_start,
        off_stop,
        alpha,
        arm_min,
        arm_max,
        best_l,
        best_b,
        best_s,
        true_l=true_lf,
        true_b=true_bf,
        trigger_time=_config_trigger_time(config, "GeD"),
        figsize=fl.get("map_plot_figsize", [10, 6]),
        dpi=int(fl.get("map_plot_dpi", 220)),
    )

    timings = fl.get("timings_s", {})
    t_io = float(timings.get("t_io", 0.0))
    t_prep = float(timings.get("t_prep", 0.0))
    t_map_main = float(fl.get("t_map_main_s", 0.0))
    wall_t0 = float(fl.get("wall_t0_unix", time.time()))
    t_total = float(time.time() - wall_t0)

    timing_csv = str(products_dir / f"{out_prefix}_timing.csv")
    _fl_save_timing_csv(timing_csv, main_nside, t_io, t_prep, t_map_main, t_total)

    summary_rows = fl.get("summary_rows") or []
    nsides_run = fl.get("nsides_run") or [main_nside]
    if len(nsides_run) > 1 and summary_rows:
        out_sum = str(products_dir / f"{out_prefix}_nside_summary.csv")
        _fl_save_nside_summary_csv(out_sum, summary_rows)
        out_tbl = str(plots_dir / f"{out_prefix}_nside_summary.png")
        _fl_save_nside_table_png(
            out_tbl,
            summary_rows,
            trigger_time=_config_trigger_time(config, "GeD"),
            figsize_width=float(fl.get("nside_table_figsize_width", 14)),
            figsize_base_height=float(fl.get("nside_table_figsize_base_height", 2.4)),
            figsize_row_height=float(fl.get("nside_table_figsize_row_height", 0.38)),
            dpi=int(fl.get("nside_table_dpi", 240)),
        )
    else:
        out_sum = None
        out_tbl = None

    ulc = dict(config.get("unbinned_light_curve", {}))
    # Feed the reconstructed location back into the unbinned light-curve stage.
    ulc["l"] = best_l
    ulc["b"] = best_b
    config["unbinned_light_curve"] = ulc

    tsmap_existing = config.get("tsmap")
    if isinstance(tsmap_existing, dict):
        tsmap_merged = dict(tsmap_existing)
        tsmap_merged["fast_localize_coordinates"] = {"l_deg": best_l, "b_deg": best_b}
        config["tsmap"] = tsmap_merged

    if bool(fl.get("make_lc", False)):
        # Optional compatibility hook for the standalone legacy time-series script.
        lc_script = str(fl.get("lc_script", "make_timeseries_all.py"))
        if Path(lc_script).is_file():
            lc_arm_min = arm_min if fl.get("lc_arm_min") is None else float(fl["lc_arm_min"])
            lc_arm_max = arm_max if fl.get("lc_arm_max") is None else float(fl["lc_arm_max"])
            cmd = [
                "python",
                lc_script,
                "--data",
                str(config["source_path"]),
                "--tmin",
                str(config["tstart"]),
                "--tmax",
                str(config["tstop"]),
                "--l",
                str(best_l),
                "--b",
                str(best_b),
                "--arm-min",
                str(lc_arm_min),
                "--arm-max",
                str(lc_arm_max),
                "--bin",
                str(float(fl.get("lc_bin", 1.0))),
                "--outdir",
                str(products_dir.parent),
                "--out-prefix",
                out_prefix,
                "--no-warning",
            ]
            if bool(fl.get("lc_diagnostics", False)):
                cmd.append("--diagnostics")
            subprocess.run(cmd, check=True)

    offset_deg = (
        _fl_angsep_deg(true_lf, true_bf, best_l, best_b)
        if true_lf is not None and true_bf is not None
        else None
    )
    results_payload = {
        "status": "ok",
        "main_nside": main_nside,
        "best_l_deg": best_l,
        "best_b_deg": best_b,
        "best_S_sigma": best_s,
        "best_ipix": ip_best,
        "offset_vs_true_deg": offset_deg,
        "localization_csv": out_csv,
        "significance_map_png": out_png,
        "timing_csv": timing_csv,
        "nside_summary_csv": out_sum,
        "nside_summary_png": out_tbl,
        "timings_s": {"t_io": t_io, "t_prep": t_prep, "t_map": t_map_main, "t_total": t_total},
    }
    gcn_query_result = query_relevant_grb_notices(
        config=config,
        task_id="Duration_and_Localization_Results",
        tstart=on_start,
        tstop=on_stop,
    )
    results_payload["gcn_notice_query"] = gcn_query_result
    config["fast_localize"] = {**fl, **results_payload}
    config["fast_localize_results_yaml"] = str(results_yaml)
    config.setdefault("gcn_notice_queries", {})[
        "Duration_and_Localization_Results"
    ] = gcn_query_result
    _record_pipeline_task(
        config,
        task_id="Duration_and_Localization_Results",
        task_name="Duration and Localization Results",
        input_data={"config_path": config_path, **fl},
        output_data=results_payload,
        start_time=task_start,
    )
    fhf._save_yaml(str(results_yaml), results_payload)
    fhf._save_yaml(config_path, config)
    return str(results_yaml)

#########################################################
# TASK 6: TS_Map_on_different_timescales
#########################################################
def compute_ts_map(
    config_path: str) -> str:
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
    moc_plot_name = str(tsmap_cfg.get("moc_plot_name", "tsmap_moc.png"))
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
    fast_plot_path = plots_dir / fast_plot_name
    trigger_time = _config_trigger_time(config, "GeD")
    fast_caption = (
        "COSI GeD fixed-resolution HEALPix test-statistic skymap in Galactic "
        "coordinates using a Mollweide projection. Pixel colors encode the test "
        "statistic (TS); larger values indicate positions more consistent with a "
        "transient source. The fuchsia cross marks the maximum-TS position."
    )
    fast_title, fast_metadata = _plot_identity(
        "GeD",
        fast_caption,
        trigger_time,
        f"Fixed-resolution TS skymap (NSIDE={nside})",
        preview_title="GeD TS Map",
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
        "COSI GeD Multi-Resolution coverage test-statistic skymap in Galactic "
        "coordinates using a Mollweide projection. Pixel colors encode the test "
        "statistic (TS), while grey boundaries show the adaptive HEALPix cells. "
        "The red cross marks the maximum-TS cell."
    )
    moc_title, moc_metadata = _plot_identity(
        "GeD",
        moc_caption,
        trigger_time,
        f"Multi-resolution TS skymap (maximum cell NSIDE={moc_nside})",
        preview_title="GeD TS Map",
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
    config_path: str) -> str:
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
        containment=float(lightcurve_cfg.get("containment", 0.5)),
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
        "COSI GeD light curve extracted around the selected TS-map position. "
        f"The blue step histogram gives the counts in {bin_size:g} s bins after "
        f"applying the {float(lightcurve_cfg.get('containment', 0.5)):.0%} "
        "point-source-response containment mask. The orange dashed line marks "
        "the configured trigger-window start."
    )
    lightcurve_title, lightcurve_metadata = _plot_identity(
        "GeD",
        lightcurve_caption,
        _config_trigger_time(config, "GeD"),
        "Point-source-response selected light curve",
        preview_title="GeD Light Curve",
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

    lightcurve_plot_path = plots_dir / "lightcurve.png"
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
        "containment": float(lightcurve_cfg.get("containment", 0.5)),
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
def duration(config_path: str) -> str:
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
            save_path=str(plots_dir / "lc_analysis.png"),
            figsize=duration_cfg.get("plot_figsize", [10, 4]),
            dpi=int(duration_cfg.get("plot_dpi", 150)),
            trigger_time=_config_trigger_time(config, "GeD"),
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
            str(plots_dir / "lc_analysis.png")
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
        "COSI GeD light-curve duration analysis. Grey points with error bars "
        "show the measured count rate, the red dotted curve is the fitted "
        "background, and the blue step curve is the Bayesian-blocks model. "
        "Olive dashed vertical lines delimit the inferred signal interval used "
        "for the duration estimate."
    )
    title, metadata = _plot_identity(
        "GeD",
        caption,
        trigger_time,
        "Bayesian-blocks light-curve analysis",
        preview_title="GeD Light Curve",
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
