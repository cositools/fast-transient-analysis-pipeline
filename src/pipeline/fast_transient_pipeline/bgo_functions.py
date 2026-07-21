from __future__ import annotations

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import math
from typing import Any

#########################################################
# TASK 1: Preprocessing
#########################################################
# Centralized defaults for every BGO analysis stage. The DAG passes the same
# structure, and preprocessing deep-merges user YAML overrides into it.
BGO_ANALYSIS_DEFAULTS: dict[str, Any] = {
    "duration": {
        "lightcurve_key": "light_curve",
        "p0": 10e-5,
        "is_rate": False,
        "panels": ["z0", "z1", "x0", "x1", "y0", "y1"],
    },
    "background": {
        "buffer": 0.0,
        "order": 2,
    },
    "light_curve": {
        "panel": None,
        "show": False,
    },
    "significance": {
        "min_signal_counts": 10,
        "min_background_counts": 10,
    },
    "localization_bctools": {
        "nside": 64,
        "counts_order": ["BGO_X0", "BGO_X1", "BGO_Y0", "BGO_Y1", "BGO_Z0", "BGO_Z1"],
        "output_plot_name": "bgo_localization.png",
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    from copy import deepcopy

    # Keep the default tree immutable across DAG runs.
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
    # Airflow task outputs and numpy products need to be converted before YAML
    # persistence; otherwise PyYAML can emit implementation-specific tags.
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


def _ensure_structured_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    # Keep BGO aligned with the GeD YAML schema: one top-level run section
    # contains resolved inputs and per-task bookkeeping.
    pipeline_name = str(config.get("pipeline_name", "BGO"))
    run_cfg = config.setdefault(pipeline_name, {})
    run_cfg["name"] = config.get("cosidag_id", "cosidag_BGO")
    run_cfg.setdefault("trigger_time", config.get("trigger_time", _now_iso()))
    run_cfg["input_resolved"] = _to_yaml_safe(
        config.get(
            "input_resolved",
            {
                "lightcurve_path": config.get("lightcurve_path"),
                "soft_lut_path": config.get("soft_lut_path"),
                "medium_lut_path": config.get("medium_lut_path"),
                "hard_lut_path": config.get("hard_lut_path"),
                "orientation_path": config.get("orientation_path"),
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
    # Use one task-record schema for every stage so monitoring and outbox code
    # can read task status without special cases.
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
    base_dir = Path(data_dir)
    # Products stay in the COSIDAG products directory; plots are grouped beside it.
    plots_dir = base_dir / ".." / "plots"
    products_dir = base_dir
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    return plots_dir, products_dir


def preprocess_data(lightcurve):
    """Preprocess BGO inputs and persist the shared YAML pipeline config."""
    import fast_helper_functions as fhf

    if isinstance(lightcurve, str):
        config = fhf._load_yaml(lightcurve)
    elif isinstance(lightcurve, dict):
        config = dict(lightcurve)
    else:
        raise TypeError("preprocess_data expects a dict or a YAML path string")

    # Promote nested analysis sections to top-level keys for the task wrappers,
    # while preserving a single canonical `analysis_config` block in YAML.
    analysis_config = _deep_merge(BGO_ANALYSIS_DEFAULTS, config.get("analysis_config", {}))
    config["analysis_config"] = analysis_config
    for section, defaults in analysis_config.items():
        if isinstance(defaults, dict):
            config[section] = _deep_merge(defaults, config.get(section, {}))
        else:
            config.setdefault(section, defaults)

    lightcurve_path = config.get("lightcurve_path") or config.get("lightcurve_file")
    if not lightcurve_path:
        raise ValueError("Missing required preprocessing field: lightcurve_path")
    if not os.path.exists(lightcurve_path):
        raise FileNotFoundError(f"lightcurve_file not found or missing from resolve_inputs: {lightcurve_path}")

    config["lightcurve_path"] = str(lightcurve_path)
    config["lightcurve_file"] = str(lightcurve_path)
    default_data_dir = str(Path(lightcurve_path).parent)
    data_dir = config.get("data_dir", default_data_dir)
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)
    config["data_dir"] = str(Path(data_dir))
    config["plots_dir"] = str(plots_dir)
    config["products_dir"] = str(products_dir)

    config.setdefault("duration_p0", config["duration"].get("p0", 10e-5))
    config.setdefault("duration_is_rate", config["duration"].get("is_rate", False))
    config.setdefault("duration_panels", config["duration"].get("panels", ["z0", "z1", "x0", "x1", "y0", "y1"]))

    _ensure_structured_pipeline_config(config)
    _record_pipeline_task(
        config,
        task_id="PreProcessing_BGO",
        task_name="PreProcessing BGO",
        input_data={
            "lightcurve_path": config.get("lightcurve_path"),
            "soft_lut_path": config.get("soft_lut_path"),
            "medium_lut_path": config.get("medium_lut_path"),
            "hard_lut_path": config.get("hard_lut_path"),
            "orientation_path": config.get("orientation_path"),
            "analysis_config": analysis_config,
        },
        output_data={
            "data_dir": config["data_dir"],
            "plots_dir": config["plots_dir"],
            "products_dir": config["products_dir"],
        },
    )

    config_path = config.get("config_path", str(products_dir / "pipeline_config.yaml"))
    fhf._save_yaml(config_path, config)
    return config_path

#########################################################
# TASK 2: Duration on different binning (Bayesian Blocks)
#########################################################
def get_duration(
    lightcurve: np.ndarray, 
    p0: float = 0.05, 
    isRate: bool = False, 
    panels: list[str] = ["z0", "z1", "x0", "x1", "y0", "y1"]
):
    """Analyze a (multi-detector) light curve and estimate T90 via Bayesian Blocks.

    Assumptions on the input format (as used in this notebook)
    ----------------------------------------------------------
    - Time bin centroids are stored in `lightcurve[:, 0, 0]`
    - The signal for each detector/panel is stored in `lightcurve[:, i, 1]`

    Parameters
    ----------
    lightcurve : `np.ndarray`
        Light curve array.
    p0 : `float`
        False alarm probability for Bayesian Blocks.
    isRate : `bool`
        If True, the input signal is a rate (counts/s) and is converted to counts/bin.
    panels : `list[str]`
        Detector/panel names to process.

    Returns
    -------
    `tuple`
        A tuple with the following elements (in this exact order):

        - tstart : `float`
            Start time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstart`).
            Units are the same as the input time axis.

        - tstop : `float`
            Stop time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstop`).
            Units are the same as the input time axis.

        - t90 : `float`
            Estimated T90 duration (the interval containing 90% of the signal fluence/counts, as
            implemented by `bb_lc.duration(quantile=0.9)`). Same time units as the input.

        - t90_err_low : `float`
            Lower uncertainty estimate on T90, from `bb_lc.duration_error(0.9, ...)`.
            Same time units as the input.

        - t90_err_high : `float`
            Upper uncertainty estimate on T90, from `bb_lc.duration_error(0.9, ...)`.
            Same time units as the input.
            
        - signal_range : `TimeRange`
            TimeRange containing the start and stop times of the signal interval inferred by Bayesian Blocks.
    """
    from gdt.core.data_primitives import TimeBins
    from bctools.analysis import BayesianBlocksLightcurve

    data = lightcurve

    # Time axis (bin centroids). Assumed identical for all panels.
    time = data[:, 0, 0]

    # Temporal bin width (assumes uniform binning).
    # Needed also to convert rate -> counts when isRate=True.
    bin_width = time[1] - time[0]

    signal = {}
    for i, panel in enumerate(panels):
        y = data[:, i, 1]
        if isRate:
            # Convert rate (counts/s) to counts per bin.
            y = y * bin_width
        signal[panel] = y

    # Construct light curve object
    # Build bin edges from centroids.
    bin_edges = np.zeros(len(time) + 1)
    bin_edges[1:-1] = (time[:-1] + time[1:]) / 2
    bin_edges[0] = time[0] - (time[1] - time[0]) / 2
    bin_edges[-1] = time[-1] + (time[-1] - time[-2]) / 2
    lo_edges, hi_edges = bin_edges[:-1], bin_edges[1:]

    # Per-bin exposure: here assumed full and constant.
    exposure = np.full(len(time), bin_width)


    lc = {}  # TimeBins per panel
    lc_timebins = {}
    for panel in panels:
        signal_panel = signal[panel]
        lc[panel] = TimeBins(signal_panel, lo_edges, hi_edges, exposure)
        lc_timebins[panel] = {
            "signal": signal_panel.tolist(),
            "lo_edges": lo_edges.tolist(),
            "hi_edges": hi_edges.tolist(),
            "exposure": exposure.tolist(),
        }
    # If you want a combined light curve across panels, you could sum TimeBins,
    # but here we select a single "best" detector (see below).
    # if len(panels) > 1: lc_psum = TimeBins.sum([lcs for lcs in lc.values()])
    # else: lc_psum = lc[panels[0]]

    # Heuristic panel selection: pick the detector with the highest peak counts.
    best_panel = None
    best_value = float('-inf')

    for i in range(6):
        panel = panels[i]
        value = max(lc[panel].counts)

        if value > best_value:
            best_value = value
            best_panel = panel
            
    lc_psum = lc[best_panel]

    lc_sel = lc_psum
   

    # Bayesian Blocks step:
    # - Segment the selected light curve into blocks (piecewise-constant rate).
    # - Infer the signal interval (`signal_range`).
    # - From that interval, estimate duration metrics (e.g. T90) and uncertainties.
    #
    # This can fail for edge cases (e.g. too few bins, pathological data, no
    # meaningful change points, numerical issues). In that case we return a
    # "safe" tuple with sentinel values but *the same structure/length* as
    # the successful output, so downstream code can unpack reliably.
    try:
        bb_lc = BayesianBlocksLightcurve(lc_sel)
        bb_lc.compute_bayesian_blocks(p0=p0)

        signal_range = bb_lc.signal_range
        t90 = bb_lc.duration(quantile=.9)
        t90_error = bb_lc.duration_error(.9, nsamples=100)

        bb_lc_timebins = {
            "lo_edges": bb_lc.bb_lightcurve.lo_edges.tolist(),
            "hi_edges": bb_lc.bb_lightcurve.hi_edges.tolist(),
            "rates": bb_lc.bb_lightcurve.rates.tolist(),
        }

    except Exception as e:
        print(e)
        print('WARNING: Bayesian Blocks failed; returning sentinel values')
        return (
            lc_timebins,       # per-panel TimeBins serialized dict
            {},                # Bayesian-blocks light curve serialized dict
            -9999,            # tstart
            -9999,            # tstop
            -9999,            # t90
            -9999,            # t90_err_low
            -9999,            # t90_err_high
        )

    return (
        lc_timebins, 
        bb_lc_timebins,
        signal_range.tstart, 
        signal_range.tstop, 
        t90, 
        t90_error[0], 
        t90_error[1]
    )

    # Note: `plt.show` without parentheses does nothing. If you need to display figures:
    # plt.show()


#########################################################
# TASK 3: Background Extraction and Data Preparation
#########################################################
def fit_background_gdt(
    lc: TimeBins, 
    tstart: float, 
    tstop: float, 
    buffer: float = 0.0, 
    order: int = 2
):
    """
    Fit a polynomial background model on a GDT TimeBins light curve,
    excluding the inferred signal window.

    Parameters
    ----------
    lc: TimeBins
        Detector light curve. It must expose counts, lo_edges, hi_edges,
        and exposure arrays.
    tstart : float
        Start time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstart`).
        Units are the same as the input time axis.
    tstop : float
        Stop time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstop`).
        Units are the same as the input time axis.
    buffer : float, optional
        Extra time margin excluded around the signal window.
    order : int, optional
        Polynomial order.

    Returns
    -------
    result : dict
        Dizionario con:
        - "model"           : oggetto Polynomial fittato
        - "mask_bkg"        : maschera booleana dei bin usati nel fit
        - "bkg_rate"        : background stimato in rate
        - "bkg_rate_err"    : errore sul background rate
        - "bkg_counts"      : background stimato in counts/bin
        - "bkg_counts_err"  : errore in counts/bin
        - "net_counts"      : observed counts - background counts
        - "net_rate"        : rate osservato - background rate
    """
    from gdt.core.background.binned import Polynomial
    
    excl_start = tstart - buffer
    excl_stop = tstop + buffer

    # Only bins fully outside the excluded signal region are used for the fit.
    mask_bkg = (lc.hi_edges <= excl_start) | (lc.lo_edges >= excl_stop)

    n_bkg_bins = np.sum(mask_bkg)
    if n_bkg_bins < (order + 2):
        raise RuntimeError(
            f"Troppi pochi bin di background ({n_bkg_bins}) "
            f"per un polinomio di ordine {order}"
        )

    # Match the bctools/GDT polynomial-background API shape: (N, 1) counts.
    bkg_model = Polynomial(
        counts=lc.counts[mask_bkg][:, np.newaxis],
        tstart=lc.lo_edges[mask_bkg],
        tstop=lc.hi_edges[mask_bkg],
        exposure=lc.exposure[mask_bkg]
    )

    bkg_model.fit(order=order)

    # `interpolate()` returns rates, not counts, so counts/bin are recovered
    # using the bin exposure below.
    bkg_rate, bkg_rate_err = bkg_model.interpolate(
        tstart=lc.lo_edges,
        tstop=lc.hi_edges
    )

    # Collapse from shape (N, 1) to shape (N,).
    bkg_rate = np.squeeze(bkg_rate)
    bkg_rate_err = np.squeeze(bkg_rate_err)

    # Convert background rate to expected background counts per bin.
    bkg_counts = bkg_rate * lc.exposure
    bkg_counts_err = bkg_rate_err * lc.exposure

    # Observed rate for the same bins.
    obs_rate = lc.counts / lc.exposure

    # Net signal after subtracting the fitted background.
    net_counts = lc.counts - bkg_counts
    net_rate = obs_rate - bkg_rate

    return {
        "model": bkg_model,
        "mask_bkg": mask_bkg,
        "bkg_rate": bkg_rate,
        "bkg_rate_err": bkg_rate_err,
        "bkg_counts": bkg_counts,
        "bkg_counts_err": bkg_counts_err,
        "net_counts": net_counts,
        "net_rate": net_rate,
    }


def background_extraction_and_data_preparation(
    # Serialized TimeBins per panel:
    # <panel_name>: {"signal": <counts>, "lo_edges": <lo>, "hi_edges": <hi>, "exposure": <exp>}
    lc_timebins: dict[str, dict[str, list[float]]], 
    tstart: float, 
    tstop: float, 
    buffer: float = 0.0, 
    order: int = 2,
    panels: list[str] = ["z0", "z1", "x0", "x1", "y0", "y1"]
):
    """
    Fit background for each panel and prepare data for the analysis.

    Parameters
    ----------
    lc_timebins : `dict[str, dict[str, list[float]]]`
        Dictionary of light curves for each panel.
        <panel_name>: {"signal": <signal_counts>, "lo_edges": <lo_edges>, "hi_edges": <hi_edges>, "exposure": <exposure>}
    tstart : `float`
        Start time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstart`).
    tstop : `float`
        Stop time of the signal interval inferred by Bayesian Blocks (`bb_lc.signal_range.tstop`).
    buffer : `float`, optional
        Extra time margin excluded around the signal window.
    order : `int`, optional
        Polynomial order.
    panels : `list[str]`, optional
        List of panel names to process.

    Returns
    -------
    signal_counts_arr : `np.ndarray`
        Array of signal counts for each panel.
    background_counts_arr : `np.ndarray`
        Array of background counts for each panel.
    net_counts_arr : `np.ndarray`
        Array of net counts for each panel.
    """
    from gdt.core.data_primitives import TimeBins

    results = []
    signal_counts_arr = []
    background_counts_arr = []
    net_counts_arr = []

    print(panels)

    lc_panels = {
        panel: TimeBins(
            lc_timebins[panel]["signal"], 
            lc_timebins[panel]["lo_edges"], 
            lc_timebins[panel]["hi_edges"], 
            lc_timebins[panel]["exposure"]
        )
        for panel in panels
    }
    
    # Fit the background independently for each BGO shield panel, then reduce
    # each fitted light curve to the counts inside the signal interval.
    for p in panels:
        print("Panel:", p)
        lc = lc_panels[p]
        res = fit_background_gdt(lc, tstart, tstop, buffer=buffer, order=order)
        results.append(res)

        # The signal interval uses bins fully contained in [tstart, tstop].
        mask_sig = (lc.lo_edges >= tstart) & (lc.hi_edges <= tstop)
        
        signal_counts = np.sum(lc.counts[mask_sig])
        background_counts = np.sum(res["bkg_counts"][mask_sig])
        net_counts = signal_counts - background_counts

        signal_counts_arr.append(signal_counts)
        background_counts_arr.append(background_counts)
        net_counts_arr.append(net_counts)

    # Downstream tasks and localization expect compact arrays, not TimeBins.
    signal_counts_arr = np.array(signal_counts_arr)
    background_counts_arr = np.array(background_counts_arr)
    net_counts_arr = np.array(net_counts_arr)

    return signal_counts_arr, background_counts_arr, net_counts_arr

#########################################################
# TASK 4: Light Curve Generation
#########################################################
def light_curve_generation(
    bayes_output: tuple,
    analysis_dir: str | Path,
    panel: str | None = None,
    buffer: float = 0.0,
    order: int = 2,
    show: bool = False
):
    """
    Generate and save light-curve plots from Bayesian-Blocks step output.

    Parameters
    ----------
    bayes_output : tuple
        Output tuple produced by `get_duration`:
        (lc_timebins, bb_lc_timebins, tstart, tstop, t90, t90_err_low, t90_err_high)
    analysis_dir : str | Path
        Base analysis directory where `plots/` will be created.
    panel : str | None, optional
        Panel to plot. If None, the panel with the highest peak counts is used.
    buffer : float, optional
        Extra exclusion margin around signal interval for background fit.
    order : int, optional
        Polynomial order for background fitting.
    show : bool, optional
        If True, show figures interactively.

    Returns
    -------
    tuple[str, str]
        Paths of the saved plots: (light_curve_path, background_fit_path)
    """
    from gdt.core.data_primitives import TimeBins

    lc_timebins, bb_lc_timebins, tstart, tstop, t90, t90_err_low, t90_err_high = bayes_output

    if not isinstance(lc_timebins, dict) or len(lc_timebins) == 0:
        raise ValueError("Invalid `bayes_output`: lc_timebins must be a non-empty dict.")
    if not isinstance(bb_lc_timebins, dict) or len(bb_lc_timebins) == 0:
        raise ValueError("Invalid `bayes_output`: bb_lc_timebins must be a non-empty dict.")

    lc_panels = {
        p: TimeBins(
            lc_timebins[p]["signal"],
            lc_timebins[p]["lo_edges"],
            lc_timebins[p]["hi_edges"],
            lc_timebins[p]["exposure"]
        )
        for p in lc_timebins
    }

    if panel is None:
        panel = max(lc_panels, key=lambda p: np.max(lc_panels[p].counts))
    if panel not in lc_panels:
        raise ValueError(f"Panel `{panel}` not found in lc_timebins: {list(lc_panels.keys())}")

    lc_sel = lc_panels[panel]
    bkg_res = fit_background_gdt(lc_sel, tstart, tstop, buffer=buffer, order=order)

    plots_dir = Path(analysis_dir) / ".." / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Plot 1: raw counts light curve
    fig1, ax1 = plt.subplots(figsize=(10, 4))
    ax1.step(lc_sel.centroids, lc_sel.counts, where="mid")
    ax1.set_xlabel("Time [s]")
    ax1.set_ylabel("Counts / bin")
    ax1.set_title(f"Light curve ({panel})")
    ax1.grid(True, alpha=0.3)
    fig1.tight_layout()
    light_curve_path = plots_dir / f"light_curve_{panel}.png"
    fig1.savefig(light_curve_path, dpi=150)

    # Plot 2: raw rate + fitted background + Bayesian blocks + signal boundaries
    fig2, ax2 = plt.subplots(figsize=(10, 4))
    ax2.plot(
        lc_sel.centroids,
        bkg_res["bkg_rate"],
        color="red",
        ls=":",
        label="Fitted background"
    )
    ax2.errorbar(
        lc_sel.centroids,
        lc_sel.rates,
        xerr=[lc_sel.centroids - lc_sel.lo_edges, lc_sel.hi_edges - lc_sel.centroids],
        yerr=lc_sel.rate_uncertainty,
        ls="none",
        color=".7",
        label="Raw data"
    )
    ax2.plot(
        np.append(bb_lc_timebins["lo_edges"], bb_lc_timebins["hi_edges"][-1]),
        np.append(bb_lc_timebins["rates"], bb_lc_timebins["rates"][-1]),
        drawstyle="steps-post",
        label="Bayesian blocks",
    )
    ax2.axvline(tstart, ls="--", color="olive", label="Signal start/stop")
    ax2.axvline(tstop, ls="--", color="olive")
    ax2.set_xlabel("Time [s]")
    ax2.set_ylabel("Rate [counts/s]")
    ax2.set_title(
        f"Background fit ({panel}) - T90={t90:.3f} (+{t90_err_high:.3f}/-{t90_err_low:.3f}) s"
    )
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    fig2.tight_layout()
    background_fit_path = plots_dir / f"light_curve_background_t90_bblocks_{panel}.png"
    fig2.savefig(background_fit_path, dpi=150)

    if show:
        plt.show()
    else:
        plt.close(fig1)
        plt.close(fig2)

    return str(light_curve_path), str(background_fit_path)

#########################################################
# TASK 5: Li&Ma calculation of the significance
#########################################################
# ------------------------
# Helper function for Li&Ma calculation of the significance
# ------------------------
def li_ma(s, b, t_on, t_off):
    """
    Li&Ma calculation of the significance.

    Parameters
    ----------
    s : float
        Signal counts.
    b : float
        Background counts.
    t_on : float
        On time.
    t_off : float
        Off time.

    Returns
    -------
    SA : float
        Significance.

    References
    ----------
    Li & Ma (1983) Eq. 17; vector-safe.
    """
    alpha = t_on/t_off
    
    SA = math.sqrt(2) * math.sqrt(s * math.log(((1 + alpha) / alpha) * ( s / (s + b))) + b * math.log((1 + alpha) * (b / ( s + b ))));
    
    return SA
# ------------------------
# Main function for significance analysis
# ------------------------
def significance_analysis(signal_counts_arr: np.ndarray, 
                          background_counts_arr: np.ndarray, 
                          duration: float) -> np.ndarray:
    """
    Significance analysis.

    Parameters
    ----------
    signal_counts_arr : np.ndarray
        Array of signal counts.
    background_counts_arr : np.ndarray
        Array of background counts.
    duration : float
        Duration of the signal.

    Returns
    -------
    sigmas : np.ndarray
        Array of significances.
    """
    sigmas = []

    for S, B in zip(signal_counts_arr, background_counts_arr):
        if S < 10 or B < 10:
            sigmas.append(0)
        else:
            sigmas.append(li_ma(S, B, duration, duration))
            
    sigmas = np.array(sigmas)
    return sigmas


#########################################################
# TASK 7: Localization bc_tools
#########################################################
# ------------------------
# Helpers
# ------------------------
def ra_dec_to_theta_phi(ra: float, dec: float) -> tuple[float, float]:
    """
    Convert RA and Dec to theta and phi.
    """
    theta = 90 - dec
    phi = ra
    return theta, phi
# ------------------------
def spherical_to_radec_deg(theta_deg: float, phi_deg: float) -> tuple[float, float]:
    """
    Convert theta and phi to RA and Dec.
    """
    dec = 90.0 - theta_deg
    ra = phi_deg % 360.0
    return ra, dec
# -----------------------------------------------------------------------------
# Main pipeline step: localize GRB with BGO BC tools
# -----------------------------------------------------------------------------
def localize_bctools(
    data_folder: str,
    soft_lut_path: str,
    medium_lut_path: str,
    hard_lut_path: str,
    orientation_file: str,
    s_counts_arr: np.ndarray,
    b_counts_arr: np.ndarray,
    tstart: float,
) -> tuple[str, dict]:
    """
    Run BGO localization with BC tools.

    The upstream BGO duration/background tasks produce counts in panel order
    [z0, z1, x0, x1, y0, y1]. The BC-tools LUTs advertise detector labels such
    as BGO_X0 and BGO_Z1, so the localization helper reorders counts by label.
    """
    from cosipy.nonimaging.bgo.ACSLocalizerBCT import ACSLocalizerBCT
    import numpy as np
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from bctools.loc import NormLocLike, TSMap
    from scoords import Attitude

    def _attitude_from_orientation_file(path: str, time_grb: float):
        # Current data challenges provide spacecraft pointing as FITS tables.
        # Legacy `.ori` text files are still supported for older runs.
        lower_path = str(path).lower()
        if lower_path.endswith((".fits", ".fit", ".fits.gz", ".fit.gz")):
            from astropy.io import fits

            with fits.open(path, memmap=True) as hdul:
                table = hdul[1].data
                times_col = np.asarray(table["TimeStamp"], dtype=float)
                idx = int(np.argmin(np.abs(times_col - float(time_grb))))
                x_l_deg, x_b_deg = [float(value) for value in table["XPointings"][idx]]
                z_l_deg, z_b_deg = [float(value) for value in table["ZPointings"][idx]]

            print("Nearest index:", idx)
            print("Nearest time:", times_col[idx])
            print("Nearest X pointing (l, b):", x_l_deg, x_b_deg)
            print("Nearest Z pointing (l, b):", z_l_deg, z_b_deg)
            x_pointing = SkyCoord(x_l_deg * u.deg, x_b_deg * u.deg, frame="galactic")
            z_pointing = SkyCoord(z_l_deg * u.deg, z_b_deg * u.deg, frame="galactic")
            return Attitude.from_axes(x=x_pointing, z=z_pointing, frame="galactic")

        data = np.loadtxt(
            path,
            usecols=(1, 2, 3, 4, 5, 6, 7, 8),
            delimiter=" ",
            skiprows=1,
            comments=("#", "EN"),
        )
        times_col = data[:, 0]
        idx = int(np.argmin(np.abs(times_col - float(time_grb))))
        nearest_row = data[idx]
        print("Nearest index:", idx)
        print("Nearest time:", times_col[idx])
        print("Nearest row:", nearest_row)
        i = min(idx + 1, len(data) - 1)
        x_pointing = SkyCoord(data[:, 2][i] * u.deg, data[:, 1][i] * u.deg, frame="galactic")
        z_pointing = SkyCoord(data[:, 4][i] * u.deg, data[:, 3][i] * u.deg, frame="galactic")
        return Attitude.from_axes(x=x_pointing, z=z_pointing, frame="galactic")


    print("[localize_grb] Initializing BGOLocalizerBCT...")
    nside = 64
    plots_dir = Path(data_folder) / ".." / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    # Initialize the non-imaging localizer so both supported LUT formats are
    # loaded through the same package entrypoint.
    localizer = ACSLocalizerBCT(
        soft_loctable_path=soft_lut_path,
        medium_loctable_path=medium_lut_path,
        hard_loctable_path=hard_lut_path,
        output_dir=str(plots_dir),
        nside=nside,
    )
    print("[localize_grb] Localizer ready.")

    # Use the Bayesian-Blocks signal start as the timestamp for the attitude row.
    time_grb = tstart
    print("[localize_grb] tstart (GRB time):", time_grb)
    attitude = _attitude_from_orientation_file(orientation_file, time_grb)

    # LocalLocTable inputs need attitude projection first; HealpixLocTable inputs
    # are already on a sky grid and can be evaluated directly.
    result = localizer.localize(s_counts_arr, b_counts_arr, attitude=attitude)
    print("[localize_grb] Localization result:", result)
    # The localization result contains the best-fit Galactic coordinates (l, b) and a TS map.
    best_loc = SkyCoord(l=result['l']*u.deg, b=result['b']*u.deg, frame="galactic")
    # Plot the TS map with the best-fit location marked.
    ts_map = result["ts_map"]
    # Plot the TS map with the best-fit location marked.
    img, ax = ts_map.plot(cont=0.9)
    ax.grid(alpha=0.5)
    ax.scatter(
        best_loc.l.to(u.deg).value,#best_loc.icrs.ra.to(u.deg).value,
        best_loc.b.to(u.deg).value,#best_loc.icrs.dec.to(u.deg).value,
        color="blue",
        transform=ax.get_transform("world"),
        s=2,
        label="Best localization"
        )
    # Add legend 
    ax.legend(loc="upper right", frameon=True)
    # Save the plot
    plot_path = plots_dir / "bgo_localization.png"
    fig = ax.figure
    fig.savefig(plot_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return str(plots_dir)
