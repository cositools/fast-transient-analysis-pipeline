
from __future__ import annotations

import os
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import math
from typing import Any

#########################################################
# TASK 0: Configuration File format for .Yaml file
#########################################################
# Centralized defaults for every BGO analysis stage. The DAG passes the same
# structure, and preprocessing deep-merges user YAML overrides into it.
BGO_ANALYSIS_DEFAULTS: dict[str, Any] = {
    "duration": {
        "lightcurve_key": "light_curve",
        "p0": 10e-5,
        "is_rate": False,
        "panels": ["z1", "z0", "x1", "x0", "y1", "y0"],
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
        "counts_order": ["BGO_Z1", "BGO_Z0", "BGO_X1", "BGO_X0", "BGO_Y1", "BGO_Y0"],
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


#########################################################
# TASK 1: Source Data Extraction from FITS
#########################################################
def extract_source_data_from_fits(
    pipeline_config,
    grb_fits_path: str,
    grb_file_name: str,
    panels: list[str] = ["z1", "z0", "x1", "x0", "y1", "y0"],
    plot: bool = False,
    save_plot: bool = False,
    plot_counts: bool = False,
    output_dir: str | Path | None = None,
):
    """
    Extract source counts, background counts, GRB time and Bayesian-Blocks
    results directly from an ACS FITS file.

    Parameters
    ----------
    grb_fits_path : str
        Path to the ACS FITS file.
    grb_file_name : str
        GRB identifier used as output prefix.
    panels : list[str], optional
        Panel order used by the analyzer and returned arrays.
    plot : bool, optional
        Generate Bayesian-Blocks and background plots.
    save_plot : bool, optional
        Save plots instead of displaying them.
    plot_counts : bool, optional
        Generate the preliminary ACS counts/rate plots.
    output_dir : str | Path | None, optional
        Directory used when saving plots.

    Returns
    -------
    s_counts : np.ndarray
        Counts observed in the signal interval.
    b_counts : np.ndarray
        Estimated background counts in the signal interval.
    grb_time : float
        Signal start converted to Unix time.
    bblocks_analysis_results : dict
        Complete Bayesian-Blocks analysis output.
    """
    from cosipy.nonimaging.bgo.ACSDataAnalyzer import ACSDataAnalyzer

    import fast_helper_functions as fhf

    if isinstance(pipeline_config, str):
        config = fhf._load_yaml(pipeline_config)
    elif isinstance(pipeline_config, dict):
        config = dict(pipeline_config)
    else:
        raise TypeError(
            "extract_source_data_from_fits expects the pipeline "
            "configuration as a dict or YAML path string"
        )

    analysis_config = _deep_merge(
        BGO_ANALYSIS_DEFAULTS,
        config.get("analysis_config", {}),
    )

    config["analysis_config"] = analysis_config

    for section, defaults in analysis_config.items():
        if isinstance(defaults, dict):
            config[section] = _deep_merge(
                defaults,
                config.get(section, {}),
            )
        else:
            config.setdefault(section, defaults)

    if not grb_fits_path:
        raise ValueError(
            "Missing required preprocessing field: grb_fits_path"
        )

    if not os.path.exists(grb_fits_path):
        raise FileNotFoundError(
            f"ACS FITS file not found: {grb_fits_path}"
        )

        
    config["lightcurve_path"] = str(grb_fits_path)
    config["lightcurve_file"] = str(grb_fits_path)

    default_data_dir = str(Path(grb_fits_path).parent)
    data_dir = config.get("data_dir", default_data_dir)

    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    config["data_dir"] = str(Path(data_dir))
    config["plots_dir"] = str(plots_dir)
    config["products_dir"] = str(products_dir)

    if output_dir is None:
        output_dir = plots_dir

    config.setdefault(
        "duration_p0",
        config["duration"].get("p0", 10e-5),
    )

    config.setdefault(
        "duration_is_rate",
        config["duration"].get("is_rate", False),
    )

    config.setdefault(
        "duration_panels",
        config["duration"].get(
            "panels",
            ["z1", "z0", "x1", "x0", "y1", "y0"],
        ),
    )

    task_start = _now_iso()

    analyzer = ACSDataAnalyzer()

    if output_dir is not None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        analyzer.output_dir = str(output_dir)

    if save_plot and output_dir is None:
        raise ValueError(
            "output_dir is required when save_plot=True"
        )

    if plot_counts:
        analyzer.plot_acs_from_fits(
            grb_fits_path,
            plot_counts=True,
        )

    (
        s_counts,
        b_counts,
        grb_time,
        bblocks_analysis_results,
    ) = analyzer.extract_source_data_from_fits(
        grb_fits_path,
        plot=plot,
        save_plot=save_plot,
        prefix=grb_file_name,
        panels=panels,
    )
    
    if np.isscalar(s_counts) and s_counts == -1:
        raise RuntimeError(
            f"Bayesian Blocks analysis failed for {grb_fits_path}"
        )
    
    _ensure_structured_pipeline_config(config)

    _record_pipeline_task(      
        config,
        task_id="Extract_Source_Data_From_FITS",
        task_name="Extract Source Data From FITS",
            start_time=task_start,
            input_data={
                "grb_fits_path": str(grb_fits_path),
                "soft_lut_path": config.get("soft_lut_path"),
                "medium_lut_path": config.get("medium_lut_path"),
                "hard_lut_path": config.get("hard_lut_path"),
                "orientation_path": config.get("orientation_path"),
                "panels": list(panels),
                "plot": bool(plot),
                "save_plot": bool(save_plot),
                "plot_counts": bool(plot_counts),
                "analysis_config": analysis_config,
            },
            output_data={
                "data_dir": config["data_dir"],
                "plots_dir": config["plots_dir"],
                "products_dir": config["products_dir"],
                "grb_file_name": grb_file_name,
                "grb_time": float(grb_time),
                "signal_tstart": float(
                    bblocks_analysis_results["signal_tstart"]
                ),
                "signal_tstop": float(
                    bblocks_analysis_results["signal_tstop"]
                ),
                "t90": float(
                    bblocks_analysis_results["t90"]
                ),
                "t90_err_low": float(
                    bblocks_analysis_results["t90_err_low"]
                ),
                "t90_err_high": float(
                    bblocks_analysis_results["t90_err_high"]
                ),
                "significance": float(
                    bblocks_analysis_results["significance"]
                ),
                "significance_peak": float(
                    bblocks_analysis_results["significance_peak"]
                ),
                "signal_counts": np.asarray(
                    s_counts,
                    dtype=float,
                ).tolist(),
                "background_counts": np.asarray(
                    b_counts,
                    dtype=float,
                ).tolist(),
            },
    )
    
    config_path = config.get(
        "config_path",
        str(products_dir / "pipeline_config.yaml"),
    )

    fhf._save_yaml(config_path, config)

    return (
        np.asarray(s_counts, dtype=float),
        np.asarray(b_counts, dtype=float),
        float(grb_time),
        bblocks_analysis_results,
        config_path,
    )

#########################################################
# TASK 2: Li&Ma calculation of the significance
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
def significance_analysis(
                        pipeline_config,
                        signal_counts_arr: np.ndarray,
                        background_counts_arr: np.ndarray,
                        duration: float,
                    ) -> np.ndarray:
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
    import fast_helper_functions as fhf

    if isinstance(pipeline_config, str):
        config_path = pipeline_config
        config = fhf._load_yaml(config_path)
    elif isinstance(pipeline_config, dict):
        config = dict(pipeline_config)
        config_path = config.get("config_path")
    else:
        raise TypeError(
            "significance_analysis expects a config dict or YAML path"
        )
    
    sigmas = []

    for S, B in zip(signal_counts_arr, background_counts_arr):
        if S < 10 or B < 10:
            sigmas.append(0)
        else:
            sigmas.append(li_ma(S, B, duration, duration))
            
    sigmas = np.array(sigmas)

    _record_pipeline_task(
        config,
        task_id="Significance_Analysis",
        task_name="Significance Analysis",
        input_data={
            "signal_counts": np.asarray(
                signal_counts_arr,
                dtype=float,
            ).tolist(),
            "background_counts": np.asarray(
                background_counts_arr,
                dtype=float,
            ).tolist(),
            "duration": float(duration),
        },
        output_data={
            "sigmas": sigmas.tolist(),
        },
    )

    fhf._save_yaml(config_path, config)

    return sigmas


#########################################################
# TASK 3: Localization bc_tools
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
    pipeline_config,
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

    The FITS extraction task produces counts in panel order [z1, z0, x1, x0, y1, y0].
    as BGO_X0 and BGO_Z1, so the localization helper reorders counts by label.
    """
    from cosipy.nonimaging.bgo.ACSLocalizerBCT import ACSLocalizerBCT
    import numpy as np
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from bctools.loc import NormLocLike, TSMap
    from scoords import Attitude

    import fast_helper_functions as fhf

    if isinstance(pipeline_config, str):
        config_path = pipeline_config
        config = fhf._load_yaml(config_path)
    elif isinstance(pipeline_config, dict):
        config = dict(pipeline_config)
        config_path = config.get("config_path")
    else:
        raise TypeError(
            "localize_bctools expects a config dict or YAML path"
        )

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
            return Attitude.from_axes(x=x_pointing, z=z_pointing, frame="icrs")

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

        #indexing
        i = min(idx + 1, len(data) - 1) 

        x_pointing = SkyCoord(data[:, 2][i] * u.deg, data[:, 1][i] * u.deg, frame="galactic")
        z_pointing = SkyCoord(data[:, 4][i] * u.deg, data[:, 3][i] * u.deg, frame="galactic")
        return Attitude.from_axes(x=x_pointing, z=z_pointing, frame="icrs")


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
    #GRB trigger time used to retrieve the spacecraft attitude.
    time_grb = tstart
    print("[localize_grb] tstart (GRB time):", time_grb)
    attitude = _attitude_from_orientation_file(orientation_file, time_grb)

   
    # LocalLocTable inputs need attitude projection first; HealpixLocTable inputs
    # are already on a sky grid and can be evaluated directly.
    result = localizer.localize(s_counts_arr, b_counts_arr, attitude=attitude)
    print("[localize_grb] Localization result:", result)
    # The localization result contains the best-fit Galactic coordinates (l, b) and a TS map.
    # Plot the TS map with the best-fit location marked.
    # Save the localization plot without opening an interactive window inside Airflow.
    localizer.plot(result ,show=False, save_path=f"{plots_dir}/bgo_localization.png")
    """"
    # ---------------------------------------------------------
    # Independent Galactic-coordinate localization plot
    # ---------------------------------------------------------
    import matplotlib.pyplot as plt
    import astropy.units as u

    ts_map = result["ts_map"]

    # TSMap.plot() creates the Mollweide/WCSAxes projection
    img, ax = ts_map.plot()

    # Galactic-coordinate helpers
    lon = ax.coords[0]
    lat = ax.coords[1]

    # Axis labels
    lon.set_axislabel(
        r"Galactic longitude $l$ [deg]",
        fontsize=12,
        minpad=0.8,
    )
    lat.set_axislabel(
        r"Galactic latitude $b$ [deg]",
        fontsize=12,
        minpad=0.8,
    )

    # Major ticks every 15 degrees
    lon.set_ticks(
        spacing=30 * u.deg,
        color="black",
    )
    lat.set_ticks(
        spacing=20 * u.deg,
        color="black",
    )

    # Tick labels
    lon.set_ticklabel(
        fontsize=9,
        exclude_overlapping=True,
    )
    lat.set_ticklabel(
        fontsize=9,
        exclude_overlapping=True,
    )

    # Dashed Galactic grid every 15 degrees
    lon.grid(
        color="white",
        alpha=0.45,
        linestyle="--",
        linewidth=0.7,
    )
    lat.grid(
        color="white",
        alpha=0.45,
        linestyle="--",
        linewidth=0.7,
    )

    # Best localization in Galactic coordinates
    best_l = float(result["l"])
    best_b = float(result["b"])

    true_l = 275.042
    true_b = 13.899

    ax.scatter(
        best_l,
        best_b,
        transform=ax.get_transform("world"),
        s=1,
        linewidths=2.0,
        color="blue",
        zorder=10,
        label="Best localization",
    )
    ax.scatter(
        true_l,
        true_b,
        transform=ax.get_transform("world"),
        marker="x",
        s=1,
        linewidths=2.0,
        color="red",
        zorder=10,
        label="true position",
    )

    # Position label with an offset in display coordinates
    ax.legend(
        loc="upper right",
        frameon=True,
        fontsize=10,
    )

    ax.set_title(
        (
            f"$\\sqrt{{TS}}={result['sqrt_ts']:.2f}$, "
            f"$l={best_l:.2f}^\\circ$, "
            f"$b={best_b:.2f}^\\circ$, "
            f"$A_{{90}}={result['cont_area_deg2']:.2f}\\ "
            f"\\mathrm{{deg}}^2$"
        ),
        fontsize=14,
        pad=18,
    )

    plot_path = plots_dir / "bgo_localization.png"

    plt.savefig(
        plot_path,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()
    print(f"Plot salvato in: {plot_path}")
    """

    return str(plots_dir)


