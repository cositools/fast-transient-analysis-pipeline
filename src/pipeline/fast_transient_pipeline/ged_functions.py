import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import subprocess
import time
import warnings
import yaml
from typing import Any

from matplotlib.lines import Line2D

# TODO: standardize the path to the helper functions
try:
    from . import fast_helper_functions as fhf
except ImportError:
    import fast_helper_functions as fhf

def _ensure_pipeline_dirs(data_dir: str) -> tuple[Path, Path]:
    base_dir = Path(data_dir)
    plots_dir = base_dir / ".." / "plots"
    products_dir = base_dir / "products"
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    return plots_dir, products_dir

#########################################################
# TASK 1: Preprocessing
# WIP: Implement the preprocessing
#########################################################
def preprocess_data(
    lightcurve,
    eps_time: float = 0.000000001,
):
    """
    Preprocess the pipeline inputs and persist them in a YAML config.

    The function accepts either:
    - a dict containing input keys, or
    - a string path to an existing YAML file to enrich/normalize.
    """
    if isinstance(lightcurve, str):
        config = fhf._load_yaml(lightcurve)
    elif isinstance(lightcurve, dict):
        config = dict(lightcurve)
    else:
        raise TypeError("preprocess_data expects a dict or a YAML path string")
    # Required keys for the preprocessing
    required_keys = [
        "source_path",
        "background_path",
        "orientation_path",
        "response_path",
    ]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(f"Missing required preprocessing fields: {missing}")
    # Data directory
    default_data_dir = str(Path(config["source_path"]).resolve().parent)
    data_dir = config.get("data_dir", default_data_dir)
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)
    # Define folders
    config["data_dir"] = str(Path(data_dir))
    config["plots_dir"] = str(plots_dir)
    config["products_dir"] = str(products_dir)
    # Emsure folders exist
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    # ------------------------------------------------
    # Default parameters for the Unbinned Light Curve
    # -----------------------------------------------
    config.setdefault(
        "unbinned_light_curve",
        {
            "arm-min": -15.0,           # ARM gate min [deg].
            "arm-max": 15.0,            # ARM gate max [deg].
            "bin": 1.0,                 # Light curve bin size [s].
            "out-prefix": "lc",         # Output prefix (no extension).
            "arm-hist-bins": 30,        # Number of bins for ARM histogram in diagnostics figure.
        },
    )
    #------------------------------------------------
    # Default parameters for the TS map
    #------------------------------------------------
    nside = 16
    config.setdefault(
        # Default spectrum for the TS map
        "default_spectrum",
        {
            "index": -2.2,               # Spectral index.
            "K": 10.0,                   # Normalization factor.
            "K_unit": "1 / (cm2 keV s)", # Normalization factor unit.
            "piv": 100.0,                # Pivot energy.
            "piv_unit": "keV",           # Pivot energy unit.
        },
    )
    config.setdefault("tsmap", {
        "nside": nside,
        "energy_channel": [2, 3],
        "cpu_cores": 8,
        "selected_method": "moc",
        "selected_coordinates": {
            "l_deg": 0,
            "b_deg": 0,
        },
    })
    # ------------------------------------------------
    # Default parameters for binning Data
    #------------------------------------------------
    config.setdefault(
        "binning_data",
        {
            "bin_size": 1,
            "eps_bkg_preburst": 20,
            "eps_bkg_postburst": 20,
            "nside": nside,
        },
    )
    #------------------------------------------------
    # Default parameters for the light curve
    #------------------------------------------------
    config.setdefault("light_curve", {
        "bin_size": 1,
        "nside": nside,
        "used_coordinates": {
            "l_deg": 0,
            "b_deg": 0,
        },
    })
    #------------------------------------------------
    # Default parameters for the duration
    #------------------------------------------------
    config.setdefault(
        "duration",
        {
            "lightcurve_path": "",
            "p0": 0.05,
            "is_rate": False,
            "panels": ["ged"],
        },
    )
    # Fast ARM-gated HEALPix localization (Li & Ma), YAML-driven reimplementation of fast_localize_grb.
    config.setdefault(
        "fast_localize",
        {
            "off_pre": 20.0,
            "off_gap": 5.0,
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
        },
    )
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
    
    grb_tstart, grb_tstop, grb_duration = fhf._infer_time_bounds_from_fits(str(config["source_path"]))
    print(
        "[preprocess_data] Inferred tstart/tstop/duration from source FITS: "
        f"{grb_tstart} -> {grb_tstop} -> {grb_duration}"
    )
    config["tstart"] = grb_tstart - eps_time
    config["tstop"] = grb_tstop + eps_time
    config["duration"] = grb_duration

    config_path = config.get("config_path", str(products_dir / "pipeline_config.yaml"))
    fhf._save_yaml(config_path, config)
    return config_path


#########################################################
# TASK 2: Unbinned_Light_Curve_Generation
# WIP: Implement the unbinned light curve generation
#########################################################
def unbinned_light_curve_generation(config_path: str) -> str:
    """
    Generate an ARM-gated unbinned light curve from event FITS input.

    This is a YAML-driven reimplementation of fast_grb/fast_ops_grb_timeseries.py.
    It preserves the plotting/diagnostics logic while integrating with pipeline config.
    """
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
        fhf._save_yaml(config["unbinned_lightcurve_yaml"], payload)
        fhf._save_yaml(config_path, config)
        return config["unbinned_lightcurve_yaml"]

    from astropy.io import fits
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    with fits.open(source_path, memmap=True) as hdul:
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

    src = SkyCoord(l=l_deg * u.deg, b=b_deg * u.deg, frame="galactic")
    reco = SkyCoord(l=np.asarray(l_evt) * u.deg, b=np.asarray(b_evt) * u.deg, frame="galactic")
    arm = reco.separation(src).deg - np.asarray(phi_deg)
    m_arm = (arm >= arm_min) & (arm <= arm_max)
    t_g = t[m_arm]
    n_gated = int(t_g.size)

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

    out_lc = os.path.join(plots_dir, f"{out_prefix}_lc.{fmt}")
    plt.figure(figsize=(8, 4))
    plt.step(centers, counts_gated, where="mid", color="k")
    plt.xlabel("Time [s]")
    plt.ylabel(f"Counts / {bin_size:.3f} s")
    plt.title(f"ARM-gated LC [{arm_min:.1f},{arm_max:.1f}] deg | l={l_deg:.3f}, b={b_deg:.3f}")
    plt.grid(True, ls="--", alpha=0.5)
    plt.savefig(out_lc, dpi=200, bbox_inches="tight")
    plt.close()

    out_diag = None
    if diagnostics:
        out_diag = os.path.join(plots_dir, f"{out_prefix}_diagnostics.{fmt}")
        fig, ax = plt.subplots(2, 2, figsize=(14, 10), constrained_layout=True)
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
        fig.suptitle(f"GRB time-series diagnostics — {out_prefix}", fontsize=14)
        fig.savefig(out_diag, dpi=200, bbox_inches="tight")
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
    fhf._save_yaml(config["unbinned_lightcurve_yaml"], payload)
    fhf._save_yaml(config_path, config)
    return config["unbinned_lightcurve_yaml"]


#########################################################
# TASK 3: Binning
# WIP: Implement the binning
#########################################################
def bin_data(
    config_path: str,
) -> tuple[str, str]:
    """
    Bin source and background data with a single entrypoint.
    Returns (source_binned_file_path, background_binned_file_path).

    Args:
        config_path: Path to the yaml configuration file.
    """
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    source_path = config["source_path"]
    background_path = config["background_path"]
    tstart = config["tstart"]
    tstop = config["tstop"]

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
        data_folder = os.path.dirname(unbinned_file_path)

        print(f"[bin_data:{label}] Found unbinned fits file: {unbinned_file_path}")

        extension = unbinned_file_path.split(".")[-1]
        binned_file_name = unbinned_file_path.replace("_unbinned_", "_binned_").replace(f".{extension}", "")
        binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")

        print(f"[bin_data:{label}] Expected output file: {binned_file_path}")

        if os.path.exists(binned_file_path):
            print(f"[bin_data:{label}] Binned file already exists: {binned_file_path}")
            print(f"[bin_data:{label}] Skipping binning step.")
            return binned_file_path

        print(f"[bin_data:{label}] Binned file not found. Proceeding with binning process...")
        print(f"[bin_data:{label}] Creating binning configuration...")

        config = {
            "data_file": unbinned_file_path,
            "ori_file": "NA",
            "unbinned_output": "fits",
            "time_bins": 1,
            "energy_bins": [100.0, 158.489, 251.189, 398.107, 630.957, 1000.0, 1584.89, 2511.89, 3981.07, 6309.57, 10000.0],
            "phi_pix_size": 6,
            "nside": 8,
            "scheme": "ring",
            "tstart": tstart,
            "tstop": tstop,
        }

        inputs_path = os.path.join(data_folder, inputs_filename)
        with open(inputs_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False)

        print(f"[bin_data:{label}] Created binning configuration: {inputs_path}")
        print(f"[bin_data:{label}] Initializing COSIpy BinnedData analysis...")
        analysis = BinnedData(inputs_path)
        print(f"[bin_data:{label}] BinnedData analysis object created successfully")

        print(f"[bin_data:{label}] Starting binning process for output: {binned_file_name}")
        analysis.get_binned_data(
            unbinned_data=unbinned_file_path,
            output_name=binned_file_name,
            psichi_binning="local",
        )

        print(f"[bin_data:{label}] Binning completed successfully")
        print(f"[bin_data:{label}] Output file created: {binned_file_path}")

        if os.path.exists(binned_file_path):
            file_size = os.path.getsize(binned_file_path) / (1024 * 1024)
            print(f"[bin_data:{label}] Output file verified: {file_size:.2f} MB")
        else:
            print(f"[bin_data:{label}] Warning: Expected output file not found: {binned_file_path}")

        return binned_file_path

    source_binned_file_path = _bin_single_data(
        unbinned_file_path=source_path,
        label="source",
        inputs_filename="inputs_grb.yaml",
        tstart=tstart,
        tstop=tstop,
    )

    background_binned_file_path = _bin_single_data(
        unbinned_file_path=background_path,
        label="background",
        inputs_filename="inputs_bg.yaml",
        tstart=tstart,
        tstop=tstop,
    )

    return source_binned_file_path, background_binned_file_path


# =============================================================================
# Fast GRB localization (Li & Ma HEALPix map) — helpers (notebook logic)
# =============================================================================
def _fl_load_events_simple(
    fits_path: str,
    col_time: str = "TimeTags",
    col_l: str = "Chi galactic",
    col_b: str = "Psi galactic",
    col_phi: str = "Phi",
    memmap: bool = True,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    from astropy.io import fits

    with fits.open(fits_path, memmap=memmap) as hdul:
        data = hdul[1].data
        t = np.asarray(data[col_time], dtype=np.float64)
        lon = np.asarray(data[col_l], dtype=np.float64)
        lat = np.asarray(data[col_b], dtype=np.float64)
        phi_deg = np.degrees(np.asarray(data[col_phi], dtype=np.float64))
    return t, lon, lat, phi_deg


def _fl_lb_to_unitvec(l_deg: np.ndarray, b_deg: np.ndarray) -> np.ndarray:
    ll = np.radians(np.asarray(l_deg, dtype=np.float64))
    bb = np.radians(np.asarray(b_deg, dtype=np.float64))
    cb = np.cos(bb)
    x = cb * np.cos(ll)
    y = cb * np.sin(ll)
    z = np.sin(bb)
    return np.vstack([x, y, z]).T


def _fl_healpix_pixel_area_sr(nside: int) -> float:
    return float(4.0 * np.pi / (12.0 * nside**2))


def _fl_healpix_mean_spacing_deg(nside: int) -> float:
    a_pix_sr = _fl_healpix_pixel_area_sr(nside)
    a_pix_deg2 = a_pix_sr * (180.0 / np.pi) ** 2
    return float(np.sqrt(a_pix_deg2))


def _fl_healpix_res_from_nside(nside: int) -> int:
    return int(np.round(np.log2(int(nside))))


def _fl_wrap_lon_deg(l_deg: float) -> float:
    return ((l_deg + 180.0) % 360.0) - 180.0


def _fl_angsep_deg(l1: float, b1: float, l2: float, b2: float) -> float:
    v1 = _fl_lb_to_unitvec(np.array([l1]), np.array([b1]))[0]
    v2 = _fl_lb_to_unitvec(np.array([l2]), np.array([b2]))[0]
    c = np.clip(np.dot(v1, v2), -1.0, 1.0)
    return float(np.degrees(np.arccos(c)))


def _fl_li_ma(Non: np.ndarray, Noff: np.ndarray, alpha: float) -> np.ndarray:
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
    event_chunk: int = 200_000,
) -> np.ndarray:
    n = event_cvec.shape[0]
    m = svecs.shape[0]
    counts = np.zeros(m, dtype=np.int64)
    for j0 in range(0, n, event_chunk):
        j1 = min(j0 + event_chunk, n)
        c = event_cvec[j0:j1]
        phi = event_phi_deg[j0:j1]
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
    event_chunk: int = 200_000,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    import healpy as hp

    npix = hp.nside2npix(nside)
    sig = np.zeros(npix, dtype=np.float64)
    non = np.zeros(npix, dtype=np.int64)
    noff = np.zeros(npix, dtype=np.int64)
    svec_all = np.array(hp.pix2vec(nside, np.arange(npix))).T
    for i0 in range(0, npix, pix_chunk):
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
    header = (
        "Res,NSIDE,Npix,Mean_spacing_deg,Area_sr,Best_l_deg,Best_b_deg,Best_S_sigma,"
        "Offset_deg,Map_time_s"
    )
    arr = np.array(rows, dtype=float)
    np.savetxt(out_path, arr, delimiter=",", header=header, comments="")
    return out_path


def _fl_save_timing_csv(out_path: str, nside: int, t_io: float, t_prep: float, t_map: float, t_total: float) -> str:
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
) -> str:
    import healpy as hp

    mean_spacing = _fl_healpix_mean_spacing_deg(nside)
    offset = None
    if true_l is not None and true_b is not None:
        offset = _fl_angsep_deg(true_l, true_b, best_l, best_b)
    title = (
        f"ARM-gated HEALPix Significance Map (NSIDE={nside}, mean spacing≈{mean_spacing:.2f}°)\n"
        f"ON=[{on_start:.3f},{on_stop:.3f}]  OFF=[{off_start:.3f},{off_stop:.3f}]  "
        f"alpha={alpha:.3f}  ARM=[{arm_min:.1f},{arm_max:.1f}]°\n"
        f"Max pixel significance: {best_s:.2f} σ"
        + (f" | Offset = {offset:.2f}°" if offset is not None else "")
    )
    plt.figure(figsize=(10, 6), dpi=220)
    hp.mollview(sig_map, title=title, unit="σ", cmap="inferno")
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
            f"True:  (l,b)=({true_lw:.2f}°, {true_b:.2f}°)\n"
            f"Reco: (l,b)=({best_lw:.2f}°, {best_b:.2f}°)\n"
            f"S_max = {best_s:.2f} σ\n"
            f"Offset = {offset:.2f}°"
        )
    else:
        info = f"Reco: (l,b)=({best_lw:.2f}°, {best_b:.2f}°)\n" f"S_max = {best_s:.2f} σ"
    ax = plt.gca()
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
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    return out_path


def _fl_save_nside_table_png(out_path: str, rows: list[list[float]]) -> str:
    col_labels = [
        "Res",
        "NSIDE",
        "NPixels",
        "Mean Spacing (deg)",
        "Area (sterad)",
        "Best l (deg)",
        "Best b (deg)",
        "Smax (σ)",
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
                f"{off:.2f}" if np.isfinite(off) else "—",
                f"{tm:.2f}",
            ]
        )
    fig, ax = plt.subplots(figsize=(14, 2.4 + 0.38 * max(len(disp), 1)), dpi=240)
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
    ax.set_title("HEALPix Pixel Information (Run Summary)", fontsize=14, weight="bold", pad=14)
    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
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
    from astropy.utils.exceptions import AstropyWarning

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
    arm_min = float(fl.get("arm_min", -13.0))
    arm_max = float(fl.get("arm_max", 13.0))

    on_start, on_stop = tstart, tstop
    off_start = tstart - off_pre
    off_stop = tstart - off_gap
    if off_stop <= off_start:
        raise ValueError("OFF window invalid: need off_pre > off_gap.")
    ton = on_stop - on_start
    toff = off_stop - off_start
    alpha = ton / toff

    col_time = str(fl.get("col_time", "TimeTags"))
    col_l = str(fl.get("col_l", "Chi galactic"))
    col_b = str(fl.get("col_b", "Psi galactic"))
    col_phi = str(fl.get("col_phi", "Phi"))

    t0_io = time.perf_counter()
    t_grb, l_grb, b_grb, phi_grb = _fl_load_events_simple(
        source_path, col_time, col_l, col_b, col_phi, memmap=True
    )
    t_bkg, l_bkg, b_bkg, phi_bkg = _fl_load_events_simple(
        background_path, col_time, col_l, col_b, col_phi, memmap=True
    )
    t_io = time.perf_counter() - t0_io

    t0_prep = time.perf_counter()
    m_on_grb = (t_grb >= on_start) & (t_grb < on_stop)
    m_on_bkg = (t_bkg >= on_start) & (t_bkg < on_stop)
    on_l = np.concatenate([l_grb[m_on_grb], l_bkg[m_on_bkg]])
    on_b = np.concatenate([b_grb[m_on_grb], b_bkg[m_on_bkg]])
    on_phi = np.concatenate([phi_grb[m_on_grb], phi_bkg[m_on_bkg]])

    m_off_bkg = (t_bkg >= off_start) & (t_bkg < off_stop)
    off_l = l_bkg[m_off_bkg]
    off_bb = b_bkg[m_off_bkg]
    off_phi = phi_bkg[m_off_bkg]

    if on_l.size == 0:
        raise RuntimeError("No ON events found in [tmin,tmax].")
    if off_l.size == 0:
        raise RuntimeError(
            "No OFF background events found in [tmin-off_pre, tmin-off_gap]."
        )

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
        },
        "arm_gate_deg": {"min": arm_min, "max": arm_max},
        "n_on_events": int(on_l.size),
        "n_off_events": int(off_l.size),
        "timings_s": {"t_io": float(t_io), "t_prep": float(t_prep)},
        "wall_t0_unix": float(wall_t0),
        "source_path": source_path,
        "background_path": background_path,
    }
    config["fast_localize"] = {**fl, **payload}
    config["fast_localize_prep_yaml"] = str(prep_yaml)
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
    import healpy as hp

    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    fl = dict(config.get("fast_localize", {}))
    data_dir = config["data_dir"]
    _, products_dir = _ensure_pipeline_dirs(data_dir)
    products_dir = Path(products_dir)

    if fl.get("status") == "skipped":
        out = products_dir / f"{fl.get('out_prefix', 'grb')}_fast_localize_skymap.yaml"
        p = {"status": "skipped", "reason": "fast_localize prep was skipped"}
        config["fast_localize_skymap_yaml"] = str(out)
        fhf._save_yaml(str(out), p)
        fhf._save_yaml(config_path, config)
        return str(out)
    out_prefix = str(fl.get("out_prefix", "grb"))
    skymap_yaml = products_dir / f"{out_prefix}_fast_localize_skymap.yaml"

    prep_npz = fl.get("prep_npz")
    if not prep_npz or not Path(prep_npz).is_file():
        raise FileNotFoundError(
            "Missing fast_localize prep .npz. Run light_curve_analysis first."
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
    config["fast_localize"] = {**fl, **payload}
    config["fast_localize_skymap_yaml"] = str(skymap_yaml)
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
    import healpy as hp

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
        p = {"status": "skipped"}
        config["fast_localize_results_yaml"] = str(results_yaml)
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
        _fl_save_nside_table_png(out_tbl, summary_rows)
    else:
        out_sum = None
        out_tbl = None

    ulc = dict(config.get("unbinned_light_curve", {}))
    ulc["l"] = best_l
    ulc["b"] = best_b
    config["unbinned_light_curve"] = ulc

    tsmap_existing = config.get("tsmap")
    if isinstance(tsmap_existing, dict):
        tsmap_merged = dict(tsmap_existing)
        tsmap_merged["fast_localize_coordinates"] = {"l_deg": best_l, "b_deg": best_b}
        config["tsmap"] = tsmap_merged

    if bool(fl.get("make_lc", False)):
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
                str(config["tmin"]),
                "--tmax",
                str(config["tmax"]),
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
    config["fast_localize"] = {**fl, **results_payload}
    config["fast_localize_results_yaml"] = str(results_yaml)
    fhf._save_yaml(str(results_yaml), results_payload)
    fhf._save_yaml(config_path, config)
    return str(results_yaml)

#########################################################
# TASK 6: TS_Map_on_different_timescales
# IMPLEMENTED: Implement the TS map on different timescales
#########################################################
def compute_ts_map(
    config_path: str) -> str:
    """
    Compute TS map for the source and background data.
    Returns the path to the TS map file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")


    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    import logging
    logging.basicConfig(level = logging.INFO)
    import gc
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from astropy.time import Time
    from cosipy import FastTSMap, MOCTSMap, SpacecraftHistory
    from histpy import Histogram
    from mhealpy import HealpixMap
    from threeML import Powerlaw

    # Define the spectrum
    spectrum_cfg = config.get("default_spectrum", {})
    spectrum = Powerlaw()
    spectrum.index.value = float(spectrum_cfg.get("index", -2.2))
    spectrum.K.value = float(spectrum_cfg.get("K", 10.0))
    spectrum.piv.value = float(spectrum_cfg.get("piv", 100.0))
    spectrum.K.unit = u.Unit(spectrum_cfg.get("K_unit", "1 / (cm2 keV s)"))
    spectrum.piv.unit = u.Unit(spectrum_cfg.get("piv_unit", "keV"))
    # Open the source data
    data, bkg_model = fhf.aggregate_data(config["source_path"], config["background_path"], config["tstart"], config["tstop"])
    # Get the tsmap configuration
    tsmap_cfg = config.get("tsmap", {})
    # Get the nside
    nside = int(config.get("tsmap_nside", tsmap_cfg.get("nside", 16)))
    # Get the energy channel
    energy_channel = config.get("tsmap_energy_channel", tsmap_cfg.get("energy_channel", [2, 3]))
    # Get the cpu cores
    cpu_cores = int(config.get("tsmap_cpu_cores", tsmap_cfg.get("cpu_cores", 8)))

    # Open the orientation file
    ori_full = SpacecraftHistory.open(config["orientation_path"])
    grb_ori = ori_full.select_interval(
        Time(config["tstart"], format = "unix"), 
        Time(config["tstop"], format = "unix"))

    # Task 7.1 - FastTSMap
    fast = FastTSMap(
        data=data,
        bkg_model=bkg_model,
        orientation=grb_ori,
        response_path=config["response_path"],
        cds_frame="local",
    )
    fast_ts = fast.fit(
        nside=nside,
        energy_channel=energy_channel,
        spectrum=spectrum,
        cpu_cores=cpu_cores,
    )
    fast_idx = int(np.argmax(fast_ts))
    fast_max_ts = float(np.max(fast_ts))
    fast_map = HealpixMap(nside=nside, scheme="nested", coordsys="galactic")
    fast_coo = fast_map.pix2skycoord(fast_idx)
    fast_l = float(fast_coo.l.value)
    fast_b = float(fast_coo.b.value)
    fast_coord = SkyCoord(l=fast_l, b=fast_b, unit=(u.deg, u.deg), frame="galactic")
    fast_plot_path = plots_dir / "tsmap_fast.png"
    fast.plot_ts(fast_ts, skycoord=fast_coord, 
                save_plot = True, save_dir = str(plots_dir),
                save_name = "tsmap_fast.png", dpi = 300)

    # Task 7.2 - MOCTSMap
    moc = MOCTSMap(
        data=data,
        bkg_model=bkg_model,
        response_path=config["response_path"],
        orientation=grb_ori,
        cds_frame="local",
    )
    moc_ts, moc_uniq = moc.fit(
        max_nside=nside,
        energy_channel=energy_channel,
        spectrum=spectrum,
    )
    moc_idx = int(np.argmax(moc_ts))
    moc_max_ts = float(moc_ts[moc_idx])
    max_uniq = int(moc_uniq[moc_idx])
    order = int(np.floor(np.log2(max_uniq / 4) / 2))
    moc_nside = 2 ** order
    moc_pix = int(max_uniq - 4 * moc_nside * moc_nside)
    moc_map = HealpixMap(nside=moc_nside, scheme="nested", coordsys="galactic")
    moc_coo = moc_map.pix2skycoord(moc_pix)
    moc_l = float(moc_coo.l.value)
    moc_b = float(moc_coo.b.value)
    moc_coord = SkyCoord(l=moc_l, b=moc_b, unit=(u.deg, u.deg), frame="galactic")
    moc_plot_path = plots_dir / "tsmap_moc.png"
    MOCTSMap.plot_ts(moc_ts, moc_uniq, skycoord=moc_coord, 
                save_plot = True, save_dir = str(plots_dir),
                save_name = "tsmap_moc.png", dpi = 300)

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
        "selected_method": "moc",
        "selected_coordinates": {"l_deg": moc_l, "b_deg": moc_b},
    }
    config["tsmap"] = tsmap_payload
    config["tsmap_yaml"] = str(products_dir / "tsmap_results.yaml")

    fhf._save_yaml(config["tsmap_yaml"], tsmap_payload)
    fhf._save_yaml(config_path, config)
    return config["tsmap_yaml"]

#########################################################
# TASK 7: Light_Curve
# IMPLEMENTED: Implement the light curve
#########################################################
def light_curve(
    config_path: str) -> str:
    """
    Generate the light curve of the source and background data.
    Returns the path to the light curve file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    config = fhf._load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

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
    
    lightcurve_nside = int(config["light_curve"]["nside"])

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
        psr_norm = psr_map / np.sum(psr_map, axis=-1, keepdims=True)
        sort_idx = np.argsort(psr_norm, axis=-1)[..., ::-1]
        sorted_vals = np.take_along_axis(psr_norm, sort_idx, axis=-1)
        cumsum_vals = np.cumsum(sorted_vals, axis=-1)
        mask_sorted = (cumsum_vals < containment).astype(float)
        mask = np.empty_like(mask_sorted)
        np.put_along_axis(mask, sort_idx, mask_sorted, axis=-1)
        return mask

    def _load_data(signal_full: Histogram, bkg_full: Histogram, window_start: float, window_stop: float) -> np.ndarray:
        """
        Load the data from the signal and background files.
        Args:
            signal_full: Signal full.
            bkg_full: Background full.
            window_start: Window start.
            window_stop: Window stop.
        """
        def _time_to_index(edges, t):
            # index of the left edge <= t (clipped to valid limits)
            idx = np.searchsorted(edges, t, side="left")
            idx = np.clip(idx, 0, len(edges) - 1)
            return idx
        bkg_edges = bkg_full.axes["Time"].edges.value
        sig_edges = signal_full.axes["Time"].edges.value

        # robust indices (no == comparison on float)
        bkg_tmin_idx = _time_to_index(bkg_edges, window_start)
        bkg_tmax_idx = _time_to_index(bkg_edges, window_stop)

        sig_tmin_idx = _time_to_index(sig_edges, window_start)
        sig_tmax_idx = _time_to_index(sig_edges, window_stop)

        # ensure at least 1 bin
        if bkg_tmax_idx <= bkg_tmin_idx:
            bkg_tmax_idx = min(bkg_tmin_idx + 1, len(bkg_edges) - 1)
        if sig_tmax_idx <= sig_tmin_idx:
            sig_tmax_idx = min(sig_tmin_idx + 1, len(sig_edges) - 1)

        bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx, :].project(["Em", "Phi", "PsiChi"])
        signal = signal_full.slice[sig_tmin_idx:sig_tmax_idx, :].project(["Em", "Phi", "PsiChi"])

        return (signal + bkg).contents
    
    # Build the PSR map and the mask map
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
    mask_map = _mask_from_cumdist_vectorized(input_psr, containment=0.5)
    
    counts = []
    time_centers = []
    bin_size = config["light_curve"]["bin_size"]
    eps_preburst = config["binning_data"]["eps_bkg_preburst"]
    eps_postburst = config["binning_data"]["eps_bkg_postburst"]
    tstart = config["tstart"]
    tstop = config["tstop"]
    
    i = tstart - eps_preburst
    tend = tstop - bin_size + eps_postburst
    while i < tend:
        j = i + bin_size
        data_map = _load_data(signal_full, bkg_full, i, j)
        masked_data = mask_map * data_map
        counts.append(float(masked_data.sum()))
        time_centers.append(float((i + j) / 2.0))
        i = j

    # Build bin edges
    N = int(((config["tstop"] + 20) - (config["tstart"] - 20)) / bin_size)
    bins = np.linspace(config["tstart"] - 20, config["tstop"] + 20, N + 1)

    plt.figure(figsize=(10, 4))
    plt.step(bins, counts)
    plt.xlabel("Time (s)")
    plt.ylabel("Counts")
    plt.title(f"GRB light curve (bin = {bin_size}s)")
    plt.axvline(x=config["tstart"], linestyle="--", linewidth=1.5)

    lightcurve_plot_path = plots_dir / "lightcurve.png"
    plt.tight_layout()
    plt.savefig(lightcurve_plot_path, dpi=150)
    plt.close()

    lightcurve_payload = {
        "plot_path": str(lightcurve_plot_path),
        "bin_size": bin_size,
        "nside": lightcurve_nside,
        "time_bins": bins,
        "counts": counts,
        "used_coordinates": {"l_deg": lon, "b_deg": lat},
    }
    config["light_curve"] = lightcurve_payload
    config["lightcurve_yaml"] = str(products_dir / "lightcurve_results.yaml")

    fhf._save_yaml(config["lightcurve_yaml"], lightcurve_payload)
    fhf._save_yaml(config_path, config)
    return config["lightcurve_yaml"]

#########################################################
# TASK 8: Duration  
# IMPLEMENTED: Implement the duration
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
    panels = list(
        config.get(
            "duration_panels",
            duration_cfg.get("panels", ["ged"]),
        )
    )

    if lightcurve_path:
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
    ) = get_duration(lightcurve=lightcurve, p0=p0, isRate=is_rate, panels=panels)

    if bb_lc is not None and lc_sel is not None:
        plot_duration(lc_sel, bb_lc, tstart, tstop, save_path=str(plots_dir / "duration_plot.png"))

    duration_payload = {
        "input_lightcurve_path": lightcurve_path,
        "p0": p0,
        "is_rate": is_rate,
        "panels": panels,
        "lc_timebins": lc_timebins,
        "bb_lc_timebins": bb_lc_timebins,
        "tstart": float(tstart),
        "tstop": float(tstop),
        "t90": float(t90),
        "t90_err_low": float(t90_err_low),
        "t90_err_high": float(t90_err_high),
    }

    config["duration"] = duration_payload
    config["duration_yaml"] = str(products_dir / "duration_results.yaml")
    # aliases for downstream operators expecting top-level keys
    config["duration_lightcurve_path"] = lightcurve_path
    config["duration_p0"] = p0
    config["duration_is_rate"] = is_rate
    config["duration_panels"] = panels
    config["tstart"] = float(tstart)
    config["tstop"] = float(tstop)
    config["t90"] = float(t90)
    config["t90_err_low"] = float(t90_err_low)
    config["t90_err_high"] = float(t90_err_high)

    fhf._save_yaml(config["duration_yaml"], duration_payload)
    fhf._save_yaml(config_path, config)
    return config["duration_yaml"]


def get_duration(
    lightcurve: np.ndarray,
    p0: float = 0.05,
    isRate: bool = False,
    panels: list[str] = ["z0", "z1", "x0", "x1", "y0", "y1"],
) -> tuple[
    Any, 
    Any, 
    dict[str, dict[str, list[float]]], 
    dict[str, list[float]], 
    float, 
    float, 
    float, 
    float, 
    float
]:
    """
    Analyze a multi-detector light curve and estimate T90 via Bayesian Blocks.

    Args:
        lightcurve: Light curve data.
        p0: False alarm probability for Bayesian Blocks.
        isRate: If True, the input signal is a rate (counts/s) and is converted to counts/bin.
        panels: List of panels to process.

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
    from gdt.core.data_primitives import TimeBins
    from bctools.analysis import BayesianBlocksLightcurve

    data = lightcurve
    time = data[:, 0, 0]
    bin_width = time[1] - time[0]

    signal: dict[str, np.ndarray] = {}
    for i, panel in enumerate(panels):
        y = data[:, i, 1]
        if isRate:
            y = y * bin_width
        signal[panel] = y

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

    best_panel = max(panels, key=lambda p: max(lc[p].counts))
    lc_sel = lc[best_panel]

    try:
        bb_lc = BayesianBlocksLightcurve(lc_sel)
        bb_lc.compute_bayesian_blocks(p0=p0)
        signal_range = bb_lc.signal_range
        t90 = bb_lc.duration(quantile=0.9)
        t90_error = bb_lc.duration_error(0.9, nsamples=100)
        bb_lc_timebins = {
            "lo_edges": bb_lc.bb_lightcurve.lo_edges.tolist(),
            "hi_edges": bb_lc.bb_lightcurve.hi_edges.tolist(),
            "rates": bb_lc.bb_lightcurve.rates.tolist(),
        }
    except Exception as e:
        print(e)
        print("WARNING: Bayesian Blocks failed; returning sentinel values")
        return (
            None,
            None,
            lc_timebins,
            {},
            -9999.0,
            -9999.0,
            -9999.0,
            -9999.0,
            -9999.0,
        )

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
    plt.figure(figsize=(10,4))
    plt.plot(lc_sel.centroids, bb_lc.bkg_counts/lc_sel.exposure, color = 'red', ls = ':',
                    label = "Fitted background")
    plt.errorbar(lc_sel.centroids, lc_sel.rates, xerr = [lc_sel.centroids-lc_sel.lo_edges, 
    lc_sel.hi_edges-lc_sel.centroids],
                        yerr = lc_sel.rate_uncertainty, 
                        ls = 'none', color = '.7',
                        label = 'Raw data')
    # Signal light curve
    lc_bayes = bb_lc.bb_lightcurve
    # Bayesian blocks light curve
    plt.plot(np.append(lc_bayes.lo_edges, lc_bayes.hi_edges[-1]),
                    np.append(lc_bayes.rates, lc_bayes.rates[-1]),
                    drawstyle = 'steps-post',
                    label = 'Bayesian blocks')
    # Vertical lines showing the start and stop of the identified signal
    plt.axvline(bb_lc.signal_range.tstart, ls = "--", color = 'olive', label = "Signal start/stop")
    plt.axvline(bb_lc.signal_range.tstop, ls = "--", color = 'olive')
    # Add legend and grid
    plt.legend()
    plt.grid(True, alpha=0.3)
    # Save the plot
    if save_path:
        plt.savefig(save_path, dpi=150)
    plt.close()

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