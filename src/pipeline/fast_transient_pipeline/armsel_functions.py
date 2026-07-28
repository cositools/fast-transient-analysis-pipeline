"""Scientific task functions for unbinned ARM selection and localization."""

from __future__ import annotations

from typing import Any

try:
    from .ged_functions import (
        _config_trigger_time,
        _ensure_pipeline_dirs,
        _is_fits_path,
        _now_iso,
        _plot_identity,
        _record_pipeline_task,
    )
except ImportError:
    from ged_functions import (
        _config_trigger_time,
        _ensure_pipeline_dirs,
        _is_fits_path,
        _now_iso,
        _plot_identity,
        _record_pipeline_task,
    )

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
