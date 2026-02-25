#!/usr/bin/env python3
import argparse
import os
import warnings
import subprocess
import time
from typing import Optional

import numpy as np
import healpy as hp
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.utils.exceptions import AstropyWarning
from matplotlib.lines import Line2D


# ----------------------------
# FITS I/O
# ----------------------------
def load_events_simple(
    fits_path,
    col_time="TimeTags",
    col_l="Chi galactic",
    col_b="Psi galactic",
    col_phi="Phi",
    memmap=True,
):
    """Load required event columns (time, l, b, phi[deg])."""
    with fits.open(fits_path, memmap=memmap) as hdul:
        data = hdul[1].data
        t = np.asarray(data[col_time], dtype=np.float64)
        l = np.asarray(data[col_l], dtype=np.float64)
        b = np.asarray(data[col_b], dtype=np.float64)
        phi_deg = np.degrees(np.asarray(data[col_phi], dtype=np.float64))
    return t, l, b, phi_deg


# ----------------------------
# Geometry helpers
# ----------------------------
def lb_to_unitvec(l_deg, b_deg):
    """Galactic lon/lat [deg] -> unit vectors (N,3)."""
    l = np.radians(np.asarray(l_deg, dtype=np.float64))
    b = np.radians(np.asarray(b_deg, dtype=np.float64))
    cb = np.cos(b)
    x = cb * np.cos(l)
    y = cb * np.sin(l)
    z = np.sin(b)
    return np.vstack([x, y, z]).T


def healpix_pixel_area_sr(nside):
    """HEALPix pixel area in steradians."""
    return float(4.0 * np.pi / (12.0 * nside**2))


def healpix_mean_spacing_deg(nside):
    """Mean spacing ~ sqrt(pixel area) in deg."""
    A_pix_sr = healpix_pixel_area_sr(nside)
    A_pix_deg2 = A_pix_sr * (180.0 / np.pi) ** 2
    return float(np.sqrt(A_pix_deg2))


def healpix_res_from_nside(nside):
    """HEALPix 'Res' index used in the HEALPix table: Res = log2(NSIDE)."""
    # NSIDE should be power of 2; still handle safely.
    return int(np.round(np.log2(int(nside))))


def wrap_lon_deg(l_deg):
    """Wrap longitude to [-180, 180] for Mollweide readability."""
    return ((l_deg + 180.0) % 360.0) - 180.0


def angsep_deg(l1, b1, l2, b2):
    """Great-circle angular separation [deg] using unit vectors."""
    v1 = lb_to_unitvec([l1], [b1])[0]
    v2 = lb_to_unitvec([l2], [b2])[0]
    c = np.clip(np.dot(v1, v2), -1.0, 1.0)
    return float(np.degrees(np.arccos(c)))


# ----------------------------
# Li & Ma
# ----------------------------
def li_ma(Non, Noff, alpha):
    """Li & Ma (1983) Eq. 17; vector-safe."""
    Non = np.asarray(Non, dtype=np.float64)
    Noff = np.asarray(Noff, dtype=np.float64)
    alpha = float(alpha)

    S = np.zeros_like(Non, dtype=np.float64)
    m = (Non > 0) & (Noff > 0) & (alpha > 0)
    if not np.any(m):
        return S

    Non_m = Non[m]
    Noff_m = Noff[m]
    tot = Non_m + Noff_m

    term1 = Non_m * np.log(((1 + alpha) / alpha) * (Non_m / tot))
    term2 = Noff_m * np.log((1 + alpha) * (Noff_m / tot))
    S2 = 2.0 * (term1 + term2)
    S[m] = np.sqrt(np.maximum(S2, 0.0))
    return S


# ----------------------------
# Core counting (memory-safe)
# ----------------------------
def count_arm_pass_for_pixels_chunked(
    event_cvec, event_phi_deg,
    svecs,
    arm_min, arm_max,
    event_chunk=200_000,
):
    """
    Count ARM-pass events for each trial source vector in svecs, chunking over events.

    event_cvec: (N,3)
    event_phi_deg: (N,)
    svecs: (M,3)
    returns: (M,) int64 counts
    """
    N = event_cvec.shape[0]
    M = svecs.shape[0]
    counts = np.zeros(M, dtype=np.int64)

    for j0 in range(0, N, event_chunk):
        j1 = min(j0 + event_chunk, N)
        c = event_cvec[j0:j1]              # (Nc,3)
        phi = event_phi_deg[j0:j1]         # (Nc,)

        dots = c @ svecs.T                 # (Nc,M)
        np.clip(dots, -1.0, 1.0, out=dots)
        alpha_deg = np.degrees(np.arccos(dots))
        arm = alpha_deg - phi[:, None]
        m = (arm >= arm_min) & (arm <= arm_max)
        counts += np.count_nonzero(m, axis=0).astype(np.int64)

        # help GC on big runs
        del dots, alpha_deg, arm, m

    return counts


def build_significance_map(
    nside,
    on_cvec, on_phi_deg,
    off_cvec, off_phi_deg,
    arm_min, arm_max,
    alpha,
    pix_chunk=256,
    event_chunk=200_000,
):
    """
    Build HEALPix Li&Ma significance map in pixel chunks AND event chunks.
    Returns: sig_map, non_map, noff_map
    """
    npix = hp.nside2npix(nside)
    sig = np.zeros(npix, dtype=np.float64)
    non = np.zeros(npix, dtype=np.int64)
    noff = np.zeros(npix, dtype=np.int64)

    svec_all = np.array(hp.pix2vec(nside, np.arange(npix))).T  # (npix,3)

    for i0 in range(0, npix, pix_chunk):
        i1 = min(i0 + pix_chunk, npix)
        svecs = svec_all[i0:i1]  # (M,3)

        Non = count_arm_pass_for_pixels_chunked(
            on_cvec, on_phi_deg, svecs, arm_min, arm_max, event_chunk=event_chunk
        )
        Noff = count_arm_pass_for_pixels_chunked(
            off_cvec, off_phi_deg, svecs, arm_min, arm_max, event_chunk=event_chunk
        )

        non[i0:i1] = Non
        noff[i0:i1] = Noff
        sig[i0:i1] = li_ma(Non, Noff, alpha)

    return sig, non, noff


# ----------------------------
# Output helpers
# ----------------------------
def ensure_outdir(outdir):
    os.makedirs(outdir, exist_ok=True)


def save_localization_csv(out_path, nside, sig_map, topk=50):
    idx = np.argsort(sig_map)[::-1][:topk]
    rows = []
    for ipix in idx:
        theta, phi = hp.pix2ang(nside, int(ipix))
        l = float(np.degrees(phi))
        b = float(90.0 - np.degrees(theta))
        rows.append((int(ipix), l, b, float(sig_map[ipix])))

    header = "ipix,l_deg,b_deg,S_li_ma"
    np.savetxt(out_path, np.array(rows, dtype=float), delimiter=",", header=header, comments="")
    return out_path


def save_nside_summary_csv(out_path, rows):
    header = "Res,NSIDE,Npix,Mean_spacing_deg,Area_sr,Best_l_deg,Best_b_deg,Best_S_sigma,Offset_deg,Map_time_s"
    arr = np.array(rows, dtype=float)
    np.savetxt(out_path, arr, delimiter=",", header=header, comments="")
    return out_path


def save_timing_csv(out_path, nside, t_io, t_prep, t_map, t_total):
    header = "nside,t_io_s,t_prep_s,t_map_s,t_total_s"
    arr = np.array([[float(nside), float(t_io), float(t_prep), float(t_map), float(t_total)]], dtype=float)
    np.savetxt(out_path, arr, delimiter=",", header=header, comments="")
    return out_path


def save_pretty_significance_map(
    out_path,
    nside, sig_map,
    on_start, on_stop,
    off_start, off_stop,
    alpha, arm_min, arm_max,
    best_l, best_b, best_S,
    true_l=None, true_b=None,
):
    mean_spacing = healpix_mean_spacing_deg(nside)

    offset = None
    if (true_l is not None) and (true_b is not None):
        offset = angsep_deg(true_l, true_b, best_l, best_b)

    title = (
        f"ARM-gated HEALPix Significance Map (NSIDE={nside}, mean spacing≈{mean_spacing:.2f}°)\n"
        f"ON=[{on_start:.3f},{on_stop:.3f}]  OFF=[{off_start:.3f},{off_stop:.3f}]  "
        f"alpha={alpha:.3f}  ARM=[{arm_min:.1f},{arm_max:.1f}]°\n"
        f"Max pixel significance: {best_S:.2f} σ"
        + (f" | Offset = {offset:.2f}°" if offset is not None else "")
    )

    plt.figure(figsize=(10, 6), dpi=220)
    hp.mollview(sig_map, title=title, unit="σ", cmap="inferno")
    hp.graticule(color="white", alpha=0.35)

    best_lw = wrap_lon_deg(best_l)
    hp.projplot(best_lw, best_b, lonlat=True, marker="*", markersize=20,
                color="cyan", mec="black", mew=1.5)

    legend_handles = [
        Line2D([], [], marker="*", linestyle="None", markersize=12,
               color="cyan", mec="black", label="Reconstructed GRB"),
    ]

    if (true_l is not None) and (true_b is not None):
        true_lw = wrap_lon_deg(true_l)
        hp.projplot(true_lw, true_b, lonlat=True, marker="*", markersize=20,
                    color="magenta", mec="black", mew=1.5)
        legend_handles.insert(
            0,
            Line2D([], [], marker="*", linestyle="None", markersize=12,
                   color="magenta", mec="black", label="True GRB (reference)")
        )

        info = (
            f"True:  (l,b)=({true_lw:.2f}°, {true_b:.2f}°)\n"
            f"Reco: (l,b)=({best_lw:.2f}°, {best_b:.2f}°)\n"
            f"S_max = {best_S:.2f} σ\n"
            f"Offset = {offset:.2f}°"
        )
    else:
        info = (
            f"Reco: (l,b)=({best_lw:.2f}°, {best_b:.2f}°)\n"
            f"S_max = {best_S:.2f} σ"
        )

    ax = plt.gca()
    ax.legend(handles=legend_handles, loc="lower left",
              bbox_to_anchor=(0.02, -0.05), framealpha=0.92)

    ax.text(
        0.02, 0.98, info,
        transform=ax.transAxes,
        fontsize=9,
        color="white",
        ha="left", va="top",
        bbox=dict(facecolor="black", alpha=0.60, boxstyle="round,pad=0.35")
    )

    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    return out_path


def save_nside_table_png(out_path, rows):
    """
    Save a clean table figure (PNG) from summary rows.
    rows: list of [Res,NSIDE,Npix,Mean_spacing_deg,Area_sr,Best_l,Best_b,Best_S,Offset,Map_time_s]
    """
    col_labels = [
        "Res", "NSIDE", "NPixels", "Mean Spacing (deg)", "Area (sterad)",
        "Best l (deg)", "Best b (deg)", "Smax (σ)", "Offset (deg)", "Map time (s)"
    ]

    disp = []
    for r in rows:
        res, ns, npix, ms, area_sr, bl, bb, bs, off, tm = r
        disp.append([
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
        ])

    fig, ax = plt.subplots(figsize=(14, 2.4 + 0.38 * max(len(disp), 1)), dpi=240)
    ax.axis("off")
    tbl = ax.table(cellText=disp, colLabels=col_labels, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.25)

    # style header row + grid
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


# ----------------------------
# Programmatic entry point (DAG / ExternalPythonOperator)
# ----------------------------
def run_fast_localize_grb(
    data: str,
    bkg: str,
    tmin: float,
    tmax: float,
    off_pre: float = 20.0,
    off_gap: float = 5.0,
    arm_min: float = -13.0,
    arm_max: float = 13.0,
    nside: int = 32,
    nsides: Optional[str] = None,
    pix_chunk: int = 256,
    event_chunk: int = 200000,
    topk: int = 50,
    outdir: str = "outputs",
    out_prefix: str = "grb",
    true_l: Optional[float] = None,
    true_b: Optional[float] = None,
    no_warning: bool = False,
    make_lc: bool = False,
    lc_bin: float = 1.0,
    lc_arm_min: Optional[float] = None,
    lc_arm_max: Optional[float] = None,
    lc_diagnostics: bool = False,
    lc_script: str = "make_timeseries_all.py",
):
    """
    Programmatic entry point: same logic as main(), callable from DAGs or other code.
    Returns the output directory path.
    """
    if no_warning:
        warnings.filterwarnings("ignore", message="Could not memory map array", category=AstropyWarning)

    ensure_outdir(outdir)
    t0_total = time.perf_counter()

    on_start = tmin
    on_stop = tmax
    off_start = tmin - off_pre
    off_stop = tmin - off_gap

    if off_stop <= off_start:
        raise ValueError("OFF window invalid: need off_pre > off_gap.")

    Ton = on_stop - on_start
    Toff = off_stop - off_start
    alpha = Ton / Toff

    t0_io = time.perf_counter()
    t_grb, l_grb, b_grb, phi_grb = load_events_simple(data)
    t_bkg, l_bkg, b_bkg, phi_bkg = load_events_simple(bkg)
    t_io = time.perf_counter() - t0_io

    t0_prep = time.perf_counter()
    m_on_grb = (t_grb >= on_start) & (t_grb < on_stop)
    m_on_bkg = (t_bkg >= on_start) & (t_bkg < on_stop)
    on_l = np.concatenate([l_grb[m_on_grb], l_bkg[m_on_bkg]])
    on_b = np.concatenate([b_grb[m_on_grb], b_bkg[m_on_bkg]])
    on_phi = np.concatenate([phi_grb[m_on_grb], phi_bkg[m_on_bkg]])

    m_off_bkg = (t_bkg >= off_start) & (t_bkg < off_stop)
    off_l = l_bkg[m_off_bkg]
    off_b = b_bkg[m_off_bkg]
    off_phi = phi_bkg[m_off_bkg]

    if on_l.size == 0:
        raise RuntimeError("No ON events found in [tmin,tmax].")
    if off_l.size == 0:
        raise RuntimeError("No OFF background events found in [tmin-off_pre, tmin-off_gap].")

    on_cvec = lb_to_unitvec(on_l, on_b)
    off_cvec = lb_to_unitvec(off_l, off_b)
    on_phi_deg = np.asarray(on_phi, dtype=np.float64)
    off_phi_deg = np.asarray(off_phi, dtype=np.float64)
    t_prep = time.perf_counter() - t0_prep

    
    nsides_list = [nside]

    summary_rows = []
    best_for_main_nside = None

    for nside_run in nsides_list:
        t0_map = time.perf_counter()
        sig_map, non_map, noff_map = build_significance_map(
            nside=nside_run,
            on_cvec=on_cvec, on_phi_deg=on_phi_deg,
            off_cvec=off_cvec, off_phi_deg=off_phi_deg,
            arm_min=arm_min, arm_max=arm_max,
            alpha=alpha,
            pix_chunk=pix_chunk,
            event_chunk=event_chunk,
        )
        t_map = time.perf_counter() - t0_map

        ip_best = int(np.argmax(sig_map))
        th, ph = hp.pix2ang(nside_run, ip_best)
        best_l = float(np.degrees(ph))
        best_b = float(90.0 - np.degrees(th))
        best_S = float(sig_map[ip_best])

        mean_spacing = healpix_mean_spacing_deg(nside_run)
        area_sr = healpix_pixel_area_sr(nside_run)
        res = healpix_res_from_nside(nside_run)
        offset = np.nan
        if (true_l is not None) and (true_b is not None):
            offset = angsep_deg(true_l, true_b, best_l, best_b)

        summary_rows.append([
            float(res), float(nside_run), float(hp.nside2npix(nside_run)),
            float(mean_spacing), float(area_sr), float(best_l), float(best_b),
            float(best_S), float(offset), float(t_map),
        ])

        if (len(nsides_list) == 1) or (nside_run == nside):
            best_for_main_nside = (nside_run, sig_map, best_l, best_b, best_S)

    t_total = time.perf_counter() - t0_total

    if nsides is not None:
        out_sum = os.path.join(outdir, f"{out_prefix}_nside_summary.csv")
        save_nside_summary_csv(out_sum, summary_rows)
        print(f"Wrote: {out_sum}")
        out_tbl = os.path.join(outdir, f"{out_prefix}_nside_summary.png")
        save_nside_table_png(out_tbl, summary_rows)
        print(f"Wrote: {out_tbl}")

    main_nside_for_timing = nside
    t_map_main = np.nan
    for r in summary_rows:
        if int(r[1]) == int(main_nside_for_timing):
            t_map_main = float(r[-1])
            break
    if not np.isfinite(t_map_main):
        t_map_main = float(summary_rows[0][-1])

    timing_csv = os.path.join(outdir, f"{out_prefix}_timing.csv")
    save_timing_csv(timing_csv, main_nside_for_timing, t_io, t_prep, t_map_main, t_total)
    print(f"Wrote: {timing_csv}")

    print("\n========== PIPELINE PERFORMANCE ==========")
    print(f"FITS I/O time           : {t_io:8.2f} s")
    print(f"Event prep time         : {t_prep:8.2f} s")
    print(f"TOTAL pipeline time     : {t_total:8.2f} s")
    print("==========================================\n")

    if nsides is not None:
        print("Per-NSIDE map runtime:")
        for r in summary_rows:
            res, ns, _, _, _, _, _, smax, off, mt = r
            if np.isfinite(off):
                print(f"  RES={int(res):2d}  NSIDE={int(ns):4d}  map_time={mt:7.2f} s  Smax={smax:6.2f} σ  offset={off:6.2f}°")
            else:
                print(f"  RES={int(res):2d}  NSIDE={int(ns):4d}  map_time={mt:7.2f} s  Smax={smax:6.2f} σ")
        print("")

    if best_for_main_nside is None:
        raise RuntimeError("Internal error: did not store best result for main NSIDE.")

    nside_out, sig_map, best_l, best_b, best_S = best_for_main_nside
    print(f"BEST l={best_l:.6f} deg  b={best_b:.6f} deg  S={best_S:.3f} sigma  (NSIDE={nside_out})")

    out_csv = os.path.join(outdir, f"{out_prefix}_localization_top{topk}.csv")
    save_localization_csv(out_csv, nside_out, sig_map, topk=topk)
    print(f"Wrote: {out_csv}")

    out_png = os.path.join(outdir, f"{out_prefix}_significance_map.png")
    save_pretty_significance_map(
        out_png, nside_out, sig_map,
        on_start, on_stop, off_start, off_stop,
        alpha, arm_min, arm_max,
        best_l, best_b, best_S,
        true_l=true_l, true_b=true_b,
    )
    print(f"Wrote: {out_png}")

    if make_lc:
        lc_arm_min_val = arm_min if lc_arm_min is None else lc_arm_min
        lc_arm_max_val = arm_max if lc_arm_max is None else lc_arm_max
        cmd = [
            "python", lc_script,
            "--data", data,
            "--tmin", str(tmin),
            "--tmax", str(tmax),
            "--l", str(best_l),
            "--b", str(best_b),
            "--arm-min", str(lc_arm_min_val),
            "--arm-max", str(lc_arm_max_val),
            "--bin", str(lc_bin),
            "--outdir", outdir,
            "--out-prefix", out_prefix,
            "--no-warning",
        ]
        if lc_diagnostics:
            cmd.append("--diagnostics")
        print("Calling time-series script:", " ".join(cmd))
        subprocess.run(cmd, check=True)

    return outdir


# ----------------------------
# Main
# ----------------------------
def main():
    p = argparse.ArgumentParser(
        description="Fast GRB localization: HEALPix Li&Ma significance map using ARM-gated ON/OFF."
    )
    p.add_argument("--data", required=True, help="GRB unbinned FITS (events in HDU=1).")
    p.add_argument("--bkg", required=True, help="Background unbinned FITS (events in HDU=1).")

    p.add_argument("--tmin", type=float, required=True, help="Burst start time (absolute seconds).")
    p.add_argument("--tmax", type=float, required=True, help="Burst stop time (absolute seconds).")

    p.add_argument("--off-pre", type=float, default=20.0,
                   help="OFF window length before tmin [s]. OFF start = tmin-off-pre.")
    p.add_argument("--off-gap", type=float, default=5.0,
                   help="Gap before burst [s]. OFF stop = tmin-off-gap.")

    p.add_argument("--arm-min", type=float, default=-13.0, help="ARM gate min [deg].")
    p.add_argument("--arm-max", type=float, default=+13.0, help="ARM gate max [deg].")

    # Single NSIDE or sweep
    p.add_argument("--nside", type=int, default=32, help="HEALPix NSIDE for localization map.")
    p.add_argument("--nsides", default=None,
                   help="Optional sweep, e.g. '4,8,16,32,64'. If set, saves a summary CSV and table PNG.")

    p.add_argument("--pix-chunk", type=int, default=256, help="Pixel chunk size.")
    p.add_argument("--event-chunk", type=int, default=200000, help="Event chunk size (memory control).")
    p.add_argument("--topk", type=int, default=50, help="Number of top candidates saved to CSV.")

    p.add_argument("--outdir", default="outputs", help="Output directory.")
    p.add_argument("--out-prefix", default="grb", help="Output prefix.")

    p.add_argument("--true-l", type=float, default=None, help="Optional: true Galactic l for plotting/offset.")
    p.add_argument("--true-b", type=float, default=None, help="Optional: true Galactic b for plotting/offset.")

    p.add_argument("--no-warning", action="store_true",
                   help="Suppress Astropy mmap warning (recommended on cluster FS).")

    # Optional: call script #2 after localization
    p.add_argument("--make-lc", action="store_true",
                   help="After localization, call make_timeseries_all.py with best (l,b).")
    p.add_argument("--lc-bin", type=float, default=1.0, help="Bin size for time-series script.")
    p.add_argument("--lc-arm-min", type=float, default=None, help="ARM min for LC script (defaults to --arm-min).")
    p.add_argument("--lc-arm-max", type=float, default=None, help="ARM max for LC script (defaults to --arm-max).")
    p.add_argument("--lc-diagnostics", action="store_true", help="Pass --diagnostics to time-series script.")
    p.add_argument("--lc-script", default="make_timeseries_all.py", help="Path to script #2.")

    args = p.parse_args()

    if args.no_warning:
        warnings.filterwarnings("ignore", message="Could not memory map array", category=AstropyWarning)

    ensure_outdir(args.outdir)

    # ======= TOTAL timer starts here =======
    t0_total = time.perf_counter()

    # Define ON and OFF windows (absolute time)
    on_start = args.tmin
    on_stop = args.tmax
    off_start = args.tmin - args.off_pre
    off_stop = args.tmin - args.off_gap

    if off_stop <= off_start:
        raise ValueError("OFF window invalid: need off_pre > off_gap.")

    Ton = on_stop - on_start
    Toff = off_stop - off_start
    alpha = Ton / Toff

    # ---- I/O timing ----
    t0_io = time.perf_counter()
    t_grb, l_grb, b_grb, phi_grb = load_events_simple(args.data)
    t_bkg, l_bkg, b_bkg, phi_bkg = load_events_simple(args.bkg)
    t_io = time.perf_counter() - t0_io

    # ---- Prep timing ----
    t0_prep = time.perf_counter()

    # ON lists: GRB + BG within ON window
    m_on_grb = (t_grb >= on_start) & (t_grb < on_stop)
    m_on_bkg = (t_bkg >= on_start) & (t_bkg < on_stop)

    on_l = np.concatenate([l_grb[m_on_grb], l_bkg[m_on_bkg]])
    on_b = np.concatenate([b_grb[m_on_grb], b_bkg[m_on_bkg]])
    on_phi = np.concatenate([phi_grb[m_on_grb], phi_bkg[m_on_bkg]])

    # OFF lists: BG only within OFF window
    m_off_bkg = (t_bkg >= off_start) & (t_bkg < off_stop)
    off_l = l_bkg[m_off_bkg]
    off_b = b_bkg[m_off_bkg]
    off_phi = phi_bkg[m_off_bkg]

    if on_l.size == 0:
        raise RuntimeError("No ON events found in [tmin,tmax].")
    if off_l.size == 0:
        raise RuntimeError("No OFF background events found in [tmin-off_pre, tmin-off_gap].")

    # Precompute cone-axis vectors
    on_cvec = lb_to_unitvec(on_l, on_b)
    off_cvec = lb_to_unitvec(off_l, off_b)
    on_phi_deg = np.asarray(on_phi, dtype=np.float64)
    off_phi_deg = np.asarray(off_phi, dtype=np.float64)

    t_prep = time.perf_counter() - t0_prep

    # Determine which nsides to run
    if args.nsides is None:
        nsides = [args.nside]
    else:
        nsides = [int(x.strip()) for x in args.nsides.split(",") if x.strip()]
        if len(nsides) == 0:
            raise ValueError("Parsed --nsides is empty. Example: --nsides 4,8,16,32,64")

    summary_rows = []
    best_for_main_nside = None

    # Run localization for each NSIDE requested
    for nside in nsides:
        t0_map = time.perf_counter()

        sig_map, non_map, noff_map = build_significance_map(
            nside=nside,
            on_cvec=on_cvec, on_phi_deg=on_phi_deg,
            off_cvec=off_cvec, off_phi_deg=off_phi_deg,
            arm_min=args.arm_min, arm_max=args.arm_max,
            alpha=alpha,
            pix_chunk=args.pix_chunk,
            event_chunk=args.event_chunk,
        )

        t_map = time.perf_counter() - t0_map

        ip_best = int(np.argmax(sig_map))
        th, ph = hp.pix2ang(nside, ip_best)
        best_l = float(np.degrees(ph))
        best_b = float(90.0 - np.degrees(th))
        best_S = float(sig_map[ip_best])

        mean_spacing = healpix_mean_spacing_deg(nside)
        area_sr = healpix_pixel_area_sr(nside)
        res = healpix_res_from_nside(nside)

        offset = np.nan
        if (args.true_l is not None) and (args.true_b is not None):
            offset = angsep_deg(args.true_l, args.true_b, best_l, best_b)

        summary_rows.append([
            float(res),
            float(nside),
            float(hp.nside2npix(nside)),
            float(mean_spacing),
            float(area_sr),
            float(best_l),
            float(best_b),
            float(best_S),
            float(offset),
            float(t_map),
        ])

        if (len(nsides) == 1) or (nside == args.nside):
            best_for_main_nside = (nside, sig_map, best_l, best_b, best_S)

    # ======= TOTAL timer ends here =======
    t_total = time.perf_counter() - t0_total

    # Save NSIDE summary table if a sweep was requested
    if args.nsides is not None:
        out_sum = os.path.join(args.outdir, f"{args.out_prefix}_nside_summary.csv")
        save_nside_summary_csv(out_sum, summary_rows)
        print(f"Wrote: {out_sum}")

        out_tbl = os.path.join(args.outdir, f"{args.out_prefix}_nside_summary.png")
        save_nside_table_png(out_tbl, summary_rows)
        print(f"Wrote: {out_tbl}")

    # Save timing CSV for the "main" nside run (or only run)
    main_nside_for_timing = args.nside
    t_map_main = np.nan
    for r in summary_rows:
        if int(r[1]) == int(main_nside_for_timing):  # r[1] is NSIDE
            t_map_main = float(r[-1])
            break
    if not np.isfinite(t_map_main):
        t_map_main = float(summary_rows[0][-1])

    timing_csv = os.path.join(args.outdir, f"{args.out_prefix}_timing.csv")
    save_timing_csv(timing_csv, main_nside_for_timing, t_io, t_prep, t_map_main, t_total)
    print(f"Wrote: {timing_csv}")

    # Print performance report
    print("\n========== PIPELINE PERFORMANCE ==========")
    print(f"FITS I/O time           : {t_io:8.2f} s")
    print(f"Event prep time         : {t_prep:8.2f} s")
    print(f"TOTAL pipeline time     : {t_total:8.2f} s")
    print("==========================================\n")

    # Per-NSIDE map timing report (useful for sweeps)
    if args.nsides is not None:
        print("Per-NSIDE map runtime:")
        for r in summary_rows:
            res, ns, _, _, _, _, _, smax, off, mt = r
            if np.isfinite(off):
                print(f"  RES={int(res):2d}  NSIDE={int(ns):4d}  map_time={mt:7.2f} s  Smax={smax:6.2f} σ  offset={off:6.2f}°")
            else:
                print(f"  RES={int(res):2d}  NSIDE={int(ns):4d}  map_time={mt:7.2f} s  Smax={smax:6.2f} σ")
        print("")

    # Produce per-run products for main NSIDE
    if best_for_main_nside is None:
        raise RuntimeError("Internal error: did not store best result for main NSIDE.")

    nside, sig_map, best_l, best_b, best_S = best_for_main_nside

    # Print pipeline-friendly best result
    print(f"BEST l={best_l:.6f} deg  b={best_b:.6f} deg  S={best_S:.3f} sigma  (NSIDE={nside})")

    # Save CSV of top candidates
    out_csv = os.path.join(args.outdir, f"{args.out_prefix}_localization_top{args.topk}.csv")
    save_localization_csv(out_csv, nside, sig_map, topk=args.topk)
    print(f"Wrote: {out_csv}")

    # Save pretty map image (like your example)
    out_png = os.path.join(args.outdir, f"{args.out_prefix}_significance_map.png")
    save_pretty_significance_map(
        out_png, nside, sig_map,
        on_start, on_stop, off_start, off_stop,
        alpha, args.arm_min, args.arm_max,
        best_l, best_b, best_S,
        true_l=args.true_l, true_b=args.true_b,
    )
    print(f"Wrote: {out_png}")

    # Optional: call script #2 using best localization
    if args.make_lc:
        lc_arm_min = args.arm_min if args.lc_arm_min is None else args.lc_arm_min
        lc_arm_max = args.arm_max if args.lc_arm_max is None else args.lc_arm_max

        cmd = [
            "python", args.lc_script,
            "--data", args.data,
            "--tmin", str(args.tmin),
            "--tmax", str(args.tmax),
            "--l", str(best_l),
            "--b", str(best_b),
            "--arm-min", str(lc_arm_min),
            "--arm-max", str(lc_arm_max),
            "--bin", str(args.lc_bin),
            "--outdir", args.outdir,
            "--out-prefix", args.out_prefix,
            "--no-warning",
        ]
        if args.lc_diagnostics:
            cmd.append("--diagnostics")

        print("Calling time-series script:", " ".join(cmd))
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
