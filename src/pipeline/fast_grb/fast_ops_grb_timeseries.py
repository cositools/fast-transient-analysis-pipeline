#!/usr/bin/env python3
import argparse
import os
import warnings

import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.utils.exceptions import AstropyWarning


def calculate_arm(true_l, true_b, event_l, event_b, phi_deg):
    src = SkyCoord(l=true_l * u.deg, b=true_b * u.deg, frame="galactic")
    reco = SkyCoord(l=np.asarray(event_l) * u.deg, b=np.asarray(event_b) * u.deg, frame="galactic")
    sep_deg = reco.separation(src).deg
    return sep_deg - np.asarray(phi_deg)


def load_events(
    fits_path,
    col_time="TimeTags",
    col_l="Chi galactic",
    col_b="Psi galactic",
    col_phi="Phi",
):
    with fits.open(fits_path, memmap=True) as hdul:
        data = hdul[1].data
        t = data[col_time].astype(np.float64)
        l = data[col_l].astype(np.float64)
        b = data[col_b].astype(np.float64)
        phi_deg = np.rad2deg(data[col_phi].astype(np.float64))
    return t, l, b, phi_deg


def make_time_bins(tmin, tmax, bin_size):
    edges = np.arange(tmin, tmax + bin_size, bin_size, dtype=np.float64)
    centers = 0.5 * (edges[:-1] + edges[1:])
    return edges, centers


def histogram_counts(times, edges):
    c, _ = np.histogram(times, bins=edges)
    return c.astype(np.int64)


def ensure_outdir(path):
    os.makedirs(path, exist_ok=True)


def save_main_outputs(outdir, out_prefix, centers, counts, bin_size, l_deg, b_deg, arm_min, arm_max, fmt):
    out_csv = os.path.join(outdir, f"{out_prefix}_lc.csv")
    header = "t_center_s,counts"
    np.savetxt(out_csv, np.column_stack([centers, counts]), delimiter=",", header=header, comments="")

    out_fig = os.path.join(outdir, f"{out_prefix}_lc.{fmt}")
    plt.figure(figsize=(8, 4))
    plt.step(centers, counts, where="mid", color="k")
    plt.xlabel("Time [s]")
    plt.ylabel(f"Counts / {bin_size:.3f} s")
    plt.title(f"ARM-gated LC [{arm_min:.1f},{arm_max:.1f}] deg | l={l_deg:.3f}, b={b_deg:.3f}")
    plt.grid(True, ls="--", alpha=0.5)
    plt.savefig(out_fig, dpi=200, bbox_inches="tight")
    plt.close()

    return out_csv, out_fig


def save_diagnostics_figure(
    outdir,
    out_prefix,
    tmin,
    tmax,
    centers,
    counts_all,
    counts_gated,
    bin_size,
    arm_vals,
    l_deg,
    b_deg,
    arm_min,
    arm_max,
    n_total,
    n_time,
    n_gated,
    fmt,
    arm_hist_bins=30,
):
    out_diag = os.path.join(outdir, f"{out_prefix}_diagnostics.{fmt}")

    # Use constrained_layout to avoid overlap
    fig, ax = plt.subplots(2, 2, figsize=(14, 10), constrained_layout=True)

    # (1) Raw LC (all events in time window)
    ax[0, 0].step(centers, counts_all, where="mid", color="black")
    ax[0, 0].set_title("Light curve (all events, time-windowed)")
    ax[0, 0].set_xlabel("Time [s]")
    ax[0, 0].set_ylabel(f"Counts / {bin_size:.3f} s")
    ax[0, 0].grid(True, ls="--", alpha=0.4)

    # (2) ARM-gated LC
    ax[0, 1].step(centers, counts_gated, where="mid", color="tab:blue")
    ax[0, 1].set_title(f"Light curve (ARM-gated [{arm_min:.1f},{arm_max:.1f}] deg)")
    ax[0, 1].set_xlabel("Time [s]")
    ax[0, 1].set_ylabel(f"Counts / {bin_size:.3f} s")
    ax[0, 1].grid(True, ls="--", alpha=0.4)

    # (3) ARM histogram
    ax[1, 0].hist(arm_vals, bins=arm_hist_bins, color="tab:orange", edgecolor="black", alpha=0.7)
    ax[1, 0].axvline(arm_min, color="red", ls="--")
    ax[1, 0].axvline(arm_max, color="red", ls="--")
    ax[1, 0].set_title("ARM distribution (time-windowed events)")
    ax[1, 0].set_xlabel("ARM [deg]")
    ax[1, 0].set_ylabel("Counts")
    ax[1, 0].grid(True, ls="--", alpha=0.4)

    # (4) Summary textbox (use real newlines, not \\n)
    ax[1, 1].axis("off")
    frac_time = (n_time / n_total) if n_total > 0 else 0.0
    frac_gated = (n_gated / n_time) if n_time > 0 else 0.0

    text = (
        "Inputs\n"
        f"  l,b (deg): {l_deg:.6f}, {b_deg:.6f}\n"
        f"  Time window (s): [{tmin:.3f}, {tmax:.3f}]\n"
        f"  Bin size (s): {bin_size:.3f}\n"
        f"  ARM gate (deg): [{arm_min:.1f}, {arm_max:.1f}]\n\n"
        "Event statistics\n"
        f"  Total events in file: {n_total}\n"
        f"  In time window: {n_time}  (fraction {frac_time:.3f})\n"
        f"  ARM-gated: {n_gated}  (fraction {frac_gated:.3f})\n"
    )

    ax[1, 1].text(
        0.02, 0.98, text,
        va="top", ha="left",
        fontsize=10, family="monospace"
    )

    fig.suptitle(f"GRB time-series diagnostics — {out_prefix}", fontsize=14)
    fig.savefig(out_diag, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return out_diag


def run_grb_timeseries(
    data: str,
    tmin: float,
    tmax: float,
    l: float,
    b: float,
    arm_min: float = -15.0,
    arm_max: float = 15.0,
    bin: float = 1.0,
    out_prefix: str = "lc",
    outdir: str = ".",
    format: str = "png",
    no_warning: bool = False,
    diagnostics: bool = False,
    arm_hist_bins: int = 30,
):
    """
    Programmatic entry point: same logic as main(), callable from DAGs or other code.
    Returns the output directory path. Use allow_empty=True to write zero-count outputs
    when no events fall in the time window instead of raising.
    """
    if no_warning:
        warnings.filterwarnings("ignore", message="Could not memory map array", category=AstropyWarning)

    # Ensure output directory exists
    ensure_outdir(outdir)

    # Load events
    l_deg, b_deg = l, b
    t_all, l_all, b_all, phi_deg_all = load_events(data)
    n_total = int(t_all.size)

    # Time window cut
    m_time = (t_all >= tmin) & (t_all < tmax)
    if not np.any(m_time):
        raise RuntimeError("No events in the requested time window.")

    # Time window cut events
    t = t_all[m_time]
    l = l_all[m_time]
    b = b_all[m_time]
    phi_deg = phi_deg_all[m_time]
    n_time = int(t.size)

    # ARM + gate
    arm = calculate_arm(l_deg, b_deg, l, b, phi_deg)
    m_arm = (arm >= arm_min) & (arm <= arm_max)
    t_g = t[m_arm]
    n_gated = int(t_g.size)

    # LC binning
    edges, centers = make_time_bins(tmin, tmax, bin)

    # LCs (time-windowed)
    counts_all = histogram_counts(t, edges)
    counts_gated = histogram_counts(t_g, edges)

    # Save main outputs
    out_csv, out_lc = save_main_outputs(
        outdir=outdir,
        out_prefix=out_prefix,
        centers=centers,
        counts=counts_gated,
        bin_size=bin,
        l_deg=l_deg,
        b_deg=b_deg,
        arm_min=arm_min,
        arm_max=arm_max,
        fmt=format
    )
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_lc}")

    # Diagnostics (optional)
    if diagnostics:
        out_diag = save_diagnostics_figure(
            outdir=outdir,
            out_prefix=out_prefix,
            tmin=tmin,
            tmax=tmax,
            centers=centers,
            counts_all=counts_all,
            counts_gated=counts_gated,
            bin_size=bin,
            arm_vals=arm,
            l_deg=l_deg,
            b_deg=b_deg,
            arm_min=arm_min,
            arm_max=arm_max,
            n_total=n_total,
            n_time=n_time,
            n_gated=n_gated,
            fmt=format,
            arm_hist_bins=arm_hist_bins
        )
        print(f"Wrote: {out_diag}")

    return outdir



def main():
    parser = argparse.ArgumentParser(
        description="Generate ARM-gated light curve for a given localization and time window."
    )
    parser.add_argument("--data", required=True, help="Input unbinned FITS file (events table in HDU=1).")
    parser.add_argument("--tmin", type=float, required=True, help="Start time (absolute seconds).")
    parser.add_argument("--tmax", type=float, required=True, help="Stop time (absolute seconds).")
    parser.add_argument("--l", type=float, required=True, help="Localization Galactic longitude [deg].")
    parser.add_argument("--b", type=float, required=True, help="Localization Galactic latitude [deg].")
    parser.add_argument("--arm-min", type=float, default=-15.0, help="ARM gate min [deg].")
    parser.add_argument("--arm-max", type=float, default=+15.0, help="ARM gate max [deg].")
    parser.add_argument("--bin", type=float, default=1.0, help="Light curve bin size [s].")
    parser.add_argument("--out-prefix", default="lc", help="Output prefix (no extension).")
    parser.add_argument("--outdir", default=".", help="Output directory.")
    parser.add_argument("--format", default="png", choices=["png", "pdf"], help="Plot file format.")
    parser.add_argument("--no-warning", action="store_true",
                        help="Suppress Astropy mmap warning (recommended on cluster FS).")

    parser.add_argument("--diagnostics", action="store_true",
                        help="Also save a multi-panel diagnostics figure.")
    parser.add_argument("--arm-hist-bins", type=int, default=30,
                        help="Number of bins for ARM histogram in diagnostics figure.")

    args = parser.parse_args()

    if args.no_warning:
        warnings.filterwarnings("ignore", message="Could not memory map array", category=AstropyWarning)

    ensure_outdir(args.outdir)

    # Load events
    t_all, l_all, b_all, phi_deg_all = load_events(args.data)
    n_total = int(t_all.size)

    # Time window cut
    m_time = (t_all >= args.tmin) & (t_all < args.tmax)
    if not np.any(m_time):
        raise RuntimeError("No events in the requested time window.")

    t = t_all[m_time]
    l = l_all[m_time]
    b = b_all[m_time]
    phi_deg = phi_deg_all[m_time]
    n_time = int(t.size)

    # ARM + gate
    arm = calculate_arm(args.l, args.b, l, b, phi_deg)
    m_arm = (arm >= args.arm_min) & (arm <= args.arm_max)
    t_g = t[m_arm]
    n_gated = int(t_g.size)

    # LC binning
    edges, centers = make_time_bins(args.tmin, args.tmax, args.bin)

    # LCs (time-windowed)
    counts_all = histogram_counts(t, edges)
    counts_gated = histogram_counts(t_g, edges)

    # Save main outputs
    out_csv, out_lc = save_main_outputs(
        outdir=args.outdir,
        out_prefix=args.out_prefix,
        centers=centers,
        counts=counts_gated,
        bin_size=args.bin,
        l_deg=args.l,
        b_deg=args.b,
        arm_min=args.arm_min,
        arm_max=args.arm_max,
        fmt=args.format
    )
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_lc}")

    # Diagnostics (optional)
    if args.diagnostics:
        out_diag = save_diagnostics_figure(
            outdir=args.outdir,
            out_prefix=args.out_prefix,
            tmin=args.tmin,
            tmax=args.tmax,
            centers=centers,
            counts_all=counts_all,
            counts_gated=counts_gated,
            bin_size=args.bin,
            arm_vals=arm,
            l_deg=args.l,
            b_deg=args.b,
            arm_min=args.arm_min,
            arm_max=args.arm_max,
            n_total=n_total,
            n_time=n_time,
            n_gated=n_gated,
            fmt=args.format,
            arm_hist_bins=args.arm_hist_bins
        )
        print(f"Wrote: {out_diag}")

    return args.outdir

if __name__ == "__main__":
    main()
