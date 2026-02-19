"""
Utility functions used by the cosipipe_lcurve DAG with DockerOperator.
All functions are self-contained and do not rely on DAG-level globals.

- bin_grb_source: bin the GRB data source based on the bin_grb.py script
- bin_background_data: bin the background data based on the bin_bg.py script
- plot_lightcurve_from_cells: plot the light curve based on the lightcurve.ipynb script
"""

from __future__ import annotations

from pathlib import Path
import os
import yaml
import glob

# The following function is used to log messages to stderr so it doesn't interfere with XCom stdout
# this avoid memory issues when using XCom to pass the logs of the container
def log(msg):
    """Log message to stderr so it doesn't interfere with XCom stdout"""
    sys.stderr.write(str(msg) + "\n")
    sys.stderr.flush()

# -----------------------------
# Binning (source & background)
# -----------------------------
def bin_grb_source(unbinned_file_path: str, data_folder: str) -> str:
    """
    Bin GRB data source based on the bin_grb.py script
    """
    log(f"[bin_grb_source] Found GRB unbinned fits file: {unbinned_file_path}")

    # Define output file path
    # Extract basename first to avoid path issues
    unbinned_basename = os.path.basename(unbinned_file_path)
    extension = unbinned_basename.split(".")[-1]
    binned_file_name = unbinned_basename.replace("_unbinned_", "_binned_").replace("."+extension, "")
    binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")
    
    log(f"[bin_grb_source] Expected output file: {binned_file_path}")

    # Check if binned file already exists
    if os.path.exists(binned_file_path):
        log(f"[bin_grb_source] Binned GRB file already exists: {binned_file_path}")
        log("[bin_grb_source] Skipping GRB binning step.")
        return binned_file_path
    
    log("[bin_grb_source] Binned GRB file not found. Proceeding with binning process...")
    
    # Construct paths
    # Create inputs.yaml configuration for binning
    log("[bin_grb_source] Creating GRB binning configuration...")
    config = {
        "data_file": unbinned_file_path,
        "ori_file": "NA",
        "unbinned_output": "fits",
        "time_bins": 1,
        "energy_bins": [100., 158.489, 251.189, 398.107, 630.957, 1000., 1584.89, 2511.89, 3981.07, 6309.57, 10000.],
        "phi_pix_size": 6,
        "nside": 8,
        "scheme": "ring",
        "tmin": 1836496300.00,
        "tmax": 1836496389.00
    }
    
    # Write inputs.yaml to the data folder
    inputs_path = os.path.join(data_folder, "inputs_grb.yaml")
    with open(inputs_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    log(f"[bin_grb_source] ✓ Created GRB binning configuration: {inputs_path}")
    log(f"[bin_grb_source] Configuration details:")
    log(f"[bin_grb_source]   - Time bins: {config['time_bins']} seconds")
    log(f"[bin_grb_source]   - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    log(f"[bin_grb_source]   - Phi pixel size: {config['phi_pix_size']} degrees")
    log(f"[bin_grb_source]   - Nside: {config['nside']}")
    log(f"[bin_grb_source]   - Scheme: {config['scheme']}")
    log(f"[bin_grb_source]   - Time range: {config['tmin']} to {config['tmax']}")
    log(f"[bin_grb_source]   - GRB duration: {config['tmax'] - config['tmin']} seconds")
    
    # Execute the binning process
    log("[bin_grb_source] Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    log("[bin_grb_source] ✓ BinnedData analysis object created successfully")
    
    log(f"[bin_grb_source] Starting binning process for output: {binned_file_name}")
    log("[bin_grb_source] This may take several minutes depending on data size...")
    
    # Change to data_folder to ensure file is created in the correct location
    original_cwd = os.getcwd()
    try:
        os.chdir(data_folder)
        analysis.get_binned_data(
            unbinned_data=unbinned_file_path,
            output_name=binned_file_name,
            psichi_binning="galactic"
        )
    finally:
        os.chdir(original_cwd)
    
    log(f"[bin_grb_source] GRB data binning completed successfully!")
    
    # Verify the output file was created
    if os.path.exists(binned_file_path):
        file_size = os.path.getsize(binned_file_path) / (1024 * 1024)  # Size in MB
        log(f"[bin_grb_source] Output file verified: {file_size:.2f} MB")
    else:
        # Try to find the file - it might have been created with a different path or name
        log(f"[bin_grb_source] Warning: Expected output file not found: {binned_file_path}")
        # Search for .hdf5 files in data_folder that match the pattern
        pattern = os.path.join(data_folder, f"*{binned_file_name}*.hdf5")
        matches = glob.glob(pattern)
        if matches:
            log(f"[bin_grb_source] Found file at alternative location: {matches[0]}")
            binned_file_path = matches[0]
        else:
            # List all .hdf5 files in data_folder for debugging
            all_hdf5 = glob.glob(os.path.join(data_folder, "*.hdf5"))
            log(f"[bin_grb_source] Available .hdf5 files in {data_folder}: {all_hdf5}")
            raise FileNotFoundError(f"Binned GRB file not found at expected location: {binned_file_path}")
    
    return binned_file_path


def bin_background_data(unbinned_file_path: str, data_folder: str) -> str:
    """
    Bin background data based on the bin_bg.py script
    """
    log(f"[bin_background_data] Found background unbinned fits file: {unbinned_file_path}")

    # Define output file path
    # Extract basename first to avoid path issues
    unbinned_basename = os.path.basename(unbinned_file_path)
    extension = unbinned_basename.split(".")[-1]
    log(f"[bin_background_data] Extension: {extension}")
    binned_file_name = unbinned_basename.replace("_unbinned_", "_binned_").replace("."+extension, "")
    log(f"[bin_background_data] Binned file name: {binned_file_name}")
    binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")
    
    log(f"[bin_background_data] Expected output file: {binned_file_path}")

    # Check if binned file already exists
    if os.path.exists(binned_file_path):
        log(f"[bin_background_data] Binned background file already exists: {binned_file_path}")
        log("[bin_background_data] Skipping background binning step.")
        return binned_file_path
    
    log("[bin_background_data] Binned background file not found. Proceeding with binning process...")
    
    # Construct paths
    # Create inputs.yaml configuration for binning
    log("[bin_background_data] Creating background binning configuration...")
    config = {
        "data_file": unbinned_file_path,
        "ori_file": "NA",
        "unbinned_output": "fits",
        "time_bins": 1,
        "energy_bins": [100., 158.489, 251.189, 398.107, 630.957, 1000., 1584.89, 2511.89, 3981.07, 6309.57, 10000.],
        "phi_pix_size": 6,
        "nside": 8,
        "scheme": "ring",
        "tmin": 1835487300.0,
        "tmax": 1843467255.0
    }
    
    # Write inputs.yaml to the data folder
    inputs_path = os.path.join(data_folder, "inputs_bg.yaml")
    with open(inputs_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    log(f"[bin_background_data] ✓ Created background binning configuration: {inputs_path}")
    log(f"[bin_background_data] Configuration details:")
    log(f"[bin_background_data]   - Time bins: {config['time_bins']} seconds")
    log(f"[bin_background_data]   - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    log(f"[bin_background_data]   - Phi pixel size: {config['phi_pix_size']} degrees")
    log(f"[bin_background_data]   - Nside: {config['nside']}")
    log(f"[bin_background_data]   - Scheme: {config['scheme']}")
    log(f"[bin_background_data]   - Time range: {config['tmin']} to {config['tmax']}")
    
    # Execute the binning process
    log("[bin_background_data] Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    log("[bin_background_data] ✓ BinnedData analysis object created successfully")
    
    log(f"[bin_background_data] Starting binning process for output: {binned_file_name}")
    log("[bin_background_data] This may take several minutes depending on data size...")
    
    # Change to data_folder to ensure file is created in the correct location
    original_cwd = os.getcwd()
    try:
        os.chdir(data_folder)
        analysis.get_binned_data(
            unbinned_data=unbinned_file_path,
            # Use output_file without the extension
            output_name=binned_file_name,
            psichi_binning="galactic"
        )
    finally:
        os.chdir(original_cwd)
    
    log(f"[bin_background_data] Background data binning completed successfully!")
    
    # Verify the output file was created
    if os.path.exists(binned_file_path):
        file_size = os.path.getsize(binned_file_path) / (1024 * 1024)  # Size in MB
        log(f"[bin_background_data] ✓ Output file verified: {file_size:.2f} MB")
    else:
        # Try to find the file - it might have been created with a different path or name
        log(f"[bin_background_data] Warning: Expected output file not found: {binned_file_path}")
        # Search for .hdf5 files in data_folder that match the pattern
        pattern = os.path.join(data_folder, f"*{binned_file_name}*.hdf5")
        matches = glob.glob(pattern)
        if matches:
            log(f"[bin_background_data] Found file at alternative location: {matches[0]}")
            binned_file_path = matches[0]
        else:
            # List all .hdf5 files in data_folder for debugging
            all_hdf5 = glob.glob(os.path.join(data_folder, "*.hdf5"))
            log(f"[bin_background_data] Available .hdf5 files in {data_folder}: {all_hdf5}")
            raise FileNotFoundError(f"Binned background file not found at expected location: {binned_file_path}")
    
    return binned_file_path


# -----------------------------
# Light curve plotting (full cell logic)
# -----------------------------
def plot_lightcurve_from_cells(grb_signal_path: str, 
                               background_path: str, 
                               orientation_path: str, 
                               response_path: str, 
                               data_dir: str) -> str:
    """
    Plot the light curve based on the grb_signal_path, background_path, orientation_path, and response_path
    """

    # Imports used by the notebook cells
    import numpy as np
    import matplotlib.pyplot as plt
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from histpy import Histogram
    from cosipy.background_estimation import ContinuumEstimation
    from cosipy.spacecraftfile import SpacecraftFile
    from cosipy.response import FullDetectorResponse

    # ---- helpers (as in notebook) ----
    def load_projected_psr(psr_file):
        instance = ContinuumEstimation()
        psr_hist = instance.load_psr_from_file(psr_file)
        projected = psr_hist.project(['Em', 'Phi', 'PsiChi'])
        data = projected.contents.value
        return projected, data

    def mask_from_cumdist_vectorized(psr_map, containment=0.4):
        psr_norm = psr_map / np.sum(psr_map, axis=-1, keepdims=True)
        sort_idx = np.argsort(psr_norm, axis=-1)[..., ::-1]
        sorted_vals = np.take_along_axis(psr_norm, sort_idx, axis=-1)
        cumsum_vals = np.cumsum(sorted_vals, axis=-1)
        mask_sorted = (cumsum_vals < containment).astype(float)
        mask = np.empty_like(mask_sorted)
        np.put_along_axis(mask, sort_idx, mask_sorted, axis=-1)
        # NOTE: like in your cell, we do NOT invert the mask.
        return mask

    def create_psr(l, b, ori_file: str, response_file: str):
        ori = SpacecraftFile.parse_from_file(ori_file)
        coord = SkyCoord(l=l * u.deg, b=b * u.deg, frame="galactic")
        scatt_map = ori.get_scatt_map(coord, nside=16, coordsys='galactic')
        with FullDetectorResponse.open(response_file) as response:
            psr = response.get_point_source_response(coord=coord, scatt_map=scatt_map)
            log("Works!")
            return psr

    def get_signal_window(signal):
        grb_tmin = signal.axes["Time"].edges.min()
        grb_tmax = signal.axes["Time"].edges.max()
        log(f"The GRB duration is {grb_tmax - grb_tmin} from {grb_tmin} to {grb_tmax}")
        return grb_tmin, grb_tmax

    def load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop):
        # Slice & project Signal only if within GRB window
        if tstart >= window_start.value and tstop <= window_stop.value:
            signal_tmin_idx = np.where(signal_full.axes['Time'].edges.value == tstart)[0][0]
            signal_tmax_idx = np.where(signal_full.axes["Time"].edges.value == tstop)[0][0]
            signal = signal_full.slice[signal_tmin_idx:signal_tmax_idx, :]
            signal = signal.project(['Em', 'Phi', 'PsiChi'])
        # Background always sliced on the same interval
        bkg_tmin_idx = np.where(bkg_full.axes['Time'].edges.value == tstart)[0][0]
        bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == tstop)[0][0]
        bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx, :]
        bkg = bkg.project(['Em', 'Phi', 'PsiChi'])
        # Add signal only inside window
        if tstart >= window_start.value and tstop <= window_stop.value:
            data = signal + bkg
        else:
            data = bkg
        return data, data.contents.todense()

    log(f"Opening histograms for GRB signal file: {grb_signal_path} and background file: {background_path}")
    log(f"Building PSR + mask for orientation file: {orientation_path} and response file: {response_path}")
    # ---- build PSR + mask ----
    psr_map = create_psr(171.56, -4.780, ori_file=orientation_path, response_file=response_path)
    input_psr = psr_map.project(['Em', 'Phi', 'PsiChi']).contents.value
    mask_map = mask_from_cumdist_vectorized(input_psr, containment=0.5)

    # ---- open histograms ----
    signal_full = Histogram.open(grb_signal_path)
    bkg_full = Histogram.open(background_path)

    # ---- time scan around the GRB window ----
    window_start, window_stop = get_signal_window(signal_full)

    counts = []
    bin_size = 5  # seconds
    tstart = window_start.value - 20
    while tstart < window_stop.value + 20:
        tstop = tstart + bin_size
        _, data_map = load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop)
        masked_data = mask_map * data_map
        counts.append(masked_data.sum())
        tstart = tstop

    # Build bin edges like in the notebook
    N = int(((window_stop.value + 20) - (window_start.value - 20)) / bin_size)
    bins = np.linspace(window_start.value - 20, window_stop.value + 20, N + 1)

    # ---- plot ----
    plt.figure(figsize=(10, 4))
    plt.step(bins, counts)
    plt.xlabel("Time (s)")
    plt.ylabel("Counts")
    plt.title(f"GRB light curve (bin = {bin_size}s)")
    plt.axvline(x=window_start.value, linestyle='--', linewidth=1.5)
    out_png = Path(data_dir) / "lightcurve.png"
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()

    log(f"Light curve saved to: {out_png}")

    return str(out_png)


if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="COSI lightcurve pipeline operations")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # bin_grb_source command
    parser_bin_grb = subparsers.add_parser('bin_grb_source', help='Bin GRB source data')
    parser_bin_grb.add_argument('--unbinned_file_path', required=True, help='Path to unbinned GRB file')
    parser_bin_grb.add_argument('--data_folder', required=True, help='Data folder for output')
    
    # bin_background_data command
    parser_bin_bkg = subparsers.add_parser('bin_background_data', help='Bin background data')
    parser_bin_bkg.add_argument('--unbinned_file_path', required=True, help='Path to unbinned background file')
    parser_bin_bkg.add_argument('--data_folder', required=True, help='Data folder for output')
    
    # plot_lightcurve_from_cells command
    parser_plot = subparsers.add_parser('plot_lightcurve_from_cells', help='Plot lightcurve from cells')
    parser_plot.add_argument('--grb_signal_path', required=True, help='Path to GRB signal file')
    parser_plot.add_argument('--background_path', required=True, help='Path to background file')
    parser_plot.add_argument('--orientation_path', required=True, help='Path to orientation file')
    parser_plot.add_argument('--response_path', required=True, help='Path to response file')
    parser_plot.add_argument('--data_folder', required=True, help='Data folder for output')
    
    args = parser.parse_args()
    
    if args.command == 'bin_grb_source':
        result = bin_grb_source(args.unbinned_file_path, args.data_folder)
        print(result)
        sys.exit(0)
    elif args.command == 'bin_background_data':
        result = bin_background_data(args.unbinned_file_path, args.data_folder)
        print(result)
        sys.exit(0)
    elif args.command == 'plot_lightcurve_from_cells':
        result = plot_lightcurve_from_cells(
            grb_signal_path=args.grb_signal_path,
            background_path=args.background_path,
            orientation_path=args.orientation_path,
            response_path=args.response_path,
            data_dir=args.data_folder  # Function uses data_dir, argparse uses data_folder
        )
        print(result)
        sys.exit(0)
    else:
        parser.print_help()
        sys.exit(1)

