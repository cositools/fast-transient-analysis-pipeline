"""
Helper functions for the fast transient analysis pipeline.

Functions:
# IMPLEMENTED:
- _load_yaml: load the YAML file
# IMPLEMENTED:
- _save_yaml: save the YAML file
# IMPLEMENTED:
- load_source_data: load the source data from the fits file
# IMPLEMENTED:
- load_background_data: load the background data from the fits file 
# IMPLEMENTED:
- slice_background: slice the background from the fits file
# IMPLEMENTED:
- create_background_model: create the background model
# IMPLEMENTED:
- aggregate_data: aggregate the data from the fits file and create the background model
# IMPLEMENTED:
- extract_bkg_window: extract the background window from the background file
# TODO:
- load_data: load the data from the fits file
# TODO:
- load_orientation: load the orientation from the fits file
# TODO:
- load_response: load the response from the fits file
# TODO:
- load_spectrum: load the spectrum from the fits file
# TODO:
- load_nside: load the nside from the fits file
# TODO:
- load_energy_channel: load the energy channel from the fits file
# TODO:
- load_cpu_cores: load the cpu cores from the fits file
"""

from histpy import Histogram
import numpy as np
import yaml
from typing import Any, Optional


def _load_yaml(path: str) -> dict[str, Any]:
    """
    Load the YAML file
    """
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader) or {}

#########################################################

def _save_yaml(path: str, payload: dict[str, Any]) -> None:
    """
    Save the YAML file
    """
    with open(path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)

#########################################################

def load_source_data(source_path: str) -> tuple[Histogram, np.ndarray]:
    """
    Load the source data from the fits file
    """
    # Open the source data
    print(f"Opening source data from {source_path}")
    signal = Histogram.open(source_path)
    source_times = signal.axes["Time"].edges.value
    print("--------------------------------\n"
          f"Source minimum time:     {np.min(source_times)}\n"
          f"Source maximum time:     {np.max(source_times)}\n"
          f"Source duration:         {np.max(source_times) - np.min(source_times)}\n"
          "--------------------------------")
    
    return signal, source_times

#########################################################

def load_background_data(background_path: str) -> tuple[Histogram, np.ndarray]:
    """
    Load the background data from the fits file
    """
    # Open the background data
    print(f"Opening background data from {background_path}")
    bkg_full = Histogram.open(background_path)
    bkg_times = bkg_full.axes['Time'].edges.value
    print("--------------------------------\n"
          f"Background minimum time:     {np.min(bkg_times)}\n"
          f"Background maximum time:     {np.max(bkg_times)}\n"
          f"Background duration:         {np.max(bkg_times) - np.min(bkg_times)}\n"
          "--------------------------------")
    return bkg_full, bkg_times

#########################################################

# Optional output typing for python 3.9 and below
def _infer_time_bounds_from_fits(
    source_path: str
) -> Optional[tuple[float, float, float]]:
    """
    Infer (tmin, tmax) from the source FITS event file.
    """
    from astropy.io import fits
    
    if not source_path.lower().endswith((".fits", ".fits.gz", ".fit", ".fit.gz")):
        raise ValueError(f"Source path is not a FITS file: {source_path}")

    # Read the GRB signal
    with fits.open(source_path) as signal:
        # get the starting and ending time tag of the GRB
        times = signal[1].data["TimeTags"]
        tstart = float(np.min(times))
        tstop = float(np.max(times))
        duration = tstop - tstart
    return tstart, tstop, duration


def extract_bkg_window(
    background_path: str, 
    source_path: str, 
    eps_time: float = 0.000000001,
) -> np.ndarray:
    """
    Extract the background window from the background file. 
    The eps_time is the adding time in seconds to add before and after the source time 
    to extract the background. It is a safety margin to avoid edge effects.
    """
    from astropy.io import fits

    with fits.open(background_path) as bkg_full:
        tstart, tstop, _ = _infer_time_bounds_from_fits(source_path)
        mask = (bkg_full['TimeTags'] >= tstart - eps_time) & (bkg_full['TimeTags'] <= tstop + eps_time)
        bkg_cut = bkg_full[mask]
        return bkg_cut

#########################################################

def slice_background(
    bkg_full: Histogram, 
    bkg_times: np.ndarray,
    tstart: float, 
    tstop: float,
) -> np.ndarray:
    """
    Slice the background from the fits file
    """
    print(f"Slicing background from {tstart} to {tstop}")
    # Find the indices of the background from tstart and tstop
    bkg_tstart_idx = int(np.searchsorted(bkg_times, tstart, side="left"))
    bkg_tstop_idx  = int(np.searchsorted(bkg_times, tstop, side="left"))
    # Slice the background
    bkg = bkg_full.slice[bkg_tstart_idx:bkg_tstop_idx, :]
    return bkg

#########################################################

def create_background_model(
    bkg: Histogram,
    bkg_times: np.ndarray,
    delta: float,
) -> Histogram:
    """
    Create the background model
    """
    # Get the duration of the background
    bkg_duration = np.ptp(bkg_times)  # max - min    
    # Create the background model
    bkg_model = bkg
    # Normalize the background model
    bkg_model /= bkg_duration / delta
    # Add a small value to the background model to avoid division by zero
    bkg_model += 1e-9
    return bkg_model

#########################################################

def aggregate_data(
    source_path: str, 
    background_path: str, 
    tstart: float = None, 
    tstop: float = None,
) -> tuple[Histogram, Histogram]:
    """
    Aggregate the data from the fits file

    Parameters
    ----------
    source_path: str
        The path to the source data file.
    background_path: str
        The path to the background data file.
    tstart: float
        The start time of the data.
    tstop: float
        The stop time of the data.

    Returns
    -------
    data: Histogram
        The aggregated data.
    bkg_model: Histogram
        The background model.
    """
    from astropy.time import Time

    # Compute delta
    tstart_time = Time(tstart, format='unix')
    tstop_time = Time(tstop, format='unix')
    delta = tstop_time - tstart_time
    delta = float(delta.to_value('s'))
    
    # Load the source data
    signal, _ = load_source_data(source_path)
    # Project the source data
    #signal = signal.project(['Em', 'Phi', 'PsiChi'])

    # Load the background data
    bkg_full, bkg_times = load_background_data(background_path)

    if tstart is not None and tstop is not None:
        # Slice the background
        bkg = slice_background(bkg_full, bkg_times, tstart, tstop)
    else:
        bkg = bkg_full

    # Project the background data
    #bkg = bkg.project(['Em', 'Phi', 'PsiChi'])
    
    # Create the background model
    bkg_model = create_background_model(bkg, bkg_times, delta)
    
    # Assemble the data
    data = bkg_model + signal
    return data, bkg_model