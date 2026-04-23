import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import yaml
from typing import Any


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader) or {}


def _save_yaml(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)


def _ensure_pipeline_dirs(data_dir: str) -> tuple[Path, Path]:
    base_dir = Path(data_dir)
    plots_dir = base_dir / "plots"
    products_dir = base_dir / "products"
    plots_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)
    return plots_dir, products_dir

#########################################################
# TASK 1: Preprocessing
# TODO: Implement the preprocessing
#########################################################
def preprocess_data(lightcurve):
    """
    Preprocess the pipeline inputs and persist them in a YAML config.

    The function accepts either:
    - a dict containing input keys, or
    - a string path to an existing YAML file to enrich/normalize.
    """
    if isinstance(lightcurve, str):
        config = _load_yaml(lightcurve)
    elif isinstance(lightcurve, dict):
        config = dict(lightcurve)
    else:
        raise TypeError("preprocess_data expects a dict or a YAML path string")

    nside = 16

    required_keys = [
        "source_path",
        "background_path",
        "orientation_path",
        "response_path",
    ]
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(f"Missing required preprocessing fields: {missing}")

    default_data_dir = str(Path(config["source_path"]).resolve().parent)
    data_dir = config.get("data_dir", default_data_dir)
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    config["data_dir"] = str(Path(data_dir))
    config["plots_dir"] = str(plots_dir)
    config["products_dir"] = str(products_dir)
    config.setdefault(
        "default_spectrum",
        {
            "index": -2.2,
            "K": 10.0,
            "K_unit": "1 / (cm2 keV s)",
            "piv": 100.0,
            "piv_unit": "keV",
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
    config.setdefault("light_curve", {
        "lightcurve_bin_size": 1,
        "nside": nside,
        "used_coordinates": {
            "l_deg": 0,
            "b_deg": 0,
        },
    })
    # Backward-compatible top-level aliases used by downstream tasks.
    config.setdefault("tsmap_nside", config["tsmap"].get("nside", nside))
    config.setdefault("tsmap_energy_channel", config["tsmap"].get("energy_channel", [2, 3]))
    config.setdefault("tsmap_cpu_cores", config["tsmap"].get("cpu_cores", 8))
    config.setdefault("lightcurve_nside", config["light_curve"].get("nside", nside))
    config.setdefault(
        "lightcurve_bin_size",
        config["light_curve"].get("lightcurve_bin_size", 1),
    )

    config_path = config.get("config_path", str(products_dir / "pipeline_config.yaml"))
    _save_yaml(config_path, config)
    return config_path


#########################################################
# TASK 2: Unbinned_Light_Curve_Generation
# TODO: Implement the unbinned light curve generation
#########################################################
def unbinned_light_curve_generation(lightcurve: str) -> str:
    """
    Generate the unbinned light curve from the lightcurve.
    Returns the path to the unbinned light curve.
    """
    raise NotImplementedError("Not implemented yet")


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
    tmin = config["tmin"]
    tmax = config["tmax"]

    from cosipy import BinnedData

    def _bin_single_data(
        unbinned_file_path: str,
        label: str,
        inputs_filename: str,
        tmin: float,
        tmax: float,
    ) -> str:
        """
        Bin a single data file.
        Returns the path to the binned file.

        Args:
            unbinned_file_path: Path to the unbinned fits file.
            label: Label of the data file.
            inputs_filename: Name of the inputs file.
            tmin: Minimum time of the data.
            tmax: Maximum time of the data.
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
            "tmin": tmin,
            "tmax": tmax,
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
        tmin=tmin,
        tmax=tmax,
    )

    background_binned_file_path = _bin_single_data(
        unbinned_file_path=background_path,
        label="background",
        inputs_filename="inputs_bg.yaml",
        tmin=tmin,
        tmax=tmax,
    )

    return source_binned_file_path, background_binned_file_path

#########################################################
# TASK 4: Light_Curve_Analysis
# TODO: Implement the light curve analysis
#########################################################
def light_curve_analysis(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Analyze the light curve of the source and background data.
    Returns the path to the light curve file.
    """
    raise NotImplementedError("Not implemented yet")

#########################################################
# TASK 5: Duration_and_Localization_Results
# TODO: Implement the duration and localization results
#########################################################
def duration_and_localization_results(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Compute the duration and localization results of the source and background data.
    Returns the path to the duration and localization results file.
    """
    raise NotImplementedError("Not implemented yet")

#########################################################
# TASK 6: TS_Map_on_different_timescales
# TODO: Implement the TS map on different timescales
#########################################################
def compute_ts_map(
    config_path: str) -> str:
    """
    Compute TS map for the source and background data.
    Returns the path to the TS map file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    config = _load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    source_path = config["source_path"]
    background_path = config["background_path"]
    orientation_path = config["orientation_path"]
    response_path = config["response_path"]
    data_dir = config["data_dir"]
    plots_dir, products_dir = _ensure_pipeline_dirs(data_dir)

    import gc
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from astropy.time import Time
    from cosipy import FastTSMap, MOCTSMap, SpacecraftHistory
    from histpy import Histogram
    from mhealpy import HealpixMap
    from threeML import Powerlaw

    spectrum_cfg = config.get("default_spectrum", {})
    spectrum = Powerlaw()
    spectrum.index.value = float(spectrum_cfg.get("index", -2.2))
    spectrum.K.value = float(spectrum_cfg.get("K", 10.0))
    spectrum.piv.value = float(spectrum_cfg.get("piv", 100.0))
    spectrum.K.unit = u.Unit(spectrum_cfg.get("K_unit", "1 / (cm2 keV s)"))
    spectrum.piv.unit = u.Unit(spectrum_cfg.get("piv_unit", "keV"))

    signal = Histogram.open(source_path)
    grb_tmin = signal.axes["Time"].edges.min()
    grb_tmax = signal.axes["Time"].edges.max()
    signal = signal.project(["Em", "PsiChi", "Phi"])

    bkg_full = Histogram.open(background_path)
    bkg_times = bkg_full.axes["Time"].edges.value
    bkg_tmin_idx = np.searchsorted(bkg_times, grb_tmin.value, side="left")
    bkg_tmax_idx = np.searchsorted(bkg_times, grb_tmax.value, side="right")
    bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx, :]
    bkg = bkg.project(["Em", "PsiChi", "Phi"])

    data = bkg + signal
    bkg_full_duration = np.ptp(bkg_times)
    bkg_model = bkg_full.project(["Em", "PsiChi", "Phi"])
    bkg_model /= bkg_full_duration / 40

    ori_full = SpacecraftHistory.open(orientation_path)
    grb_ori = ori_full.select_interval(Time(grb_tmin, format = "unix"), Time(grb_tmax, format = "unix"))

    del bkg_full
    del ori_full
    _ = gc.collect()

    tsmap_cfg = config.get("tsmap", {})
    nside = int(config.get("tsmap_nside", tsmap_cfg.get("nside", 16)))
    energy_channel = config.get(
        "tsmap_energy_channel",
        tsmap_cfg.get("energy_channel", [2, 3]),
    )
    cpu_cores = int(config.get("tsmap_cpu_cores", tsmap_cfg.get("cpu_cores", 8)))

    # Task 7.1 - FastTSMap
    fast = FastTSMap(
        data=data,
        bkg_model=bkg_model,
        orientation=grb_ori,
        response_path=response_path,
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
        response_path=response_path,
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

    _save_yaml(config["tsmap_yaml"], tsmap_payload)
    _save_yaml(config_path, config)
    return config["tsmap_yaml"]

#########################################################
# TASK 7: Light_Curve
# TODO: Implement the light curve
#########################################################
def light_curve(
    config_path: str) -> str:
    """
    Generate the light curve of the source and background data.
    Returns the path to the light curve file.

    Args:
        config_path: Path to the yaml configuration file.
    """
    config = _load_yaml(config_path)
    if not config:
        raise ValueError(f"Empty or invalid config file: {config_path}")

    source_path = config["source_path"]
    background_path = config["background_path"]
    orientation_path = config["orientation_path"]
    response_path = config["response_path"]
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

    def mask_from_cumdist_vectorized(psr_map, containment=0.5):
        psr_norm = psr_map / np.sum(psr_map, axis=-1, keepdims=True)
        sort_idx = np.argsort(psr_norm, axis=-1)[..., ::-1]
        sorted_vals = np.take_along_axis(psr_norm, sort_idx, axis=-1)
        cumsum_vals = np.cumsum(sorted_vals, axis=-1)
        mask_sorted = (cumsum_vals < containment).astype(float)
        mask = np.empty_like(mask_sorted)
        np.put_along_axis(mask, sort_idx, mask_sorted, axis=-1)
        return mask

    light_curve_cfg = config.get("light_curve", {})
    lightcurve_nside = int(
        config.get("lightcurve_nside", light_curve_cfg.get("nside", 16))
    )

    def create_psr(l_deg, b_deg, ori_file: str, rsp_file: str, nside: int):
        grb_ori = SpacecraftHistory.open(ori_file)
        coord = SkyCoord(l=l_deg * u.deg, b=b_deg * u.deg, frame="galactic")
        scatt_map = grb_ori.get_scatt_map(nside=nside)
        with FullDetectorResponse.open(rsp_file) as response:
            return response.get_point_source_response(coord=coord, scatt_map=scatt_map)

    def get_signal_window(signal_hist):
        return signal_hist.axes["Time"].edges.min(), signal_hist.axes["Time"].edges.max()

    def load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop):
        if tstart >= window_start.value and tstop <= window_stop.value:
            signal_tmin_idx = np.where(signal_full.axes["Time"].edges.value == tstart)[0][0]
            signal_tmax_idx = np.where(signal_full.axes["Time"].edges.value == tstop)[0][0]
            signal = signal_full.slice[signal_tmin_idx:signal_tmax_idx, :]
            signal = signal.project(["Em", "Phi", "PsiChi"])
        bkg_tmin_idx = np.where(bkg_full.axes["Time"].edges.value == tstart)[0][0]
        bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == tstop)[0][0]
        bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx, :]
        bkg = bkg.project(["Em", "Phi", "PsiChi"])
        if tstart >= window_start.value and tstop <= window_stop.value:
            data = signal + bkg
        else:
            data = bkg
        return data.contents.todense()

    psr_map = create_psr(
        lon,
        lat,
        ori_file=orientation_path,
        rsp_file=response_path,
        nside=lightcurve_nside,
    )
    input_psr = psr_map.project(["Em", "Phi", "PsiChi"]).contents.value
    mask_map = mask_from_cumdist_vectorized(input_psr, containment=0.5)

    signal_full = Histogram.open(source_path)
    bkg_full = Histogram.open(background_path)
    window_start, window_stop = get_signal_window(signal_full)

    counts = []
    bin_size = int(
        config.get(
            "lightcurve_bin_size",
            light_curve_cfg.get("lightcurve_bin_size", light_curve_cfg.get("bin_size", 5)),
        )
    )
    tstart = window_start.value - 20
    while tstart < window_stop.value + 20:
        tstop = tstart + bin_size
        data_map = load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop)
        masked_data = mask_map * data_map
        counts.append(float(masked_data.sum()))
        tstart = tstop

    n_bins = int(((window_stop.value + 20) - (window_start.value - 20)) / bin_size)
    bins = np.linspace(window_start.value - 20, window_stop.value + 20, n_bins)

    plt.figure(figsize=(10, 4))
    plt.step(bins, counts)
    plt.xlabel("Time (s)")
    plt.ylabel("Counts")
    plt.title(f"GRB light curve (bin = {bin_size}s)")
    plt.axvline(x=window_start.value, linestyle="--", linewidth=1.5)

    lightcurve_plot_path = plots_dir / "lightcurve.png"
    plt.tight_layout()
    plt.savefig(lightcurve_plot_path, dpi=150)
    plt.close()

    lightcurve_payload = {
        "plot_path": str(lightcurve_plot_path),
        "bin_size": bin_size,
        "nside": lightcurve_nside,
        "used_coordinates": {"l_deg": lon, "b_deg": lat},
    }
    config["light_curve"] = lightcurve_payload
    config["lightcurve_yaml"] = str(products_dir / "lightcurve_results.yaml")

    _save_yaml(config["lightcurve_yaml"], lightcurve_payload)
    _save_yaml(config_path, config)
    return config["lightcurve_yaml"]

#########################################################
# TASK 8: Duration  
# TODO: Implement the duration
#########################################################
def duration(source_binned_file_path: str, background_binned_file_path: str) -> str:
    """
    Compute the duration of the source and background data.
    Returns the path to the duration file.
    """
    raise NotImplementedError("Not implemented yet")

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