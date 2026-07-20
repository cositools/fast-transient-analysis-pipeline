# dags/init_pipelines.py
# Airflow 2.x — Initialize and stage COSI pipeline data
# - staged raw subfolders (source/background/orientation/response)
# - staging & background cut executed in Docker Container via DockerOperator
# - run folder under DEST_MAP/YYYY_MM/YYMMDDXXX/products with symlinks and cut result

from __future__ import annotations
import json, os, re, shutil
from datetime import datetime
from pathlib import Path
from typing import Dict

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import timedelta

# === Config ===
# The image to use for scientific tasks (cosipy environment)
# Ideally, this should be configurable via Variable or Env, but hardcoded for now as requested.
# Using 'fast-transient-analysis-pipeline:latest' assuming it is built locally.
CONTAINER_IMAGE = "fast-transient-analysis-pipeline:latest"

# Scripts inside the container (mounted via modules_pool in the container, but here we assume the container has them or we mount them)
# Wait, if we use DockerOperator, we are launching a NEW container. 
# We need to make sure the code is available inside THAT container.
# If 'fast-transient-analysis-pipeline' image contains the code in /app or similar, we use that path.
# Assuming standard structure in image: /home/gamma/airflow/pipeline/... is NOT guaranteed unless we mount it.
# However, user said "fast-transient-analysis-pipeline" module. 
# Let's assume the image has the code or we mount the workspace.
# For robustness, we mount the workspace into the DockerOperator container too.

# We also need the pipeline scripts. Assuming they are in the workspace under fast-transient-analysis-pipeline/src/pipeline
# and mapped to /home/gamma/airflow/pipeline inside the container for consistency with previous scripts.
# Or simpler: we execute the script from the mounted workspace directly.

STAGE_SCRIPT = "/home/gamma/workspace/fast-transient-analysis-pipeline/src/pipeline/stage_files.py"
BKG_CUT_SCRIPT = "/home/gamma/workspace/fast-transient-analysis-pipeline/src/pipeline/bkg_cut.py"

# === Paths ===
RAW_ROOT = Path("/home/gamma/workspace/data/raw")
RAW_SUBDIRS = {
    "source": RAW_ROOT / "source",
    "background": RAW_ROOT / "background",
    "orientation": RAW_ROOT / "orientation",
    "response": RAW_ROOT / "response",
}

DEST_MAP = {
    "lcurve": Path("/home/gamma/workspace/data/lcurve"),
    "tsmap": Path("/home/gamma/workspace/data/tsmap"),
    "fast": Path("/home/gamma/workspace/data/fast_localize_grb"),
    "tdrss": Path("/home/gamma/workspace/data/tdrss"),
}

AUTO_INPUT = "__default__"

def wasabi_keys(base: str, folder: str, file_names: list[str]) -> list[str]:
    return [f"{base}/{folder}/{file_name}" for file_name in file_names]

# === Wasabi keys exposed in the Airflow trigger form ===
DATA_CHALLENGE_DOWNLOAD_KEYS = {
    "DC3": {
        "response": wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Responses",
            [
                "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5.zip",
            ],
        ) + wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Responses/extended_source_response",
            [
                "extended_source_response_511_merged.h5.gz",
                "extended_source_response_continuum_merged.h5.gz",
                "extended_source_response_Al26_merged.h5.gz",
                "extended_source_response_Ti44_merged.h5.gz",
                "extended_source_response_Fe60_low_merged.h5.gz",
                "extended_source_response_Fe60_high_merged.h5.gz",
            ],
        ),
        "orientation": wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Orientation",
            [
                "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
            ],
        ),
        "source": wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Sources",
            [
                "Globular_Cluster_Tuc_47_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Globular_Cluster_Omega_Cen_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Globular_Cluster_NGC_6397_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Globular_Cluster_NGC_6121_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Positrons_from_26Al_line_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Positrons_from_26Al_cont_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Positrons_from_44Ti_line_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Positrons_from_44Ti_cont_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Narrow_Bulge_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Broad_Bulge_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Vela_SNR_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "nova_co_511keV_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "nova_co_continuum_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "nova_co_478keV_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "4C21p35_noflare_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "4C21p35_flare_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PSRB1259_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "1E1740_compow_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "cygX1_hard_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRS1758_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "LS5039_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PSRJ1846_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "MAXIJ1820_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "MAXIJ1348_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "26Al_Cyg_Region_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "26Al_NE2001_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "60Fe_Cyg_Region_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "60Fe_NE2001_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn090424592_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn100612726_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn110605183_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn131122490_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn140329295_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn161004964_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn170405777_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn180504136_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn180703876_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn080802386_flux150_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF051103_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF070201_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF070222_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF180128A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF200415A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF231115A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "511_testing_point_source_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "511_testing_extended_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "crab_standard_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Crab_Flat_Spectrum_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "LMC_Gaussian_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "LMC_Gaussian_511_x100_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "M31_Gaussian_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "M31_Gaussian_511_x100_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Virgo_Gaussian_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Virgo_Gaussian_511_x100_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NFW_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_4151_bright_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_4151_EC200_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_4151_EC1000_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_4151_faint_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_1068_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "3C279_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PSRB1259_10x_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "1E1740_twocompt_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "cygX1_soft_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "cygX3_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "LS5039_10x_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "1RXSJ170849_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PSRJ1846_3months_unbinned_data_100x_filtered_with_SAAcut.fits.gz",
                "CasApartiallyresolved_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "CasAfullyresolved_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "CasAG16distribution_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "CasAunresolved_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "CasAsymmetric_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "eeg_Bur_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "eeg_NFW_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "gg_Bur_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "gg_NFW_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn080802386_flux300_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "MgtBurst_bright_complex_10x_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ),
        "background": wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Backgrounds/Ge",
            [
                "CosmicPhotons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GalTotal_SA100_F98_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SecondaryElectrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Total_BG_without_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "AlbedoNeutrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "AlbedoPhotons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SAA_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryProtons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryAlphas_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryElectrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryPositrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SecondaryProtons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SecondaryPositrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ),
    },
    "DC4": {
        "response": wasabi_keys(
            "COSI-SMEX/DC4/Data",
            "Responses",
            [
                "Response26Al.o4.e1805_1812.s10036231691364.m1045.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "Response44Ti.o4.e1154_1160.s9607532021290.m1215.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "Response511.o4.e509_513.s20881894470591.m2555.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "Response60FeHigh.o4.e1329_1336.s10201526728102.m1287.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "Response60FeLow.o4.e1170_1176.s9552269354945.m1188.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse.h5",
                "ResponseContinuum.o3.pol.e200_10000.b4.p12.relx.s10396905069491.m420.filtered.binnedpolarization.11D.h5",
            ],
        ) + wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Responses/extended_source_response",
            [
                "extended_source_response_511_merged.h5.gz",
                "extended_source_response_continuum_merged.h5.gz",
                "extended_source_response_Al26_merged.h5.gz",
                "extended_source_response_Ti44_merged.h5.gz",
                "extended_source_response_Fe60_low_merged.h5.gz",
                "extended_source_response_Fe60_high_merged.h5.gz",
            ],
        ),
        "orientation": wasabi_keys(
            "COSI-SMEX/DC4/Data",
            "Orientation",
            [
                "DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits",
                "DC4_final_530km_3_month_with_slew_15sbins_GalacticEarth_SAA.fits",
            ],
        ),
        "source": wasabi_keys(
            "COSI-SMEX/DC4/Data",
            "Sources",
            [
                "CasAG16distribution_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "cygX3_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GS1903_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Kepler_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "MgtBurst_bright_complex_unbinned_data_filtered_with_SAAcut.fits.gz",
                "OrEr_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "positron_annihilation_in_flight_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Positrons_Central_Source_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "positrons_thin_disk_cont_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "positrons_thin_disk_line_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SN1987A_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Tycho_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "USco_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "VelaJr_44Ti_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "LMC_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "M31_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Virgo_511_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_1068_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "NGC_4151_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "1RXSJ170849_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "magnetar2_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "4C71p07_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "3C454p3_low_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "3C454p3_high_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "CenA_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Crab_DC4_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Annihilation_eeg_NFW_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "Decay_gg_NFW_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ) + wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Sources",
            [
                "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn090424592_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn100612726_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn110605183_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn131122490_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn140329295_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn161004964_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn170405777_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn180504136_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn180703876_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_bn080802386_flux150_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF051103_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF070201_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF070222_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF180128A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF200415A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GRB_MGF231115A_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ),
        "background": wasabi_keys(
            "COSI-SMEX/DC4/Data",
            "Backgrounds",
            [
                "Total_DC4_BG_3months_unbinned_data_timecut_GRB_bn090424592.fits.gz",
                "Total_DC4_BG_3months_unbinned_data_filtered_with_SAAcut_withSAAbck.fits.gz",
                "SecondaryPositrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SAA_3months_unbinned_data_filtered_with_SAAcut_statreduced_akaHEPD01result.fits.gz",
                "AlbedoPhotons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "SecondaryProtons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "AlbedoNeutrons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryPositrons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryElectrons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryAlphas_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
                "PrimaryProtons_WithDetCstunbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ) + wasabi_keys(
            "COSI-SMEX/DC3/Data",
            "Backgrounds/Ge",
            [
                "CosmicPhotons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "GalTotal_SA100_F98_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
                "SecondaryElectrons_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
            ],
        ),
    },
}

def dropdown_choices(kind: str) -> list[str]:
    result = [AUTO_INPUT]
    for data_challenge in DATA_CHALLENGE_DOWNLOAD_KEYS.values():
        for value in data_challenge[kind]:
            if value not in result:
                result.append(value)
    return result

# === Default Wasabi keys ===
DATA_CHALLENGE_DEFAULTS = {
    "DC3": {
        "wasabi_base": "COSI-SMEX/DC3/Data",
        "response": "COSI-SMEX/DC3/Data/Responses/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5.zip",
        "orientation": "COSI-SMEX/DC3/Data/Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
        "source": "COSI-SMEX/DC3/Data/Sources/GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        "background": "COSI-SMEX/DC3/Data/Backgrounds/Ge/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
    },
    "DC4": {
        "wasabi_base": "COSI-SMEX/DC4/Data",
        "response": "COSI-SMEX/DC4/Data/Responses/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse.h5",
        "orientation": "COSI-SMEX/DC4/Data/Orientation/DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits",
        # The DC4 GRB tutorials use legacy GRB simulations from the DC3 source
        # area together with DC4 response, orientation, and background products.
        "source": "COSI-SMEX/DC3/Data/Sources/GRB_bn090424592_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        "background": "COSI-SMEX/DC4/Data/Backgrounds/Total_DC4_BG_3months_unbinned_data_filtered_with_SAAcut_withSAAbck.fits.gz",
    },
}

# === Helpers ===
def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def next_run_products_dir(base_dest: Path) -> Path:
    # Determines the next run directory based on date and incremental index
    now = datetime.now()
    yyyy_mm = now.strftime("%Y_%m")
    yymmdd = now.strftime("%y%m%d")
    base_month_dir = ensure_dir(base_dest / yyyy_mm)
    pat = re.compile(rf"^{yymmdd}(\d{{3}})$")
    max_idx = -1
    for d in base_month_dir.iterdir():
        if d.is_dir() and pat.match(d.name):
            max_idx = max(max_idx, int(pat.match(d.name).group(1)))
    run_dir = ensure_dir(base_month_dir / f"{yymmdd}{max_idx+1:03d}")
    return ensure_dir(run_dir / "products")


def make_symlinks(target_dir: Path, files_by_kind: Dict[str, str]) -> Dict[str, str]:
    """Create symlinks in target_dir for all paths in files_by_kind."""
    result = {}
    for kind, paths in files_by_kind.items():
        link = target_dir / Path(paths).name
        if not link.exists():
            link.symlink_to(paths)
            print(f"[symlink] {link} -> {paths}")
        result[kind] = str(link)
    return result

# === DAG ===
with DAG(
    dag_id="init_pipelines",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cosiflow", "init", "docker"],
    description="Initialize and stage COSI pipeline data (Docker Version)",
    params={
        "data_challenge": Param(default="DC4", enum=list(DATA_CHALLENGE_DEFAULTS.keys())),
        "response_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("response"),
            type="string",
            description="Wasabi response key. Use __default__ for the selected Data Challenge preset.",
        ),
        "orientation_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("orientation"),
            type="string",
            description="Wasabi orientation key. Use __default__ for the selected Data Challenge preset.",
        ),
        "source_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("source"),
            type="string",
            description="Wasabi source key. Use __default__ for the selected Data Challenge preset.",
        ),
        "background_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("background"),
            type="string",
            description="Wasabi background key. Use __default__ for the selected Data Challenge preset.",
        ),
        "destination": Param(default="tdrss", enum=list(DEST_MAP.keys())),  
        "eps_time": Param(
            default=50, 
            type="number",
            description="Time in seconds for the pre and post-burst background cut around the source time " \
                        "window. Default is 1 second.",),
    },
) as dag:

    # === 1. Prepare raw directories ===
    def prepare_raw_dirs():
        # Ensure that all raw data subdirectories exist locally (Airflow side)
        # Note: DockerOperator will also need these to exist on host if we mount them.
        for d in RAW_SUBDIRS.values():
            ensure_dir(d)
        return {k: str(v) for k, v in RAW_SUBDIRS.items()}

    t_prepare = PythonOperator(task_id="prepare_raw_dirs", python_callable=prepare_raw_dirs)

    # === 2. Resolve configuration ===
    def resolve_config(**context):
        # Resolve parameters and input paths
        p = context["params"]
        data_challenge = p["data_challenge"].upper()
        defaults = DATA_CHALLENGE_DEFAULTS[data_challenge]

        def selected_or_default(kind: str) -> str:
            value = p[f"{kind}_path"]
            if not value or value == AUTO_INPUT:
                return defaults[kind]
            return value

        return {
            "data_challenge": data_challenge,
            "wasabi_base": defaults["wasabi_base"],
            "destination_root": str(DEST_MAP[p["destination"]]),
            "eps_time": float(p["eps_time"]),
            "inputs": {
                "response": selected_or_default("response"),
                "orientation": selected_or_default("orientation"),
                "source": selected_or_default("source"),
                "background": selected_or_default("background"),
            },
            "dirs": {k: str(v) for k, v in RAW_SUBDIRS.items()}
        }

    t_resolve = PythonOperator(task_id="resolve_config", python_callable=resolve_config)

    # Use DockerOperator to run the staging script
    # We mount the host workspace so the container can write to data/raw
    # NOTE: 'mounts' requires the absolute path on the HOST machine.
    # Since we are inside Airflow, we can't easily know the host path unless passed as env var.
    # HOWEVER, if we share the same volume mounts as the airflow container, we can use 'volumes' in DockerOperator
    # referring to the named volume or host path.
    #
    # The workspace path on the host machine
    # This should match the actual host path where the cosi directory is located
    HOST_WORKSPACE_PATH = os.getenv("HOST_WORKSPACE_PATH")
    if not HOST_WORKSPACE_PATH:
        raise ValueError("HOST_WORKSPACE_PATH is not set. "
        "Set env var HOST_WORKSPACE_PATH in docker-compose or Airflow Variable COSIDAG_DOCKER_HOST_WORKSPACE_PATH.")

    # === 3. Stage all files ===
    t_stage = DockerOperator(
        task_id="stage_all_files",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success", # 'True' is deprecated/invalid in recent provider versions
        mount_tmp_dir=False,
        execution_timeout=timedelta(hours=2),
        # Mount fast-transient-analysis-pipeline code and shared data only
        mounts=[
            Mount(source=f"{HOST_WORKSPACE_PATH}/fast-transient-analysis-pipeline", target="/home/gamma/workspace/fast-transient-analysis-pipeline", type="bind"),
            Mount(source=f"{HOST_WORKSPACE_PATH}/cosiflow/data", target="/home/gamma/workspace/data", type="bind"),
        ],
        command=[
            "python", STAGE_SCRIPT,
            "--inputs", "{{ ti.xcom_pull(task_ids='resolve_config')['inputs'] | tojson }}",
            "--dirs", "{{ ti.xcom_pull(task_ids='resolve_config')['dirs'] | tojson }}",
            "--wasabi-base", "{{ ti.xcom_pull(task_ids='resolve_config')['wasabi_base'] }}"
        ],
        # docker_url is removed -> Airflow uses DOCKER_HOST env var automatically
        network_mode="bridge",
        do_xcom_push=True, # Capture the JSON output from stdout
    )

    # === 4. Create products directory ===
    def create_products_dir(ti):
        # Create the unique products directory for this run
        cfg = ti.xcom_pull(task_ids="resolve_config")
        pdir = next_run_products_dir(Path(cfg["destination_root"]))
        return {"products_dir": str(pdir)}

    t_products = PythonOperator(task_id="create_products_dir", python_callable=create_products_dir)

    # === 5. Create symlinks ===
    def create_symlinks(ti):
        """
        Create symlinks in products/ and return the paths for the downstream tasks.
        """
        staged_json = ti.xcom_pull(task_ids="stage_all_files")
        if isinstance(staged_json, str):
            staged = json.loads(staged_json)
        else:
            staged = staged_json

        products_dir = Path(ti.xcom_pull(task_ids="create_products_dir")["products_dir"])

        files_by_kind = {
            "source":      staged["source"],
            "response":    staged["response"],
            "orientation": staged["orientation"],
            "background":  staged["background"],
        }

        return make_symlinks(products_dir, files_by_kind)

    t_link = PythonOperator(task_id="create_symlinks", python_callable=create_symlinks)

    #=== 6. Background cut ===
    t_bkgcut = DockerOperator(
        task_id="background_cut",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=f"{HOST_WORKSPACE_PATH}/fast-transient-analysis-pipeline", target="/home/gamma/workspace/fast-transient-analysis-pipeline", type="bind"),
            Mount(source=f"{HOST_WORKSPACE_PATH}/cosiflow/data", target="/home/gamma/workspace/data", type="bind"),
        ],
        command=[
            "python", BKG_CUT_SCRIPT,
            "{{ ti.xcom_pull(task_ids='create_symlinks')['source'] }}",
            "{{ ti.xcom_pull(task_ids='create_symlinks')['background'] }}",
            "--eps_time", "{{ ti.xcom_pull(task_ids='resolve_config')['eps_time'] }}"
        ],
        # docker_url is removed -> Airflow uses DOCKER_HOST env var automatically
        network_mode="bridge",
    )

    t_prepare >> t_resolve >> t_stage >> t_products >> t_link >> t_bkgcut
