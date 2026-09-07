# dags/init_pipelines.py
# Airflow 2.x — Initialize and stage COSI pipeline data
# - staged raw subfolders (source/background/orientation/response)
# - staging & background cut executed in the managed COSIpy environment
# - run folder under DEST_MAP/YYYY_MM/YYMMDDXXX/products with symlinks and cut result

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

# Airflow loads this DAG through a ``*.cfmodule`` symlink whose directory is
# not automatically importable as a Python package. Add the real DAG folder so
# the adjacent pure configuration module can always be resolved.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from init_pipeline_config import (
    AUTO_INPUT,
    BGO_DEFAULTS,
    BGO_ORIENTATION_KEYS,
    DATA_CHALLENGE_DEFAULTS,
    DEST_MAP,
    PIPELINE_BRANCHES,
    normalize_pipeline_branch,
    postprocessing_task_for_branch,
    raw_dirs_for_branch,
    resolve_pipeline_config,
)

# === Config ===
COSIPY_PYTHON = os.getenv("COSIPY_PYTHON", "/home/gamma/envs/cosipy/bin/python")
STAGE_SCRIPT = "/home/gamma/airflow/pipeline/stage_files.py"
BKG_CUT_SCRIPT = "/home/gamma/airflow/pipeline/bkg_cut.py"

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
    links_by_name = {}
    for kind, paths in files_by_kind.items():
        link = target_dir / Path(paths).name
        if link.name in links_by_name:
            raise ValueError(
                f"[init_pipelines] inputs {links_by_name[link.name]!r} and {kind!r} "
                f"have the same basename {link.name!r}; refusing to overwrite a product"
            )
        links_by_name[link.name] = kind
        if link.is_symlink() and link.resolve() != Path(paths).resolve():
            raise FileExistsError(f"[init_pipelines] existing symlink has a different target: {link}")
        if link.exists() and not link.is_symlink():
            raise FileExistsError(f"[init_pipelines] product path already exists: {link}")
        if not link.exists() and not link.is_symlink():
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
    tags=["cosiflow", "init"],
    description="Initialize and stage GeD or BGO COSI pipeline data",
    params={
        "pipeline_branch": Param(
            default="GeD",
            enum=list(PIPELINE_BRANCHES),
            type="string",
            title="Pipeline branch",
            description="Select the data family to resolve, stage, and initialize.",
            section="General",
        ),
        "destination": Param(
            default="tdrss",
            enum=list(DEST_MAP.keys()),
            type="string",
            description="GeD output root. BGO always uses tdrss.",
            section="General",
        ),
        "data_challenge": Param(
            default="DC4",
            enum=list(DATA_CHALLENGE_DEFAULTS.keys()),
            type="string",
            section="GeD inputs",
        ),
        "source_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("source"),
            type="string",
            description="Wasabi source/GRB key. Use __default__ for the selected Data Challenge preset.",
            section="GeD inputs",
        ),
        "background_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("background"),
            type="string",
            description="Wasabi background key. Use __default__ for the selected Data Challenge preset.",
            section="GeD inputs",
        ),
        "orientation_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("orientation"),
            type="string",
            description="Wasabi orientation key. Use __default__ for the selected Data Challenge preset.",
            section="GeD inputs",
        ),
        "response_path": Param(
            default=AUTO_INPUT,
            enum=dropdown_choices("response"),
            type="string",
            description="Wasabi response key. Use __default__ for the selected Data Challenge preset.",
            section="GeD inputs",
        ),
        "eps_time": Param(
            default=50,
            type="number",
            minimum=0,
            description="Seconds before and after the source time window used by the GeD background cut.",
            section="GeD inputs",
        ),
        "lightcurve_path": Param(
            default=AUTO_INPUT,
            examples=[AUTO_INPUT, BGO_DEFAULTS["lightcurve"]],
            type="string",
            description="BGO light-curve NPZ key. Use __default__ for the BGO preset.",
            section="BGO inputs",
        ),
        "soft_lut_path": Param(
            default=AUTO_INPUT,
            examples=[AUTO_INPUT, BGO_DEFAULTS["soft_lut"]],
            type="string",
            description="BGO soft-spectrum LUT key. Use __default__ for the BGO preset.",
            section="BGO inputs",
        ),
        "medium_lut_path": Param(
            default=AUTO_INPUT,
            examples=[AUTO_INPUT, BGO_DEFAULTS["medium_lut"]],
            type="string",
            description="BGO medium-spectrum LUT key. Use __default__ for the BGO preset.",
            section="BGO inputs",
        ),
        "hard_lut_path": Param(
            default=AUTO_INPUT,
            examples=[AUTO_INPUT, BGO_DEFAULTS["hard_lut"]],
            type="string",
            description="BGO hard-spectrum LUT key. Use __default__ for the BGO preset.",
            section="BGO inputs",
        ),
        "bgo_orientation_path": Param(
            default=AUTO_INPUT,
            enum=[AUTO_INPUT, *BGO_ORIENTATION_KEYS],
            type="string",
            description="BGO orientation key. Use __default__ for the DC4 orientation preset.",
            section="BGO inputs",
        ),
    },
) as dag:

    # === 1. Prepare raw directories ===
    def prepare_raw_dirs(**context):
        branch = normalize_pipeline_branch(context["params"].get("pipeline_branch"))
        dirs = raw_dirs_for_branch(branch)
        for directory in dirs.values():
            ensure_dir(Path(directory))
        return dirs

    t_prepare = PythonOperator(task_id="prepare_raw_dirs", python_callable=prepare_raw_dirs)

    # === 2. Resolve configuration ===
    def resolve_config(ti, **context):
        return resolve_pipeline_config(
            context["params"], ti.xcom_pull(task_ids="prepare_raw_dirs")
        )

    t_resolve = PythonOperator(task_id="resolve_config", python_callable=resolve_config)

    # === 3. Stage all files ===
    t_stage = BashOperator(
        task_id="stage_all_files",
        bash_command='''
set -euo pipefail
"$COSIPY_PYTHON" "$STAGE_SCRIPT" \
  --inputs "$STAGE_INPUTS" \
  --dirs "$STAGE_DIRS" \
  --wasabi-base "$WASABI_BASE"
''',
        env={
            "COSIPY_PYTHON": COSIPY_PYTHON,
            "STAGE_SCRIPT": STAGE_SCRIPT,
            "STAGE_INPUTS": "{{ ti.xcom_pull(task_ids='resolve_config')['inputs'] | tojson }}",
            "STAGE_DIRS": "{{ ti.xcom_pull(task_ids='resolve_config')['dirs'] | tojson }}",
            "WASABI_BASE": "{{ ti.xcom_pull(task_ids='resolve_config')['wasabi_base'] }}",
        },
        append_env=True,
        do_xcom_push=True,
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

        cfg = ti.xcom_pull(task_ids="resolve_config")
        expected_keys = set(cfg["inputs"])
        if set(staged) != expected_keys:
            raise ValueError(
                f"[init_pipelines:{cfg['pipeline_branch']}] staged keys must be "
                f"{sorted(expected_keys)}, got {sorted(staged)}"
            )
        return make_symlinks(products_dir, staged)

    t_link = PythonOperator(task_id="create_symlinks", python_callable=create_symlinks)

    # === 6. Select branch-specific post-processing ===
    def choose_postprocessing(ti):
        cfg = ti.xcom_pull(task_ids="resolve_config")
        return postprocessing_task_for_branch(cfg["pipeline_branch"])

    t_branch = BranchPythonOperator(
        task_id="select_postprocessing",
        python_callable=choose_postprocessing,
    )

    # GeD-only background cut. BGO branches around this task entirely.
    t_bkgcut = BashOperator(
        task_id="background_cut",
        bash_command='''
set -euo pipefail
"$COSIPY_PYTHON" "$BKG_CUT_SCRIPT" "$SOURCE_PATH" "$BACKGROUND_PATH" --eps_time "$EPS_TIME"
''',
        env={
            "COSIPY_PYTHON": COSIPY_PYTHON,
            "BKG_CUT_SCRIPT": BKG_CUT_SCRIPT,
            "SOURCE_PATH": "{{ ti.xcom_pull(task_ids='create_symlinks')['source'] }}",
            "BACKGROUND_PATH": "{{ ti.xcom_pull(task_ids='create_symlinks')['background'] }}",
            "EPS_TIME": "{{ ti.xcom_pull(task_ids='resolve_config')['eps_time'] }}",
        },
        append_env=True,
    )

    t_bgo_complete = EmptyOperator(task_id="bgo_staging_complete")
    t_complete = EmptyOperator(
        task_id="initialization_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_prepare >> t_resolve >> t_stage >> t_products >> t_link >> t_branch
    t_branch >> [t_bkgcut, t_bgo_complete]
    [t_bkgcut, t_bgo_complete] >> t_complete
