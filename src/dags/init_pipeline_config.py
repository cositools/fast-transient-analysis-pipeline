"""Pure configuration helpers for the ``init_pipelines`` Airflow DAG.

This module intentionally has no Airflow imports so branch selection and input
resolution can be unit-tested without importing an Airflow installation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional


AUTO_INPUT = "__default__"
PIPELINE_BRANCHES = ("GeD", "BGO")

RAW_ROOT = Path("/home/gamma/workspace/data/raw")
DEST_MAP = {
    "lcurve": Path("/home/gamma/workspace/data/lcurve"),
    "tsmap": Path("/home/gamma/workspace/data/tsmap"),
    "fast": Path("/home/gamma/workspace/data/fast_localize_grb"),
    "tdrss": Path("/home/gamma/workspace/data/tdrss"),
}

# Every input category has its own raw directory. In particular, the BGO LUTs
# do not share a download directory, so equal basenames cannot overwrite one
# another during staging.
RAW_SUBDIRS_BY_BRANCH = {
    "GeD": {
        "source": RAW_ROOT / "source",
        "background": RAW_ROOT / "background",
        "orientation": RAW_ROOT / "orientation",
        "response": RAW_ROOT / "response",
    },
    "BGO": {
        "lightcurve": RAW_ROOT / "bgo" / "lightcurve",
        "soft_lut": RAW_ROOT / "bgo" / "soft_lut",
        "medium_lut": RAW_ROOT / "bgo" / "medium_lut",
        "hard_lut": RAW_ROOT / "bgo" / "hard_lut",
        "orientation": RAW_ROOT / "bgo" / "orientation",
    },
}

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

BGO_WASABI_BASE = "COSI-SMEX/develop/Data/Responses/BGO"
BGO_DEFAULTS = {
    "lightcurve": f"{BGO_WASABI_BASE}/bn240810880light_curve.npz",
    "soft_lut": f"{BGO_WASABI_BASE}/soft_local_loc_table_run17.pkl",
    "medium_lut": f"{BGO_WASABI_BASE}/medium_local_loc_table_run17.pkl",
    "hard_lut": f"{BGO_WASABI_BASE}/hard_local_loc_table_run17.pkl",
    "orientation": "COSI-SMEX/DC4/Data/Orientation/DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits",
}
BGO_ORIENTATION_KEYS = (
    "COSI-SMEX/DC3/Data/Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
    "COSI-SMEX/DC4/Data/Orientation/DC4_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.fits",
)

INPUT_PARAM_BY_BRANCH = {
    "GeD": {
        "source": "source_path",
        "background": "background_path",
        "orientation": "orientation_path",
        "response": "response_path",
    },
    "BGO": {
        "lightcurve": "lightcurve_path",
        "soft_lut": "soft_lut_path",
        "medium_lut": "medium_lut_path",
        "hard_lut": "hard_lut_path",
        "orientation": "bgo_orientation_path",
    },
}


def _parameter_error(branch: str, parameter: str, reason: str) -> ValueError:
    return ValueError(
        f"[init_pipelines:{branch}] invalid parameter {parameter!r}: {reason}"
    )


def normalize_pipeline_branch(value: Any = None) -> str:
    """Return the canonical branch name, defaulting legacy triggers to GeD."""
    if value is None:
        return "GeD"
    if not isinstance(value, str) or not value.strip():
        raise _parameter_error("unknown", "pipeline_branch", "expected GeD or BGO")
    normalized = {branch.lower(): branch for branch in PIPELINE_BRANCHES}.get(
        value.strip().lower()
    )
    if normalized is None:
        raise _parameter_error(
            str(value), "pipeline_branch", f"expected one of {list(PIPELINE_BRANCHES)}"
        )
    return normalized


def raw_dirs_for_branch(branch: str) -> Dict[str, str]:
    branch = normalize_pipeline_branch(branch)
    return {kind: str(path) for kind, path in RAW_SUBDIRS_BY_BRANCH[branch].items()}


def _selected_or_default(
    params: Mapping[str, Any],
    *,
    branch: str,
    kind: str,
    parameter: str,
    defaults: Mapping[str, str],
) -> str:
    value = params.get(parameter, AUTO_INPUT)
    if value is None or value == AUTO_INPUT or (
        isinstance(value, str) and not value.strip()
    ):
        return defaults[kind]
    if not isinstance(value, str) or not value.strip():
        raise _parameter_error(
            branch, parameter, "expected a Wasabi key, local path, or __default__"
        )
    return value.strip()


def resolve_pipeline_config(
    params: Mapping[str, Any], dirs: Optional[Mapping[str, str]] = None
) -> Dict[str, Any]:
    """Resolve only the inputs used by the selected pipeline branch."""
    branch = normalize_pipeline_branch(params.get("pipeline_branch"))
    input_params = INPUT_PARAM_BY_BRANCH[branch]

    if branch == "GeD":
        data_challenge_value = params.get("data_challenge", "DC4")
        if not isinstance(data_challenge_value, str):
            raise _parameter_error(branch, "data_challenge", "expected DC3 or DC4")
        data_challenge = data_challenge_value.upper()
        if data_challenge not in DATA_CHALLENGE_DEFAULTS:
            raise _parameter_error(
                branch,
                "data_challenge",
                f"expected one of {list(DATA_CHALLENGE_DEFAULTS)}",
            )
        defaults = DATA_CHALLENGE_DEFAULTS[data_challenge]

        destination = params.get("destination", "tdrss")
        if destination not in DEST_MAP:
            raise _parameter_error(
                branch, "destination", f"expected one of {list(DEST_MAP)}"
            )
        try:
            eps_time = float(params.get("eps_time", 50))
        except (TypeError, ValueError) as exc:
            raise _parameter_error(branch, "eps_time", "expected a number") from exc
        if eps_time < 0:
            raise _parameter_error(branch, "eps_time", "must be non-negative")

        payload: Dict[str, Any] = {
            "branch": branch,
            "pipeline_branch": branch,
            "data_challenge": data_challenge,
            "wasabi_base": defaults["wasabi_base"],
            "destination": destination,
            "destination_root": str(DEST_MAP[destination]),
            "eps_time": eps_time,
        }
    else:
        defaults = BGO_DEFAULTS
        # cosidag_BGO monitors only tdrss. GeD-only values such as destination,
        # data_challenge, and eps_time are deliberately not inspected here.
        payload = {
            "branch": branch,
            "pipeline_branch": branch,
            "wasabi_base": BGO_WASABI_BASE,
            "destination": "tdrss",
            "destination_root": str(DEST_MAP["tdrss"]),
        }

    payload["inputs"] = {
        kind: _selected_or_default(
            params,
            branch=branch,
            kind=kind,
            parameter=parameter,
            defaults=defaults,
        )
        for kind, parameter in input_params.items()
    }

    expected_dirs = set(input_params)
    resolved_dirs = dict(dirs or raw_dirs_for_branch(branch))
    if set(resolved_dirs) != expected_dirs:
        raise RuntimeError(
            f"[init_pipelines:{branch}] raw directory keys must be "
            f"{sorted(expected_dirs)}, got {sorted(resolved_dirs)}"
        )
    payload["dirs"] = resolved_dirs
    return payload


def postprocessing_task_for_branch(branch: str) -> str:
    """Return the only post-staging task that should run for a branch."""
    if normalize_pipeline_branch(branch) == "GeD":
        return "background_cut"
    return "bgo_staging_complete"
