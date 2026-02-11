# dags/init_pipelines.py
# Airflow 2.x — Initialize and stage COSI pipeline data
# - staged raw subfolders (source/background/orientation/response)
# - staging & background cut executed in Docker Container via DockerOperator
# - run folder under DEST_MAP/YYYY_MM/YYMMDDXXX/products with symlinks and cut result

from __future__ import annotations
import json, os, re, shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

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

WORKSPACE_MOUNT = Mount(source="/Users/riccardofalco/cosi", target="/home/gamma/workspace", type="bind")
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
}

# === Default Wasabi keys ===
WASABI_DEFAULTS = {
    "response":    "Responses/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5.zip",
    "orientation": "Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
    "source":      "Sources/GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
    "background":  "Backgrounds/Ge/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
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

def make_symlinks(target_dir: Path, files_by_kind: Dict[str, List[Path]]) -> Dict[str, List[str]]:
    result = {}
    for kind, paths in files_by_kind.items():
        result[kind] = []
        for p in paths:
            link = target_dir / p.name
            if not link.exists():
                link.symlink_to(p)
                print(f"[symlink] {link} -> {p}")
            # append anyway, even if it already existed
            result[kind].append(str(link))
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
        "response_path": Param(default=WASABI_DEFAULTS["response"], type="string"),
        "orientation_path": Param(default=WASABI_DEFAULTS["orientation"], type="string"),
        "source_path": Param(default=WASABI_DEFAULTS["source"], type="string"),
        "background_path": Param(default=WASABI_DEFAULTS["background"], type="string"),
        "destination": Param(default="tsmap", enum=["lcurve", "tsmap"]),
        "eps_time": Param(default=1e-9, type="number"),
    },
) as dag:

    def prepare_raw_dirs():
        # Ensure that all raw data subdirectories exist locally (Airflow side)
        # Note: DockerOperator will also need these to exist on host if we mount them.
        for d in RAW_SUBDIRS.values():
            ensure_dir(d)
        return {k: str(v) for k, v in RAW_SUBDIRS.items()}

    t_prepare = PythonOperator(task_id="prepare_raw_dirs", python_callable=prepare_raw_dirs)

    def resolve_config(**context):
        # Resolve parameters and input paths
        p = context["params"]
        return {
            "destination_root": str(DEST_MAP[p["destination"]]),
            "eps_time": float(p["eps_time"]),
            "inputs": {
                "response": p["response_path"],
                "orientation": p["orientation_path"],
                "source": p["source_path"],
                "background": p["background_path"],
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
    # The user environment seems to have workspace at /Users/riccardofalco/cosi
    # We hardcode the mount for now based on the user workspace info provided.
    
    HOST_WORKSPACE_PATH = "/Users/riccardofalco/cosi"

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
            "--dirs", "{{ ti.xcom_pull(task_ids='resolve_config')['dirs'] | tojson }}"
        ],
        # docker_url is removed -> Airflow uses DOCKER_HOST env var automatically
        network_mode="bridge",
        do_xcom_push=True, # Capture the JSON output from stdout
    )

    def create_products_dir(ti):
        # Create the unique products directory for this run
        cfg = ti.xcom_pull(task_ids="resolve_config")
        pdir = next_run_products_dir(Path(cfg["destination_root"]))
        return {"products_dir": str(pdir)}

    t_products = PythonOperator(task_id="create_products_dir", python_callable=create_products_dir)

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
            "{{ ti.xcom_pull(task_ids='create_symlinks')['source'][0] }}",
            "{{ ti.xcom_pull(task_ids='create_symlinks')['background'][0] }}",
            "--eps_time", "{{ ti.xcom_pull(task_ids='resolve_config')['eps_time'] }}"
        ],
        # docker_url is removed -> Airflow uses DOCKER_HOST env var automatically
        network_mode="bridge",
    )

    def create_symlinks(ti):
        """
        Creates symlinks in products/ for staged files.
        This runs in Airflow (PythonOperator) because it manages the filesystem structure.
        """
        # Parse the XCom from t_stage (which is a JSON string from stdout)
        staged_json = ti.xcom_pull(task_ids="stage_all_files")
        if isinstance(staged_json, str):
            staged = json.loads(staged_json)
        else:
            staged = staged_json

        products_dir = Path(ti.xcom_pull(task_ids="create_products_dir")["products_dir"])

        files_by_kind = {
            "source":      [Path(p) for p in staged.get("source", [])],
            "response":    [Path(p) for p in staged.get("response", [])],
            "orientation": [Path(p) for p in staged.get("orientation", [])],
            "background":  [Path(p) for p in staged.get("background", [])],
        }

        return make_symlinks(products_dir, files_by_kind)

    t_link = PythonOperator(task_id="create_symlinks", python_callable=create_symlinks)

    t_prepare >> t_resolve >> t_stage >> t_products >> t_link >> t_bkgcut
