from datetime import datetime
import os, sys
sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable


def build_custom(dag):
    # The workspace path on the host machine
    # This should match the actual host path where the cosi directory is located
    HOST_WORKSPACE_PATH = os.getenv("HOST_WORKSPACE_PATH")
    if not HOST_WORKSPACE_PATH:
        raise ValueError("HOST_WORKSPACE_PATH is not set. "
        "Set env var HOST_WORKSPACE_PATH in docker-compose or Airflow Variable COSIDAG_DOCKER_HOST_WORKSPACE_PATH.")
    
    CONTAINER_IMAGE = "fast-transient-analysis-pipeline:latest"
    
    # Path to the script INSIDE the container
    SCRIPT_PATH = "/home/gamma/workspace/fast-transient-analysis-pipeline/src/pipeline/ts_map/cosipipe_tsmap_ops_cosidag.py"

    # Common mounts
    MOUNTS = [
        Mount(source=f"{HOST_WORKSPACE_PATH}/fast-transient-analysis-pipeline", target="/home/gamma/workspace/fast-transient-analysis-pipeline", type="bind"),
        Mount(source=f"{HOST_WORKSPACE_PATH}/cosiflow/data", target="/home/gamma/workspace/data", type="bind"),
    ]

    # ----- Operators (IMPORTANT: pass dag=dag) -----

    # Pull the run_dir produced by COSIDAG's sensor:
    #   key='detected_folder' from task_id='check_new_file'
    RUN_DIR_JINJA = "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}"
    RUN_DIR = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='run_dir') }}"
    GRB_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BKG_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    ORI_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RSP_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"

    bin_grb = DockerOperator(
        task_id="bin_grb_source",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        command=[
            "python", SCRIPT_PATH, "bin_grb_data",
            "--unbinned_file_path", GRB_FILE,
            "--data_folder", RUN_DIR
        ],
        network_mode="bridge",
        do_xcom_push=True,
        xcom_all=False,
        dag=dag,
    )

    bin_bkg = DockerOperator(
        task_id="bin_background",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        command=[
            "python", SCRIPT_PATH, "bin_background_data",
            "--unbinned_file_path", BKG_FILE,
            "--data_folder", RUN_DIR
        ],
        network_mode="bridge",
        do_xcom_push=True,
        xcom_all=False,
        dag=dag,
    )

    GRB_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_grb_source', key='return_value') }}"
    # GRB_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_grb_source', key='return_value').split('\n')[-1] if ti.xcom_pull(task_ids='bin_grb_source', key='return_value') else '' }}"
    BKG_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_background', key='return_value') }}"
    # BKG_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_background', key='return_value').split('\n')[-1] if ti.xcom_pull(task_ids='bin_background', key='return_value') else '' }}"

    ts_map = DockerOperator(
        task_id="ts_map_computation",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        command=[
            "python", SCRIPT_PATH, "compute_ts_map",
            "--grb_signal_path", GRB_BINNED_FILE,
            "--background_path", BKG_BINNED_FILE,
            "--orientation_path", ORI_FILE,
            "--response_path", RSP_FILE,
            "--data_folder", RUN_DIR
        ],
        environment={
            "TSMAP_CPU_CORES": "8",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        },
        network_mode="bridge",
        # StatusCode 137 = OOM kill: TS map + 3ML need enough RAM (tune if still failing)
        mem_limit="8g",
        do_xcom_push=True,
        dag=dag,
    )

    ts_map_mulres = DockerOperator(
        task_id="ts_map_mulres_computation",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        command=[
            "python", SCRIPT_PATH, "compute_ts_map_mulres",
            "--grb_signal_path", GRB_BINNED_FILE,
            "--background_path", BKG_BINNED_FILE,
            "--orientation_path", ORI_FILE,
            "--response_path", RSP_FILE,
            "--data_folder", RUN_DIR
        ],
        environment={
            "TSMAP_CPU_CORES": "8",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
            },
        network_mode="bridge",
        mem_limit="8g",
        do_xcom_push=True,
        dag=dag,
    )

    # [bin_grb, bin_bkg] >> aggregate >> [ts_map, ts_map_mulres]
    bin_grb >> [ts_map, ts_map_mulres]
    bin_bkg >> [ts_map, ts_map_mulres]


with COSIDAG(
    dag_id="cosidag_tsmap_dock",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/tsmap"],
    level=3,
    # Let the sensor accept only the deepest-level leaf (products)
    only_basename="products",
    # Robustness against partial writes
    idle_seconds=5,
    min_files=1,
    # ready_marker="_SUCCESS",   # enable if you create a sentinel at end-of-write
    # controlled parallelism:
    max_active_runs=2,         # up to 2 DAG runs in parallel
    max_active_tasks=4,        # up to 8 tasks in parallel in the DAG
    concurrency=8,             # local alternative limit (Airflow <2.7)
    date_queries=f"=={datetime.now().strftime('%Y%m%d')}",
    file_patterns={
        "grb_file": "GRB*_unbinned_*.fits*",
        "background_file": "Total_BG*_unbinned_*_window.fits*",
        "orientation_file": "*.ori",
        "response_file": "Response*.h5",
    },
    select_policy="latest_mtime",   # oppure "first"
    build_custom=build_custom,
    tags=["cosidag", "tsmap", "dock"]
) as dag:
    pass
