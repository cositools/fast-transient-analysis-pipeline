from datetime import datetime
import sys
sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

# To reset the DAG, delete the Variable:
# airflow variables set COSIDAG_PROCESSED::cosidag_lcurve []

def build_custom(dag):

    HOST_WORKSPACE_PATH = "/Users/riccardofalco/cosi"
    CONTAINER_IMAGE = "fastpipeline:latest"
    
    # Path to the script INSIDE the container
    SCRIPT_PATH = "/home/gamma/workspace/fastpipeline/src/pipeline/lcurve/cosipipe_lc_ops_cosidag.py"

    # Common mounts
    MOUNTS = [
        Mount(source=f"{HOST_WORKSPACE_PATH}/fastpipeline", target="/home/gamma/workspace/fastpipeline", type="bind"),
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
            "python", SCRIPT_PATH, "bin_grb_source",
            "--unbinned_file_path", GRB_FILE,
            "--data_folder", RUN_DIR
        ],
        network_mode="bridge",
        do_xcom_push=True,
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
        dag=dag,
    )

    GRB_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_grb_source', key='return_value') }}"
    BKG_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_background', key='return_value') }}"

    plot_lightcurve = DockerOperator(
        task_id="plot_lightcurve",
        image=CONTAINER_IMAGE,
        api_version='auto',
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=MOUNTS,
        command=[
            "python", SCRIPT_PATH, "plot_lightcurve_from_cells",
            "--grb_signal_path", GRB_BINNED_FILE,
            "--background_path", BKG_BINNED_FILE,
            "--orientation_path", ORI_FILE,
            "--response_path", RSP_FILE,
            "--data_folder", RUN_DIR
        ],
        network_mode="bridge",
        do_xcom_push=True,
        dag=dag,
    )

    # [bin_grb, bin_bkg] >> aggregate >> [plot_lightcurve]
    [bin_grb, bin_bkg] >> plot_lightcurve


with COSIDAG(
    dag_id="cosidag_lcurve",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/lcurve"],
    level=3,
    # Let the sensor accept only the deepest-level leaf (products)
    only_basename="products",
    # Robustness against partial writes
    idle_seconds=5,
    min_files=1,
    # ready_marker="_SUCCESS",   # enable if you create a sentinel at end-of-write
    # controlled parallelism:
    max_active_runs=2,         # up to 2 DAG runs in parallel
    max_active_tasks=8,        # up to 8 tasks in parallel in the DAG
    concurrency=8,             # local alternative limit (Airflow <2.7)
    date_queries=f"=={datetime.now().strftime('%Y%m%d')}",
    select_policy="latest_mtime",   # or "first"
    file_patterns={
        "grb_file": "GRB*_unbinned_*.fits*",
        "background_file": "Total_BG*_unbinned_*_window.fits*",
        "orientation_file": "*.ori",
        "response_file": "Response*.h5",
    },
    auto_retrig=True,   # enable automatic retrigger
    build_custom=build_custom,
    tags=["cosidag", "lcurve"],
) as dag:
    pass
