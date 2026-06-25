"""
COSIDAG Fast GRB Localization: esecuzione con ExternalPythonOperator e virtualenv cosipy.
Esegue fast_ops_localize_grb.py (mappa HEALPix Li&Ma ARM-gated) su cartelle monitorate.
tmin/tmax: da conf alla trigger (es. {"tmin": 1841896404.0, "tmax": 1841896448.26}) o da
Airflow Variables fast_localize_grb_tmin e fast_localize_grb_tmax.
"""

from datetime import datetime
import sys

sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.operators.python import ExternalPythonOperator


def build_custom(dag):
    EXTERNAL_PYTHON = cfg("EXTERNAL_PYTHON", "/home/gamma/envs/cosipy/bin/python")
    LIB_DIR = cfg(
        "FAST_GRB_LIB_DIR",
        "/home/gamma/airflow/pipeline/fast-transient-analysis-pipeline.cfmodule/fast_grb",
    )
    # ----- Default values -----
    TMIN = 1841896404.0
    TMAX = 1841896448.2616737
    OFF_PRE = 20.0
    ARM_MIN = -13
    ARM_MAX = 13
    NSIDES = [4,8,16,32,64]
    N_SIDE = 32
    TRUE_L = 41.6460638775
    TRUE_B = 19.2471581697
    OUT_PREFIX = "bn110605183"
    NO_WARNING = True
    MAKE_LC = False
    LC_BIN = 1.0
    LC_ARM_MIN = None
    LC_ARM_MAX = None
    LC_DIAGNOSTICS = False
    LC_SCRIPT = "make_timeseries_all.py"
    PIX_CHUNK = 256
    EVENT_CHUNK = 200000
    TOPK = 50
    OFF_GAP = 5.0
    GRB_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BKG_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    RUN_DIR = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='run_dir') }}"


    # ----- Callable eseguita nell'interprete esterno (venv cosipy) -----
    def _run_fast_localize_grb(lib_dir: str, **kwargs):
        import sys
        sys.path.insert(0, lib_dir)
        import fast_ops_localize_grb
        # tmin/tmax possono arrivare come stringa da Jinja
        kwargs["tmin"] = float(kwargs["tmin"])
        kwargs["tmax"] = float(kwargs["tmax"])
        return fast_ops_localize_grb.run_fast_localize_grb(
            data=kwargs["data"],
            bkg=kwargs["bkg"],
            tmin=kwargs["tmin"],
            tmax=kwargs["tmax"],
            off_pre=kwargs["off_pre"],
            off_gap=kwargs["off_gap"],
            arm_min=kwargs["arm_min"],
            arm_max=kwargs["arm_max"],
            nside=kwargs["nside"],
            nsides=kwargs.get("nsides"),
            pix_chunk=kwargs["pix_chunk"],
            event_chunk=kwargs["event_chunk"],
            topk=kwargs["topk"],
            outdir=kwargs["outdir"],
            out_prefix=kwargs["out_prefix"],
            true_l=kwargs.get("true_l"),
            true_b=kwargs.get("true_b"),
            no_warning=kwargs["no_warning"],
            make_lc=kwargs["make_lc"],
            lc_bin=kwargs["lc_bin"],
            lc_arm_min=kwargs.get("lc_arm_min"),
            lc_arm_max=kwargs.get("lc_arm_max"),
            lc_diagnostics=kwargs["lc_diagnostics"],
            lc_script=kwargs["lc_script"],
        )

    run_localize = ExternalPythonOperator(
        task_id="fast_localize_grb",
        python=EXTERNAL_PYTHON,
        python_callable=_run_fast_localize_grb,
        op_kwargs={
            "lib_dir": LIB_DIR,
            "data": GRB_FILE,
            "bkg": BKG_FILE,
            "tmin": TMIN,
            "tmax": TMAX,
            "off_pre": OFF_PRE,
            "off_gap": OFF_GAP,
            "arm_min": ARM_MIN,
            "arm_max": ARM_MAX,
            "nside": N_SIDE,
            "nsides": NSIDES,
            "pix_chunk": PIX_CHUNK,
            "event_chunk": EVENT_CHUNK,
            "topk": TOPK,
            "outdir": RUN_DIR,
            "out_prefix": OUT_PREFIX,
            "true_l": TRUE_L,
            "true_b": TRUE_B,
            "no_warning": NO_WARNING,
            "make_lc": MAKE_LC,
            "lc_bin": LC_BIN,
            "lc_arm_min": LC_ARM_MIN,
            "lc_arm_max": LC_ARM_MAX,
            "lc_diagnostics": LC_DIAGNOSTICS,
            "lc_script": LC_SCRIPT,
        },
        dag=dag,
    )


with COSIDAG(
    dag_id="cosidag_fast_localize_grb",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/fast_localize_grb"],
    description="Fast GRB Localization",
    level=3,
    only_basename="products",
    idle_seconds=5,
    min_files=1,
    max_active_runs=2,
    max_active_tasks=8,
    concurrency=8,
    date_queries=f"=={datetime.now().strftime('%Y%m%d')}",
    file_patterns={
        "grb_file": "GRB*_unbinned_*.fits*",
        "background_file": "regex:^Total.*BG.*_unbinned_(?!.*_window).*\\.fits(?:\\.gz)?$",
    },
    select_policy="latest_mtime",
    build_custom=build_custom,
    tags=["cosidag", "fast_grb", "localize", "extpy"],
) as dag:
    pass
