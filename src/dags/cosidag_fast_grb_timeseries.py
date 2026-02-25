"""
COSIDAG Fast GRB Time-series: esecuzione con ExternalPythonOperator e virtualenv cosipy.
Esegue fast_ops_grb_timeseries.py (light curve ARM-gated) per una data localizzazione (l,b).
Parametri l, b, tmin, tmax: da conf alla trigger (es. {"l": 12.5, "b": -3.2, "tmin": ..., "tmax": ...})
oppure da Airflow Variables fast_timeseries_l, fast_timeseries_b, fast_timeseries_tmin, fast_timeseries_tmax.
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
    TMIN = 1841896384
    TMAX = 1841896448.2616737
    L_DEG = 41.6460638775
    B_DEG = 19.2471581697
    ARM_MIN = -15.0
    ARM_MAX = 15.0
    BIN = 1.0
    OUT_PREFIX = "lc"
    FORMAT = "png"
    NO_WARNING = True
    DIAGNOSTICS = True
    ARM_HIST_BINS = 30
    RUN_DIR = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='run_dir') }}"
    GRB_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"

    # ----- Callable executed in the external interpreter (venv cosipy) -----
    def _run_fast_grb_timeseries(lib_dir: str, **kwargs):
        import sys
        sys.path.insert(0, lib_dir)
        import fast_ops_grb_timeseries
        return fast_ops_grb_timeseries.run_grb_timeseries(
            data=kwargs["data"],
            tmin=kwargs["tmin"],
            tmax=kwargs["tmax"],
            l=kwargs["l"],
            b=kwargs["b"],
            arm_min=kwargs["arm_min"],
            arm_max=kwargs["arm_max"],
            bin=kwargs["bin"],
            out_prefix=kwargs["out_prefix"],
            outdir=kwargs["outdir"],
            format=kwargs["format"],
            no_warning=kwargs["no_warning"],
            diagnostics=kwargs["diagnostics"],
            arm_hist_bins=kwargs["arm_hist_bins"]
        )

    # ----- Operators -----
    

    run_timeseries = ExternalPythonOperator(
        task_id="fast_grb_timeseries",
        python=EXTERNAL_PYTHON,
        python_callable=_run_fast_grb_timeseries,
        op_kwargs={
            "lib_dir": LIB_DIR,
            "data": GRB_FILE,
            "tmin": TMIN,
            "tmax": TMAX,
            "l": L_DEG,
            "b": B_DEG,
            "arm_min": ARM_MIN,
            "arm_max": ARM_MAX,
            "bin": BIN,
            "out_prefix": OUT_PREFIX,
            "outdir": RUN_DIR,
            "format": FORMAT,
            "no_warning": NO_WARNING,
            "diagnostics": DIAGNOSTICS,
            "arm_hist_bins": ARM_HIST_BINS
        },
        dag=dag,
    )


with COSIDAG(
    dag_id="cosidag_fast_grb_timeseries",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/fast_localize_grb"],
    level=3,
    only_basename="products",
    idle_seconds=5,
    min_files=1,
    max_active_runs=2,
    max_active_tasks=8,
    concurrency=8,
    date_queries=f"=={datetime.now().strftime('%Y%m%d')}",
    file_patterns={
        "grb_file": "GRB*_unbinned_*.fits",
    },
    select_policy="latest_mtime",
    build_custom=build_custom,
    tags=["cosidag", "fast_grb", "timeseries", "extpy"],
) as dag:
    pass
