"""
COSIDAG BGO (top row).

This DAG is the Phase-1 interface draft for the BGO branch of the fast transient pipeline.
"""

from datetime import datetime

import sys

sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ExternalPythonOperator
from numpy import ndarray


def build_custom(dag):
    # ==============================================
    # 1. External interpreters + library dirs (same conventions as other DAGs)
    # ==============================================
    EXTERNAL_PYTHON_COSIPY = cfg("EXTERNAL_PYTHON_COSIPY", "/home/gamma/envs/cosipy/bin/python")
    EXTERNAL_PYTHON_BGO    = cfg("EXTERNAL_PYTHON_BGO", "/home/gamma/envs/bct/bin/python")
    LIB_DIR_FAST_TRANSIENT_PIPELINE = cfg(
        "FAST_GRB_LIB_DIR",
        "/home/gamma/airflow/pipeline/fast-transient-analysis-pipeline.cfmodule/fast_transient_pipeline",
    )
    # ==============================================
    # 2. Inputs from COSIDAG sensor/resolve_inputs
    # ==============================================
    LIGHTCURVE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='lightcurve_file') }}"
    SOFT_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='soft_lut_file') }}"
    MEDIUM_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='medium_lut_file') }}"
    HARD_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='hard_lut_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    # ==============================================
    # 3. External callables
    # ==============================================
    # ---- 3.1. _run_preprocessing
    def _run_preprocessing(lib_dir: str, lightcurve_file: str):
        print("Run preprocessing")
        return None
    
    # ---- 3.2. _run_get_duration
    def _run_get_duration(lib_dir: str, lightcurve_file: str):
        import sys
        import os
        import numpy as np

        if not lightcurve_file or not os.path.exists(lightcurve_file):
            raise FileNotFoundError(
                f"lightcurve_file not found or missing from resolve_inputs: {lightcurve_file}"
            )

        sys.path.insert(0, lib_dir)
        from bgo_functions import get_duration

        lightcurve = np.load(lightcurve_file)['light_curve']
        lc_timebins, bb_lc_timebins, tstart, tstop, t90, t90_err_low, t90_err_high = get_duration(
            lightcurve,
            p0=10e-5,
            isRate=False,
            panels=["z0", "z1", "x0", "x1", "y0", "y1"],
        )
        return {
            "lc_timebins": lc_timebins,
            "bb_lc_timebins": bb_lc_timebins,
            "tstart": float(tstart),
            "tstop": float(tstop),
            "t90": float(t90),
            "t90_err_low": float(t90_err_low),
            "t90_err_high": float(t90_err_high),
            "lightcurve_file": lightcurve_file,
        }

    # ---- 3.3. _run_background_extraction
    def _run_background_extraction(
        lib_dir: str, 
        lightcurve_file: str, 
        lc_timebins: dict[str, dict[str, list[float]]], 
        tstart: float, 
        tstop: float
        ):
        import sys
        import os
        import numpy as np
        import json

        if not lightcurve_file or not os.path.exists(lightcurve_file):
            raise FileNotFoundError(
                f"lightcurve_file not found or missing from resolve_inputs: {lightcurve_file}"
            )

        sys.path.insert(0, lib_dir)
        from bgo_functions import background_extraction_and_data_preparation

        signal_counts, background_counts, net_counts = background_extraction_and_data_preparation(
            lc_timebins, float(tstart), float(tstop)
        )

        return {
            "signal_counts": signal_counts.tolist(),
            "background_counts": background_counts.tolist(),
            "net_counts": net_counts.tolist(),
            "lightcurve_file": lightcurve_file,
            "tstart": float(tstart),
            "tstop": float(tstop),
        }

    # ---- 3.4. _run_light_curve_generation
    def _run_light_curve_generation(
        lib_dir: str,
        lightcurve_file: str,
        lc_timebins: dict[str, dict[str, list[float]]],
        bb_lc_timebins: dict[str, list[float]],
        tstart: float,
        tstop: float,
        t90: float,
        t90_err_low: float,
        t90_err_high: float,
    ):
        import sys
        import os

        if not lightcurve_file or not os.path.exists(lightcurve_file):
            raise FileNotFoundError(
                f"lightcurve_file not found or missing from resolve_inputs: {lightcurve_file}"
            )

        sys.path.insert(0, lib_dir)
        from bgo_functions import light_curve_generation

        analysis_dir = os.path.dirname(os.path.abspath(lightcurve_file))
        plot_counts_path, plot_background_path = light_curve_generation(
            bayes_output=(
                lc_timebins,
                bb_lc_timebins,
                float(tstart),
                float(tstop),
                float(t90),
                float(t90_err_low),
                float(t90_err_high),
            ),
            analysis_dir=analysis_dir,
            show=False,
        )

        return {
            "lightcurve_file": lightcurve_file,
            "analysis_dir": analysis_dir,
            "plots_dir": os.path.join(analysis_dir, "plots"),
            "plot_counts_path": plot_counts_path,
            "plot_background_path": plot_background_path,
            "tstart": float(tstart),
            "tstop": float(tstop),
            "t90": float(t90),
            "t90_err_low": float(t90_err_low),
            "t90_err_high": float(t90_err_high),
        }

    # ---- 3.5. _run_localization_bctools
    def _run_localization_bctools(
        lib_dir: str,
        lightcurve_file: str,
        soft_lut_path: str,
        medium_lut_path: str,
        hard_lut_path: str,
        orientation_file: str,
        s_counts_arr: ndarray,
        b_counts_arr: ndarray,
        tstart: float,
    ):
        import sys
        import os
        import numpy as np

        if not lightcurve_file or not os.path.exists(lightcurve_file):
            raise FileNotFoundError(
                f"lightcurve_file not found or missing from resolve_inputs: {lightcurve_file}"
            )

        sys.path.insert(0, lib_dir)
        from bgo_functions import localize_bctools

        data_folder = os.path.dirname(os.path.abspath(lightcurve_file))
        output_dir = localize_bctools(
            data_folder=data_folder,
            soft_lut_path=soft_lut_path,
            medium_lut_path=medium_lut_path,
            hard_lut_path=hard_lut_path,
            orientation_file=orientation_file,
            s_counts_arr=s_counts_arr,
            b_counts_arr=b_counts_arr,
            tstart=tstart,
        )

        return {
            "lightcurve_file": lightcurve_file,
            "data_folder": output_dir,
            "soft_lut_path": soft_lut_path,
            "medium_lut_path": medium_lut_path,
            "hard_lut_path": hard_lut_path,
            "localization_plots_dir": output_dir,
        }

    # ---- 3.6. _run_significance_analysis
    def _run_significance_analysis(
        lib_dir: str,
        signal_counts_arr: ndarray,
        background_counts_arr: ndarray,
        duration: float,
    ):
        import sys
        import os
        import numpy as np

        sys.path.insert(0, lib_dir)
        from bgo_functions import significance_analysis

        sigmas = significance_analysis(signal_counts_arr, background_counts_arr, duration)

        return {
            "sigmas": sigmas.tolist(),
        }
    # ======================================================================
    # DAG-BGO (top row)
    # ======================================================================
    # Node 1. PreProcessing_BGO
    # bgo_pre_processing = EmptyOperator(task_id="PreProcessing_BGO", dag=dag)
    bgo_pre_processing = ExternalPythonOperator(
        task_id="PreProcessing_BGO",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_preprocessing,
        op_kwargs={"lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE, "lightcurve_file": LIGHTCURVE_FILE},
        dag=dag,
    )
    # Node 2. Duration_on_different_binning
    bgo_duration_on_bins = ExternalPythonOperator(
        task_id="Duration_on_different_binning",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_get_duration,
        op_kwargs={"lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE, "lightcurve_file": LIGHTCURVE_FILE},
        dag=dag,
    )
    # Node 3. Background_extraction_and_data_preparation
    bgo_background_extraction_and_prep = ExternalPythonOperator(
        task_id="Background_extraction_and_data_preparation",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_background_extraction,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "lightcurve_file": LIGHTCURVE_FILE,
            "lc_timebins": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['lc_timebins'] }}",
            "tstart": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['tstart'] }}",
            "tstop": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['tstop'] }}",
        },
        dag=dag,
    )
    # Node 4. Light_Curve_generation
    bgo_light_curve_generation = ExternalPythonOperator(
        task_id="Light_Curve_generation",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_light_curve_generation,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "lightcurve_file": LIGHTCURVE_FILE,
            "lc_timebins": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['lc_timebins'] }}",
            "bb_lc_timebins": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['bb_lc_timebins'] }}",
            "tstart": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['tstart'] }}",
            "tstop": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['tstop'] }}",
            "t90": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['t90'] }}",
            "t90_err_low": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['t90_err_low'] }}",
            "t90_err_high": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['t90_err_high'] }}",
        },
        dag=dag,
    )
    # Node 5. Significance_Analysis (Li&Ma)
    bgo_significance_analysis = ExternalPythonOperator(
        task_id="Significance_Analysis",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_significance_analysis,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "signal_counts_arr": "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value')['signal_counts'] }}",
            "background_counts_arr": "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value')['background_counts'] }}",
            "duration": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['t90'] }}",
        },
        dag=dag,
    )
    # Localization block (3 internal branches)
    # Node 6. Localization_Chi2
    bgo_localization_chi2 = EmptyOperator(task_id="Localization_Chi2", dag=dag)
    # Node 7. Localization_bc_tools
    S_COUNTS_ARR = "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value')['signal_counts'] }}"
    B_COUNTS_ARR = "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value')['background_counts'] }}"
    TSTART = "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value')['tstart'] }}"

    bgo_localization_bc_tools = ExternalPythonOperator(
        task_id="Localization_bc_tools",
        python=EXTERNAL_PYTHON_BGO,
        python_callable=_run_localization_bctools,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "lightcurve_file": LIGHTCURVE_FILE,
            "soft_lut_path": SOFT_LUT,
            "medium_lut_path": MEDIUM_LUT,
            "hard_lut_path": HARD_LUT,
            "orientation_file": ORIENTATION_FILE,
            "s_counts_arr": S_COUNTS_ARR,
            "b_counts_arr": B_COUNTS_ARR,
            "tstart": TSTART,
        },
        do_xcom_push=True,
        dag=dag,
    )
    # Node 8. Localization_DL
    bgo_localization_dl = EmptyOperator(task_id="Localization_DL", dag=dag)

    # Node 9. Localization_Results
    bgo_localization_results = EmptyOperator(task_id="Localization_Results", dag=dag)
    # Node 10. Classification_BGO
    bgo_classification = EmptyOperator(task_id="Classification_BGO", dag=dag)
    # Node 11. GCN_BGO
    bgo_gcn = EmptyOperator(task_id="GCN_BGO", dag=dag)

    # Diagram wiring:
    # Node 1 -> Node 2
    bgo_pre_processing >> bgo_duration_on_bins
    # Node 2 -> Node 3, Node 2 -> Node 4, Node 2 -> Node 5
    bgo_duration_on_bins >> [
        bgo_background_extraction_and_prep,
        bgo_light_curve_generation,
    ]
    # Node 3 -> Node 6, Node 3 -> Node 7, Node 3 -> Node 8
    bgo_background_extraction_and_prep >> [
        bgo_localization_chi2,
        bgo_localization_bc_tools,
        bgo_localization_dl,
        bgo_significance_analysis,
    ]
    for node in [bgo_localization_chi2, bgo_localization_bc_tools, bgo_localization_dl, bgo_significance_analysis]: 
        node >> bgo_localization_results
    # Node 9 -> Node 10, Node 11
    bgo_localization_results >> [bgo_classification, bgo_gcn]
    # Node 10 -> Node 11
    bgo_classification >> bgo_gcn

    


with COSIDAG(
    dag_id="cosidag_BGO",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="BGO branch of the fast transient pipeline",
    level=3,
    only_basename="products",
    idle_seconds=5,
    min_files=1,
    max_active_runs=2,
    max_active_tasks=32,
    concurrency=32,
    date_queries=f"<={datetime.now().strftime('%Y%m%d')}",
    select_policy="latest_mtime",
    file_patterns={
        # Fast transient inputs
        "lightcurve_file": "*.npz",
        "soft_lut_file": "soft_lut_*.pkl",
        "medium_lut_file": "medium_lut_*.pkl",
        "hard_lut_file": "hard_lut_*.pkl",
        "orientation_file": "*.ori",
    },
    auto_retrig=True,
    render_template_as_native_obj=True,
    build_custom=build_custom,
    monitoring_folders=[
        "/home/gamma/workspace/data/tdrss",
    ],
    tags=["cosidag", "fast", "pipe", "phase1", "bgo"],
) as dag:
    pass