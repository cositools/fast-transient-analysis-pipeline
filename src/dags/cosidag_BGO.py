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
from airflow.operators.python import ExternalPythonOperator, PythonOperator


def build_custom(dag):
    # ==============================================
    # 1. External interpreters + library dirs
    # ==============================================
    # Keep heavyweight scientific stacks isolated. The Airflow worker only
    # orchestrates tasks; each stage runs in the environment that owns its
    # dependencies.
    EXTERNAL_PYTHON_COSIPY = cfg("EXTERNAL_PYTHON_COSIPY", "/home/gamma/envs/cosipy/bin/python")
    EXTERNAL_PYTHON_BGO    = cfg("EXTERNAL_PYTHON_BGO", "/home/gamma/envs/bct/bin/python")
    EXTERNAL_PYTHON_NIMCOSIPY = cfg(
        "EXTERNAL_PYTHON_NIMCOSIPY",
        "/home/gamma/envs/nimcosipy/bin/python",
    )
    LIB_DIR_FAST_TRANSIENT_PIPELINE = cfg(
        "FAST_GRB_LIB_DIR",
        "/home/gamma/airflow/pipeline/fast-transient-analysis-pipeline.cfmodule/fast_transient_pipeline",
    )
    # ==============================================
    # 2. Inputs from COSIDAG sensor/resolve_inputs
    # ==============================================
    # The COSIDAG sensor resolves concrete file paths and exposes them through
    # XCom. Preprocessing persists those paths into pipeline_config.yaml.
    GRB_FITS_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_fits_file') }}"
    SOFT_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='soft_lut_file') }}"
    MEDIUM_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='medium_lut_file') }}"
    HARD_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='hard_lut_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    TRIGGER_TIME = "{{ ts }}"
    BGO_ANALYSIS_CONFIG = {
        "duration": {
            "lightcurve_key": "light_curve",
            "p0": 10e-5,
            "is_rate": False,
            "panels": ["z1", "z0", "x1", "x0", "y1", "y0"],
        },
        "background": {
            "buffer": 0.0,
            "order": 2,
        },
        "light_curve": {
            "panel": None,
            "show": False,
        },
        "significance": {
            "min_signal_counts": 10,
            "min_background_counts": 10,
        },
        "localization_bctools": {
            "nside": 64,
            "counts_order": ["BGO_Z1", "BGO_Z0", "BGO_X1", "BGO_X0", "BGO_Y1", "BGO_Y0"],
            "output_plot_name": "bgo_localization.png",
        },
    }
    # ==============================================
    # 3. External callables
    # ==============================================
    # ExternalPythonOperator serializes these callables into temporary scripts.
    # Imports therefore stay inside the callable so they resolve in the selected
    # virtual environment, not in the scheduler environment.
    # ---- 3.1 data_preparation from .FITS
    def _run_extract_source_data(
    lib_dir: str,
    grb_fits_path: str,
    soft_lut_path: str,
    medium_lut_path: str,
    hard_lut_path: str,
    orientation_file: str,
    trigger_time: str,
    analysis_config: dict,
    ):
        import os
        import sys
        from pathlib import Path

        if not grb_fits_path or not os.path.exists(grb_fits_path):
            raise FileNotFoundError(
                f"GRB FITS file not found from resolve_inputs: {grb_fits_path}"
            )

        sys.path.insert(0, lib_dir)

        from bgo_functions import extract_source_data_from_fits

        grb_file_name = Path(grb_fits_path).stem
        output_dir = str(Path(grb_fits_path).parent / "plots")

        pipeline_config = {
            "pipeline_name": "BGO",
            "cosidag_id": "cosidag_BGO",
            "trigger_time": trigger_time,
            "lightcurve_path": grb_fits_path,
            "lightcurve_file": grb_fits_path,
            "soft_lut_path": soft_lut_path,
            "medium_lut_path": medium_lut_path,
            "hard_lut_path": hard_lut_path,
            "orientation_path": orientation_file,
            "analysis_config": analysis_config,
        }

        (
            s_counts,
            b_counts,
            grb_time,
            bblocks_analysis_results,
            config_path,
        ) = extract_source_data_from_fits(
            pipeline_config=pipeline_config,
            grb_fits_path=grb_fits_path,
            grb_file_name=grb_file_name,
            panels=["z1", "z0", "x1", "x0", "y1", "y0"],
            plot=True,
            save_plot=True,
            plot_counts=False,
            output_dir=output_dir,
        )

        return {
            "grb_fits_path": grb_fits_path,
            "grb_file_name": grb_file_name,
            "config_path": config_path,
            "signal_counts": s_counts.tolist(),
            "background_counts": b_counts.tolist(),
            "grb_time": float(grb_time),
            "signal_tstart": float(
                bblocks_analysis_results["signal_tstart"]
            ),
            "signal_tstop": float(
                bblocks_analysis_results["signal_tstop"]
            ),
            "t90": float(
                bblocks_analysis_results["t90"]
            ),
            "t90_err_low": float(
                bblocks_analysis_results["t90_err_low"]
            ),
            "t90_err_high": float(
                bblocks_analysis_results["t90_err_high"]
            ),
            "significance": float(
                bblocks_analysis_results["significance"]
            ),
            "significance_peak": float(
                bblocks_analysis_results["significance_peak"]
            ),
            "plots_dir": output_dir,
        }
        

    # ---- 3.5. _run_localization_bctools
    def _run_localization_bctools(
        lib_dir: str,
        extract_result=None,
        soft_lut_path: str = None,
        medium_lut_path: str = None,
        hard_lut_path: str = None,
        orientation_file: str = None,
    ):
        import ast
        import sys
        import os
        import numpy as np
        import yaml

        sys.path.insert(0, lib_dir)

        from bgo_functions import localize_bctools

        def _as_mapping(value):
            if isinstance(value, dict):
                return value

            if isinstance(value, str) and value:
                for loader in (
                    yaml.safe_load,
                    ast.literal_eval,
                ):
                    try:
                        loaded = loader(value)
                    except Exception:
                        continue

                    if isinstance(loaded, dict):
                        return loaded

            return {}

        extract_result = _as_mapping(extract_result)

        missing_keys = [
            key
            for key in [
                "signal_counts",
                "background_counts",
                "grb_time",
                "grb_fits_path",
            ]
            if key not in extract_result
        ]

        if missing_keys:
            raise KeyError(
                f"Missing keys from extraction result: {missing_keys}"
            )

        data_folder = os.path.dirname(
            os.path.abspath(
                extract_result["grb_fits_path"]
            )
        )

        output_dir = localize_bctools(
            pipeline_config=extract_result["config_path"],
            data_folder=data_folder,
            soft_lut_path=soft_lut_path,
            medium_lut_path=medium_lut_path,
            hard_lut_path=hard_lut_path,
            orientation_file=orientation_file,
            s_counts_arr=np.asarray(
                extract_result["signal_counts"],
                dtype=float,
            ),
            b_counts_arr=np.asarray(
                extract_result["background_counts"],
                dtype=float,
            ),
            tstart=float(
                extract_result["grb_time"]
            ),
        )

        result = {
            "grb_fits_path": extract_result["grb_fits_path"],
            "data_folder": output_dir,
            "soft_lut_path": soft_lut_path,
            "medium_lut_path": medium_lut_path,
            "hard_lut_path": hard_lut_path,
            "localization_plots_dir": output_dir,
        }

        return result

    # ---- 3.6. _run_significance_analysis
    def _run_significance_analysis(
        lib_dir: str,
        extract_result=None,
    ):
        import ast
        import sys
        import numpy as np
        import yaml

        sys.path.insert(0, lib_dir)

        from bgo_functions import significance_analysis

        def _as_mapping(value):
            # Tolerate both native dictionaries and templated string
            # representations received from Airflow XCom.
            if isinstance(value, dict):
                return value

            if isinstance(value, str) and value:
                for loader in (yaml.safe_load, ast.literal_eval):
                    try:
                        loaded = loader(value)
                    except Exception:
                        continue

                    if isinstance(loaded, dict):
                        return loaded

            return {}

        extract_result = _as_mapping(extract_result)

        missing_keys = [
            key
            for key in [
                "signal_counts",
                "background_counts",
                "t90",
            ]
            if key not in extract_result
        ]

        if missing_keys:
            raise KeyError(
                f"Missing keys from extraction result: {missing_keys}"
            )

        sigmas = significance_analysis(
            extract_result["config_path"],
            np.asarray(
                extract_result["signal_counts"],
                dtype=float,
            ),
            np.asarray(
                extract_result["background_counts"],
                dtype=float,
            ),
            float(extract_result["t90"]),
        )

        result = {
            "sigmas": sigmas.tolist(),
        }

        return result

    # ---- 3.7. _queue_gcn_outbox_notice
    def _queue_gcn_outbox_notice(
        lib_dir: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        trigger_time: str,
        config_path=None,
        duration_result=None,
        significance_result=None,
        localization_result=None,
        topic=None,
    ):
        import os
        import sys

        sys.path.insert(0, lib_dir)
        from gcn.outbox import queue_cosi_alert

        return queue_cosi_alert(
            pipeline="BGO",
            instrument="BGO Shields",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            trigger_time=trigger_time,
            topic=topic,
            source_product_dir=os.path.dirname(config_path) if config_path else (localization_result or {}).get("localization_plots_dir"),
            source_config_path=config_path,
            products={
                "duration_result": duration_result,
                "significance_result": significance_result,
                "localization_result": localization_result,
            },
        )
    # ======================================================================
    # DAG-BGO (top row)
    # ======================================================================
    #The extraction task fans out to significance and localization branches.
    # Node 1. BGO data extraction from FITS
    bgo_extract_source_data = ExternalPythonOperator(
    task_id="Extract_Source_Data_From_FITS",
    python=EXTERNAL_PYTHON_NIMCOSIPY,
    python_callable=_run_extract_source_data,
    op_kwargs={
        "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
        "grb_fits_path": GRB_FITS_FILE,
        "soft_lut_path": SOFT_LUT,
        "medium_lut_path": MEDIUM_LUT,
        "hard_lut_path": HARD_LUT,
        "orientation_file": ORIENTATION_FILE,
        "trigger_time": TRIGGER_TIME,
        "analysis_config": BGO_ANALYSIS_CONFIG,
    },
    do_xcom_push=True,
    dag=dag,
)
    # Node 2. Significance_Analysis (Li&Ma)
    bgo_significance_analysis = ExternalPythonOperator(
    task_id="Significance_Analysis",
    python=EXTERNAL_PYTHON_COSIPY,
    python_callable=_run_significance_analysis,
    op_kwargs={
        "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
        "extract_result": "{{ ti.xcom_pull(task_ids='Extract_Source_Data_From_FITS', key='return_value') }}",
    },
    dag=dag,
)
    # Localization block. Chi2 and DL are placeholders; bc_tools is the active
    # branch and runs in nimcosipy because it needs cosipy.nonimaging.
    # Node 3. Localization_Chi2
    bgo_localization_chi2 = EmptyOperator(task_id="Localization_Chi2", dag=dag)
    # Node 4. Localization_bc_tools
    bgo_localization_bc_tools = ExternalPythonOperator(
    task_id="Localization_bc_tools",
    python=EXTERNAL_PYTHON_NIMCOSIPY,
    python_callable=_run_localization_bctools,
    op_kwargs={
        "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
        "extract_result": "{{ ti.xcom_pull(task_ids='Extract_Source_Data_From_FITS', key='return_value') }}",
        "soft_lut_path": SOFT_LUT,
        "medium_lut_path": MEDIUM_LUT,
        "hard_lut_path": HARD_LUT,
        "orientation_file": ORIENTATION_FILE,
    },
    do_xcom_push=True,
    dag=dag,
    )
    # Node 5. Localization_DL
    bgo_localization_dl = EmptyOperator(task_id="Localization_DL", dag=dag)

    # Node 6. Localization_Results
    bgo_localization_results = EmptyOperator(task_id="Localization_Results", dag=dag)
    # Node 7. Classification_BGO
    bgo_classification = EmptyOperator(task_id="Classification_BGO", dag=dag)
    # Node 8. GCN_BGO
    bgo_gcn = PythonOperator(
        task_id="GCN_BGO",
        python_callable=_queue_gcn_outbox_notice,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "GCN_BGO",
            "trigger_time": "{{ ts }}",
            "config_path": "{{ ti.xcom_pull(task_ids='Extract_Source_Data_From_FITS', key='return_value')['config_path'] }}",
            "duration_result": "{{ ti.xcom_pull(task_ids='Extract_Source_Data_From_FITS', key='return_value') }}",
            "significance_result": "{{ ti.xcom_pull(task_ids='Significance_Analysis', key='return_value') }}",
            "localization_result": "{{ ti.xcom_pull(task_ids='Localization_bc_tools', key='return_value') }}",
            "topic": cfg(
                "GCN_BGO_OUTBOUND_TOPIC",
                cfg(
                    "GCN_OUTBOUND_TOPIC_DEFAULT",
                    "gcn.notices.cosi.bgo.test.alert",
                ),
            ),
        },
        dag=dag,
    )

    # Diagram wiring. XCom carries small result dictionaries; large products and
    # shared metadata stay on disk in pipeline_config.yaml.
    bgo_extract_source_data >> [
    bgo_localization_chi2,
    bgo_localization_bc_tools,
    bgo_localization_dl,
    bgo_significance_analysis,
    ]
    for node in [
        bgo_localization_chi2,
        bgo_localization_bc_tools,
        bgo_localization_dl,
        bgo_significance_analysis,
        ]:
        node >> bgo_localization_results
    bgo_localization_results >> [
    bgo_classification,
    bgo_gcn,
    ]

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
        # Fast transient inputs. The orientation pattern excludes science files
        # containing GRB/BG and accepts legacy ORI files.
        "grb_fits_file": "*.fits",
        "soft_lut_file": "soft_*.pkl",
        "medium_lut_file": "medium_*.pkl",
        "hard_lut_file": "hard_*.pkl",
        "orientation_file": "DC4*.ori",
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
