"""
COSIDAG GeD (bottom row).

This DAG is the Phase-1 interface draft for the GeD branch of the fast transient pipeline.
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
    SOURCE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BACKGROUND_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    SOFT_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='soft_lut_file') }}"
    MEDIUM_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='medium_lut_file') }}"
    HARD_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='hard_lut_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RESPONSE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"
    # ==============================================
    # 3. External callables
    # ==============================================
    # ---- 3.1. _run_preprocessing
    def _run_preprocessing(
        lib_dir: str,
        source_file: str,
        background_file: str,
        orientation_file: str,
        response_file: str,
    ):
        import os
        import sys

        if not source_file or not os.path.exists(source_file):
            raise FileNotFoundError(f"grb_file not found or missing from resolve_inputs: {source_file}")
        if not background_file or not os.path.exists(background_file):
            raise FileNotFoundError(
                f"background_file not found or missing from resolve_inputs: {background_file}"
            )
        if not orientation_file or not os.path.exists(orientation_file):
            raise FileNotFoundError(
                f"orientation_file not found or missing from resolve_inputs: {orientation_file}"
            )
        if not response_file or not os.path.exists(response_file):
            raise FileNotFoundError(
                f"response_file not found or missing from resolve_inputs: {response_file}"
            )

        sys.path.insert(0, lib_dir)
        from deg_functions import preprocess_data

        payload = {
            "source_path": source_file,
            "background_path": background_file,
            "orientation_path": orientation_file,
            "response_path": response_file,
        }
        return preprocess_data(payload)

    # ---- 3.2. _run_data_binning
    def _run_unbinned_light_curve_generation(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import unbinned_light_curve_generation

        return unbinned_light_curve_generation(config_path)

    # ---- 3.3. _run_data_binning
    def _run_data_binning(lib_dir: str, config_path: str):
        import os
        import sys
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}

        source_path = str(config.get("source_path", ""))
        background_path = str(config.get("background_path", ""))
        if source_path.endswith(".hdf5") and background_path.endswith(".hdf5"):
            print("[Data_Binning] Input files are already binned (.hdf5). Skipping binning.")
            return {
                "source_binned_file_path": source_path,
                "background_binned_file_path": background_path,
                "config_path": config_path,
                "skipped_binning": True,
            }

        sys.path.insert(0, lib_dir)
        from deg_functions import bin_data

        source_binned_file_path, background_binned_file_path = bin_data(config_path)

        # Keep config aligned with downstream tasks expecting binned inputs.
        config["source_path"] = source_binned_file_path
        config["background_path"] = background_binned_file_path
        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        return {
            "source_binned_file_path": source_binned_file_path,
            "background_binned_file_path": background_binned_file_path,
            "config_path": config_path,
            "skipped_binning": False,
        }

    # ---- 3.4. _run_tsmap
    def _run_tsmap(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import compute_ts_map

        return compute_ts_map(config_path)

    # ---- 3.5. _run_light_curve
    def _run_light_curve(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import light_curve

        return light_curve(config_path)

    # ---- 3.6. _run_duration
    def _run_duration(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import duration

        return duration(config_path)

    def _run_light_curve_analysis(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import light_curve_analysis

        return light_curve_analysis(config_path)

    def _run_skymap_unbinned(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import skymap_unbinned

        return skymap_unbinned(config_path)

    def _run_duration_and_localization_results(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from deg_functions import duration_and_localization_results

        return duration_and_localization_results(config_path)

    # ======================================================================
    # DAG-GeD (bottom row)
    # ======================================================================
    # Node 1. PreProcessing_GeD
    ged_pre_processing = ExternalPythonOperator(
        task_id="PreProcessing_GeD",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_preprocessing,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "source_file": SOURCE_FILE,
            "background_file": BACKGROUND_FILE,
            "orientation_file": ORIENTATION_FILE,
            "response_file": RESPONSE_FILE,
        },
        dag=dag,
    )

    # Node 2. Unbinned_Light_Curve_Generation
    ged_unbinned_light_curve_generation = ExternalPythonOperator(
        task_id="Unbinned_Light_Curve_Generation",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_unbinned_light_curve_generation,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 3. Data_Binning
    ged_data_binning = ExternalPythonOperator(
        task_id="Data_Binning",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_data_binning,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )

    # Node 4. Light_Curve_Analysis (ON/OFF + Li&Ma prep)
    ged_light_curve_analysis = ExternalPythonOperator(
        task_id="Light_Curve_Analysis",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_light_curve_analysis,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 4.1. Skymap_unbinned (HEALPix significance map)
    ged_skymap_unbinned = ExternalPythonOperator(
        task_id="Skymap_unbinned",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_skymap_unbinned,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 5. Duration_and_Localization_Results (best pixel, CSV/PNG, optional LC script)
    ged_duration_and_localization_results = ExternalPythonOperator(
        task_id="Duration_and_Localization_Results",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_duration_and_localization_results,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )

    # Node 6. TS_Map_on_different_timescales
    ged_tsmap_on_timescales = ExternalPythonOperator(
        task_id="TS_Map_on_different_timescales",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_tsmap,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 7. Light_Curve
    ged_light_curve = ExternalPythonOperator(
        task_id="Light_Curve",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_light_curve,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 8. Duration
    ged_duration = ExternalPythonOperator(
        task_id="Duration",
        python=EXTERNAL_PYTHON_BGO,
        python_callable=_run_duration,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
        },
        dag=dag,
    )
    # Node 9. Spectral_Analysis
    ged_spectral_analysis = EmptyOperator(task_id="Spectral_Analysis", dag=dag)
    # Node 10. Classification_GeD
    ged_classification = EmptyOperator(task_id="Classification_GeD", dag=dag)
    # Node 11. GCN_GeD
    ged_gcn = EmptyOperator(task_id="GCN_GeD", dag=dag)

    # Diagram wiring:
    # Node 1 -> Node 2, Node 1 -> Node 3
    ged_pre_processing >> [ged_unbinned_light_curve_generation, ged_data_binning]

    # Node 2 -> Node 4 -> Node 5
    ged_unbinned_light_curve_generation >> ged_light_curve_analysis >> ged_skymap_unbinned >> ged_duration_and_localization_results
    # Node 3 -> Node 6 -> Node 7 -> Node 8 -> Node 9 -> Node 10
    ged_data_binning >> ged_tsmap_on_timescales >> ged_light_curve >> ged_duration >> ged_spectral_analysis >> ged_classification
    # Node 5 -> Node 11, Node 10 -> Node 11
    for node in [ged_classification, ged_duration_and_localization_results]:
        node >> ged_gcn


with COSIDAG(
    dag_id="cosidag_GeD",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="GeD branch of the fast transient pipeline",
    level=3,
    only_basename="products",
    idle_seconds=5,
    min_files=1,
    max_active_runs=2,
    max_active_tasks=32,
    concurrency=32,
    date_queries=f"=={datetime.now().strftime('%Y%m%d')}",
    select_policy="latest_mtime",
    file_patterns={
        # Fast transient inputs
#        # Support both production-style unbinned FITS and tutorial/classic binned HDF5.
        "grb_file": "*[Gg][Rr][Bb]*.fits*",
        "background_file": "*[Bb][Gg]*.fits*",
#        "background_window_file": "Total_BG*_unbinned_*_window.fits*",
#        "orientation_file": "*.fits*",
#        "response_file": "*.h5",
#        # BGO localization LUTs
#        "soft_lut_file": "soft_lut_*.pkl",
#        "medium_lut_file": "medium_lut_*.pkl",
#        "hard_lut_file": "hard_lut_*.pkl",
#        "grb_file": "*grb*binned*.hdf5",
#        "background_file": "*bkg*binned*.hdf5",
        # Orientation file must end with .fits or .ori and must not contain "GRB" or "BG".
        #"orientation_file": "!{*[Gg][Rr][Bb]*|*[Bb][Gg]*}*.{fits/ori}",
        "orientation_file": "regex:^(?!.*(?:GRB|BG)).*\\.(?:fits|ori)$",
        # Response file must end with .h5 and must not contain "GRB" or "BG".
        "response_file": "regex:^(?!.*(?:GRB|BG)).*\\.h5$",
    },
    auto_retrig=True,
    build_custom=build_custom,
    monitoring_folders=[
        "/home/gamma/workspace/data/tdrss",
    ],
    tags=["cosidag", "fast", "pipe", "phase1", "ged"],
) as dag:
    pass