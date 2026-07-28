"""Unbinned ARM-selection and fast-localization branch."""

from datetime import datetime
import sys

sys.path.append("/home/gamma/airflow/modules")

from airflow.operators.python import ExternalPythonOperator, PythonOperator
from cosidag import COSIDAG, cfg


def build_custom(dag):
    # Keep Airflow orchestration separate from the scientific environment.
    external_python_cosipy = cfg(
        "EXTERNAL_PYTHON_COSIPY",
        "/home/gamma/envs/cosipy/bin/python",
    )
    pipeline_lib_dir = cfg(
        "FAST_GRB_LIB_DIR",
        "/home/gamma/airflow/pipeline/"
        "fast-transient-analysis-pipeline.cfmodule/fast_transient_pipeline",
    )

    # This DAG resolves the same input products as cosidag_GeD. It does not
    # consume XComs from that DAG, so both DAGs can be scheduled independently.
    source_file = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    background_file = (
        "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    )
    orientation_file = (
        "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    )
    response_file = (
        "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"
    )

    arm_analysis_config = {
        "unbinned_light_curve": {
            "arm-min": -15.0,
            "arm-max": 15.0,
            "bin": 1.0,
            "out-prefix": "lc",
            "arm-hist-bins": 30,
            "format": "png",
            "diagnostics": True,
            "col-time": "TimeTags",
            "col-l": "Chi galactic",
            "col-b": "Psi galactic",
            "col-phi": "Phi",
            "plot_figsize": [8, 4],
            "diagnostics_figsize": [14, 10],
            "plot_dpi": 200,
        },
        "fast_localize": {
            "off_pre": 20.0,
            "off_gap": 5.0,
            "off_fallback_strategy": "on_background",
            "arm_min": -13.0,
            "arm_max": 13.0,
            "nside": 32,
            "nsides": None,
            "pix_chunk": 256,
            "event_chunk": 200000,
            "topk": 50,
            "out_prefix": "grb",
            "true_l_deg": None,
            "true_b_deg": None,
            "suppress_mmap_warning": True,
            "make_lc": False,
            "lc_bin": 1.0,
            "lc_arm_min": None,
            "lc_arm_max": None,
            "lc_diagnostics": False,
            "lc_script": "make_timeseries_all.py",
            "col_time": "TimeTags",
            "col_l": "Chi galactic",
            "col_b": "Psi galactic",
            "col_phi": "Phi",
            "map_plot_figsize": [10, 6],
            "map_plot_dpi": 220,
            "nside_table_figsize_width": 14,
            "nside_table_figsize_base_height": 2.4,
            "nside_table_figsize_row_height": 0.38,
            "nside_table_dpi": 240,
        },
    }

    def _run_preprocessing(
        lib_dir: str,
        source_path: str,
        background_path: str,
        orientation_path: str,
        response_path: str,
        analysis_config: dict,
        trigger_time: str,
    ):
        import os
        import sys

        input_paths = {
            "grb_file": source_path,
            "background_file": background_path,
            "orientation_file": orientation_path,
            "response_file": response_path,
        }
        for input_name, input_path in input_paths.items():
            if not input_path or not os.path.exists(input_path):
                raise FileNotFoundError(
                    f"{input_name} not found or missing from resolve_inputs: "
                    f"{input_path}"
                )

        sys.path.insert(0, lib_dir)
        from ged_functions import preprocess_data

        # The raw inputs and product directory are shared with GeD, while the
        # mutable YAML state is DAG-specific to prevent concurrent write races.
        config_path = os.path.join(
            os.path.dirname(source_path),
            "armsel_pipeline_config.yaml",
        )
        payload = {
            "source_path": source_path,
            "background_path": background_path,
            "orientation_path": orientation_path,
            "response_path": response_path,
            "pipeline_name": "ARMselection",
            "cosidag_id": "cosidag_ARMselection",
            "trigger_time": trigger_time,
            "config_path": config_path,
            "prepare_event_data": True,
            "preprocessing_task_id": "PreProcessing_ARMselection",
            "preprocessing_task_name": "PreProcessing ARM selection",
            "analysis_config": analysis_config,
            "input_resolved": {
                "source_path": source_path,
                "background_path": background_path,
                "orientation_path": orientation_path,
                "response_path": response_path,
            },
        }
        return preprocess_data(payload)

    def _run_armsel_stage(lib_dir: str, config_path: str, stage_name: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(
                "config_path not found from ARM preprocessing output: "
                f"{config_path}"
            )

        sys.path.insert(0, lib_dir)
        import armsel_functions

        stage = getattr(armsel_functions, stage_name)
        return stage(config_path)

    def _queue_localization_notice(
        lib_dir: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        trigger_time: str,
        config_path: str,
        localization_result=None,
        topic=None,
    ):
        import os
        import sys

        sys.path.insert(0, lib_dir)
        from gcn.outbox import queue_cosi_alert

        return queue_cosi_alert(
            pipeline="ARMselection",
            instrument="Germanium Detectors - ARM selection",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            trigger_time=trigger_time,
            topic=topic,
            source_product_dir=os.path.dirname(config_path),
            source_config_path=config_path,
            products={"localization_result": localization_result},
        )

    config_xcom = (
        "{{ ti.xcom_pull(task_ids='PreProcessing_ARMselection', "
        "key='return_value') }}"
    )

    preprocessing = ExternalPythonOperator(
        task_id="PreProcessing_ARMselection",
        python=external_python_cosipy,
        python_callable=_run_preprocessing,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "source_path": source_file,
            "background_path": background_file,
            "orientation_path": orientation_file,
            "response_path": response_file,
            "analysis_config": arm_analysis_config,
            "trigger_time": "{{ ts }}",
        },
        dag=dag,
    )

    unbinned_light_curve = ExternalPythonOperator(
        task_id="Unbinned_Light_Curve_Generation",
        python=external_python_cosipy,
        python_callable=_run_armsel_stage,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "config_path": config_xcom,
            "stage_name": "unbinned_light_curve_generation",
        },
        dag=dag,
    )

    light_curve_analysis = ExternalPythonOperator(
        task_id="Light_Curve_Analysis",
        python=external_python_cosipy,
        python_callable=_run_armsel_stage,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "config_path": config_xcom,
            "stage_name": "light_curve_analysis",
        },
        dag=dag,
    )

    skymap_unbinned = ExternalPythonOperator(
        task_id="Skymap_unbinned",
        python=external_python_cosipy,
        python_callable=_run_armsel_stage,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "config_path": config_xcom,
            "stage_name": "skymap_unbinned",
        },
        dag=dag,
    )

    localization_results = ExternalPythonOperator(
        task_id="Duration_and_Localization_Results",
        python=external_python_cosipy,
        python_callable=_run_armsel_stage,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "config_path": config_xcom,
            "stage_name": "duration_and_localization_results",
        },
        dag=dag,
    )

    gcn_armsel = PythonOperator(
        task_id="GCN_ARMselection",
        python_callable=_queue_localization_notice,
        op_kwargs={
            "lib_dir": pipeline_lib_dir,
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "GCN_ARMselection",
            "trigger_time": "{{ ts }}",
            "config_path": config_xcom,
            "localization_result": (
                "{{ ti.xcom_pull("
                "task_ids='Duration_and_Localization_Results', "
                "key='return_value') }}"
            ),
            "topic": cfg(
                "GCN_ARMSELECTION_OUTBOUND_TOPIC",
                cfg(
                    "GCN_GED_OUTBOUND_TOPIC",
                    cfg(
                        "GCN_OUTBOUND_TOPIC_DEFAULT",
                        "gcn.notices.cosi.ged.test.alert",
                    ),
                ),
            ),
        },
        dag=dag,
    )

    (
        preprocessing
        >> unbinned_light_curve
        >> light_curve_analysis
        >> skymap_unbinned
        >> localization_results
        >> gcn_armsel
    )


with COSIDAG(
    dag_id="cosidag_ARMselection",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Unbinned ARM selection and Li & Ma HEALPix localization",
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
        "grb_file": "*[Gg][Rr][Bb]*.fits*",
        "background_file": "*[Bb][Gg]*_window.fits*",
        "orientation_file": (
            "regex:^(?!.*(?:GRB|BG)).*\\.(?:fits|ori)$"
        ),
        "response_file": "regex:^(?!.*(?:GRB|BG)).*\\.h5$",
    },
    auto_retrig=True,
    build_custom=build_custom,
    monitoring_folders=["/home/gamma/workspace/data/tdrss"],
    tags=["cosidag", "fast", "pipe", "phase1", "ged", "arm-selection"],
) as dag:
    pass
