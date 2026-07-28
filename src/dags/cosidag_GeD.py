"""Binned GeD branch of the fast transient pipeline."""
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
    # Keep Airflow orchestration separate from the scientific stacks used by
    # COSIpy and BC-tools. Imports happen inside external callables.
    EXTERNAL_PYTHON_COSIPY = cfg("EXTERNAL_PYTHON_COSIPY", "/home/gamma/envs/cosipy/bin/python")
    EXTERNAL_PYTHON_BCT    = cfg("EXTERNAL_PYTHON_BCT", "/home/gamma/envs/bct/bin/python")
    LIB_DIR_FAST_TRANSIENT_PIPELINE = cfg(
        "FAST_GRB_LIB_DIR",
        "/home/gamma/airflow/pipeline/fast-transient-analysis-pipeline.cfmodule/fast_transient_pipeline",
    )
    # ==============================================
    # 2. Inputs from COSIDAG sensor/resolve_inputs
    # ==============================================
    # The sensor resolves files on disk and publishes only paths through XCom.
    # Preprocessing turns those paths into the shared pipeline_config.yaml.
    SOURCE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BACKGROUND_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RESPONSE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"
    TRIGGER_TIME = "{{ ts }}"
    GED_ANALYSIS_CONFIG = {
        "default_spectrum": {
            "index": -2.2,
            "K": 10.0,
            "K_unit": "1 / (cm2 keV s)",
            "piv": 100.0,
            "piv_unit": "keV",
        },
        "tsmap": {
            "nside": 16,
            "energy_channel": [2, 3],
            "cpu_cores": 8,
            "selected_method": "moc",
            "selected_coordinates": {"l_deg": 0, "b_deg": 0},
            "cds_frame": "local",
            "map_scheme": "nested",
            "coordsys": "galactic",
            "fast_plot_name": "tsmap_fast.png",
            "moc_plot_name": "ged_multiorder_likelihood_tsmap.png",
            "plot_dpi": 300,
        },
        "binning_data": {
            "bin_size": 1,
            "eps_bkg_preburst": 20,
            "eps_bkg_postburst": 20,
            "nside": 16,
            "cosipy_time_bins": 1,
            "cosipy_energy_bins": [
                100.0,
                158.489,
                251.189,
                398.107,
                630.957,
                1000.0,
                1584.89,
                2511.89,
                3981.07,
                6309.57,
                10000.0,
            ],
            "cosipy_phi_pix_size": 6,
            "cosipy_nside": 8,
            "cosipy_scheme": "ring",
            "cosipy_psichi_binning": "local",
            "cosipy_unbinned_output": "fits",
            "cosipy_ori_file": "NA",
        },
        "light_curve": {
            "bin_size": 1,
            "nside": 16,
            "containment": 0.5,
            "plot_figsize": [10, 4],
            "plot_dpi": 150,
            "used_coordinates": {"l_deg": 0, "b_deg": 0},
        },
        "duration": {
            "lightcurve_path": "",
            "p0": 0.05,
            "is_rate": False,
            "panels": ["ged"],
            "bayes_quantile": 0.9,
            "bayes_error_nsamples": 100,
            "sentinel_value": -9999.0,
            "plot_figsize": [10, 4],
            "plot_dpi": 150,
        },
    }
    # ==============================================
    # 3. External callables
    # ==============================================
    # ExternalPythonOperator callables must import task dependencies inside the
    # function body because they execute in a different interpreter.
    # ---- 3.1. _run_preprocessing
    def _run_preprocessing(
        lib_dir: str,
        source_file: str,
        background_file: str,
        orientation_file: str,
        response_file: str,
        analysis_config: dict,
        trigger_time: str,
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
        from ged_functions import preprocess_data

        # This payload is the root of the shared YAML state used by all later
        # stages and by the GCN outbox task.
        payload = {
            "source_path": source_file,
            "background_path": background_file,
            "orientation_path": orientation_file,
            "response_path": response_file,
            "pipeline_name": "GeD",
            "cosidag_id": "cosidag_GeD",
            "trigger_time": trigger_time,
            # The unbinned prepared event product belongs to
            # cosidag_ARMselection after the DAG split.
            "prepare_event_data": False,
            "analysis_config": analysis_config,
            "input_resolved": {
                "source_path": source_file,
                "background_path": background_file,
                "orientation_path": orientation_file,
                "response_path": response_file,
            },
        }
        return preprocess_data(payload)

    # ---- 3.3. _run_data_binning
    def _run_data_binning(lib_dir: str, config_path: str):
        import os
        import sys
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}

        sys.path.insert(0, lib_dir)
        from ged_functions import _record_pipeline_task
        from ged_functions import _build_prepared_binned_data

        source_path = str(config.get("source_path", ""))
        background_path = str(config.get("background_path", ""))
        if source_path.endswith(".hdf5") and background_path.endswith(".hdf5"):
            # Tutorial or replay inputs can already be binned. Still build the
            # canonical prepared product so downstream tasks use one contract.
            print("[Data_Binning] Input files are already binned (.hdf5). Skipping binning.")
            config["source_binned_file_path"] = source_path
            config["background_binned_file_path"] = background_path
            config["binned_data"] = {
                "source_binned_file_path": source_path,
                "background_binned_file_path": background_path,
            }
            prepared_data = _build_prepared_binned_data(config, source_path, background_path)
            _record_pipeline_task(
                config,
                task_id="Data_Binning",
                task_name="Data Binning",
                input_data={
                    "config_path": config_path,
                    "source_path": source_path,
                    "background_path": background_path,
                },
                output_data={
                    "source_binned_file_path": source_path,
                    "background_binned_file_path": background_path,
                    "skipped_binning": True,
                    "prepared_data": prepared_data,
                },
                status="skipped",
            )
            with open(config_path, "w") as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            return {
                "source_binned_file_path": source_path,
                "background_binned_file_path": background_path,
                "config_path": config_path,
                "skipped_binning": True,
            }

        from ged_functions import bin_data

        source_binned_file_path, background_binned_file_path = bin_data(config_path)

        return {
            "source_binned_file_path": source_binned_file_path,
            "background_binned_file_path": background_binned_file_path,
            "config_path": config_path,
            "skipped_binning": False,
        }

    # ---- 3.4. _run_tsmap
    def _run_tsmap(
        lib_dir: str,
        config_path: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
    ):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import compute_ts_map

        return compute_ts_map(
            config_path,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    # ---- 3.5. _run_light_curve
    def _run_light_curve(
        lib_dir: str,
        config_path: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
    ):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import light_curve

        return light_curve(
            config_path,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    # ---- 3.6. _run_duration
    def _run_duration(
        lib_dir: str,
        config_path: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
    ):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import duration

        return duration(
            config_path,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

    # ---- 3.10. _queue_gcn_outbox_notice
    def _queue_gcn_outbox_notice(
        lib_dir: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        trigger_time: str,
        config_path=None,
        duration_result=None,
        topic=None,
    ):
        import os
        import sys

        sys.path.insert(0, lib_dir)
        from gcn.outbox import queue_cosi_alert

        return queue_cosi_alert(
            pipeline="GeD",
            instrument="Germanium Detectors",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            trigger_time=trigger_time,
            topic=topic,
            source_product_dir=os.path.dirname(config_path) if config_path else None,
            source_config_path=config_path,
            products={
                "duration_result": duration_result,
            },
        )

    # ======================================================================
    # DAG-GeD (bottom row)
    # ======================================================================
    # The unbinned ARM/localization track is defined in
    # cosidag_ARMselection.py. This DAG owns only the binned COSIpy track.
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
            "analysis_config": GED_ANALYSIS_CONFIG,
            "trigger_time": TRIGGER_TIME,
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

    # Node 6. TS_Map_on_different_timescales
    ged_tsmap_on_timescales = ExternalPythonOperator(
        task_id="TS_Map_on_different_timescales",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_tsmap,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "TS_Map_on_different_timescales",
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
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "Light_Curve",
        },
        dag=dag,
    )

    # Node 8. Duration
    ged_duration = ExternalPythonOperator(
        task_id="Duration",
        python=EXTERNAL_PYTHON_BCT,
        python_callable=_run_duration,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "Duration",
        },
        dag=dag,
    )

    # Node 9. Spectral_Analysis
    ged_spectral_analysis = EmptyOperator(task_id="Spectral_Analysis", dag=dag)
    
    # Node 10. Classification_GeD
    ged_classification = EmptyOperator(task_id="Classification_GeD", dag=dag)
    
    # Node 11. GCN_GeD
    ged_gcn = PythonOperator(
        task_id="GCN_GeD",
        python_callable=_queue_gcn_outbox_notice,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "GCN_GeD",
            "trigger_time": "{{ ts }}",
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
            "duration_result": "{{ ti.xcom_pull(task_ids='Duration', key='return_value') }}",
            "topic": cfg("GCN_GED_OUTBOUND_TOPIC", cfg("GCN_OUTBOUND_TOPIC_DEFAULT", "gcn.notices.cosi.ged.test.alert")),
        },
        dag=dag,
    )

    # ==============================================
    # 4. Diagram wiring
    # ==============================================
    # Diagram wiring. Large data products stay on disk; XCom carries config
    # paths and compact result payloads.
    ged_pre_processing >> ged_data_binning
    ged_data_binning >> ged_tsmap_on_timescales >> ged_light_curve >> ged_duration >> ged_spectral_analysis >> ged_classification
    ged_classification >> ged_gcn


# ==============================================
# 5. DAG definition
# ==============================================
with COSIDAG(
    # DAG identifier - associated with the Airflow Variables COSIDAG_PROCESSED::{dag_id}
    dag_id="cosidag_GeD",
    # DAG scheduling
    start_date=datetime(2025, 1, 1),
    # DAG scheduling interval (None = manual trigger only)
    schedule_interval=None,
    catchup=False,
    # DAG description
    description="GeD branch of the fast transient pipeline",
    # Levels of subdirectories to monitor for new data products. Only the basename of the monitored directories is used in the DAG
    level=3,
    only_basename="products",
    # Number of seconds to wait for new files before considering the monitored directory idle. If idle, the DAG will trigger a run
    idle_seconds=5,
    # Minimum number of files to trigger a run. If the number of files is less than this, the DAG will not trigger a run
    min_files=1,
    # Maximum number of files to trigger a run. If the number of files is more than this, the DAG will not trigger a new run
    max_active_runs=2,
    # Maximum number of tasks to run concurrently. If the number of tasks is more than this, the DAG will not trigger new tasks until some tasks finish
    max_active_tasks=32,
    # Maximum number of concurrent DAG runs. If the number of DAG runs is more than this, the DAG will not trigger new runs until some runs finish
    concurrency=32,
    # Query string to filter data folders by date.
    date_queries=f"<={datetime.now().strftime('%Y%m%d')}",
    # Policy to select the most recent folder in a monitored directory. Options: "latest_mtime" (default), "latest_ctime", "latest_atime", "latest_name"
    select_policy="latest_mtime",
    file_patterns={
        # Source file must contain "GRB" or "grb" and end with .fits.
        "grb_file": "*[Gg][Rr][Bb]*.fits*",
        # Background file must contain "BG" or "bg" and end with .fits.
        "background_file": "*[Bb][Gg]*_window.fits*",
        # Orientation file must end with .fits or .ori and must not contain "GRB" or "BG".
        "orientation_file": "regex:^(?!.*(?:GRB|BG)).*\\.(?:fits|ori)$",
        # Response file must end with .h5 and must not contain "GRB" or "BG".
        "response_file": "regex:^(?!.*(?:GRB|BG)).*\\.h5$",
    },
    # Automatically retrigger the DAG if new files are detected in the monitored directories. If False, the DAG will only trigger once 
    auto_retrig=True,
    # Custom DAG build function
    build_custom=build_custom,
    # Monitoring folders to watch for new data products
    monitoring_folders=[
        "/home/gamma/workspace/data/tdrss",
    ],
    # DAG tags for categorization in the Airflow UI
    tags=["cosidag", "fast", "pipe", "phase1", "ged"],
) as dag:
    pass
