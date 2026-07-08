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
from airflow.operators.python import ExternalPythonOperator, PythonOperator
from numpy import ndarray


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
    SOFT_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='soft_lut_file') }}"
    MEDIUM_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='medium_lut_file') }}"
    HARD_LUT = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='hard_lut_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RESPONSE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"
    TRIGGER_TIME = "{{ ts }}"
    GED_ANALYSIS_CONFIG = {
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
            "moc_plot_name": "tsmap_moc.png",
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
            "analysis_config": analysis_config,
            "input_resolved": {
                "source_path": source_file,
                "background_path": background_file,
                "orientation_path": orientation_file,
                "response_path": response_file,
            },
        }
        return preprocess_data(payload)

    # ---- 3.2. _run_data_binning
    def _run_unbinned_light_curve_generation(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import unbinned_light_curve_generation

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
    def _run_tsmap(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import compute_ts_map

        return compute_ts_map(config_path)

    # ---- 3.5. _run_light_curve
    def _run_light_curve(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import light_curve

        return light_curve(config_path)

    # ---- 3.6. _run_duration
    def _run_duration(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import duration

        return duration(config_path)

    # ---- 3.10. _queue_gcn_outbox_notice
    def _queue_gcn_outbox_notice(
        lib_dir: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        trigger_time: str,
        config_path=None,
        duration_result=None,
        localization_result=None,
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
                "localization_result": localization_result,
            },
        )

    def _run_light_curve_analysis(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import light_curve_analysis

        return light_curve_analysis(config_path)

    def _run_skymap_unbinned(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import skymap_unbinned

        return skymap_unbinned(config_path)

    def _run_duration_and_localization_results(lib_dir: str, config_path: str):
        import os
        import sys

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        from ged_functions import duration_and_localization_results

        return duration_and_localization_results(config_path)

    # ======================================================================
    # DAG-GeD (bottom row)
    # ======================================================================
    # The GeD branch has two analysis tracks: an unbinned fast-localization
    # branch and a binned COSIpy branch. Both publish final products to GCN_GeD.
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
        python=EXTERNAL_PYTHON_BCT,
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
            "localization_result": "{{ ti.xcom_pull(task_ids='Duration_and_Localization_Results', key='return_value') }}",
            "topic": cfg("GCN_GED_OUTBOUND_TOPIC", cfg("GCN_OUTBOUND_TOPIC_DEFAULT", "gcn.notices.cosi.ged.test.alert")),
        },
        dag=dag,
    )

    # Diagram wiring. Large data products stay on disk; XCom carries config
    # paths and compact result payloads.
    ged_pre_processing >> [ged_unbinned_light_curve_generation, ged_data_binning]

    ged_unbinned_light_curve_generation >> ged_light_curve_analysis >> ged_skymap_unbinned >> ged_duration_and_localization_results
    ged_data_binning >> ged_tsmap_on_timescales >> ged_light_curve >> ged_duration >> ged_spectral_analysis >> ged_classification
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
        # Fast transient inputs.
        "grb_file": "*[Gg][Rr][Bb]*.fits*",
        "background_file": "*[Bb][Gg]*_window.fits*",
        # Orientation file must end with .fits or .ori and must not contain "GRB" or "BG".
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
