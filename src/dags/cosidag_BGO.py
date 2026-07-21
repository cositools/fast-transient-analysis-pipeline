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
    LIGHTCURVE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='lightcurve_file') }}"
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
            "panels": ["z0", "z1", "x0", "x1", "y0", "y1"],
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
            "counts_order": ["BGO_X0", "BGO_X1", "BGO_Y0", "BGO_Y1", "BGO_Z0", "BGO_Z1"],
            "output_plot_name": "bgo_localization.png",
        },
    }
    # ==============================================
    # 3. External callables
    # ==============================================
    # ExternalPythonOperator serializes these callables into temporary scripts.
    # Imports therefore stay inside the callable so they resolve in the selected
    # virtual environment, not in the scheduler environment.
    # ---- 3.1. _run_preprocessing
    def _run_preprocessing(
        lib_dir: str,
        lightcurve_file: str,
        soft_lut_path: str,
        medium_lut_path: str,
        hard_lut_path: str,
        orientation_file: str,
        analysis_config: dict,
        trigger_time: str,
    ):
        import os
        import sys

        for name, path in [
            ("lightcurve_file", lightcurve_file),
            ("soft_lut_file", soft_lut_path),
            ("medium_lut_file", medium_lut_path),
            ("hard_lut_file", hard_lut_path),
            ("orientation_file", orientation_file),
        ]:
            if not path or not os.path.exists(path):
                raise FileNotFoundError(f"{name} not found or missing from resolve_inputs: {path}")

        sys.path.insert(0, lib_dir)
        from bgo_functions import preprocess_data

        # Preprocessing is the only task that builds the canonical YAML state
        # from COSIDAG inputs. Later tasks receive this path and update it.
        payload = {
            "lightcurve_path": lightcurve_file,
            "soft_lut_path": soft_lut_path,
            "medium_lut_path": medium_lut_path,
            "hard_lut_path": hard_lut_path,
            "orientation_path": orientation_file,
            "pipeline_name": "BGO",
            "cosidag_id": "cosidag_BGO",
            "trigger_time": trigger_time,
            "analysis_config": analysis_config,
            "input_resolved": {
                "lightcurve_path": lightcurve_file,
                "soft_lut_path": soft_lut_path,
                "medium_lut_path": medium_lut_path,
                "hard_lut_path": hard_lut_path,
                "orientation_path": orientation_file,
            },
        }
        return preprocess_data(payload)
    
    # ---- 3.2. _run_get_duration
    def _run_get_duration(lib_dir: str, config_path: str):
        import sys
        import os
        import numpy as np
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        import fast_helper_functions as fhf
        from bgo_functions import get_duration
        from bgo_functions import _record_pipeline_task
        from gcn.query import query_relevant_grb_notices

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}
        lightcurve_file = config.get("lightcurve_path") or config.get("lightcurve_file")
        duration_config = config.get("duration", {})
        lightcurve_key = duration_config.get("lightcurve_key", "light_curve")
        # The BGO light-curve NPZ stores the multi-panel array under a
        # configurable key so future challenge products can keep the same DAG.
        lightcurve = np.load(lightcurve_file)[lightcurve_key]
        lc_timebins, bb_lc_timebins, tstart, tstop, t90, t90_err_low, t90_err_high = get_duration(
            lightcurve,
            p0=float(duration_config.get("p0", 10e-5)),
            isRate=bool(duration_config.get("is_rate", False)),
            panels=duration_config.get("panels", ["z0", "z1", "x0", "x1", "y0", "y1"]),
        )
        result = {
            "lc_timebins": lc_timebins,
            "bb_lc_timebins": bb_lc_timebins,
            "tstart": float(tstart),
            "tstop": float(tstop),
            "t90": float(t90),
            "t90_err_low": float(t90_err_low),
            "t90_err_high": float(t90_err_high),
            "lightcurve_file": lightcurve_file,
        }
        gcn_query_result = query_relevant_grb_notices(
            config=config,
            task_id="Duration_on_different_binning",
            tstart=tstart,
            tstop=tstop,
        )
        result["gcn_notice_query"] = gcn_query_result
        config["duration_result"] = result
        config.setdefault("gcn_notice_queries", {})["Duration_on_different_binning"] = gcn_query_result
        _record_pipeline_task(
            config,
            task_id="Duration_on_different_binning",
            task_name="Duration on different binning",
            input_data={
                "config_path": config_path,
                "lightcurve_path": lightcurve_file,
                "lightcurve_key": lightcurve_key,
                "duration": duration_config,
            },
            output_data=result,
        )
        fhf._save_yaml(config_path, config)
        return result

    # ---- 3.3. _run_background_extraction
    def _run_background_extraction(
        lib_dir: str,
        config_path: str,
    ):
        import sys
        import os
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        import fast_helper_functions as fhf
        from bgo_functions import background_extraction_and_data_preparation
        from bgo_functions import _record_pipeline_task

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}
        duration_result = config.get("duration_result", {})
        background_config = config.get("background", {})
        # Background fitting consumes the serialized TimeBins produced by the
        # duration task and returns compact per-panel count arrays.
        signal_counts, background_counts, net_counts = background_extraction_and_data_preparation(
            duration_result["lc_timebins"],
            float(duration_result["tstart"]),
            float(duration_result["tstop"]),
            buffer=float(background_config.get("buffer", 0.0)),
            order=int(background_config.get("order", 2)),
        )

        lightcurve_file = config.get("lightcurve_path") or config.get("lightcurve_file")
        result = {
            "signal_counts": signal_counts.tolist(),
            "background_counts": background_counts.tolist(),
            "net_counts": net_counts.tolist(),
            "lightcurve_file": lightcurve_file,
            "tstart": float(duration_result["tstart"]),
            "tstop": float(duration_result["tstop"]),
        }
        config["background_result"] = result
        _record_pipeline_task(
            config,
            task_id="Background_extraction_and_data_preparation",
            task_name="Background extraction and data preparation",
            input_data={
                "config_path": config_path,
                "duration_result": duration_result,
                "background": background_config,
            },
            output_data=result,
        )
        fhf._save_yaml(config_path, config)
        return result

    # ---- 3.4. _run_light_curve_generation
    def _run_light_curve_generation(
        lib_dir: str,
        config_path: str,
    ):
        import sys
        import os
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        import fast_helper_functions as fhf
        from bgo_functions import light_curve_generation
        from bgo_functions import _record_pipeline_task

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}
        lightcurve_file = config.get("lightcurve_path") or config.get("lightcurve_file")
        duration_result = config.get("duration_result", {})
        light_curve_config = config.get("light_curve", {})
        analysis_dir = os.path.dirname(os.path.abspath(lightcurve_file))
        plot_counts_path, plot_background_path = light_curve_generation(
            bayes_output=(
                duration_result["lc_timebins"],
                duration_result["bb_lc_timebins"],
                float(duration_result["tstart"]),
                float(duration_result["tstop"]),
                float(duration_result["t90"]),
                float(duration_result["t90_err_low"]),
                float(duration_result["t90_err_high"]),
            ),
            analysis_dir=analysis_dir,
            panel=light_curve_config.get("panel"),
            show=bool(light_curve_config.get("show", False)),
        )

        result = {
            "lightcurve_file": lightcurve_file,
            "analysis_dir": analysis_dir,
            "plots_dir": os.path.join(analysis_dir, "plots"),
            "plot_counts_path": plot_counts_path,
            "plot_background_path": plot_background_path,
            "tstart": float(duration_result["tstart"]),
            "tstop": float(duration_result["tstop"]),
            "t90": float(duration_result["t90"]),
            "t90_err_low": float(duration_result["t90_err_low"]),
            "t90_err_high": float(duration_result["t90_err_high"]),
        }
        config["lightcurve_result"] = result
        _record_pipeline_task(
            config,
            task_id="Light_Curve_generation",
            task_name="Light Curve generation",
            input_data={
                "config_path": config_path,
                "duration_result": duration_result,
                "light_curve": light_curve_config,
            },
            output_data=result,
        )
        fhf._save_yaml(config_path, config)
        return result

    # ---- 3.5. _run_localization_bctools
    def _run_localization_bctools(
        lib_dir: str,
        config_path: str,
        background_result=None,
        duration_result=None,
    ):
        import ast
        import sys
        import os
        import numpy as np
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        import fast_helper_functions as fhf
        from bgo_functions import localize_bctools
        from bgo_functions import _record_pipeline_task

        def _as_mapping(value):
            # Airflow native rendering usually preserves dicts, but templated
            # XComs can still arrive as strings depending on operator context.
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

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}
        lightcurve_file = config.get("lightcurve_path") or config.get("lightcurve_file")
        background_result = _as_mapping(background_result) or config.get("background_result", {})
        duration_result = _as_mapping(duration_result) or config.get("duration_result", {})
        soft_lut_path = config.get("soft_lut_path")
        medium_lut_path = config.get("medium_lut_path")
        hard_lut_path = config.get("hard_lut_path")
        orientation_file = config.get("orientation_path")
        missing_keys = [
            key
            for key in ["signal_counts", "background_counts"]
            if key not in background_result
        ]
        if missing_keys:
            raise KeyError(
                "Missing keys from Background_extraction_and_data_preparation result: "
                f"{missing_keys}"
            )
        if "tstart" not in duration_result:
            raise KeyError("Missing key from Duration_on_different_binning result: tstart")
        data_folder = os.path.dirname(os.path.abspath(lightcurve_file))
        # Pass upstream XCom payloads directly to avoid depending on the latest
        # writer of pipeline_config.yaml when parallel branches finish close together.
        output_dir = localize_bctools(
            data_folder=data_folder,
            soft_lut_path=soft_lut_path,
            medium_lut_path=medium_lut_path,
            hard_lut_path=hard_lut_path,
            orientation_file=orientation_file,
            s_counts_arr=np.asarray(background_result["signal_counts"]),
            b_counts_arr=np.asarray(background_result["background_counts"]),
            tstart=float(duration_result["tstart"]),
        )

        result = {
            "lightcurve_file": lightcurve_file,
            "data_folder": output_dir,
            "soft_lut_path": soft_lut_path,
            "medium_lut_path": medium_lut_path,
            "hard_lut_path": hard_lut_path,
            "localization_plots_dir": output_dir,
        }
        config["localization_result"] = result
        _record_pipeline_task(
            config,
            task_id="Localization_bc_tools",
            task_name="Localization bc tools",
            input_data={
                "config_path": config_path,
                "background_result": background_result,
                "duration_result": duration_result,
                "soft_lut_path": soft_lut_path,
                "medium_lut_path": medium_lut_path,
                "hard_lut_path": hard_lut_path,
                "orientation_path": orientation_file,
            },
            output_data=result,
        )
        fhf._save_yaml(config_path, config)
        return result

    # ---- 3.6. _run_significance_analysis
    def _run_significance_analysis(
        lib_dir: str,
        config_path: str,
        background_result=None,
        duration_result=None,
    ):
        import ast
        import sys
        import os
        import numpy as np
        import yaml

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"config_path not found from preprocessing output: {config_path}")

        sys.path.insert(0, lib_dir)
        import fast_helper_functions as fhf
        from bgo_functions import significance_analysis
        from bgo_functions import _record_pipeline_task

        def _as_mapping(value):
            # See localization wrapper: tolerate both native dicts and templated
            # string representations from XCom.
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

        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader) or {}
        background_result = _as_mapping(background_result) or config.get("background_result", {})
        duration_result = _as_mapping(duration_result) or config.get("duration_result", {})
        missing_keys = [
            key
            for key in ["signal_counts", "background_counts"]
            if key not in background_result
        ]
        if missing_keys:
            raise KeyError(
                "Missing keys from Background_extraction_and_data_preparation result: "
                f"{missing_keys}"
            )
        if "t90" not in duration_result:
            raise KeyError("Missing key from Duration_on_different_binning result: t90")
        sigmas = significance_analysis(
            np.asarray(background_result["signal_counts"]),
            np.asarray(background_result["background_counts"]),
            float(duration_result["t90"]),
        )

        result = {
            "sigmas": sigmas.tolist(),
        }
        config["significance_result"] = result
        _record_pipeline_task(
            config,
            task_id="Significance_Analysis",
            task_name="Significance Analysis",
            input_data={
                "config_path": config_path,
                "background_result": background_result,
                "duration_result": duration_result,
            },
            output_data=result,
        )
        fhf._save_yaml(config_path, config)
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
    # The BGO graph mirrors the Phase-1 diagram: duration fans out to plotting
    # and background work; background feeds significance and localization.
    # Node 1. PreProcessing_BGO
    # bgo_pre_processing = EmptyOperator(task_id="PreProcessing_BGO", dag=dag)
    bgo_pre_processing = ExternalPythonOperator(
        task_id="PreProcessing_BGO",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=_run_preprocessing,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "lightcurve_file": LIGHTCURVE_FILE,
            "soft_lut_path": SOFT_LUT,
            "medium_lut_path": MEDIUM_LUT,
            "hard_lut_path": HARD_LUT,
            "orientation_file": ORIENTATION_FILE,
            "analysis_config": BGO_ANALYSIS_CONFIG,
            "trigger_time": TRIGGER_TIME,
        },
        dag=dag,
    )
    
    # Node 2. Duration_on_different_binning
    bgo_duration_on_bins = ExternalPythonOperator(
        task_id="Duration_on_different_binning",
        python=EXTERNAL_PYTHON_BGO,
        python_callable=_run_get_duration,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
        },
        dag=dag,
    )
    
    # Node 3. Background_extraction_and_data_preparation
    bgo_background_extraction_and_prep = ExternalPythonOperator(
        task_id="Background_extraction_and_data_preparation",
        python=EXTERNAL_PYTHON_BGO,
        python_callable=_run_background_extraction,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
        },
        dag=dag,
    )
    
    # Node 4. Light_Curve_generation
    bgo_light_curve_generation = ExternalPythonOperator(
        task_id="Light_Curve_generation",
        python=EXTERNAL_PYTHON_BGO,
        python_callable=_run_light_curve_generation,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
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
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
            "background_result": "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value') }}",
            "duration_result": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value') }}",
        },
        dag=dag,
    )
    
    # Localization block. Chi2 and DL are placeholders; bc_tools is the active
    # branch and runs in nimcosipy because it needs cosipy.nonimaging.
    
    # Node 6. Localization_Chi2
    bgo_localization_chi2 = EmptyOperator(task_id="Localization_Chi2", dag=dag)
    
    # Node 7. Localization_bc_tools
    bgo_localization_bc_tools = ExternalPythonOperator(
        task_id="Localization_bc_tools",
        python=EXTERNAL_PYTHON_NIMCOSIPY,
        python_callable=_run_localization_bctools,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
            "background_result": "{{ ti.xcom_pull(task_ids='Background_extraction_and_data_preparation', key='return_value') }}",
            "duration_result": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value') }}",
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
    bgo_gcn = PythonOperator(
        task_id="GCN_BGO",
        python_callable=_queue_gcn_outbox_notice,
        op_kwargs={
            "lib_dir": LIB_DIR_FAST_TRANSIENT_PIPELINE,
            "dag_id": "{{ dag.dag_id }}",
            "dag_run_id": "{{ run_id }}",
            "task_id": "GCN_BGO",
            "trigger_time": "{{ ts }}",
            "config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_BGO', key='return_value') }}",
            "duration_result": "{{ ti.xcom_pull(task_ids='Duration_on_different_binning', key='return_value') }}",
            "significance_result": "{{ ti.xcom_pull(task_ids='Significance_Analysis', key='return_value') }}",
            "localization_result": "{{ ti.xcom_pull(task_ids='Localization_bc_tools', key='return_value') }}",
            "topic": cfg("GCN_BGO_OUTBOUND_TOPIC", cfg("GCN_OUTBOUND_TOPIC_DEFAULT", "gcn.notices.cosi.bgo.test.alert")),
        },
        dag=dag,
    )

    # ==============================================
    # 4. Diagram wiring
    # ==============================================
    # XCom carries small result dictionaries; large products and
    # shared metadata stay on disk in pipeline_config.yaml.
    bgo_pre_processing >> bgo_duration_on_bins
    bgo_duration_on_bins >> [
        bgo_background_extraction_and_prep,
        bgo_light_curve_generation,
    ]
    bgo_background_extraction_and_prep >> [
        bgo_localization_chi2,
        bgo_localization_bc_tools,
        bgo_localization_dl,
        bgo_significance_analysis,
    ]
    for node in [bgo_localization_chi2, bgo_localization_bc_tools, bgo_localization_dl, bgo_significance_analysis]: 
        node >> bgo_localization_results
    bgo_localization_results >> [bgo_classification, bgo_gcn]
    bgo_classification >> bgo_gcn

    

# ==============================================
# 5. DAG definition
# ==============================================
with COSIDAG(
    # DAG identifier - associated with the Airflow Variables COSIDAG_PROCESSED::{dag_id}
    dag_id="cosidag_BGO",
    # DAG scheduling
    start_date=datetime(2025, 1, 1),
    # DAG scheduling interval (None = manual trigger only)
    schedule_interval=None,
    catchup=False,
    # DAG description
    description="BGO branch of the fast transient pipeline",
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
        # Fast transient inputs. The orientation pattern excludes science files
        # containing GRB/BG and accepts both current FITS and legacy ORI files.
        "lightcurve_file": "*.npz",
        # Lookup tables for BGO localization
        "soft_lut_file": "soft_*.pkl",
        "medium_lut_file": "medium_*.pkl",
        "hard_lut_file": "hard_*.pkl",
        # Orientation file must end with .fits or .ori and must not contain "GRB" or "BG".
        "orientation_file": "regex:^(?!.*(?:GRB|BG)).*\\.(?:fits|ori)$",
    },
    # Automatically retrigger the DAG if new files are detected in the monitored directories. If False, the DAG will only trigger once 
    auto_retrig=True,
    # Render templates as native Python objects instead of strings. This allows for more complex data structures to be passed between tasks.
    render_template_as_native_obj=True,
    # Custom DAG build function
    build_custom=build_custom,
    # Monitoring folders to watch for new data products
    monitoring_folders=[
        "/home/gamma/workspace/data/tdrss",
    ],
    # DAG tags for categorization in the Airflow UI
    tags=["cosidag", "fast", "pipe", "phase1", "bgo"],
) as dag:
    pass
