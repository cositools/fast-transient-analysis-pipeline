from __future__ import annotations

import ast
import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "src" / "pipeline" / "fast_transient_pipeline"
DAGS_DIR = REPO_ROOT / "src" / "dags"


def load_plot_identity(module_path: Path):
    tree = ast.parse(module_path.read_text(), filename=str(module_path))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_plot_identity"
    )
    isolated_module = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            function,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(isolated_module)
    namespace = {"os": os}
    exec(compile(isolated_module, str(module_path), "exec"), namespace)
    return namespace["_plot_identity"]


def external_operator_kwargs_by_task(dag_path: Path) -> dict[str, dict]:
    tree = ast.parse(dag_path.read_text(), filename=str(dag_path))
    operators = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "ExternalPythonOperator":
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        task_id = ast.literal_eval(keywords["task_id"])
        op_kwargs_node = keywords["op_kwargs"]
        op_kwargs = {}
        for key_node, value_node in zip(
            op_kwargs_node.keys,
            op_kwargs_node.values,
        ):
            key = ast.literal_eval(key_node)
            try:
                op_kwargs[key] = ast.literal_eval(value_node)
            except ValueError:
                continue
        operators[task_id] = op_kwargs
    return operators


class PlotMetadataTests(unittest.TestCase):
    MODULE_PATHS = (
        PIPELINE_DIR / "ged_functions.py",
        PIPELINE_DIR / "bgo_functions.py",
    )

    def test_explicit_dag_context_is_written_to_png_metadata(self):
        for module_path in self.MODULE_PATHS:
            with self.subTest(module=module_path.name):
                plot_identity = load_plot_identity(module_path)
                with redirect_stdout(io.StringIO()):
                    _, metadata = plot_identity(
                        "GeD",
                        "Test caption",
                        dag_id="cosidag_test",
                        dag_run_id="manual__2026-07-28T12:00:00+00:00",
                        task_id="Test_Plot_Task",
                    )

                self.assertEqual(metadata["DAGID"], "cosidag_test")
                self.assertEqual(
                    metadata["DAGRunID"],
                    "manual__2026-07-28T12:00:00+00:00",
                )
                self.assertEqual(metadata["DAGTaskID"], "Test_Plot_Task")

    def test_airflow_environment_is_used_as_fallback(self):
        context = {
            "AIRFLOW_CTX_DAG_ID": "cosidag_environment",
            "AIRFLOW_CTX_DAG_RUN_ID": "scheduled__environment",
            "AIRFLOW_CTX_TASK_ID": "Environment_Task",
        }
        for module_path in self.MODULE_PATHS:
            with self.subTest(module=module_path.name):
                plot_identity = load_plot_identity(module_path)
                with patch.dict(os.environ, context, clear=False):
                    with redirect_stdout(io.StringIO()):
                        _, metadata = plot_identity("BGO", "Test caption")

                self.assertEqual(metadata["DAGID"], context["AIRFLOW_CTX_DAG_ID"])
                self.assertEqual(
                    metadata["DAGRunID"],
                    context["AIRFLOW_CTX_DAG_RUN_ID"],
                )
                self.assertEqual(
                    metadata["DAGTaskID"],
                    context["AIRFLOW_CTX_TASK_ID"],
                )

    def test_plot_calls_forward_dag_context(self):
        expected_calls = {
            "ged_functions.py": 4,
            "bgo_functions.py": 3,
        }
        for filename, expected_count in expected_calls.items():
            module_path = PIPELINE_DIR / filename
            tree = ast.parse(module_path.read_text(), filename=str(module_path))
            calls = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_plot_identity"
            ]
            self.assertEqual(len(calls), expected_count)
            for call in calls:
                with self.subTest(module=filename, line=call.lineno):
                    keywords = {keyword.arg for keyword in call.keywords}
                    self.assertTrue(
                        {"dag_id", "dag_run_id", "task_id"}.issubset(keywords)
                    )

    def test_plot_operators_receive_airflow_context(self):
        expected_tasks = {
            "cosidag_GeD.py": (
                "TS_Map_on_different_timescales",
                "Light_Curve",
                "Duration",
            ),
            "cosidag_BGO.py": (
                "Light_Curve_generation",
                "Localization_bc_tools",
            ),
        }
        for filename, task_ids in expected_tasks.items():
            operators = external_operator_kwargs_by_task(DAGS_DIR / filename)
            for task_id in task_ids:
                with self.subTest(dag=filename, task_id=task_id):
                    op_kwargs = operators[task_id]
                    self.assertEqual(op_kwargs["dag_id"], "{{ dag.dag_id }}")
                    self.assertEqual(op_kwargs["dag_run_id"], "{{ run_id }}")
                    self.assertEqual(op_kwargs["task_id"], task_id)


if __name__ == "__main__":
    unittest.main()
