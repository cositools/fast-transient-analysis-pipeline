import ast
import fnmatch
import importlib.util
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src" / "dags"))
sys.path.insert(0, str(REPO_ROOT / "src" / "pipeline"))

from init_pipeline_config import (  # noqa: E402
    BGO_DEFAULTS,
    DATA_CHALLENGE_DEFAULTS,
    postprocessing_task_for_branch,
    resolve_pipeline_config,
)
import stage_files  # noqa: E402
from stage_files import stage_inputs  # noqa: E402


AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None


def load_file_patterns(dag_path):
    tree = ast.parse(dag_path.read_text(), filename=str(dag_path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg == "file_patterns":
                return ast.literal_eval(keyword.value)
    raise AssertionError(f"No file_patterns mapping found in {dag_path}")


def matches_cosidag_pattern(name, pattern):
    if pattern.startswith("regex:"):
        return re.match(pattern[len("regex:") :], name) is not None
    return fnmatch.fnmatch(name, pattern)


class ResolveConfigTests(unittest.TestCase):
    def test_resolves_legacy_ged_defaults(self):
        config = resolve_pipeline_config({})

        self.assertEqual(config["pipeline_branch"], "GeD")
        self.assertEqual(config["data_challenge"], "DC4")
        self.assertEqual(config["destination"], "tdrss")
        self.assertEqual(
            config["inputs"],
            {
                kind: DATA_CHALLENGE_DEFAULTS["DC4"][kind]
                for kind in ("source", "background", "orientation", "response")
            },
        )
        self.assertEqual(set(config["dirs"]), set(config["inputs"]))

    def test_resolves_dc3_ged_defaults(self):
        config = resolve_pipeline_config({"data_challenge": "DC3"})

        self.assertEqual(config["pipeline_branch"], "GeD")
        self.assertEqual(config["data_challenge"], "DC3")
        self.assertEqual(
            config["inputs"],
            {
                kind: DATA_CHALLENGE_DEFAULTS["DC3"][kind]
                for kind in ("source", "background", "orientation", "response")
            },
        )

    def test_resolves_bgo_defaults_and_forces_tdrss(self):
        config = resolve_pipeline_config(
            {
                "pipeline_branch": "BGO",
                "destination": "lcurve",
                "data_challenge": "not-applicable",
                "eps_time": "not-applicable",
            }
        )

        self.assertEqual(config["pipeline_branch"], "BGO")
        self.assertEqual(config["destination"], "tdrss")
        self.assertEqual(
            config["destination_root"], "/home/gamma/workspace/data/tdrss"
        )
        self.assertEqual(config["inputs"], BGO_DEFAULTS)
        self.assertNotIn("eps_time", config)
        self.assertNotIn("data_challenge", config)

    def test_explicit_bgo_file_override(self):
        override = "COSI-SMEX/develop/Data/Responses/BGO/custom_soft.pkl"
        config = resolve_pipeline_config(
            {"pipeline_branch": "BGO", "soft_lut_path": override}
        )

        self.assertEqual(config["inputs"]["soft_lut"], override)
        self.assertEqual(config["inputs"]["medium_lut"], BGO_DEFAULTS["medium_lut"])

    def test_only_selected_branch_keys_are_resolved(self):
        bgo_config = resolve_pipeline_config(
            {
                "pipeline_branch": "BGO",
                "source_path": 123,
                "background_path": "",
                "response_path": [],
                "orientation_path": {},
            }
        )
        self.assertEqual(
            set(bgo_config["inputs"]),
            {"lightcurve", "soft_lut", "medium_lut", "hard_lut", "orientation"},
        )

        ged_config = resolve_pipeline_config(
            {"pipeline_branch": "GeD", "lightcurve_path": 123}
        )
        self.assertEqual(
            set(ged_config["inputs"]),
            {"source", "background", "orientation", "response"},
        )

    def test_selected_branch_validation_names_branch_and_parameter(self):
        with self.assertRaisesRegex(ValueError, r"init_pipelines:BGO.*soft_lut_path"):
            resolve_pipeline_config(
                {"pipeline_branch": "BGO", "soft_lut_path": ["not", "a", "path"]}
            )


class GenericStagingTests(unittest.TestCase):
    def test_generic_staging_uses_only_supplied_keys_with_mocked_downloader(self):
        inputs = {
            "lightcurve": "COSI-SMEX/example/light_curve.npz",
            "soft_lut": "/local/soft_table.pkl",
        }
        with tempfile.TemporaryDirectory() as tmp:
            dirs = {
                "lightcurve": str(Path(tmp) / "lightcurve"),
                "soft_lut": str(Path(tmp) / "soft_lut"),
            }
            downloader = Mock(
                side_effect=lambda value, target, **kwargs: str(target / Path(value).name)
            )

            staged = stage_inputs(
                inputs,
                dirs,
                wasabi_base="COSI-SMEX/example",
                downloader=downloader,
            )

        self.assertEqual(set(staged), set(inputs))
        self.assertEqual(downloader.call_count, 2)
        self.assertEqual(
            {call.kwargs["kind"] for call in downloader.call_args_list}, set(inputs)
        )
        self.assertNotIn("background", staged)

    def test_generic_staging_rejects_a_raw_target_collision(self):
        with tempfile.TemporaryDirectory() as tmp:
            inputs = {"first": "/a/same.pkl", "second": "/b/same.pkl"}
            dirs = {"first": tmp, "second": tmp}
            with self.assertRaisesRegex(ValueError, "overwrite the same raw file"):
                stage_inputs(inputs, dirs, downloader=Mock())

    def test_bgo_orientation_does_not_use_background_fits_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source" / "orientation.fits"
            source.parent.mkdir()
            source.write_bytes(b"not a FITS file; validation must not run for BGO")
            target_dir = Path(tmp) / "raw" / "bgo" / "orientation"

            with patch.object(
                stage_files,
                "validate_background_fits",
                side_effect=AssertionError("background validation was called"),
            ) as validator:
                staged = stage_inputs(
                    {"orientation": str(source)},
                    {"orientation": str(target_dir)},
                )

        self.assertEqual(Path(staged["orientation"]).name, "orientation.fits")
        validator.assert_not_called()


class BranchAndContractTests(unittest.TestCase):
    def test_docker_tasks_mount_the_airflow_heasarc_data_root(self):
        dag_source = (
            REPO_ROOT / "src" / "dags" / "cosipipe_initpipeline.py"
        ).read_text()

        self.assertIn('"HOST_DATA_PATH"', dag_source)
        self.assertEqual(
            dag_source.count(
                'Mount(source=HOST_DATA_PATH, target="/home/gamma/workspace/data"'
            ),
            2,
        )
        self.assertNotIn(
            'Mount(source=f"{HOST_WORKSPACE_PATH}/cosiflow/data"', dag_source
        )

    def test_bgo_bypasses_background_cut(self):
        self.assertEqual(
            postprocessing_task_for_branch("BGO"), "bgo_staging_complete"
        )
        self.assertEqual(postprocessing_task_for_branch("GeD"), "background_cut")

    def test_bgo_default_basenames_match_cosidag_contract(self):
        patterns = load_file_patterns(REPO_ROOT / "src" / "dags" / "cosidag_BGO.py")
        default_by_pattern = {
            "lightcurve_file": BGO_DEFAULTS["lightcurve"],
            "soft_lut_file": BGO_DEFAULTS["soft_lut"],
            "medium_lut_file": BGO_DEFAULTS["medium_lut"],
            "hard_lut_file": BGO_DEFAULTS["hard_lut"],
            "orientation_file": BGO_DEFAULTS["orientation"],
        }

        for pattern_key, path in default_by_pattern.items():
            with self.subTest(pattern_key=pattern_key, path=path):
                self.assertTrue(
                    matches_cosidag_pattern(Path(path).name, patterns[pattern_key])
                )


@unittest.skipUnless(AIRFLOW_AVAILABLE, "Airflow is not installed")
class AirflowDagDefinitionTests(unittest.TestCase):
    def test_branch_graph_and_param_sections(self):
        from cosipipe_initpipeline import dag

        self.assertEqual(dag.dag_id, "init_pipelines")
        self.assertEqual(
            dag.params.get_param("pipeline_branch").schema["enum"], ["GeD", "BGO"]
        )
        self.assertEqual(
            dag.params.get_param("pipeline_branch").schema["section"], "General"
        )
        self.assertEqual(
            dag.params.get_param("source_path").schema["section"], "GeD inputs"
        )
        self.assertEqual(
            dag.params.get_param("lightcurve_path").schema["section"], "BGO inputs"
        )
        orientation_enum = dag.params.get_param("bgo_orientation_path").schema["enum"]
        self.assertIn(BGO_DEFAULTS["orientation"], orientation_enum)
        self.assertTrue(any(value.endswith(".ori") for value in orientation_enum))
        self.assertEqual(
            set(dag.get_task("select_postprocessing").downstream_task_ids),
            {"background_cut", "bgo_staging_complete"},
        )
        self.assertEqual(
            str(dag.get_task("initialization_complete").trigger_rule),
            "none_failed_min_one_success",
        )


if __name__ == "__main__":
    unittest.main()
