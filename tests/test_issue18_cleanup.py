import ast
import logging
import unittest
from pathlib import Path
from unittest.mock import Mock


REPO_ROOT = Path(__file__).resolve().parents[1]
BGO_FUNCTIONS = (
    REPO_ROOT / "src" / "pipeline" / "fast_transient_pipeline" / "bgo_functions.py"
)


def load_function(name, namespace=None):
    """Load one dependency-free function from bgo_functions.py."""
    tree = ast.parse(BGO_FUNCTIONS.read_text(), filename=str(BGO_FUNCTIONS))
    function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )
    module = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias("annotations")], level=0),
            function,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    loaded = dict(namespace or {})
    exec(compile(module, str(BGO_FUNCTIONS), "exec"), loaded)
    return loaded[name]


class Issue18CleanupTests(unittest.TestCase):
    def test_background_error_is_english_and_preserves_threshold(self):
        require_background_bins = load_function("_require_background_bins")

        require_background_bins(4, 2)
        with self.assertRaisesRegex(
            RuntimeError,
            r"Too few background bins \(3\) for a polynomial of order 2",
        ):
            require_background_bins(3, 2)

    def test_localization_log_contains_only_scalar_summary(self):
        logger = Mock(spec=logging.Logger)
        log_summary = load_function("_log_localization_summary", {"LOGGER": logger})
        result = {"l": 12.5, "b": -8.25, "ts_map": object()}

        log_summary(result)

        logger.info.assert_called_once_with(
            "BGO localization completed: l=%.3f deg, b=%.3f deg",
            12.5,
            -8.25,
        )
        self.assertNotIn(result["ts_map"], logger.info.call_args.args)

    def test_targeted_unused_imports_and_debug_print_are_absent(self):
        bgo_tree = ast.parse(BGO_FUNCTIONS.read_text())
        imported = {
            alias.name
            for node in ast.walk(bgo_tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertNotIn("pandas", imported)
        self.assertNotIn("print(panels)", BGO_FUNCTIONS.read_text())
        self.assertNotIn("Localization result:", BGO_FUNCTIONS.read_text())

        ged_source = (
            REPO_ROOT
            / "src"
            / "pipeline"
            / "fast_transient_pipeline"
            / "ged_functions.py"
        ).read_text()
        download_source = (REPO_ROOT / "src" / "pipeline" / "download_data.py").read_text()
        self.assertNotRegex(ged_source, r"(?m)^\s*import gc\s*$")
        self.assertNotIn("from datetime import datetime", download_source)


if __name__ == "__main__":
    unittest.main()
