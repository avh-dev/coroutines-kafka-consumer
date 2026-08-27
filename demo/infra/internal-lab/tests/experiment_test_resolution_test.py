from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


HELPER = Path(__file__).resolve().parents[1] / "assets" / "helpers" / "experiment_test.py"
spec = importlib.util.spec_from_file_location("experiment_test_for_test", HELPER)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Could not load {HELPER}")
experiment_test = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = experiment_test
spec.loader.exec_module(experiment_test)


class ExperimentTestResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.definitions = Path(self.temp.name)
        (self.definitions / "baseline.yaml").write_text(
            yaml.safe_dump(
                {
                    "stubs": {"error_rate_percent": 0, "eta": {"delay_p90_ms": 2}},
                    "load_test": {"load_profile": "0 -> (60s, hold) -> 10", "workers": 2},
                    "chaos_steps": [{"at": "10s", "type": "pod_delete"}],
                    "diagnostic_steps": [{"at": "5s", "duration": "2s", "type": "packet_capture"}],
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_deep_merges_objects_and_replaces_lists(self) -> None:
        resolved = experiment_test.resolve_experiment_test(
            {
                "test": {
                    "extends": "baseline",
                    "stubs": {"eta": {"delay_p90_ms": 20}},
                    "load_test": {"workers": 8},
                    "chaos_steps": [{"at": "20s", "type": "service_restart", "target": "redis"}],
                }
            },
            self.definitions,
        )
        self.assertEqual(20, resolved.definition["stubs"]["eta"]["delay_p90_ms"])
        self.assertEqual(0, resolved.definition["stubs"]["error_rate_percent"])
        self.assertEqual(8, resolved.definition["load_test"]["workers"])
        self.assertEqual(1, len(resolved.definition["chaos_steps"]))
        self.assertEqual("service_restart", resolved.definition["chaos_steps"][0]["type"])

    def test_null_removes_inherited_field(self) -> None:
        resolved = experiment_test.resolve_experiment_test(
            {"test_definition": "baseline", "test": {"diagnostic_steps": None}},
            self.definitions,
        )
        self.assertNotIn("diagnostic_steps", resolved.definition)

    def test_supports_complete_inline_test(self) -> None:
        resolved = experiment_test.resolve_experiment_test(
            {
                "test": {
                    "stubs": {"error_rate_percent": 0},
                    "load_test": {"load_profile": "0 -> (5s, hold) -> 1"},
                }
            },
            self.definitions,
        )
        self.assertEqual("inline", resolved.source_name)

    def test_rejects_conflicting_sources(self) -> None:
        with self.assertRaisesRegex(ValueError, "conflicts"):
            experiment_test.resolve_experiment_test(
                {"test_definition": "baseline", "test": {"extends": "other"}},
                self.definitions,
            )


if __name__ == "__main__":
    unittest.main()
