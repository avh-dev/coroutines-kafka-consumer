from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition


class ExperimentDefinitionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.tests = self.root / "tests"
        self.tests.mkdir()
        (self.tests / "baseline.yaml").write_text(yaml.safe_dump({
            "stubs": {"error_rate_percent": 0, "eta": {"delay_p90_ms": 10}},
            "load_test": {"load_profile": "0 -> (1m, hold) -> 100", "base_tps": 1000, "workers": 4},
        }), encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_experiment(self, value: dict) -> Path:
        path = self.root / "experiment.yaml"
        path.write_text(yaml.safe_dump(value), encoding="utf-8")
        return path

    def test_resolves_one_fixed_lab_and_target_specific_tests(self) -> None:
        path = self.write_experiment({
            "name": "comparison",
            "lab": {"profile": "fixed-lab"},
            "test": {"extends": "baseline"},
            "base_tps": 2000,
            "targets": [
                {"name": "baseline", "profile": "spring-kafka"},
                {"name": "ckc", "profile": "ckc", "test": {
                    "load_test": {"workers": 20},
                    "stubs": {"eta": {"delay_p90_ms": 50}},
                }},
            ],
        })
        resolved = resolve_experiment_definition(path, self.tests)
        self.assertEqual("fixed-lab", resolved.lab_profile)
        self.assertEqual(["spring-kafka", "ckc"], [target.profile for target in resolved.targets])
        self.assertEqual(2000, resolved.targets[0].test.definition["load_test"]["base_tps"])
        self.assertEqual(20, resolved.targets[1].test.definition["load_test"]["workers"])
        self.assertEqual(50, resolved.targets[1].test.definition["stubs"]["eta"]["delay_p90_ms"])
        self.assertEqual(10, resolved.targets[0].test.definition["stubs"]["eta"]["delay_p90_ms"])

    def test_cli_lab_profile_override_is_experiment_wide(self) -> None:
        path = self.write_experiment({
            "test_definition": "baseline",
            "targets": [{"name": "ckc", "profile": "ckc"}],
        })
        resolved = resolve_experiment_definition(path, self.tests, lab_profile="aws-large")
        self.assertEqual("aws-large", resolved.lab_profile)

    def test_rejects_target_lab_override(self) -> None:
        path = self.write_experiment({
            "lab": {"profile": "fixed"},
            "test_definition": "baseline",
            "targets": [{"name": "ckc", "profile": "ckc", "lab": {"profile": "other"}}],
        })
        with self.assertRaisesRegex(ValueError, "cannot override"):
            resolve_experiment_definition(path, self.tests)


if __name__ == "__main__":
    unittest.main()
