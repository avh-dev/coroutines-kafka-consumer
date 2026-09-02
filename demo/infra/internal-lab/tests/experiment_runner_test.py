from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


HELPERS = Path(__file__).resolve().parents[1] / "assets/helpers"
sys.path.insert(0, str(HELPERS))
SPEC = importlib.util.spec_from_file_location("run_experiment_for_test", HELPERS / "run-experiment.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load run-experiment.py")
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RUNNER
SPEC.loader.exec_module(RUNNER)


class ExperimentRunnerTest(unittest.TestCase):
    def test_shared_application_contract_maps_to_run_test_planner_flags(self) -> None:
        command = RUNNER.command_for_run(
            Path("/opt/ckc-lab/bin/run-test.sh"),
            {
                "profile": "ckc",
                "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
                "application": {
                    "replicas": 2,
                    "resources": {"requests": {"cpu": "500m", "memory": "768Mi"}},
                    "hpa": {"enabled": True, "min_replicas": 2, "max_replicas": 6},
                },
            },
            "smoke.yaml",
            {},
        )

        self.assertIn("--replicas", command)
        self.assertIn("--demo-cpu-request", command)
        self.assertIn("--hpa-enabled", command)
        self.assertIn("--hpa-min-replicas", command)
        self.assertIn("--hpa-max-replicas", command)

    def test_each_target_runs_with_its_own_resolved_test_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            definitions = root / "workloads/test-definitions"
            definitions.mkdir(parents=True)
            (definitions / "baseline.yaml").write_text(yaml.safe_dump({
                "stubs": {"error_rate_percent": 0, "eta": {"delay_p90_ms": 10}},
                "load_test": {"load_profile": "0 -> (1s, hold) -> 100", "base_tps": 1000, "workers": 4},
            }), encoding="utf-8")
            experiment = root / "comparison.yaml"
            experiment.write_text(yaml.safe_dump({
                "name": "comparison",
                "test_definition": "baseline",
                "defaults": {"replicas": 1},
                "targets": [
                    {
                        "name": "baseline",
                        "profile": "spring-kafka",
                        "planning_latency": {"order_ms": 1, "batch_ms": 1, "telemetry_ms": 1},
                    },
                    {
                        "name": "ckc",
                        "profile": "ckc",
                        "planning_latency": {"order_ms": 1, "batch_ms": 1, "telemetry_ms": 1},
                        "test": {"load_test": {"base_tps": 2000, "workers": 20}},
                    },
                ],
            }), encoding="utf-8")
            calls: list[dict] = []

            def run_one(*args, **kwargs):
                target = args[4]
                calls.append(target)
                return {
                    "target": target["id"],
                    "resolved_test_path": target["resolved_test_path"],
                    "exit_code": 0,
                    "interrupted": False,
                    "env": {"AUDIT_LOG_ENABLED": "false"},
                    "audit_dir": "",
                }

            with (
                patch.object(RUNNER, "run_one", side_effect=run_one),
                patch.object(RUNNER, "load_sla_profile", return_value=None),
                patch.object(RUNNER, "notify"),
            ):
                summary = RUNNER.run_experiment(
                    experiment,
                    root / "run-test.sh",
                    root,
                    root / "results",
                    "set-a",
                    {},
                    None,
                )

            self.assertEqual([1000, 2000], [call["base_tps"] for call in calls])
            self.assertNotEqual(calls[0]["resolved_test_path"], calls[1]["resolved_test_path"])
            self.assertEqual(4, yaml.safe_load(Path(calls[0]["resolved_test_path"]).read_text())["load_test"]["workers"])
            self.assertEqual(20, yaml.safe_load(Path(calls[1]["resolved_test_path"]).read_text())["load_test"]["workers"])
            self.assertEqual(2, len(summary["target_resolved_tests"]))


if __name__ == "__main__":
    unittest.main()
