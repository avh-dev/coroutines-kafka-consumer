from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"


def load_helper(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HELPERS / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


definition_env = load_helper("definition_env_for_test", "definition-env.py")
chaos_runner = load_helper("chaos_runner_for_test", "run-chaos-steps.py")


BASELINE_STUBS = {
    "eta": {"delayP90Ms": 1, "delayP95Ms": 2, "delayP99Ms": 3, "delayP100Ms": 4},
    "flavour": {"delayP90Ms": 1, "delayP95Ms": 2, "delayP99Ms": 3, "delayP100Ms": 4},
    "registry": {"delayP90Ms": 1, "delayP95Ms": 2, "delayP99Ms": 3, "delayP100Ms": 4},
    "errorRatePercent": 0,
}


def degradation_params() -> dict[str, object]:
    return {
        "error_rate_percent": 5,
        "eta": {"delay_p90_ms": 100, "delay_p95_ms": 200, "delay_p99_ms": 300, "delay_p100_ms": 400},
        "flavour": {"delay_p90_ms": 100, "delay_p95_ms": 200, "delay_p99_ms": 300, "delay_p100_ms": 400},
        "registry": {"delay_p90_ms": 10, "delay_p95_ms": 20, "delay_p99_ms": 30, "delay_p100_ms": 40},
    }


class ChaosScenariosTest(unittest.TestCase):
    def normalize(self, steps: list[dict[str, object]]) -> list[dict[str, object]]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.yaml"
            return definition_env.normalized_chaos_steps(
                {"chaos_steps": steps},
                BASELINE_STUBS,
                path,
            )

    def test_duration_scenario_stays_one_semantic_scenario(self) -> None:
        scenarios = self.normalize(
            [
                {
                    "at": "2m30s",
                    "duration": "3m20s",
                    "type": "stubs_degradation",
                    "target": "demo-stubs",
                    "params": degradation_params(),
                }
            ]
        )
        self.assertEqual(1, len(scenarios))
        self.assertEqual(150, scenarios[0]["atSeconds"])
        self.assertEqual(200, scenarios[0]["durationSeconds"])
        self.assertEqual(BASELINE_STUBS, scenarios[0]["params"]["baselineSettings"])

    def test_overlapping_duration_scenarios_for_target_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.normalize(
                [
                    {
                        "at": "10s",
                        "duration": "30s",
                        "type": "service_outage",
                        "target": "redis",
                    },
                    {
                        "at": "20s",
                        "duration": "30s",
                        "type": "network_degradation",
                        "target": "redis",
                        "params": {"delay_ms": 100},
                    },
                ]
            )

    def test_legacy_command_types_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            self.normalize([{"at": "10s", "type": "set_stubs_profile", "params": degradation_params()}])

    def test_end_action_precedes_new_start_at_same_time(self) -> None:
        scenarios = [
            {"atSeconds": 10, "durationSeconds": 20, "type": "service_outage", "target": "redis", "params": {}},
            {"atSeconds": 30, "durationSeconds": 10, "type": "network_degradation", "target": "redis", "params": {}},
        ]
        events = chaos_runner.scheduled_events(scenarios)
        self.assertEqual(
            [(10, "start"), (30, "end"), (30, "start"), (40, "end")],
            [(event[0], event[3]) for event in events],
        )

    def test_active_duration_scenario_is_cleaned_up_after_failure(self) -> None:
        duration = {
            "atSeconds": 0,
            "durationSeconds": 30,
            "type": "service_outage",
            "target": "redis",
            "params": {},
        }
        instant = {"atSeconds": 5, "type": "service_restart", "target": "kafka", "params": {}}
        with (
            patch.object(chaos_runner, "wait_until"),
            patch.object(chaos_runner, "start_scenario", side_effect=[None, RuntimeError("boom")]),
            patch.object(chaos_runner, "cleanup_scenarios") as cleanup,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                chaos_runner.execute_scenarios([duration, instant], 0, "/configure-stubs", dry_run=False)
        cleanup.assert_called_once_with([duration], "/configure-stubs", dry_run=False)

    def test_each_duration_type_has_an_automatic_recovery(self) -> None:
        stubs = {
            "type": "stubs_degradation",
            "target": "demo-stubs",
            "params": {"settings": {}, "baselineSettings": BASELINE_STUBS},
        }
        with patch.object(chaos_runner, "apply_stubs_profile") as apply_stubs:
            chaos_runner.recover_scenario(stubs, "/configure-stubs", dry_run=False)
        apply_stubs.assert_called_once_with(
            {"settings": BASELINE_STUBS},
            "/configure-stubs",
            dry_run=False,
            check=True,
        )

        network = {"type": "network_degradation", "target": "kafka", "params": {}}
        with patch.object(chaos_runner, "reset_service_netem") as reset_netem:
            chaos_runner.recover_scenario(network, "/configure-stubs", dry_run=False)
        reset_netem.assert_called_once_with({"target": "kafka"}, dry_run=False)

        outage = {"type": "service_outage", "target": "redis", "params": {}}
        with patch.object(chaos_runner, "docker_service") as docker_service:
            chaos_runner.recover_scenario(outage, "/configure-stubs", dry_run=False)
        docker_service.assert_called_once_with(
            {"target": "redis"},
            "unpause",
            dry_run=False,
            check=True,
        )

    def test_all_current_chaos_definitions_use_new_contract(self) -> None:
        definitions = Path(__file__).resolve().parents[1] / "workloads" / "test-definitions"
        for path in sorted(definitions.glob("*.yaml")):
            definition = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if not definition.get("chaos_steps"):
                continue
            baseline = definition_env.stub_settings_from_definition(definition["stubs"], path)
            scenarios = definition_env.normalized_chaos_steps(definition, baseline, path)
            self.assertTrue(scenarios, path.name)
            self.assertFalse(
                {scenario["type"] for scenario in scenarios}
                - (chaos_runner.INSTANT_SCENARIO_TYPES | chaos_runner.DURATION_SCENARIO_TYPES),
                path.name,
            )


if __name__ == "__main__":
    unittest.main()
