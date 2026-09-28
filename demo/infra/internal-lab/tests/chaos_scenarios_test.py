from __future__ import annotations

import importlib.util
import sys
import tempfile
import threading
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
    "eta": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "flavour": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "registry": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "error_rate_percent": 0,
}

STUB_API_BASELINE = {
    "eta": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "flavour": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "registry": {"percentiles": {"p90": 1, "p95": 2, "p99": 3, "p100": 4}},
    "errorRatePercent": 0,
}


def degradation_params() -> dict[str, object]:
    return {
        "error_rate_percent": 5,
        "eta": {"percentiles": {"p90": 100, "p95": 200, "p99": 300, "p100": 400}},
        "flavour": {"percentiles": {"p90": 100, "p95": 200, "p99": 300, "p100": 400}},
        "registry": {"percentiles": {"p90": 10, "p95": 20, "p99": 30, "p100": 40}},
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
        self.assertEqual(STUB_API_BASELINE, scenarios[0]["params"]["baselineSettings"])

    def test_degradation_merges_arbitrary_percentiles_with_baseline(self) -> None:
        scenarios = self.normalize(
            [{
                "at": "10s",
                "duration": "20s",
                "type": "stubs_degradation",
                "params": {"eta": {"percentiles": {"p999": 4, "p100": 60_000}}},
            }]
        )

        self.assertEqual(
            {"p90": 1, "p95": 2, "p99": 3, "p999": 4, "p100": 60_000},
            scenarios[0]["params"]["settings"]["eta"]["percentiles"],
        )

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

    def test_deployment_scale_is_normalized_and_executed(self) -> None:
        scenarios = self.normalize(
            [{"at": "30m", "type": "deployment_scale", "target": "ckc-demo", "params": {"replicas": 3}}]
        )

        self.assertEqual(
            {
                "atSeconds": 1800,
                "type": "deployment_scale",
                "target": "ckc-demo",
                "params": {"namespace": "ckc-perf", "replicas": 3},
            },
            scenarios[0],
        )
        with patch.object(chaos_runner, "run") as run:
            chaos_runner.start_scenario(scenarios[0], "/configure-stubs", dry_run=False)
        run.assert_called_once_with(
            ["kubectl", "-n", "ckc-perf", "scale", "deployment", "ckc-demo", "--replicas=3"]
        )

    def test_deployment_scale_requires_positive_replicas(self) -> None:
        for replicas in (None, 0, True):
            with self.subTest(replicas=replicas):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    self.normalize(
                        [{"at": "1m", "type": "deployment_scale", "params": {"replicas": replicas}}]
                    )

    def test_repeating_sequence_normalizes_existing_actions_and_delays(self) -> None:
        scenarios = self.normalize(
            [{
                "at": "10m",
                "duration": "30m",
                "type": "sequence",
                "name": "replica-churn",
                "steps": [
                    {"type": "deployment_scale", "target": "ckc-demo", "params": {"replicas": 3}},
                    {"type": "delay", "duration": "30s"},
                    {"type": "deployment_scale", "target": "ckc-demo", "params": {"replicas": 2}},
                    {"type": "delay", "duration": "30s"},
                ],
            }]
        )

        self.assertEqual(600, scenarios[0]["atSeconds"])
        self.assertEqual(1800, scenarios[0]["durationSeconds"])
        self.assertEqual(60, scenarios[0]["minimumCycleSeconds"])
        self.assertEqual("replica-churn", scenarios[0]["name"])
        self.assertEqual(
            ["deployment_scale", "delay", "deployment_scale", "delay"],
            [step["type"] for step in scenarios[0]["steps"]],
        )
        self.assertEqual(
            [3, 2],
            [
                step["params"]["replicas"]
                for step in scenarios[0]["steps"]
                if step["type"] == "deployment_scale"
            ],
        )

    def test_sequence_requires_a_complete_cycle_to_fit_its_window(self) -> None:
        with self.assertRaisesRegex(ValueError, "fit at least one complete cycle"):
            self.normalize(
                [{
                    "at": "1m",
                    "duration": "20s",
                    "type": "sequence",
                    "steps": [{"type": "delay", "duration": "30s"}],
                }]
            )

    def test_sequence_uses_completed_cycle_average_before_starting_another(self) -> None:
        self.assertTrue(chaos_runner.sequence_can_start_cycle(60, [], 60))
        self.assertFalse(chaos_runner.sequence_can_start_cycle(59.9, [], 60))
        self.assertEqual(75, chaos_runner.sequence_estimated_cycle_seconds([60, 90], 30))
        self.assertTrue(chaos_runner.sequence_can_start_cycle(75, [60, 90], 30))
        self.assertFalse(chaos_runner.sequence_can_start_cycle(74.9, [60, 90], 30))

    def test_sequence_dry_run_executes_one_complete_cycle(self) -> None:
        scenario = {
            "type": "sequence",
            "name": "replica-churn",
            "durationSeconds": 120,
            "minimumCycleSeconds": 30,
            "steps": [
                {
                    "type": "deployment_scale",
                    "target": "ckc-demo",
                    "params": {"namespace": "ckc-perf", "replicas": 3},
                },
                {"type": "delay", "durationSeconds": 30},
            ],
        }
        with (
            patch.object(chaos_runner, "append_event"),
            patch.object(chaos_runner, "start_scenario") as start,
            patch.object(chaos_runner, "wait_for_deployment_rollout") as rollout,
        ):
            result = chaos_runner.execute_sequence(
                scenario,
                "/configure-stubs",
                threading.Event(),
                dry_run=True,
            )

        self.assertEqual(1, result["completedCycles"])
        self.assertEqual("dry_run", result["stopReason"])
        start.assert_called_once_with(scenario["steps"][0], "/configure-stubs", dry_run=True)
        rollout.assert_called_once_with(
            {"namespace": "ckc-perf", "replicas": 3, "target": "ckc-demo"},
            dry_run=True,
        )

    def test_sequence_does_not_block_independent_scheduled_chaos(self) -> None:
        sequence_action = {"type": "service_restart", "target": "redis", "params": {}}
        independent_action = {
            "atSeconds": 1,
            "type": "pod_delete",
            "target": "ckc-demo",
            "params": {"namespace": "ckc-perf", "selector": "app=ckc-demo"},
        }
        scenarios = [
            {
                "atSeconds": 0,
                "type": "sequence",
                "name": "service-cycle",
                "durationSeconds": 30,
                "minimumCycleSeconds": 1,
                "steps": [sequence_action, {"type": "delay", "durationSeconds": 1}],
            },
            independent_action,
        ]
        with (
            patch.object(chaos_runner, "append_event"),
            patch.object(chaos_runner, "wait_until"),
            patch.object(chaos_runner, "start_scenario") as start,
        ):
            chaos_runner.execute_scenarios(scenarios, 0, "/configure-stubs", dry_run=True)

        self.assertIn(
            ((sequence_action, "/configure-stubs"), {"dry_run": True}),
            [(call.args, call.kwargs) for call in start.call_args_list],
        )
        self.assertIn(
            ((independent_action, "/configure-stubs"), {"dry_run": True}),
            [(call.args, call.kwargs) for call in start.call_args_list],
        )

    def test_reset_cleanup_returns_sequence_to_its_final_scale(self) -> None:
        sequence = {
            "type": "sequence",
            "steps": [
                {"type": "deployment_scale", "target": "ckc-demo", "params": {"replicas": 3}},
                {"type": "delay", "durationSeconds": 30},
                {"type": "deployment_scale", "target": "ckc-demo", "params": {"replicas": 2}},
            ],
        }
        with (
            patch.object(chaos_runner, "scale_deployment") as scale,
            patch.object(chaos_runner, "wait_for_deployment_rollout") as rollout,
        ):
            chaos_runner.cleanup_scenarios([sequence], "/configure-stubs", dry_run=True)

        expected = {"target": "ckc-demo", "replicas": 2}
        scale.assert_called_once_with(expected, dry_run=True)
        rollout.assert_called_once_with(expected, dry_run=True)

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
            "params": {"settings": {}, "baselineSettings": STUB_API_BASELINE},
        }
        with patch.object(chaos_runner, "apply_stubs_profile") as apply_stubs:
            chaos_runner.recover_scenario(stubs, "/configure-stubs", dry_run=False)
        apply_stubs.assert_called_once_with(
            {"settings": STUB_API_BASELINE},
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

        crash = {"type": "service_crash", "target": "kafka", "params": {"brokerId": 2}}
        with patch.object(chaos_runner, "docker_service") as docker_service:
            chaos_runner.recover_scenario(crash, "/configure-stubs", dry_run=False)
        docker_service.assert_called_once_with(
            {"target": "kafka", "brokerId": 2},
            "start",
            dry_run=False,
            check=True,
        )

    def test_cluster_kafka_scenarios_address_individual_brokers(self) -> None:
        scenarios = self.normalize(
            [
                {"at": "10s", "duration": "20s", "type": "service_outage", "target": "kafka", "params": {"broker_id": 1}},
                {"at": "10s", "duration": "20s", "type": "service_crash", "target": "kafka", "params": {"broker_id": 2}},
                {"at": "10s", "type": "service_restart", "target": "kafka", "params": {"broker_id": 3}},
            ]
        )
        self.assertEqual([1, 2, 3], [scenario["params"]["brokerId"] for scenario in scenarios])

        with patch.dict("os.environ", {"LAB_KAFKA_IMPLEMENTATION": "apache-kafka", "LAB_KAFKA_TOPOLOGY": "cluster"}):
            self.assertEqual("ckc-perf-kafka-2", chaos_runner.service_target(scenarios[1]["params"] | {"target": "kafka"})[1]["container"])
            self.assertEqual([9093], chaos_runner.service_target(scenarios[1]["params"] | {"target": "kafka"})[1]["ports"])

    def test_invalid_kafka_broker_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "broker_id"):
            self.normalize(
                [{"at": "10s", "type": "service_restart", "target": "kafka", "params": {"broker_id": 4}}]
            )

    def test_all_current_chaos_definitions_use_new_contract(self) -> None:
        definitions = Path(__file__).resolve().parents[2] / "shared" / "workloads" / "test-definitions"
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
