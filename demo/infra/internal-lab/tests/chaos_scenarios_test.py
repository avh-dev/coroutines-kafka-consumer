from __future__ import annotations

import importlib.util
import json
import os
import subprocess
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

    def test_broker_stop_accepts_only_the_temporary_broker_and_an_explicit_mode(self) -> None:
        step = {"at": "1s", "type": "warmup_broker_stop", "params": {"mode": "planned"}}
        normalized, = self.normalize([step])
        self.assertEqual("ckc-warmup-kafka", normalized["target"])
        self.assertEqual({"mode": "planned", "recoveryTimeoutSeconds": 210}, normalized["params"])
        custom, = self.normalize([step | {"params": {"mode": "abrupt", "recovery_timeout_seconds": 360}}])
        self.assertEqual({"mode": "abrupt", "recoveryTimeoutSeconds": 360}, custom["params"])
        for invalid in (True, 0, -1, 1.5, "360", None):
            with self.subTest(recovery_timeout_seconds=invalid):
                with self.assertRaisesRegex(ValueError, "recovery_timeout_seconds must be a positive integer"):
                    self.normalize([step | {"params": {"mode": "abrupt", "recovery_timeout_seconds": invalid}}])
        with self.assertRaisesRegex(ValueError, "must be ckc-warmup-kafka"):
            self.normalize([step | {"target": "kafka"}])
        with self.assertRaisesRegex(ValueError, "must be planned or abrupt"):
            self.normalize([step | {"params": {"mode": "kill"}}])

    def test_warming_guard_uses_each_sessions_latest_phase(self) -> None:
        logs = "\n".join(
            f"Warm-up lifecycle: phase={phase}, participantId=null, sessionId={session}, reasonCode=null"
            for phase, session in [
                ("WARMING", "finished"), ("WARMING", "one"), ("WARMING", "two"),
                ("COMPLETING", "finished"), ("MAIN_READY", "null"),
            ]
        )
        self.assertEqual(["one", "two"], chaos_runner.warming_sessions(logs))

    def test_broker_guard_queries_lifecycle_lines_for_the_run_before_applying_the_limit(self) -> None:
        def phase(name: str, session: str) -> str:
            return f"Warm-up lifecycle: phase={name}, participantId=null, sessionId={session}"

        payload = {"data": {"result": [
            {"values": [["30", phase("COMPLETING", "finished")], ["20", phase("WARMING", "two")]]},
            {"values": [["15", phase("WARMING", "one")], ["10", phase("WARMING", "finished")]]},
        ]}}
        with patch.object(chaos_runner, "http_json", return_value=payload) as query:
            logs = chaos_runner.warmup_app_logs("run-366", "Warm-up lifecycle: phase=")

        self.assertEqual(["one", "two"], chaos_runner.warming_sessions(logs))
        self.assertEqual(
            '{namespace="ckc-perf",app="ckc-demo",container="demo",run_id="run-366"}'
            ' |= "Warm-up lifecycle: phase="',
            query.call_args.args[2]["query"],
        )

    def test_verified_scale_up_requires_two_replicas(self) -> None:
        step = {"at": "30s", "type": "deployment_scale", "params": {"replicas": 2, "verify_warmup": True}}
        self.assertTrue(self.normalize([step])[0]["params"]["verifyWarmup"])
        with self.assertRaisesRegex(ValueError, "requires a top-level scale to 2 replicas"):
            self.normalize([step | {"params": {"replicas": 3, "verify_warmup": True}}])

    def test_verified_scale_up_distinguishes_cold_donor_and_warm_recipient(self) -> None:
        mappings = chaos_runner.warmup_scale_up.MAPPINGS

        def event(phase, participant="null", session="null", reason="null"):
            return f"Warm-up lifecycle: phase={phase}, participantId={participant}, sessionId={session}, reasonCode={reason}"

        ready = [event("MAIN_READY", participant) for participant in mappings]
        cold = event("COLD_FALLBACK", reason="incomplete_donor_coverage")
        donor = "\n".join([cold, *ready])
        successful = "\n".join([event("WARMING", session="one"), event("RECIPIENT_FINISHED", session="one"), *ready])
        forwardings = "\n".join(
            f"Warm-up record forwarded: sessionId=one, source={source}-2@7, destination={destination}-0@5"
            for source, destination, _, _ in mappings.values()
        )
        environment = {
            "SPRING_PROFILES_ACTIVE": "ckc-spring-boot", "TRAFFIC_WARMUP_ENABLED": "true",
            "KAFKA_BOOTSTRAP_SERVERS": "main-kafka:9092",
            "TRAFFIC_WARMUP_CONTROL_KAFKA_BOOTSTRAP_SERVERS": "ckc-warmup-kafka:9092",
            "TRAFFIC_WARMUP_DESTINATION_KAFKA_BOOTSTRAP_SERVERS": "ckc-warmup-kafka:9092",
            "DEMO_KAFKA_SECURITY_PROTOCOL": "PLAINTEXT", "AUDIT_LOG_ENABLED": "true",
        }
        deployment = {"spec": {"replicas": 1, "template": {"spec": {"containers": [{
            "name": "demo", "env": [{"name": name, "value": value} for name, value in environment.items()]
        }]}}}}
        pods = [
            {"metadata": {"name": name, "uid": f"{name}-uid"}, "status": {"phase": "Running"}}
            for name in ("donor", "recipient")
        ]
        response = lambda data: subprocess.CompletedProcess([], 0, data, "")
        for logs, status in ((successful, "passed"), (donor, "failed")):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                replies = [
                    json.dumps(deployment), json.dumps({"items": pods[:1]}), donor,
                    json.dumps({"items": pods}), logs,
                ]
                if status == "passed":
                    replies.append(donor + "\n" + forwardings)
                with (
                    patch.dict(os.environ, {"TEST_RUN_ID": "test", "EXPERIMENT_EVENTS_FILE": str(root / "events.jsonl")}),
                    patch.object(chaos_runner, "run", side_effect=map(response, replies)),
                    patch.object(chaos_runner, "scale_deployment") as scale,
                ):
                    scenario = self.normalize([{
                        "at": "30s", "type": "deployment_scale", "params": {"replicas": 2, "verify_warmup": True}
                    }])[0]
                    detail = chaos_runner.start_scenario(scenario, "/configure-stubs", dry_run=False)
                    scale.assert_called_once()
                proof = json.loads((root / "diagnostics" / "warmup-scale-up.json").read_text())
                self.assertEqual(status, detail["warmupVerification"])
                self.assertEqual(status, proof["status"])
                if status == "passed":
                    self.assertEqual("one", proof["session"])
                    self.assertEqual(set(mappings), set(proof["offsets"]))
                else:
                    self.assertIn("COLD_FALLBACK", proof["error"])

    def test_planned_broker_evidence_matches_new_connection_to_old_address_and_acknowledgement(self) -> None:
        records = [
            "broker-lifecycle offset=17 timestamp=1795461568746 "
            "event=ContextDrained(epoch=current, drainId=drain, context=recipient, sessions=[session])"
        ]
        endpoint = {"observedAt": "2026-11-24T05:00:00.123Z"}
        connection = (
            "[pod/recipient] 2026-11-24T05:00:00.124Z Warm-up broker lifecycle opening final confirmation connection "
            "brokerEpoch=current drainId=drain context=recipient advertisedBroker=10.12.13.14:9092"
        )
        self.assertEqual(connection, chaos_runner.verified_drain_connection(
            records, connection, endpoint, "10.12.13.14:9092", "current"
        ))
        for stale in (
            connection.replace("00.124Z", "00.122Z"),
            connection.replace("13.14", "13.15"),
            connection.replace("context=recipient", "context=replacement"),
        ):
            self.assertIsNone(chaos_runner.verified_drain_connection(
                records, stale, endpoint, "10.12.13.14:9092", "current"
            ))

    def test_abrupt_stop_kills_kafka_and_requires_a_new_epoch_on_the_same_pod(self) -> None:
        metadata = {"name": "warmup-pod", "uid": "warmup-uid"}
        pod = {
            "metadata": metadata,
            "spec": {"containers": [{"name": "kafka", "env": [
                {"name": "KAFKA_BROKER_LIFECYCLE_TOPIC", "value": "ckc.traffic-warmup.lifecycle"}
            ]}]},
            "status": {"podIP": "10.12.13.14"},
        }
        broker_events = [
            "warmup-kafka-supervisor process=broker exit_status=137",
            "warmup-kafka-supervisor event=KafkaStopped brokerEpoch=old-epoch pid=10",
            "warmup-kafka-supervisor process=bootstrap exit_status=1",
            "warmup-kafka-supervisor event=KafkaSIGTERM brokerEpoch=failed-epoch pid=10",
            "warmup-kafka-supervisor event=KafkaStopped brokerEpoch=failed-epoch pid=10",
            "broker-lifecycle event=BrokerStarted brokerEpoch=new-epoch offset=244",
        ]
        with (
            patch.dict(os.environ, {"TEST_RUN_ID": "run-366"}),
            patch.object(chaos_runner, "warmup_app_logs", return_value=""),
            patch.object(chaos_runner, "warming_sessions", return_value=["session-1", "session-2"]),
            patch.object(chaos_runner, "select_running_pod", return_value=metadata),
            patch.object(chaos_runner, "await_warmup_broker", return_value=(metadata, "new-epoch")) as await_broker,
            patch.object(chaos_runner, "warmup_broker_logs", return_value=broker_events) as broker_logs,
            patch.object(chaos_runner, "run", side_effect=[
                subprocess.CompletedProcess([], 0, json.dumps(pod)),
                subprocess.CompletedProcess([], 0, "old-epoch"),
                subprocess.CompletedProcess([], 0),
            ]) as run,
        ):
            evidence = chaos_runner.stop_warmup_broker({"mode": "abrupt", "recoveryTimeoutSeconds": 360}, dry_run=False)
        self.assertEqual(360, await_broker.call_args.args[-1])
        self.assertEqual(("run-366", "warmup-pod"), broker_logs.call_args.args[:2])
        self.assertEqual(broker_events, evidence["brokerEvents"])
        self.assertFalse(chaos_runner.proves_abrupt_broker_stop(broker_events, "failed-epoch"))
        self.assertFalse(chaos_runner.proves_abrupt_broker_stop(broker_events[2:], "old-epoch"))
        self.assertFalse(chaos_runner.proves_abrupt_broker_stop(
            ["broker-lifecycle event=DrainRequested(epoch=old-epoch, drainId=drain)", *broker_events],
            "old-epoch",
        ))
        self.assertEqual(360, evidence["recoveryTimeoutSeconds"])
        self.assertEqual("warmup-uid", evidence["nextBrokerPodUid"])
        self.assertEqual("new-epoch", evidence["nextBrokerEpoch"])
        self.assertIn('kill -KILL "$(cat /var/run/ckc-warmup-kafka/broker.pid)"', run.call_args_list[2].args[0])

    def test_pod_index_is_optional_and_normalized_for_both_actions(self) -> None:
        for action in ("pod_delete", "pod_crash"):
            with self.subTest(action=action):
                indexed, random_choice = self.normalize([
                    {"at": "1s", "type": action, "params": {"pod_index": 2}},
                    {"at": "2s", "type": action},
                ])

                self.assertEqual(2, indexed["params"]["podIndex"])
                self.assertNotIn("podIndex", random_choice["params"])

    def test_pod_index_rejects_nonpositive_and_noninteger_values(self) -> None:
        for action in ("pod_delete", "pod_crash"):
            for value in (0, -1, True, 1.0, "1", None):
                with self.subTest(action=action, value=value):
                    with self.assertRaisesRegex(ValueError, "pod_index must be a positive integer"):
                        self.normalize([
                            {"at": "1s", "type": action, "params": {"pod_index": value}},
                        ])

    def test_index_selects_running_pods_by_creation_time_then_name(self) -> None:
        def pod(name, second, *, phase="Running", ready=True, deleting=False):
            metadata = {
                "name": name,
                "uid": f"uid-{name}",
                "creationTimestamp": f"2026-11-22T15:00:{second:02d}Z",
            }
            if deleting:
                metadata["deletionTimestamp"] = "2026-11-22T15:01:00Z"
            return {
                "metadata": metadata,
                "status": {
                    "phase": phase,
                    "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                },
            }

        items = [
            pod("same-time-z", 3, ready=False),
            pod("deleting", 1, deleting=True),
            pod("pending", 1, phase="Pending"),
            pod("oldest", 2),
            pod("same-time-a", 3, ready=False),
        ]
        result = subprocess.CompletedProcess([], 0, stdout=json.dumps({"items": items}))
        with patch.object(chaos_runner, "run", return_value=result) as run:
            for index, name in enumerate(("oldest", "same-time-a", "same-time-z"), 1):
                with self.subTest(index=index):
                    selected = chaos_runner.select_running_pod("ckc-perf", "app=demo", index)
                    self.assertEqual(name, selected["name"])
                    self.assertEqual(f"uid-{name}", selected["uid"])

            with self.assertRaisesRegex(RuntimeError, "pod_index 4 exceeds 3"):
                chaos_runner.select_running_pod("ckc-perf", "app=demo", 4)
            self.assertEqual("oldest", chaos_runner.select_running_pod("ckc-perf", "app=demo", None)["name"])
            self.assertEqual(5, run.call_count)

    def test_indexed_crash_skips_health_status_and_records_impact(self) -> None:
        scenario = {"atSeconds": 0, "type": "pod_crash", "target": "ckc-demo", "params": {"podIndex": 2}}
        pod = {"name": "recipient", "uid": "recipient-uid"}
        timestamp = "2026-11-22T15:00:00.000Z"
        with (
            patch.object(chaos_runner, "select_running_pod", return_value=pod) as select,
            patch.object(chaos_runner, "free_local_port", return_value=12345),
            patch.object(chaos_runner, "wait_for_http") as wait,
            patch.object(chaos_runner.subprocess, "Popen") as port_forward,
            patch.object(chaos_runner, "run", return_value=subprocess.CompletedProcess([], 52)) as run,
            patch.object(chaos_runner, "utc_iso", return_value=timestamp),
            patch.object(chaos_runner, "append_event") as append,
        ):
            chaos_runner.execute_scenarios([scenario], 0, "/configure-stubs", dry_run=False)

        select.assert_called_once_with("ckc-perf", "ckc.dev/component=application", 2)
        self.assertIn("pod/recipient", port_forward.call_args.args[0])
        wait.assert_called_once_with("http://127.0.0.1:12345/actuator/health", 30, require_success=False)
        self.assertEqual("http://127.0.0.1:12345/internal/crash", run.call_args.args[0][-1])
        event = append.call_args.args[0]
        self.assertEqual("completed", event["status"])
        self.assertEqual(2, event["details"]["podIndex"])
        self.assertEqual("recipient", event["details"]["podName"])
        self.assertEqual("recipient-uid", event["details"]["podUid"])
        self.assertEqual(timestamp, event["details"]["impactAt"])

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
            patch.object(chaos_runner, "start_scenario", return_value=None) as start,
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
            patch.object(chaos_runner, "start_scenario", return_value=None) as start,
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
