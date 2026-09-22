from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from .contract import validate_canonical_experiment, write_resolved_experiment
from .definition import resolve_experiment_definition


def canonical_experiment() -> dict:
    return {
        "schema_version": 2,
        "name": "portable-smoke",
        "description": "Run the same smoke workload in either environment.",
        "workload": {
            "stubs": {
                "error_rate_percent": 0,
                "eta": {"percentiles": {"p50": 10, "p999": 80, "p100": 120}},
                "flavour": {"percentiles": {"p50": 10, "p999": 80, "p100": 120}},
                "registry": {"percentiles": {"p50": 1, "p999": 4, "p100": 5}},
            },
            "load": {
                "base_tps": 100,
                "load_profile": "0 -> (10s, smoke) -> 100 -> (10s, cool-down) -> 0",
            },
            "topics": {
                "order": {
                    "kafka_topic": "order.events.v1",
                    "traffic_percent": 35,
                    "max_e2e_latency_ms": 2000,
                    "contract": {"delivery": "at_least_once", "ordering": "per_key"},
                },
                "batch": {
                    "kafka_topic": "batch.events.v1",
                    "traffic_percent": 25,
                    "max_e2e_latency_ms": 2000,
                    "contract": {"delivery": "at_least_once", "ordering": "per_key"},
                },
                "telemetry": {
                    "kafka_topic": "cauldron.events.v1",
                    "traffic_percent": 40,
                    "max_e2e_latency_ms": 1000,
                    "contract": {
                        "semantics": "freshness_first",
                        "delivery": "not_guaranteed",
                        "ordering": "not_guaranteed",
                    },
                },
            },
        },
        "targets": [{
            "name": "ckc",
            "implementation": "ckc",
            "application": {"replicas": 2},
            "runtime": {
                "env": {"AUDIT_LOG_ENABLED": True, "PROCESSING_DISPATCHER_TYPE": "FIXED"},
                "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
                "topics": {
                    "telemetry": {
                        "workers": 20,
                        "queue_capacity": 4096,
                        "processing_mode": "FRESHNESS_FIRST_DROP_OLDEST",
                    },
                },
            },
        }],
        "environments": {
            "internal-lab": {"lab": {"profile": "installed"}},
            "aws": {"region": "eu-central-1", "lab": {"profile": "smoke"}},
        },
    }


class CanonicalExperimentContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "experiment.yaml"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write(self, value: dict) -> Path:
        self.source.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
        return self.source

    def test_resolves_self_contained_experiment_for_selected_environment(self) -> None:
        experiment = canonical_experiment()
        resolved = resolve_experiment_definition(
            self.write(experiment),
            environment="aws",
        )

        self.assertEqual(2, resolved.schema_version)
        self.assertEqual("aws", resolved.environment)
        self.assertEqual("smoke", resolved.lab_profile)
        self.assertIsNone(resolved.acceptance)
        self.assertEqual(100, resolved.test.definition["load_test"]["base_tps"])
        self.assertEqual("ckc", resolved.targets[0].profile)
        self.assertEqual(2, resolved.targets[0].definition["application"]["replicas"])
        self.assertEqual(20, resolved.targets[0].definition["telemetry_workers"])
        self.assertEqual(4096, resolved.targets[0].definition["telemetry_queue_capacity"])
        self.assertEqual("per_key", resolved.snapshot["workload"]["topics"]["order"]["contract"]["ordering"])
        self.assertEqual(
            "freshness_first",
            resolved.snapshot["workload"]["topics"]["telemetry"]["contract"]["semantics"],
        )

    def test_materialized_snapshot_keeps_explicit_target_workload(self) -> None:
        experiment = canonical_experiment()
        experiment["environments"] = {"internal-lab": {"lab": {"profile": "installed"}}}
        experiment["targets"][0]["workload"] = {"load": {"base_tps": 250}}
        resolved = resolve_experiment_definition(self.write(experiment))
        output = self.root / "resolved-experiment.yaml"
        write_resolved_experiment(output, resolved.snapshot or {})
        snapshot = yaml.safe_load(output.read_text(encoding="utf-8"))

        self.assertEqual(250, snapshot["targets"][0]["workload"]["load"]["base_tps"])
        self.assertEqual(100, snapshot["workload"]["load"]["base_tps"])
        self.assertEqual(2, snapshot["targets"][0]["application"]["replicas"])
        self.assertNotIn("extends", output.read_text(encoding="utf-8"))

    def test_requires_explicit_selection_for_multiple_environments(self) -> None:
        with self.assertRaisesRegex(ValueError, "select one explicitly"):
            validate_canonical_experiment(canonical_experiment(), self.source)

    def test_rejects_unsupported_environment_capability(self) -> None:
        experiment = canonical_experiment()
        experiment["workload"]["diagnostics"] = [{
            "at": "1s",
            "type": "tcpdump",
            "name": "capture",
            "targets": ["application"],
            "duration": "1s",
        }]
        with self.assertRaisesRegex(ValueError, "diagnostics.tcpdump"):
            validate_canonical_experiment(
                experiment,
                self.source,
                environment="aws",
                capabilities={"aws": set()},
            )

    def test_checks_capabilities_added_by_target_workload_override(self) -> None:
        experiment = canonical_experiment()
        experiment["targets"][0]["workload"] = {"chaos": [{
            "at": "1s",
            "type": "pod_delete",
        }]}
        with self.assertRaisesRegex(ValueError, "chaos.pod_delete"):
            validate_canonical_experiment(experiment, self.source, environment="aws")

    def test_rejects_legacy_indirection_in_canonical_document(self) -> None:
        experiment = canonical_experiment()
        experiment["test_definition"] = "smoke"
        with self.assertRaisesRegex(ValueError, "test_definition"):
            validate_canonical_experiment(experiment, self.source, environment="aws")

    def test_merges_target_stub_percentiles_and_preserves_arbitrary_keys(self) -> None:
        experiment = canonical_experiment()
        experiment["targets"][0]["workload"] = {
            "stubs": {"eta": {"percentiles": {"p999": 1000, "p100": 60_000}}}
        }

        snapshot = validate_canonical_experiment(experiment, self.source, environment="internal-lab")

        self.assertEqual(
            {"p50": 10, "p999": 1000, "p100": 60_000},
            snapshot["targets"][0]["workload"]["stubs"]["eta"]["percentiles"],
        )

    def test_normalizes_multiple_named_measurement_windows(self) -> None:
        experiment = canonical_experiment()
        experiment["workload"]["measurement_windows"] = [
            {"name": "baseline", "start": "2m", "duration": "2m"},
            {"name": "degraded", "start": "5m", "duration": "5m"},
        ]

        resolved = resolve_experiment_definition(
            self.write(experiment),
            environment="internal-lab",
        )

        self.assertNotIn("measurement_window", resolved.test.definition["load_test"])
        self.assertEqual(
            [
                {"name": "baseline", "start_seconds": 120, "duration_seconds": 120},
                {"name": "degraded", "start_seconds": 300, "duration_seconds": 300},
            ],
            resolved.test.definition["load_test"]["measurement_windows"],
        )

    def test_rejects_ambiguous_or_unnamed_measurement_windows(self) -> None:
        experiment = canonical_experiment()
        experiment["workload"]["measurement_window"] = {
            "name": "baseline", "start": "2m", "duration": "2m",
        }
        experiment["workload"]["measurement_windows"] = [
            {"name": "degraded", "start": "5m", "duration": "5m"},
        ]
        with self.assertRaisesRegex(ValueError, "either measurement_window or measurement_windows"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

        del experiment["workload"]["measurement_window"]
        experiment["workload"]["measurement_windows"][0]["name"] = ""
        with self.assertRaisesRegex(ValueError, "name must not be empty"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

    def test_rejects_stub_distribution_without_terminal_percentile(self) -> None:
        experiment = canonical_experiment()
        experiment["workload"]["stubs"]["eta"] = {"percentiles": {"p999": 80}}

        with self.assertRaisesRegex(ValueError, "must end with p100"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

    def test_rejects_unknown_target_implementation(self) -> None:
        experiment = canonical_experiment()
        experiment["targets"][0]["implementation"] = "missing"
        with self.assertRaisesRegex(ValueError, "Unknown target implementations: missing"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

    def test_internal_lab_kafka_topology_is_normalized_and_validated(self) -> None:
        experiment = canonical_experiment()
        experiment["environments"]["internal-lab"]["lab"]["kafka_topology"] = "cluster"
        snapshot = validate_canonical_experiment(experiment, self.source, environment="internal-lab")
        self.assertEqual("cluster", snapshot["environment"]["configuration"]["lab"]["kafka_topology"])

        experiment["environments"]["internal-lab"]["lab"]["kafka_topology"] = "five-node"
        with self.assertRaisesRegex(ValueError, "kafka_topology"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

    def test_internal_lab_kafka_cluster_configuration_is_normalized_and_validated(self) -> None:
        experiment = canonical_experiment()
        experiment["environments"]["internal-lab"]["lab"]["kafka"] = {
            "topology": "cluster",
            "brokers": 3,
            "replication_factor": 3,
            "min_insync_replicas": 2,
            "resources": {"cpu_per_broker": 0.75, "memory_per_broker": "1536Mi", "heap_per_broker": "1Gi"},
        }
        snapshot = validate_canonical_experiment(experiment, self.source, environment="internal-lab")
        kafka = snapshot["environment"]["configuration"]["lab"]["kafka"]
        self.assertEqual("apache-kafka", kafka["implementation"])
        self.assertEqual(3, kafka["brokers"])
        self.assertEqual(0.75, kafka["resources"]["cpu_per_broker"])

        invalid = copy.deepcopy(experiment)
        invalid["environments"]["internal-lab"]["lab"]["kafka"]["brokers"] = 2
        with self.assertRaisesRegex(ValueError, "brokers must be 3"):
            validate_canonical_experiment(invalid, self.source, environment="internal-lab")

        invalid = copy.deepcopy(experiment)
        invalid["environments"]["internal-lab"]["lab"]["kafka"]["min_insync_replicas"] = 4
        with self.assertRaisesRegex(ValueError, "must not exceed replication_factor"):
            validate_canonical_experiment(invalid, self.source, environment="internal-lab")

        invalid = copy.deepcopy(experiment)
        invalid["environments"]["internal-lab"]["lab"]["kafka"]["resources"]["heap_per_broker"] = "2Gi"
        with self.assertRaisesRegex(ValueError, "must not exceed memory_per_broker"):
            validate_canonical_experiment(invalid, self.source, environment="internal-lab")

if __name__ == "__main__":
    unittest.main()
