from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .contract import validate_canonical_experiment, write_resolved_experiment
from .definition import resolve_experiment_definition


def canonical_experiment() -> dict:
    return {
        "schema_version": 1,
        "name": "portable-smoke",
        "description": "Run the same smoke workload in either environment.",
        "workload": {
            "stubs": {"error_rate_percent": 0},
            "load": {
                "base_tps": 100,
                "load_profile": "0 -> (10s, smoke) -> 100 -> (10s, cool-down) -> 0",
                "order_event_percent": 35,
                "batch_event_percent": 25,
                "cauldron_telemetry_percent": 40,
            },
        },
        "acceptance": {
            "criteria": [{
                "id": "no-missing",
                "source": "audit",
                "path": ["totals", "missing_terminal"],
                "operator": "eq",
                "threshold": 0,
            }],
        },
        "implementations": {
            "topics": {"order": {"kafka_topic": "order.events.v1"}},
            "profiles": {"ckc": {"spring_profile": "ckc"}},
        },
        "defaults": {
            "application": {"replicas": 2},
            "runtime": {
                "env": {"AUDIT_LOG_ENABLED": True},
                "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
            },
        },
        "targets": [{
            "name": "ckc",
            "implementation": "ckc",
            "runtime": {
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

        self.assertEqual(1, resolved.schema_version)
        self.assertEqual("aws", resolved.environment)
        self.assertEqual("smoke", resolved.lab_profile)
        self.assertEqual(experiment["acceptance"], resolved.acceptance)
        self.assertEqual(100, resolved.test.definition["load_test"]["base_tps"])
        self.assertEqual("ckc", resolved.targets[0].profile)
        self.assertEqual(2, resolved.targets[0].definition["application"]["replicas"])
        self.assertEqual(20, resolved.targets[0].definition["telemetry_workers"])
        self.assertEqual(4096, resolved.targets[0].definition["telemetry_queue_capacity"])

    def test_materialized_snapshot_merges_defaults_and_target_workload(self) -> None:
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

    def test_rejects_target_without_inline_implementation_profile(self) -> None:
        experiment = canonical_experiment()
        experiment["targets"][0]["implementation"] = "missing"
        with self.assertRaisesRegex(ValueError, "implementation profiles are missing: missing"):
            validate_canonical_experiment(experiment, self.source, environment="internal-lab")

if __name__ == "__main__":
    unittest.main()
