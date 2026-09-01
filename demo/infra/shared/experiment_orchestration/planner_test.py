from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from .planner import plan_target


REPO_ROOT = Path(__file__).resolve().parents[4]


class PlannerTest(unittest.TestCase):
    def test_plans_a_shared_target_without_environment_specific_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan, values = plan_target(
                definition_path=REPO_ROOT / "demo/infra/shared/workloads/test-definitions/smoke.yaml",
                consumer_profiles_path=REPO_ROOT / "demo/infra/shared/workloads/consumer-profiles.yaml",
                profile_name="ckc",
                output_dir=Path(directory),
                repo_dir=REPO_ROOT,
                defaults={"replicas": 2},
                target={
                    "planning_latency": {"order_ms": 50, "batch_ms": 50, "telemetry_ms": 150},
                    "env": {"PROCESSING_DISPATCHER_TYPE": "FIXED", "WORKER_DISPATCHER_THREADS": 1},
                    "application": {
                        "replicas": 2,
                        "resources": {"requests": {"cpu": "500m", "memory": "768Mi"}},
                        "hpa": {
                            "enabled": True,
                            "min_replicas": 2,
                            "max_replicas": 6,
                            "target_cpu_utilization_percentage": 70,
                            "scale_down_stabilization_window_seconds": 600,
                        },
                    },
                },
            )

        self.assertEqual("ckc", plan["profile"])
        self.assertEqual(2, plan["replica_count"])
        self.assertEqual(1, plan["worker_dispatcher_threads"])
        self.assertEqual("ckc", values["env"]["springProfilesActive"])
        self.assertEqual(1, values["env"]["workerDispatcherThreads"])
        self.assertEqual("500m", values["resources"]["requests"]["cpu"])
        self.assertEqual({
            "enabled": True,
            "minReplicas": 2,
            "maxReplicas": 6,
            "targetCPUUtilizationPercentage": 70,
            "scaleDownStabilizationWindowSeconds": 600,
        }, values["hpa"])
        self.assertEqual(
            ["order.events.v1", "batch.events.v1", "cauldron.events.v1"],
            [topic["name"] for topic in values["lab"]["kafkaTopics"]],
        )


if __name__ == "__main__":
    unittest.main()
