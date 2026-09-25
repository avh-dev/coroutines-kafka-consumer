from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

from .run import (
    KafkaExecutor,
    WARMUP_DURATION_SECONDS,
    WARMUP_PARTITIONS,
    WARMUP_RATE,
    WARMUP_RECORD_SIZE,
    notification_payload,
    warmup_record_count,
)


class KafkaWarmupTest(unittest.TestCase):
    def arguments(self, backend: str = "docker") -> argparse.Namespace:
        return argparse.Namespace(
            backend=backend,
            bootstrap_server="localhost:9092",
            replication_factor=3,
            reason="brokers replaced",
            experiment="comparison",
            log_file=Path(tempfile.gettempdir()) / "warmup.log",
            notify_hook=None,
            notification_dir=None,
            grafana_url="http://127.0.0.1:3000",
            docker_container="ckc-perf-kafka-1",
            namespace="ckc-loadtest",
            kubeconfig="/tmp/kubeconfig",
        )

    def test_fixed_workload_is_three_minutes_at_ten_thousand_records_per_second(self) -> None:
        self.assertEqual(180, WARMUP_DURATION_SECONDS)
        self.assertEqual(10_000, WARMUP_RATE)
        self.assertEqual(1_024, WARMUP_RECORD_SIZE)
        self.assertEqual(12, WARMUP_PARTITIONS)
        self.assertEqual(1_800_000, warmup_record_count())

    def test_notification_describes_the_fixed_workload_and_reason(self) -> None:
        payload = notification_payload(self.arguments())
        self.assertEqual("comparison", payload["experiment"])
        self.assertEqual("brokers replaced", payload["reason"])
        self.assertEqual(180, payload["duration_seconds"])
        self.assertEqual(10_000, payload["rate"])

    def test_docker_backend_uses_kafka_tools_inside_selected_broker(self) -> None:
        command = KafkaExecutor(self.arguments()).command("kafka-topics.sh", "--list")
        self.assertEqual(["docker", "exec", "ckc-perf-kafka-1", "env", "KAFKA_OPTS="], command[:5])
        self.assertIn("/opt/kafka/bin/kafka-topics.sh", command)

    def test_kubernetes_backend_uses_ephemeral_tool_pod(self) -> None:
        executor = KafkaExecutor(self.arguments("kubernetes"))
        command = executor.command("kafka-producer-perf-test.sh", "--help")
        self.assertEqual(["kubectl", "-n", "ckc-loadtest", "exec"], command[:4])
        self.assertIn("/opt/kafka/bin/kafka-producer-perf-test.sh", command)


if __name__ == "__main__":
    unittest.main()
