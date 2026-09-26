from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from .run import (
    KafkaExecutor,
    WARMUP_DURATION_SECONDS,
    WARMUP_CLIENT_HEAP,
    WARMUP_CLIENT_MEMORY,
    WARMUP_PARTITIONS,
    WARMUP_RATE,
    WARMUP_RECORD_SIZE,
    cleanup_stale_resources,
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

    def test_docker_backend_isolates_kafka_tools_from_selected_broker(self) -> None:
        command = KafkaExecutor(self.arguments()).command("kafka-producer-perf-test.sh", "--help")
        self.assertEqual(["docker", "run", "--rm", "--name"], command[:4])
        self.assertIn("container:ckc-perf-kafka-1", command)
        self.assertIn(WARMUP_CLIENT_MEMORY, command)
        self.assertIn(f"KAFKA_HEAP_OPTS={WARMUP_CLIENT_HEAP}", command)
        self.assertNotIn("exec", command)
        self.assertIn("/opt/kafka/bin/kafka-producer-perf-test.sh", command)

    def test_kubernetes_backend_uses_ephemeral_tool_pod(self) -> None:
        executor = KafkaExecutor(self.arguments("kubernetes"))
        command = executor.command("kafka-producer-perf-test.sh", "--help")
        self.assertEqual(["kubectl", "-n", "ckc-loadtest", "exec"], command[:4])
        self.assertIn(f"KAFKA_HEAP_OPTS={WARMUP_CLIENT_HEAP}", command)
        self.assertIn("/opt/kafka/bin/kafka-producer-perf-test.sh", command)

    def test_stale_warmup_topics_and_groups_are_removed_before_a_run(self) -> None:
        executor = Mock()
        executor.run.side_effect = [
            SimpleNamespace(stdout="order.events.v1\nckc.warmup.v1.old\n"),
            SimpleNamespace(stdout=""),
            SimpleNamespace(stdout="ckc-demo\nckc-warmup-v1-old\n"),
            SimpleNamespace(stdout=""),
        ]

        cleanup_stale_resources(executor, "localhost:9092")

        commands = [call.args for call in executor.run.call_args_list]
        self.assertIn("ckc.warmup.v1.old", commands[1])
        self.assertIn("ckc-warmup-v1-old", commands[3])


if __name__ == "__main__":
    unittest.main()
