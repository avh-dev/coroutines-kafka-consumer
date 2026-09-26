from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "assets/notify/notify-telegram.py"
SPEC = importlib.util.spec_from_file_location("notify_telegram_for_test", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load notify-telegram.py")
NOTIFY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = NOTIFY
SPEC.loader.exec_module(NOTIFY)


class NotifyTelegramTest(unittest.TestCase):
    def test_experiment_start_is_detailed(self) -> None:
        self.assertEqual(
            "\n".join([
                "🚀 CKC experiment started: comparison",
                "Environment: internal-lab · optilab",
                "Kafka: apache-kafka · cluster · 3 broker(s)",
                "Expected workload: 6m 00s",
                "Targets (2):",
                "• baseline (spring, 2 replica(s), 1000 TPS, 3m 00s)",
                "• ckc (ckc, 2 replica(s), 1000 TPS, 3m 00s)",
            ]),
            NOTIFY.message_for("experiment_started", {
                "experiment": "comparison",
                "environment": {"name": "internal-lab", "detail": "optilab"},
                "kafka": {"implementation": "apache-kafka", "topology": "cluster", "brokers": 3},
                "expected_duration_seconds": 360,
                "targets": [
                    {"name": "baseline", "profile": "spring", "replicas": 2, "base_tps": 1000, "duration_seconds": 180},
                    {"name": "ckc", "profile": "ckc", "replicas": 2, "base_tps": 1000, "duration_seconds": 180},
                ],
            }),
        )

    def test_default_events_follow_the_high_signal_lifecycle(self) -> None:
        self.assertEqual(
            {
                "experiment_started",
                "kafka_warmup_started",
                "target_started",
                "measurements_finished",
                "report_ready",
                "bundle_ready",
                "experiment_completed",
                "experiment_cancelled",
                "experiment_failed",
            },
            NOTIFY.DEFAULT_EVENTS,
        )

    def test_target_start_follows_preparation_and_identifies_the_target(self) -> None:
        self.assertEqual(
            "\n".join([
                "▶️ CKC target started: 1/3 — comparison",
                "Target: spring-kafka.jdk",
                "Profile: spring-kafka · 1 replica(s) · 5000 TPS",
                "Expected workload: 13m 00s",
            ]),
            NOTIFY.message_for("target_started", {
                "experiment": "comparison",
                "name": "spring-kafka.jdk",
                "index": 1,
                "total": 3,
                "profile": "spring-kafka",
                "replicas": 1,
                "base_tps": 5000,
                "expected_duration_seconds": 780,
            }),
        )

    def test_cancellation_is_a_single_terminal_message(self) -> None:
        self.assertEqual(
            "\n".join([
                "⏹ CKC experiment cancelled: comparison",
                "Elapsed: 1m 05s",
                "Application: stopped (0 replicas)",
                "Cleanup: clean",
            ]),
            NOTIFY.message_for("experiment_cancelled", {
                "experiment": "comparison",
                "elapsed_seconds": 65,
                "application_state": "stopped (0 replicas)",
                "cleanup_status": "clean",
            }),
        )
        self.assertEqual(
            "✅ Measurements completed · analysis started",
            NOTIFY.message_for("measurements_finished", {}),
        )

    def test_failures_are_compact_and_keep_the_exit_code(self) -> None:
        self.assertEqual(
            "❌ CKC experiment failed: comparison · exit 7\nCleanup: incomplete\nartifact upload failed",
            NOTIFY.message_for("experiment_failed", {
                "experiment": "comparison",
                "exit_code": 7,
                "cleanup_status": "incomplete",
                "error": "artifact upload failed",
            }),
        )

    def test_report_ready_is_enabled_and_contains_report_path(self) -> None:
        self.assertIn("report_ready", NOTIFY.DEFAULT_EVENTS)
        message = NOTIFY.message_for(
            "report_ready",
            {"experiment": "comparison", "reports": ["/results/comparison/report.md"]},
        )
        self.assertEqual(
            "📊 CKC report ready: comparison\n/results/comparison/report.md",
            message,
        )

    def test_bundle_and_terminal_completion_are_separate(self) -> None:
        payload = {
            "experiment": "comparison",
            "artifacts": {
                "report": "/results/report.md",
                "evidence": "/results/evidence.tar.gz",
                "audit": "/results/audit.tar.gz",
            },
        }
        self.assertEqual(
            "\n".join([
                "📦 CKC evidence bundle ready: comparison",
                "Report: /results/report.md",
                "Evidence: /results/evidence.tar.gz",
                "Audit: /results/audit.tar.gz",
            ]),
            NOTIFY.message_for("bundle_ready", payload),
        )
        self.assertEqual(
            "\n".join([
                "🏁 CKC experiment completed: comparison",
                "Elapsed: 5m 00s",
                "Targets: 2/2 succeeded",
                "Cleanup: clean",
            ]),
            NOTIFY.message_for("experiment_completed", {
                "experiment": "comparison",
                "elapsed_seconds": 300,
                "targets_succeeded": 2,
                "targets_total": 2,
                "cleanup_status": "clean",
            }),
        )


if __name__ == "__main__":
    unittest.main()
