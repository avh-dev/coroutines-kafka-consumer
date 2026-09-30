from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "assets/notify/notify-telegram.py"
SPEC = importlib.util.spec_from_file_location("notify_telegram_for_test", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load notify-telegram.py")
NOTIFY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = NOTIFY
SPEC.loader.exec_module(NOTIFY)


class NotifyTelegramTest(unittest.TestCase):
    def test_outbox_delivers_persisted_messages_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "outbox"
            first = Path(directory) / "first.json"
            second = Path(directory) / "second.json"
            first.write_text('{"experiment":"first"}\n', encoding="utf-8")
            second.write_text('{"experiment":"second"}\n', encoding="utf-8")
            with patch.object(NOTIFY.time, "time_ns", side_effect=[1, 2]):
                NOTIFY.enqueue("experiment_failed", first, root)
                NOTIFY.enqueue("experiment_failed", second, root)

            messages: list[str] = []
            result = NOTIFY.drain_outbox(root, sender=messages.append, now=100)

            self.assertEqual(["first", "second"], [message.split(": ", 1)[1].splitlines()[0] for message in messages])
            self.assertEqual({"delivered": 2, "deferred": 0, "rejected": 0}, result)
            self.assertEqual([], list((root / "pending").glob("*.json")))

    def test_outbox_retries_network_failure_without_overtaking(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "outbox"
            first = Path(directory) / "first.json"
            second = Path(directory) / "second.json"
            first.write_text('{"experiment":"first"}\n', encoding="utf-8")
            second.write_text('{"experiment":"second"}\n', encoding="utf-8")
            with patch.object(NOTIFY.time, "time_ns", side_effect=[1, 2]):
                NOTIFY.enqueue("experiment_failed", first, root)
                NOTIFY.enqueue("experiment_failed", second, root)

            failure = NOTIFY.drain_outbox(
                root,
                sender=lambda _message: (_ for _ in ()).throw(urllib.error.URLError("offline")),
                now=100,
            )
            queued = sorted((root / "pending").glob("*.json"))
            first_envelope = json.loads(queued[0].read_text(encoding="utf-8"))

            self.assertEqual({"delivered": 0, "deferred": 1, "rejected": 0}, failure)
            self.assertEqual(1, first_envelope["attempts"])
            self.assertEqual(130, first_envelope["next_attempt_at"])
            self.assertEqual(2, len(queued))
            messages: list[str] = []
            self.assertEqual(1, NOTIFY.drain_outbox(root, sender=messages.append, now=129)["deferred"])
            self.assertEqual([], messages)
            recovered = NOTIFY.drain_outbox(root, sender=messages.append, now=130)
            self.assertEqual({"delivered": 2, "deferred": 0, "rejected": 0}, recovered)

    def test_outbox_quarantines_permanent_telegram_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "outbox"
            payload = Path(directory) / "payload.json"
            payload.write_text('{"experiment":"broken"}\n', encoding="utf-8")
            NOTIFY.enqueue("experiment_failed", payload, root)
            rejection = urllib.error.HTTPError("https://api.telegram.org", 400, "Bad Request", {}, None)

            result = NOTIFY.drain_outbox(
                root,
                sender=lambda _message: (_ for _ in ()).throw(rejection),
                now=100,
            )

            self.assertEqual({"delivered": 0, "deferred": 0, "rejected": 1}, result)
            self.assertEqual(1, len(list((root / "failed").glob("*.http-400.json"))))

    def test_experiment_start_is_detailed(self) -> None:
        self.assertEqual(
            "\n".join([
                "🚀 CKC experiment started: comparison",
                "Environment: internal-lab · optilab",
                "Kafka: apache-kafka · cluster · 3 brokers",
                "Workload: 1000 TPS · 3m 00s per target · 6m 00s total",
                "Targets (2):",
                "• baseline (spring, 2 replicas)",
                "• ckc (ckc, 2 replicas)",
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

    def test_experiment_start_keeps_only_actual_workload_variants_on_targets(self) -> None:
        message = NOTIFY.message_for("experiment_started", {
            "experiment": "comparison",
            "environment": "aws · eu-central-1",
            "kafka": {"implementation": "apache-kafka", "topology": "cluster", "brokers": 3},
            "expected_duration_seconds": 360,
            "targets": [
                {"name": "baseline", "profile": "spring", "base_tps": 1000, "duration_seconds": 180},
                {"name": "ckc", "profile": "ckc", "base_tps": 2000, "duration_seconds": 180},
            ],
        })

        self.assertIn("Workload: rate varies by target · 3m 00s per target · 6m 00s total", message)
        self.assertIn("• baseline (spring, 1000 TPS)", message)
        self.assertIn("• ckc (ckc, 2000 TPS)", message)
        self.assertNotIn("3m 00s)", message)

    def test_default_events_follow_the_high_signal_lifecycle(self) -> None:
        self.assertEqual(
            {
                "experiment_started",
                "kafka_warmup_started",
                "target_started",
                "target_workload_finished",
                "measurements_finished",
                "artifact_collection_started",
                "cleanup_started",
                "cleanup_finished",
                "analysis_started",
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
                "▶️ CKC target started: 1/3",
                "Target: spring-kafka.jdk",
                "Profile: spring-kafka · placement worker · 1 replica",
                "Expected workload: 13m 00s",
            ]),
            NOTIFY.message_for("target_started", {
                "experiment": "comparison",
                "name": "spring-kafka.jdk",
                "index": 1,
                "total": 3,
                "profile": "spring-kafka",
                "placement": "worker",
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
            "✅ All target measurements and audit streams finalized",
            NOTIFY.message_for("measurements_finished", {}),
        )

    def test_aws_progress_events_explain_long_post_workload_stages(self) -> None:
        self.assertEqual(
            "\n".join([
                "⏱️ CKC workload finished: 1/1",
                "Target: ckc.fixed-12",
                "Elapsed: 20m 04s",
                "Status: success",
                "Next: finalizing audit stream",
            ]),
            NOTIFY.message_for("target_workload_finished", {
                "index": 1,
                "total": 1,
                "name": "ckc.fixed-12",
                "elapsed_seconds": 1204,
                "status": "Success",
                "next_step": "finalizing audit stream",
            }),
        )
        self.assertEqual(
            "🧹 AWS cleanup started: sizing\nMeasurements: failed"
            "\nDeleting EKS, MSK, Redis, runner, and temporary storage",
            NOTIFY.message_for("cleanup_started", {
                "experiment": "sizing",
                "measurements_status": "failed",
            }),
        )
        self.assertEqual(
            "✅ AWS cleanup finished: sizing\nStatus: clean\nNext: local analysis",
            NOTIFY.message_for("cleanup_finished", {
                "experiment": "sizing",
                "cleanup_status": "clean",
                "next_step": "local analysis",
            }),
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
