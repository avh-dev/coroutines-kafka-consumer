from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class KafkaWarmupIntegrationTest(unittest.TestCase):
    def test_reset_detects_actual_broker_replacement_and_persists_signature(self) -> None:
        script = (ROOT / "assets/libexec/reset-kafka-redis.sh").read_text(encoding="utf-8")
        self.assertIn("{{.State.StartedAt}}", script)
        self.assertIn('KAFKA_RUNTIME_BEFORE="$(kafka_runtime_signature)"', script)
        self.assertIn('KAFKA_RUNTIME_PREVIOUS="$(cat', script)
        self.assertIn('record_kafka_runtime_signature "${KAFKA_RUNTIME_AFTER}"', script)

    def test_warmup_is_apache_only_and_not_unconditionally_run_per_target(self) -> None:
        script = (ROOT / "assets/libexec/reset-kafka-redis.sh").read_text(encoding="utf-8")
        guarded = 'if [ -n "${KAFKA_WARMUP_REASON}" ] && [ "${LAB_KAFKA_IMPLEMENTATION}" = "apache-kafka" ]; then'
        self.assertIn(guarded, script)
        self.assertEqual(1, script.count('warm_apache_kafka "${KAFKA_WARMUP_REASON}"'))

    def test_runner_propagates_notification_hook_to_warmup(self) -> None:
        runner = (ROOT / "assets/helpers/run-experiment.py").read_text(encoding="utf-8")
        self.assertIn('env.setdefault("CKC_NOTIFY_HOOK", str(hook))', runner)
        self.assertIn('env.setdefault("CKC_NOTIFICATION_DIR"', runner)

    def test_warmup_does_not_create_dashboard_wide_annotations(self) -> None:
        reset = (ROOT / "assets/libexec/reset-kafka-redis.sh").read_text(encoding="utf-8")
        warmup = (ROOT.parent / "shared/kafka_warmup/run.py").read_text(encoding="utf-8")
        self.assertNotIn("--grafana-url", reset)
        self.assertNotIn("api/annotations", warmup)


if __name__ == "__main__":
    unittest.main()
