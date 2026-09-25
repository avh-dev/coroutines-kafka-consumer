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
            "🚀 CKC experiment started: comparison\nTargets: 3",
            NOTIFY.message_for("experiment_started", {"experiment": "comparison", "targets": 3}),
        )

    def test_default_progress_events_are_compact(self) -> None:
        cases = {
            "experiment_runs_finished": "✅ Load runs completed",
            "audit_analysis_started": "🔍 Audit analysis started",
            "audit_analysis_finished": "✅ Audit analysis completed",
            "experiment_finished": "🏁 Experiment completed",
        }
        for event, expected in cases.items():
            with self.subTest(event=event):
                self.assertEqual(expected, NOTIFY.message_for(event, {}))

    def test_failures_are_compact_and_keep_the_exit_code(self) -> None:
        self.assertEqual(
            "❌ Experiment failed · exit 7",
            NOTIFY.message_for("experiment_failed", {"exit_code": 7}),
        )
        self.assertEqual(
            "❌ Audit analysis failed",
            NOTIFY.message_for("audit_analysis_finished", {"analysis": [{"exit_code": 1}]}),
        )
        self.assertEqual(
            "⏹️ Experiment stopped",
            NOTIFY.message_for("experiment_failed", {"exit_code": 130}),
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


if __name__ == "__main__":
    unittest.main()
