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
    def test_report_ready_is_enabled_and_contains_report_path(self) -> None:
        self.assertIn("report_ready", NOTIFY.DEFAULT_EVENTS)
        message = NOTIFY.message_for(
            "report_ready",
            {"experiment": "comparison", "reports": ["/results/comparison/report.md"]},
        )
        self.assertEqual(
            "CKC report ready: comparison\n/results/comparison/report.md",
            message,
        )


if __name__ == "__main__":
    unittest.main()
