from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .lifecycle import load_environment_file, notify


class NotificationLifecycleTest(unittest.TestCase):
    def test_environment_file_is_parsed_without_executing_shell(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "telegram.env"
            path.write_text(
                "# local secret\nexport TELEGRAM_BOT_TOKEN='token:value'\nTELEGRAM_CHAT_ID=-123\n",
                encoding="utf-8",
            )
            environment = load_environment_file(path)

        self.assertEqual({"TELEGRAM_BOT_TOKEN": "token:value", "TELEGRAM_CHAT_ID": "-123"}, environment)

    @patch("demo.infra.shared.experiment_notifications.lifecycle.subprocess.run")
    def test_python_hook_receives_persisted_payload_and_local_environment(self, run) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            hook = root / "notify.py"
            hook.write_text("", encoding="utf-8")
            notify(
                hook,
                "bundle_ready",
                {"experiment": "comparison", "artifacts": {"evidence": "/result/evidence.tar.gz"}},
                root / "events",
                environment={"TELEGRAM_BOT_TOKEN": "secret"},
            )

            command = run.call_args.args[0]
            payload_path = Path(command[-1])
            payload = json.loads(payload_path.read_text(encoding="utf-8"))

        self.assertEqual("bundle_ready", command[-2])
        self.assertEqual("comparison", payload["experiment"])
        self.assertEqual("secret", run.call_args.kwargs["env"]["TELEGRAM_BOT_TOKEN"])


if __name__ == "__main__":
    unittest.main()
