from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


AWS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = AWS_ROOT.parents[2]
PREPARE_TOPICS_PATH = REPO_ROOT / "demo/infra/shared/test-orchestration/prepare-kafka-topics.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


prepare_topics = load_module("ckc_prepare_kafka_topics", PREPARE_TOPICS_PATH)


class TopicLifecycleTest(unittest.TestCase):
    def test_aws_lab_creation_does_not_create_business_topics(self) -> None:
        script = (AWS_ROOT / "runner-assets/bin/create-lab.sh").read_text(encoding="utf-8")
        warmup = (REPO_ROOT / "demo/infra/shared/kafka_warmup/run.py").read_text(encoding="utf-8")

        self.assertNotIn("prepare-kafka-topics.py", script)
        self.assertIn('WARMUP_TOPIC_PREFIX = "ckc.warmup.v1."', warmup)

    def test_admin_script_waits_for_exact_topic_absence_and_retries_async_deletion(self) -> None:
        script = prepare_topics.admin_script(
            "broker:9092",
            3,
            "/opt/kafka-topics.sh",
            [{"name": "order.events.v1", "partitions": 12}],
        )

        self.assertIn('--list', script)
        self.assertIn('grep -Fxq "${topic}"', script)
        self.assertNotIn('--describe --topic "${topic}" >/dev/null', script)
        self.assertIn("grep -Fqi 'marked for deletion'", script)
        self.assertIn("create_topic 'order.events.v1' 12 3", script)

    def test_admin_script_survives_msk_deletion_race(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            topics_bin = root / "kafka-topics.sh"
            topics_bin.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
state_dir="$(dirname -- "$0")"
case " $* " in
  *" --delete "*) exit 0 ;;
  *" --list "*)
    count="$(cat "${state_dir}/list-count" 2>/dev/null || printf 0)"
    count="$((count + 1))"
    printf '%s' "${count}" > "${state_dir}/list-count"
    if [ "${count}" -eq 1 ]; then printf '%s\n' order.events.v1; fi
    ;;
  *" --create "*)
    count="$(cat "${state_dir}/create-count" 2>/dev/null || printf 0)"
    count="$((count + 1))"
    printf '%s' "${count}" > "${state_dir}/create-count"
    if [ "${count}" -eq 1 ]; then
      printf '%s\n' "Topic 'order.events.v1' is marked for deletion." >&2
      exit 1
    fi
    printf '%s\n' 'Created topic order.events.v1.'
    ;;
  *" --describe "*)
    printf '%s\n' 'Topic: order.events.v1 TopicId: topic-id PartitionCount: 12 ReplicationFactor: 3'
    ;;
esac
""",
                encoding="utf-8",
            )
            topics_bin.chmod(0o755)
            script = root / "admin.sh"
            script.write_text(
                prepare_topics.admin_script(
                    "broker:9092",
                    3,
                    str(topics_bin),
                    [{"name": "order.events.v1", "partitions": 12}],
                ).replace("sleep 2", "sleep 0"),
                encoding="utf-8",
            )

            result = subprocess.run(["bash", str(script)], text=True, capture_output=True, check=False)

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("2", (root / "list-count").read_text(encoding="utf-8"))
            self.assertEqual("2", (root / "create-count").read_text(encoding="utf-8"))
            self.assertIn("CKC_TOPIC_METADATA|order.events.v1|topic-id|12", result.stdout)

    def test_failed_admin_pod_is_detected_without_sleeping(self) -> None:
        failed = json.dumps({"status": {"phase": "Failed"}})

        with patch.object(prepare_topics, "run", return_value=failed) as run_command:
            with patch.object(prepare_topics.time, "sleep") as sleep:
                phase = prepare_topics.wait_for_pod_terminal("ckc-app", "ckc-kafka-admin")

        self.assertEqual("Failed", phase)
        self.assertEqual(1, run_command.call_count)
        sleep.assert_not_called()

    def test_admin_pod_polling_waits_until_success(self) -> None:
        pending = json.dumps({"status": {"phase": "Pending"}})
        succeeded = json.dumps({"status": {"phase": "Succeeded"}})

        with patch.object(prepare_topics, "run", side_effect=[pending, succeeded]):
            with patch.object(prepare_topics.time, "sleep") as sleep:
                phase = prepare_topics.wait_for_pod_terminal("ckc-app", "ckc-kafka-admin")

        self.assertEqual("Succeeded", phase)
        sleep.assert_called_once_with(2)


if __name__ == "__main__":
    unittest.main()
