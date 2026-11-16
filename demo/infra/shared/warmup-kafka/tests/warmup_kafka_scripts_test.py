from __future__ import annotations

import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECONCILE = ROOT / "scripts/reconcile-topics.sh"
READINESS = ROOT / "scripts/readiness.sh"
SUPERVISOR = ROOT / "scripts/run.sh"


class WarmupKafkaScriptsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        fake = self.bin / "kafka-tool"
        fake.write_text(
            r"""#!/usr/bin/env bash
set -eu
state="${FAKE_KAFKA_STATE}"
tool="$(basename "$0")"
if [ "$tool" = kafka-broker-api-versions.sh ]; then
  [ ! -f "${state}/broker-down" ]
  exit
fi
topic=""
operation=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --topic) topic="$2"; shift 2 ;;
    --describe|--create) operation="$1"; shift ;;
    *) shift ;;
  esac
done
case "$operation" in
  --describe)
    printf '%s\n' "$topic" >> "${state}/described"
    if [[ "$topic" == \\Q*\\E ]]; then
      actual="${topic#\\Q}"
      actual="${actual%\\E}"
      [ -f "${state}/topic-${actual}" ] || exit 1
    else
      actual=""
      for candidate in "${state}"/topic-*; do
        [ -f "$candidate" ] || continue
        candidate="${candidate##*/topic-}"
        if [[ "$candidate" =~ ^${topic}$ ]]; then
          actual="$candidate"
          break
        fi
      done
      [ -n "$actual" ] || exit 1
    fi
    partitions="$(cat "${state}/topic-${actual}")"
    printf 'Topic: %s PartitionCount: %s ReplicationFactor: 1\n' "$actual" "$partitions"
    ;;
  --create)
    printf '1\n' > "${state}/topic-${topic}"
    printf '%s\n' "$topic" >> "${state}/created"
    ;;
esac
""",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        for name in ("kafka-broker-api-versions.sh", "kafka-topics.sh"):
            (self.bin / name).symlink_to(fake)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def environment(self, topics: str) -> dict[str, str]:
        runtime = self.root / "runtime"
        return {
            **os.environ,
            "KAFKA_BIN_DIR": str(self.bin),
            "KAFKA_BOOTSTRAP_TOPICS": topics,
            "KAFKA_RECONCILE_ONCE": "true",
            "KAFKA_OPERATION_TIMEOUT_SECONDS": "2",
            "KAFKA_READY_FILE": str(runtime / "ready"),
            "KAFKA_CLIENT_CONFIG": str(runtime / "client.properties"),
            "FAKE_KAFKA_STATE": str(self.root),
        }

    def test_reconcile_trims_deduplicates_and_creates_topics(self) -> None:
        result = subprocess.run(
            [RECONCILE],
            env=self.environment(" alpha ,beta,alpha "),
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["alpha", "beta"], (self.root / "created").read_text().splitlines())
        self.assertTrue((self.root / "runtime/ready").is_file())

    def test_reconcile_preserves_existing_topics_and_restores_them_after_data_loss(self) -> None:
        environment = self.environment("alpha,beta")
        (self.root / "topic-not-configured-anymore").write_text("1\n", encoding="utf-8")
        first = subprocess.run([RECONCILE], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, first.returncode, first.stderr)
        (self.root / "created").write_text("", encoding="utf-8")

        unchanged = subprocess.run([RECONCILE], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, unchanged.returncode, unchanged.stderr)
        self.assertEqual("", (self.root / "created").read_text(encoding="utf-8"))
        self.assertTrue((self.root / "topic-not-configured-anymore").is_file())

        (self.root / "topic-alpha").unlink()
        (self.root / "topic-beta").unlink()
        restored = subprocess.run([RECONCILE], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, restored.returncode, restored.stderr)
        self.assertEqual(["alpha", "beta"], (self.root / "created").read_text().splitlines())

    def test_reconcile_matches_topic_names_literally(self) -> None:
        (self.root / "topic-warmup-orders").write_text("1\n", encoding="utf-8")

        result = subprocess.run(
            [RECONCILE],
            env=self.environment("warmup.orders"),
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["warmup.orders"], (self.root / "created").read_text().splitlines())
        self.assertTrue((self.root / "topic-warmup.orders").is_file())
        self.assertEqual(
            [r"\Qwarmup.orders\E", r"\Qwarmup.orders\E"],
            (self.root / "described").read_text(encoding="utf-8").splitlines(),
        )

    def test_broker_unavailability_removes_readiness_with_distinct_diagnostic(self) -> None:
        environment = self.environment("alpha")
        environment["KAFKA_RECONCILE_ONCE"] = "false"
        ready = self.root / "runtime/ready"
        ready.parent.mkdir()
        ready.write_text(f"{int(time.time())}\n", encoding="utf-8")
        (self.root / "broker-down").touch()
        process = subprocess.Popen(
            [RECONCILE], env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            self.assertIn("broker_unavailable", process.stderr.readline() if process.stderr else "")
            self.assertFalse(ready.exists())
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_incompatible_partition_count_never_becomes_ready(self) -> None:
        (self.root / "topic-alpha").write_text("2\n", encoding="utf-8")
        environment = self.environment("alpha")
        environment["KAFKA_RECONCILE_ONCE"] = "false"
        ready = self.root / "runtime/ready"
        ready.parent.mkdir()
        ready.write_text(f"{int(time.time())}\n", encoding="utf-8")
        process = subprocess.Popen(
            [RECONCILE], env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if "incompatible_metadata" in (process.stderr.readline() if process.stderr else ""):
                    break
            else:
                self.fail("reconciler did not report incompatible metadata")
            self.assertFalse(ready.exists())
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_invalid_or_empty_topic_configuration_is_rejected(self) -> None:
        for topics in ("", "alpha,,beta", "bad topic"):
            with self.subTest(topics=topics):
                result = subprocess.run(
                    [RECONCILE], env=self.environment(topics), text=True, capture_output=True, timeout=5
                )
                self.assertEqual(2, result.returncode)
                self.assertIn("invalid_configuration", result.stderr)

    def test_readiness_requires_fresh_marker_and_live_children(self) -> None:
        runtime = self.root / "runtime"
        runtime.mkdir()
        for name in ("broker", "reconciler"):
            (runtime / f"{name}.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
        ready = runtime / "ready"
        ready.write_text(f"{int(time.time())}\n", encoding="utf-8")
        environment = {**os.environ, "KAFKA_RUNTIME_DIR": str(runtime), "KAFKA_READY_MAX_AGE_SECONDS": "2"}

        self.assertEqual(0, subprocess.run([READINESS], env=environment).returncode)
        ready.write_text(f"{int(time.time()) - 3}\n", encoding="utf-8")
        self.assertNotEqual(0, subprocess.run([READINESS], env=environment).returncode)

    def test_supervisor_stops_broker_when_reconciler_exits(self) -> None:
        runtime = self.root / "supervisor"
        broker = self.root / "broker.sh"
        reconciler = self.root / "reconciler.sh"
        broker.write_text("#!/usr/bin/env bash\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n", encoding="utf-8")
        reconciler.write_text("#!/usr/bin/env bash\nexit 7\n", encoding="utf-8")
        broker.chmod(0o755)
        reconciler.chmod(0o755)

        result = subprocess.run(
            [SUPERVISOR, broker],
            env={
                **os.environ,
                "KAFKA_RUNTIME_DIR": str(runtime),
                "KAFKA_RECONCILER_COMMAND": str(reconciler),
                "KAFKA_SHUTDOWN_TIMEOUT_SECONDS": "2",
            },
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(7, result.returncode, result.stderr)
        self.assertIn("process=reconciler", result.stderr)
        self.assertFalse((runtime / "broker.pid").exists())


if __name__ == "__main__":
    unittest.main()
