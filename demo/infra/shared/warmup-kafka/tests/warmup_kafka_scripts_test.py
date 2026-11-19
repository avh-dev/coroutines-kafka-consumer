from __future__ import annotations

import os
import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "scripts/bootstrap-topics.sh"
READINESS = ROOT / "scripts/readiness.sh"
SUPERVISOR = ROOT / "scripts/run.sh"
PREWARM = ROOT / "scripts/prewarm-broker.sh"


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
    [ ! -f "${state}/broker-down" ] || exit 1
    printf '__ALL__\n' >> "${state}/described"
    for candidate in "${state}"/topic-*; do
      [ -f "$candidate" ] || continue
      actual="${candidate##*/topic-}"
      partitions="$(cat "$candidate")"
      printf 'Topic: %s PartitionCount: %s ReplicationFactor: 1\n' "$actual" "$partitions"
    done
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
        for name in ("kafka-topics.sh",):
            (self.bin / name).symlink_to(fake)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def environment(self, topics: str) -> dict[str, str]:
        runtime = self.root / "runtime"
        return {
            **os.environ,
            "KAFKA_BIN_DIR": str(self.bin),
            "KAFKA_BOOTSTRAP_TOPICS": topics,
            "KAFKA_BOOTSTRAP_TIMEOUT_SECONDS": "2",
            "KAFKA_BOOTSTRAP_BACKOFF_SECONDS": "1",
            "KAFKA_OPERATION_TIMEOUT_SECONDS": "2",
            "KAFKA_PREWARM_ENABLED": "false",
            "KAFKA_READY_FILE": str(runtime / "ready"),
            "KAFKA_CLIENT_CONFIG": str(runtime / "client.properties"),
            "FAKE_KAFKA_STATE": str(self.root),
        }

    def test_bootstrap_trims_deduplicates_and_creates_topics(self) -> None:
        result = subprocess.run(
            [BOOTSTRAP],
            env=self.environment(" alpha ,beta,alpha "),
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["alpha", "beta"], (self.root / "created").read_text().splitlines())
        self.assertTrue((self.root / "runtime/ready").is_file())

    def test_bootstrap_preserves_existing_topics_and_restores_them_after_data_loss(self) -> None:
        environment = self.environment("alpha,beta")
        (self.root / "topic-not-configured-anymore").write_text("1\n", encoding="utf-8")
        first = subprocess.run([BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, first.returncode, first.stderr)
        (self.root / "created").write_text("", encoding="utf-8")

        unchanged = subprocess.run([BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, unchanged.returncode, unchanged.stderr)
        self.assertEqual("", (self.root / "created").read_text(encoding="utf-8"))
        self.assertTrue((self.root / "topic-not-configured-anymore").is_file())

        (self.root / "topic-alpha").unlink()
        (self.root / "topic-beta").unlink()
        restored = subprocess.run([BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, restored.returncode, restored.stderr)
        self.assertEqual(["alpha", "beta"], (self.root / "created").read_text().splitlines())

    def test_bootstrap_matches_topic_names_literally(self) -> None:
        (self.root / "topic-warmup-orders").write_text("1\n", encoding="utf-8")

        result = subprocess.run(
            [BOOTSTRAP],
            env=self.environment("warmup.orders"),
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(["warmup.orders"], (self.root / "created").read_text().splitlines())
        self.assertTrue((self.root / "topic-warmup.orders").is_file())
        self.assertEqual(
            ["__ALL__", "__ALL__"],
            (self.root / "described").read_text(encoding="utf-8").splitlines(),
        )

    def test_broker_unavailability_fails_bounded_bootstrap_with_distinct_diagnostic(self) -> None:
        environment = self.environment("alpha")
        ready = self.root / "runtime/ready"
        ready.parent.mkdir()
        ready.write_text("previous-start\n", encoding="utf-8")
        (self.root / "broker-down").touch()
        result = subprocess.run(
            [BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10
        )

        self.assertEqual(1, result.returncode)
        self.assertIn("broker_unavailable", result.stderr)
        self.assertIn("bootstrap_timeout", result.stderr)
        self.assertFalse(ready.exists())

    def test_incompatible_partition_count_never_becomes_ready(self) -> None:
        (self.root / "topic-alpha").write_text("2\n", encoding="utf-8")
        environment = self.environment("alpha")
        ready = self.root / "runtime/ready"
        ready.parent.mkdir()
        ready.write_text("previous-start\n", encoding="utf-8")
        result = subprocess.run(
            [BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10
        )

        self.assertEqual(1, result.returncode)
        self.assertIn("incompatible_metadata", result.stderr)
        self.assertFalse(ready.exists())

    def test_readiness_is_written_only_after_successful_prewarm(self) -> None:
        prewarm = self.root / "prewarm.sh"
        prewarm.write_text(
            "#!/usr/bin/env bash\nprintf 'called\\n' > \"${FAKE_KAFKA_STATE}/prewarmed\"\n",
            encoding="utf-8",
        )
        prewarm.chmod(0o755)
        environment = self.environment("alpha")
        environment.update({
            "KAFKA_PREWARM_ENABLED": "true",
            "KAFKA_PREWARM_COMMAND": str(prewarm),
        })

        result = subprocess.run(
            [BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue((self.root / "prewarmed").is_file())
        self.assertTrue((self.root / "runtime/ready").is_file())
        self.assertIn("broker_prewarm_completed", result.stderr)

    def test_failed_prewarm_keeps_readiness_closed(self) -> None:
        prewarm = self.root / "failed-prewarm.sh"
        prewarm.write_text("#!/usr/bin/env bash\nexit 9\n", encoding="utf-8")
        prewarm.chmod(0o755)
        environment = self.environment("alpha")
        environment.update({
            "KAFKA_PREWARM_ENABLED": "true",
            "KAFKA_PREWARM_COMMAND": str(prewarm),
        })

        result = subprocess.run(
            [BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10
        )

        self.assertEqual(1, result.returncode)
        self.assertIn("broker_prewarm_failed", result.stderr)
        self.assertFalse((self.root / "runtime/ready").exists())

    def test_stopping_bootstrap_stops_active_prewarm(self) -> None:
        prewarm = self.root / "blocking-prewarm.sh"
        prewarm.write_text(
            "#!/usr/bin/env bash\n"
            "trap 'printf stopped > \"${FAKE_KAFKA_STATE}/prewarm-stopped\"; exit 0' TERM INT HUP\n"
            "printf started > \"${FAKE_KAFKA_STATE}/prewarm-started\"\n"
            "while :; do sleep 1; done\n",
            encoding="utf-8",
        )
        prewarm.chmod(0o755)
        environment = self.environment("alpha")
        environment.update({
            "KAFKA_PREWARM_ENABLED": "true",
            "KAFKA_PREWARM_COMMAND": str(prewarm),
        })
        process = subprocess.Popen(
            [BOOTSTRAP], env=environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not (self.root / "prewarm-started").exists():
                time.sleep(0.05)
            self.assertTrue((self.root / "prewarm-started").exists())
            process.terminate()
            process.communicate(timeout=5)
            self.assertTrue((self.root / "prewarm-stopped").exists())
            self.assertFalse((self.root / "runtime/ready").exists())
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)

    def test_invalid_or_empty_topic_configuration_is_rejected(self) -> None:
        for topics in ("", "alpha,,beta", "bad topic"):
            with self.subTest(topics=topics):
                result = subprocess.run(
                    [BOOTSTRAP], env=self.environment(topics), text=True, capture_output=True, timeout=5
                )
                self.assertEqual(2, result.returncode)
                self.assertIn("invalid_configuration", result.stderr)

    def test_readiness_requires_current_marker_live_broker_and_open_listener(self) -> None:
        runtime = self.root / "runtime"
        runtime.mkdir()
        (runtime / "broker.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
        ready = runtime / "ready"
        ready.write_text("current-start\n", encoding="utf-8")
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        accepted = threading.Thread(target=lambda: listener.accept()[0].close())
        accepted.start()
        environment = {
            **os.environ,
            "KAFKA_RUNTIME_DIR": str(runtime),
            "KAFKA_READINESS_PORT": str(port),
        }

        try:
            self.assertEqual(0, subprocess.run([READINESS], env=environment).returncode)
            accepted.join(timeout=2)
            listener.close()
            self.assertNotEqual(0, subprocess.run([READINESS], env=environment).returncode)
        finally:
            listener.close()
            accepted.join(timeout=2)

    def test_prewarm_rejects_an_unbounded_timeout_before_starting_java(self) -> None:
        result = subprocess.run(
            [PREWARM],
            env={**os.environ, "KAFKA_PREWARM_TIMEOUT_SECONDS": "0"},
            text=True,
            capture_output=True,
            timeout=5,
        )

        self.assertEqual(2, result.returncode)
        self.assertIn("must be a positive integer", result.stderr)

    def test_forced_prewarm_timeout_is_logged_before_restart_removes_evidence_file(self) -> None:
        java = self.root / "blocked-java.sh"
        java.write_text(
            "#!/usr/bin/env bash\ntrap '' TERM\nwhile :; do sleep 1; done\n",
            encoding="utf-8",
        )
        java.chmod(0o755)
        environment = self.environment("alpha")
        evidence = self.root / "runtime/prewarm.json"
        environment.update({
            "KAFKA_PREWARM_ENABLED": "true",
            "KAFKA_PREWARM_COMMAND": str(PREWARM),
            "KAFKA_PREWARM_JAVA_COMMAND": str(java),
            "KAFKA_PREWARM_TIMEOUT_SECONDS": "1",
            "KAFKA_PREWARM_KILL_AFTER_SECONDS": "1",
            "KAFKA_PREWARM_EVIDENCE_FILE": str(evidence),
        })

        result = subprocess.run(
            [BOOTSTRAP], env=environment, text=True, capture_output=True, timeout=10
        )

        self.assertNotEqual(0, result.returncode)
        self.assertFalse((self.root / "runtime/ready").exists())
        self.assertIn('"outcome":"failed"', evidence.read_text(encoding="utf-8"))

        broker = self.root / "restart-broker.sh"
        bootstrap = self.root / "restart-bootstrap.sh"
        broker.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        bootstrap.write_text("#!/usr/bin/env bash\nsleep 1\n", encoding="utf-8")
        broker.chmod(0o755)
        bootstrap.chmod(0o755)
        subprocess.run(
            [SUPERVISOR, broker],
            env={
                **os.environ,
                "KAFKA_RUNTIME_DIR": str(self.root / "runtime"),
                "KAFKA_BOOTSTRAP_COMMAND": str(bootstrap),
                "KAFKA_SHUTDOWN_TIMEOUT_SECONDS": "1",
            },
            text=True,
            capture_output=True,
            timeout=5,
        )
        self.assertFalse(evidence.exists())
        self.assertIn("warmup-kafka-prewarm final_evidence=", result.stderr)
        self.assertIn("prewarm_process_exit_", result.stderr)

    def test_forced_timeout_logs_final_failure_without_overwriting_pending_counters(self) -> None:
        java = self.root / "pending-java.sh"
        java.write_text(
            "#!/usr/bin/env bash\n"
            "printf '{\"outcome\":\"pending_cleanup\",\"accepted_records\":17}\\n' > \"${6}\"\n"
            "trap '' TERM\n"
            "while :; do sleep 1; done\n",
            encoding="utf-8",
        )
        java.chmod(0o755)
        evidence = self.root / "runtime/pending-prewarm.json"
        environment = {
            **os.environ,
            "KAFKA_PREWARM_JAVA_COMMAND": str(java),
            "KAFKA_PREWARM_TIMEOUT_SECONDS": "1",
            "KAFKA_PREWARM_KILL_AFTER_SECONDS": "1",
            "KAFKA_PREWARM_EVIDENCE_FILE": str(evidence),
        }

        result = subprocess.run(
            [PREWARM], env=environment, text=True, capture_output=True, timeout=5
        )

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(
            {"outcome": "pending_cleanup", "accepted_records": 17},
            json.loads(evidence.read_text(encoding="utf-8")),
        )
        self.assertIn("warmup-kafka-prewarm final_evidence=", result.stderr)
        self.assertIn('"outcome":"failed"', result.stderr)
        self.assertIn('"exit_code":', result.stderr)

    def test_supervisor_stops_broker_when_bootstrap_fails(self) -> None:
        runtime = self.root / "supervisor"
        broker = self.root / "broker.sh"
        bootstrap = self.root / "bootstrap.sh"
        broker.write_text("#!/usr/bin/env bash\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n", encoding="utf-8")
        bootstrap.write_text("#!/usr/bin/env bash\nexit 7\n", encoding="utf-8")
        broker.chmod(0o755)
        bootstrap.chmod(0o755)

        result = subprocess.run(
            [SUPERVISOR, broker],
            env={
                **os.environ,
                "KAFKA_RUNTIME_DIR": str(runtime),
                "KAFKA_BOOTSTRAP_COMMAND": str(bootstrap),
                "KAFKA_SHUTDOWN_TIMEOUT_SECONDS": "2",
            },
            text=True,
            capture_output=True,
            timeout=10,
        )

        self.assertEqual(7, result.returncode, result.stderr)
        self.assertIn("process=bootstrap", result.stderr)
        self.assertFalse((runtime / "broker.pid").exists())

    def test_supervisor_keeps_broker_running_after_successful_bootstrap(self) -> None:
        runtime = self.root / "successful-supervisor"
        broker = self.root / "successful-broker.sh"
        bootstrap = self.root / "successful-bootstrap.sh"
        broker.write_text("#!/usr/bin/env bash\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n", encoding="utf-8")
        bootstrap.write_text(
            "#!/usr/bin/env bash\nprintf 'ready\\n' > \"${KAFKA_READY_FILE}\"\n",
            encoding="utf-8",
        )
        broker.chmod(0o755)
        bootstrap.chmod(0o755)
        process = subprocess.Popen(
            [SUPERVISOR, broker],
            env={
                **os.environ,
                "KAFKA_RUNTIME_DIR": str(runtime),
                "KAFKA_READY_FILE": str(runtime / "ready"),
                "KAFKA_BOOTSTRAP_COMMAND": str(bootstrap),
                "KAFKA_SHUTDOWN_TIMEOUT_SECONDS": "2",
            },
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and (
                not (runtime / "ready").exists() or (runtime / "bootstrap.pid").exists()
            ):
                time.sleep(0.05)
            self.assertTrue((runtime / "ready").exists())
            self.assertIsNone(process.poll(), "supervisor exited after successful bootstrap")
            self.assertFalse((runtime / "bootstrap.pid").exists())
        finally:
            process.terminate()
            process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
