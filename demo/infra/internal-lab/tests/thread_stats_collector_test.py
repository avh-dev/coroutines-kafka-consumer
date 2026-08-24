from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


HELPER_PATH = Path(__file__).resolve().parents[1] / "assets" / "helpers" / "collect-thread-stats.py"
SPEC = importlib.util.spec_from_file_location("thread_stats_collector_for_test", HELPER_PATH)
assert SPEC is not None and SPEC.loader is not None
collector_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collector_module
SPEC.loader.exec_module(collector_module)


def completed(stdout: str = "", stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


class ThreadStatsCollectorTest(unittest.TestCase):
    def collector(self, output_dir: Path, responses: list[subprocess.CompletedProcess[str]]):
        values = iter(responses)

        def runner(_command: list[str], _timeout_seconds: int) -> subprocess.CompletedProcess[str]:
            return next(values)

        return collector_module.ThreadStatsCollector(
            output_dir=output_dir,
            interval_seconds=60,
            namespace="ckc-perf",
            selector="app.kubernetes.io/name=ckc-demo",
            port=8080,
            endpoint="actuator/threadstats/json",
            request_timeout_seconds=20,
            command_runner=runner,
            clock=lambda: datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc),
        )

    def test_collects_full_snapshot_into_each_pod_directory(self) -> None:
        pods = {
            "items": [
                {"metadata": {"name": "ckc-demo-b", "uid": "uid-b"}},
                {"metadata": {"name": "ckc-demo-a", "uid": "uid-a"}},
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            collector = self.collector(
                output_dir,
                [
                    completed(json.dumps(pods)),
                    completed('{"pod":"a","groups":[{"name":"worker"}]}'),
                    completed('{"pod":"b","groups":[]}'),
                ],
            )
            collector.prepare()
            collector.collect_cycle()

            index = [json.loads(line) for line in collector.index_path.read_text(encoding="utf-8").splitlines()]
            summary = json.loads(collector.summary_path.read_text(encoding="utf-8"))

            self.assertEqual(["ckc-demo-a", "ckc-demo-b"], [item["pod"] for item in index])
            self.assertTrue(all(item["status"] == "success" for item in index))
            self.assertEqual("uid-a", index[0]["pod_uid"])
            self.assertEqual({"pod": "a", "groups": [{"name": "worker"}]}, json.loads((output_dir / index[0]["path"]).read_text()))
            self.assertEqual(2, summary["snapshot_attempts"])
            self.assertEqual(2, summary["successful_snapshots"])
            self.assertEqual(100.0, summary["coverage_percent"])
            self.assertEqual(["uid-a"], summary["pods"]["ckc-demo-a"]["uids"])

    def test_records_snapshot_failure_without_creating_json_artifact(self) -> None:
        pods = {"items": [{"metadata": {"name": "ckc-demo-a", "uid": "uid-a"}}]}
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            collector = self.collector(
                output_dir,
                [completed(json.dumps(pods)), completed(stderr="pod proxy failed", returncode=1)],
            )
            collector.prepare()
            collector.collect_cycle()

            record = json.loads(collector.index_path.read_text(encoding="utf-8"))
            summary = json.loads(collector.summary_path.read_text(encoding="utf-8"))

            self.assertEqual("failed", record["status"])
            self.assertEqual("pod proxy failed", record["error"])
            self.assertIsNone(record["path"])
            self.assertEqual(1, summary["failed_snapshots"])
            self.assertEqual(0.0, summary["coverage_percent"])
            self.assertFalse(any(output_dir.glob("ckc-demo-a/*.json")))

    def test_records_discovery_failure_and_keeps_collector_usable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            collector = self.collector(output_dir, [completed(stderr="cluster unavailable", returncode=1)])
            collector.prepare()
            collector.collect_cycle()

            record = json.loads(collector.index_path.read_text(encoding="utf-8"))
            summary = json.loads(collector.summary_path.read_text(encoding="utf-8"))

            self.assertEqual("discovery_failed", record["status"])
            self.assertEqual(1, summary["pod_discovery_failures"])
            self.assertEqual(0, summary["snapshot_attempts"])
            self.assertIsNone(summary["coverage_percent"])


if __name__ == "__main__":
    unittest.main()
