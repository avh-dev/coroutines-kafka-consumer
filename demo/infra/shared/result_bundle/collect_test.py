from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from .collect import create_prometheus_blocks, log_window, run_window, warmup_metric_windows


class PrometheusBlockExportTest(unittest.TestCase):
    def test_metrics_exclude_pre_measurement_warmup_while_logs_keep_orchestration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "run-metadata.json").write_text(json.dumps({
                "orchestration_started_at": "2026-09-25T10:00:00Z",
                "started_at": "2026-09-25T10:04:00Z",
            }), encoding="utf-8")
            (run_dir / "run-status.json").write_text(json.dumps({
                "started_at": "2026-09-25T10:04:00Z",
                "ended_at": "2026-09-25T10:05:00Z",
            }), encoding="utf-8")

            metrics_start, _ = run_window(run_dir)
            logs_start, _ = log_window(run_dir)

        self.assertEqual(datetime(2026, 9, 25, 10, 4, tzinfo=timezone.utc), metrics_start)
        self.assertEqual(datetime(2026, 9, 25, 10, 0, tzinfo=timezone.utc), logs_start)

    def test_warmup_evidence_covers_preparation_and_run_end(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "run-metadata.json").write_text(json.dumps({
                "orchestration_started_at": "2026-09-25T10:00:00Z",
            }), encoding="utf-8")
            start = datetime(2026, 9, 25, 10, 4, tzinfo=timezone.utc)
            end = datetime(2026, 9, 25, 10, 7, tzinfo=timezone.utc)

            windows = warmup_metric_windows([run_dir], start, end)
            self.assertEqual(
                (datetime(2026, 9, 25, 10, tzinfo=timezone.utc),
                 end),
                windows["container_cpu_usage_seconds_total"],
            )
            self.assertEqual(windows["kube_pod_labels"], windows["demo_warmup_recipient_records"])
            self.assertNotIn("kafka_consumergroup_lag", windows)

    @mock.patch("demo.infra.shared.result_bundle.collect.os.getgid", return_value=2345)
    @mock.patch("demo.infra.shared.result_bundle.collect.os.getuid", return_value=1234)
    @mock.patch("demo.infra.shared.result_bundle.collect.subprocess.run")
    def test_promtool_runs_as_the_calling_runtime_user(
        self,
        run: mock.Mock,
        _getuid: mock.Mock,
        _getgid: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            openmetrics = root / "metrics.openmetrics"
            openmetrics.write_text("# EOF\n", encoding="utf-8")

            self.assertEqual(0, create_prometheus_blocks(openmetrics, root / "output"))

        command = run.call_args.args[0]
        self.assertEqual("1234:2345", command[command.index("-u") + 1])


if __name__ == "__main__":
    unittest.main()
