from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from dashboard import metric_names_from_dashboard, patch_dashboard
from presentation import experiment_panel_markdown


class DashboardTest(unittest.TestCase):
    def test_patches_time_summary_environment_and_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.json"
            target = root / "target.json"
            source.write_text(json.dumps({
                "uid": "old",
                "title": "Old",
                "time": {"from": "now-30m", "to": "now"},
                "panels": [
                    {"id": 1, "type": "row", "title": "Run", "gridPos": {"y": 0}},
                    {"id": 2, "type": "row", "title": "Host", "gridPos": {"y": 1}, "panels": []},
                    {"id": 4, "type": "row", "title": "Pod", "gridPos": {"y": 2}, "panels": [
                        {"id": 5, "type": "timeseries", "title": "Context Switches"},
                    ]},
                    {"id": 3, "type": "timeseries", "title": "CPU", "gridPos": {"y": 3}, "targets": [{"expr": 'container_cpu_usage_seconds_total{namespace="ckc-perf"}'}]},
                ],
            }))
            result = patch_dashboard(
                source,
                target,
                title="AWS run",
                markdown="hello",
                start=datetime(2026, 8, 30, 10, 1, 20, tzinfo=timezone.utc),
                end=datetime(2026, 8, 30, 10, 11, 40, tzinfo=timezone.utc),
                excluded_row_titles={"Host"},
                excluded_panel_titles={"Context Switches"},
                substitutions={'namespace="ckc-perf"': 'namespace="ckc-app"'},
            )
            dashboard = json.loads(target.read_text())
            self.assertEqual("ckc-experiment", dashboard["uid"])
            self.assertEqual({"from": "2026-08-30T10:01Z", "to": "2026-08-30T10:12Z"}, dashboard["time"])
            self.assertEqual("Experiment", dashboard["panels"][0]["title"])
            self.assertNotIn("Host", [panel.get("title") for panel in dashboard["panels"]])
            self.assertNotIn("Context Switches", json.dumps(dashboard))
            self.assertIn('namespace="ckc-app"', dashboard["panels"][3]["targets"][0]["expr"])
            self.assertEqual(["Host"], result["excluded_rows"])
            self.assertEqual(["Context Switches"], result["excluded_panels"])

    def test_extracts_dashboard_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "dashboard.json").write_text(json.dumps({"targets": [{"expr": "rate(demo_requests_total[5m])"}]}))
            self.assertEqual(["demo_requests_total"], metric_names_from_dashboard(root))

    def test_shared_experiment_panel_links_run_and_logs_to_exact_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "run-metadata.json").write_text(json.dumps({
                "test_name": "ckc.fixed.2",
                "test_definition": "6min",
                "load_test": {"base_tps": 12000},
                "application": {"run_profile": "ckc", "replica_count": 2},
                "run_plan": {"topics": [{
                    "name": "order", "partitions": 2, "worker_concurrency": 105, "poll_loop_concurrency": 1,
                }]},
            }))
            (run_dir / "run-status.json").write_text(json.dumps({
                "status": "COMPLETED",
                "started_at": "2026-08-31T10:01:20Z",
                "ended_at": "2026-08-31T10:07:40Z",
            }))
            markdown = experiment_panel_markdown(
                result_type="run",
                result_dir=run_dir,
                run_dirs=[run_dir],
                start=datetime(2026, 8, 31, 10, 1, 20, tzinfo=timezone.utc),
                end=datetime(2026, 8, 31, 10, 7, 40, tzinfo=timezone.utc),
                loki_selector='{run_id="run-a"}',
            )
            self.assertIn("[Reset time range](/d/ckc-experiment/ckc-experiment?", markdown)
            self.assertIn("from=1788170460000", markdown)
            self.assertIn("to=1788170880000", markdown)
            self.assertIn("[Open logs](/explore?", markdown)
            self.assertIn("run_id", markdown)
            self.assertIn("[ckc.fixed.2](/d/ckc-experiment/ckc-experiment?", markdown)
            self.assertIn("2/105/1", markdown)


if __name__ == "__main__":
    unittest.main()
