from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from dashboard import metric_names_from_dashboard, patch_dashboard, result_log_window
from presentation import experiment_panel_markdown


class DashboardTest(unittest.TestCase):
    def test_thread_stats_category_panels_use_explicit_category_labels(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        panels: list[dict] = []

        def collect(items: list[dict]) -> None:
            for panel in items:
                if panel.get("id") in {102, 103, 104}:
                    panels.append(panel)
                collect(panel.get("panels", []))

        collect(dashboard["panels"])
        self.assertEqual(3, len(panels))
        for panel in panels:
            self.assertEqual(1, len(panel["targets"]))
            target = panel["targets"][0]
            self.assertIn(
                "sum by (category, ${pod_grouping})",
                target["expr"],
            )
            self.assertNotIn('category="', target["expr"])
            self.assertNotIn("category_order", target["expr"])
            self.assertNotIn("ordered_category", target["expr"])
            self.assertNotIn("group=~", target["expr"])
            self.assertNotIn("group!~", target["expr"])
            self.assertEqual(
                "{{category}} {{pod_legend}}",
                target["legendFormat"],
            )
            self.assertEqual("A", target["refId"])

    def test_thread_stats_detail_panels_preserve_category_and_group(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        targets: list[dict] = []

        def collect(items: list[dict]) -> None:
            for panel in items:
                if panel.get("id") in set(range(89, 94)) | set(range(96, 101)):
                    targets.extend(panel.get("targets", []))
                collect(panel.get("panels", []))

        collect(dashboard["panels"])
        self.assertEqual(10, len(targets))
        for target in targets:
            self.assertIn(
                'label_replace(',
                target["expr"],
            )
            self.assertIn(
                '"category", "$1", "category", "^[0-9]+\\\\. (.+)$"',
                target["expr"],
            )
            self.assertIn("group", target["expr"])
            self.assertIn("{{category}} / {{group}}", target["legendFormat"])

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

    def test_log_window_includes_orchestration_before_workload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "run-status.json").write_text(json.dumps({
                "orchestration_started_at": "2026-08-31T09:58:20Z",
                "started_at": "2026-08-31T10:01:20Z",
                "ended_at": "2026-08-31T10:07:40Z",
            }))
            start, end = result_log_window(run_dirs=[run_dir])

        self.assertEqual(datetime(2026, 8, 31, 9, 56, 20, tzinfo=timezone.utc), start)
        self.assertEqual(datetime(2026, 8, 31, 10, 9, 40, tzinfo=timezone.utc), end)

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
                "orchestration_started_at": "2026-08-31T09:58:20Z",
                "started_at": "2026-08-31T10:01:20Z",
                "ended_at": "2026-08-31T10:07:40Z",
            }))
            markdown = experiment_panel_markdown(
                result_type="run",
                result_dir=run_dir,
                run_dirs=[run_dir],
                start=datetime(2026, 8, 31, 10, 1, 20, tzinfo=timezone.utc),
                end=datetime(2026, 8, 31, 10, 7, 40, tzinfo=timezone.utc),
                logs_start=datetime(2026, 8, 31, 9, 58, 20, tzinfo=timezone.utc),
                logs_end=datetime(2026, 8, 31, 10, 7, 40, tzinfo=timezone.utc),
                loki_selector='{run_id="run-a"}',
            )
            self.assertIn("[Reset time range](/d/ckc-experiment/ckc-experiment?", markdown)
            self.assertIn("from=1788170460000", markdown)
            self.assertIn("to=1788170880000", markdown)
            self.assertIn("[Open logs](/explore?", markdown)
            self.assertIn("1788170280000", markdown)
            self.assertIn("run_id", markdown)
            self.assertIn("[ckc.fixed.2](/d/ckc-experiment/ckc-experiment?", markdown)
            self.assertIn("2/105/1", markdown)


if __name__ == "__main__":
    unittest.main()
