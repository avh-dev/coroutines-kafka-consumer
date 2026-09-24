from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from dashboard import (
    _latest_kafka_mode,
    materialize_environment_dashboard,
    metric_names_from_dashboard,
    patch_dashboard,
    result_log_window,
)
from prepare import has_parallel_consumer_target
from presentation import experiment_panel_markdown


class DashboardTest(unittest.TestCase):
    def test_detects_parallel_consumer_targets_from_run_metadata(self) -> None:
        self.assertTrue(has_parallel_consumer_target([
            {"application": {"run_profile": "spring-kafka"}},
            {"application": {"run_profile": "confluent-reactor", "profile": "confluent-parallel-reactor"}},
        ]))

    def test_rejects_non_parallel_consumer_targets(self) -> None:
        self.assertFalse(has_parallel_consumer_target([
            {"application": {"run_profile": "spring-kafka"}},
            {"application": {"run_profile": "ckc"}},
        ]))

    def test_dashboard_contains_native_parallel_consumer_metrics(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        rows = [panel for panel in dashboard["panels"] if panel.get("title") == "Confluent Parallel Consumer"]

        self.assertEqual(1, len(rows))
        self.assertEqual(12, len(rows[0]["panels"]))
        expressions = [
            target["expr"]
            for panel in rows[0]["panels"]
            for target in panel.get("targets", [])
        ]
        self.assertTrue(all('pod=~"$pod"' in expression for expression in expressions))
        self.assertTrue(any("pc_inflight_records" in expression for expression in expressions))
        self.assertTrue(any("pc_offsets_encoding_usage_total" in expression for expression in expressions))

    def test_dashboard_contains_native_kafka_consumer_client_metrics(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        rows = [panel for panel in dashboard["panels"] if panel.get("title") == "Kafka Consumer Client Runtime"]

        self.assertEqual(1, len(rows))
        self.assertEqual(12, len(rows[0]["panels"]))
        expressions = [
            target["expr"]
            for panel in rows[0]["panels"]
            for target in panel.get("targets", [])
        ]
        self.assertTrue(all('pod=~"$pod"' in expression for expression in expressions))
        self.assertTrue(any("kafka_consumer_last_poll_seconds_ago" in expression for expression in expressions))
        self.assertTrue(any("kafka_consumer_coordinator_commit_latency_max" in expression for expression in expressions))
        self.assertTrue(any("kafka_consumer_fetch_manager_records_lag_max" in expression for expression in expressions))

    def test_dashboard_keeps_record_drops_out_of_ckc_runtime(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        rows = {panel.get("title"): panel for panel in dashboard["panels"]}

        cauldron_panels = rows["Cauldron Events"]["panels"]
        drop_panels = [
            panel for panel in cauldron_panels
            if panel.get("title") == "Cauldron Events Drop Throughput"
        ]
        self.assertEqual(1, len(drop_panels))
        target = drop_panels[0]["targets"][0]
        self.assertIn("demo_ckc_record_dropped_total", target["expr"])
        self.assertIn('consumer_id=\"cauldron_events\"', target["expr"])
        self.assertNotIn("order_events|batch_events", target["expr"])
        self.assertEqual("{{reason}} {{pod_legend}}", target["legendFormat"])
        self.assertEqual("ops", drop_panels[0]["fieldConfig"]["defaults"]["unit"])

        ckc_titles = {panel.get("title") for panel in rows["CKC Runtime"]["panels"]}
        self.assertNotIn("CKC Drop Throughput", ckc_titles)

    def test_consumer_panels_explain_metrics_and_duration_filter(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        documented_rows = {
            "Order Events",
            "Batch Events",
            "Cauldron Events",
            "CKC Runtime",
            "Confluent Parallel Consumer",
        }

        for row in dashboard["panels"]:
            if row.get("title") not in documented_rows:
                continue
            for panel in row["panels"]:
                self.assertTrue(panel.get("description"), panel["title"])

        duration_panels = [
            panel
            for row in dashboard["panels"]
            for panel in row.get("panels", [])
            if "Processing Duration" in panel.get("title", "")
            or "End-to-End Latency" in panel.get("title", "")
        ]
        self.assertEqual(6, len(duration_panels))
        self.assertTrue(all("Duration statistic" in panel["description"] for panel in duration_panels))

        duration_filter = next(
            variable
            for variable in dashboard["templating"]["list"]
            if variable.get("name") == "duration_percentile"
        )
        self.assertEqual("Duration statistic", duration_filter["label"])
        self.assertIn("p99", duration_filter["description"])

    def test_all_metric_panels_have_usage_descriptions(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))

        def metric_panels(items: list[dict]) -> list[dict]:
            return [
                child
                for panel in items
                for child in ([panel] + metric_panels(panel.get("panels", [])))
                if child.get("type") not in {"row", "text"} and child.get("targets")
            ]

        undocumented = [panel["title"] for panel in metric_panels(dashboard["panels"]) if not panel.get("description")]
        self.assertEqual([], undocumented)

    def test_dashboard_controls_are_scoped_and_explained(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        variables = dashboard["templating"]["list"]

        self.assertEqual(
            ["pod", "pod_grouping", "duration_percentile", "event_type_grouping"],
            [variable["name"] for variable in variables],
        )
        self.assertEqual(
            ["Application pods", "Pod grouping", "Duration statistic", "Event types"],
            [variable["label"] for variable in variables],
        )
        self.assertTrue(all(variable.get("description") for variable in variables))

    def test_application_resources_show_total_and_average_per_pod(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        row = next(panel for panel in dashboard["panels"] if panel.get("title") == "Application Resources")
        panels = {panel["title"]: panel for panel in row["panels"]}

        self.assertIn("sum(rate(container_cpu_usage_seconds_total", panels["Application Container CPU — Total"]["targets"][0]["expr"])
        self.assertIn("avg(rate(container_cpu_usage_seconds_total", panels["Application Container CPU — Average per Pod"]["targets"][0]["expr"])
        self.assertIn("sum(container_memory_working_set_bytes", panels["Application Container Working Set — Total"]["targets"][0]["expr"])
        self.assertIn("avg(container_memory_working_set_bytes", panels["Application Container Working Set — Average per Pod"]["targets"][0]["expr"])
        for title, panel in panels.items():
            if not title.startswith("Application Container"):
                continue
            self.assertNotIn("${pod_grouping}", " ".join(target.get("expr", "") for target in panel.get("targets", [])))

    def test_redis_panels_do_not_depend_on_event_type_control(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        row = next(panel for panel in dashboard["panels"] if panel.get("title") == "Redis Calls")
        panels = {panel["title"]: panel for panel in row["panels"]}

        self.assertEqual(
            {"Redis Command Rate — Total", "Redis Command Rate — By Command", "Redis Command Latency"},
            set(panels),
        )
        expressions = " ".join(target["expr"] for panel in panels.values() for target in panel["targets"])
        self.assertNotIn("event_type_grouping", expressions)
        self.assertNotIn("breakdown", panels["Redis Command Rate — Total"]["targets"][0]["expr"])
        self.assertIn("command", panels["Redis Command Rate — By Command"]["targets"][0]["expr"])

    def test_dashboard_aggregates_offset_trackers_and_exposes_commit_metadata(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        runtime = next(panel for panel in dashboard["panels"] if panel.get("title") == "CKC Runtime")
        panels = {panel["title"]: panel for panel in runtime["panels"]}

        total = panels["CKC Offset Tracker Total Capacity"]["targets"][0]
        maximum = panels["CKC Offset Tracker Maximum Partition Capacity"]["targets"][0]
        self.assertIn("sum by (consumer_id, ${pod_grouping})", total["expr"])
        self.assertIn("max by (consumer_id, ${pod_grouping})", maximum["expr"])
        self.assertNotIn("partition, ${pod_grouping}", total["expr"])
        self.assertNotIn("partition, ${pod_grouping}", maximum["expr"])
        self.assertEqual("short", panels["CKC Offset Tracker Total Capacity"]["fieldConfig"]["defaults"]["unit"])
        self.assertEqual(
            "short",
            panels["CKC Offset Tracker Maximum Partition Capacity"]["fieldConfig"]["defaults"]["unit"],
        )

        size_expressions = " ".join(
            target["expr"] for target in panels["CKC Commit Metadata Size"]["targets"]
        )
        self.assertIn("demo_ckc_commit_metadata_size_sum", size_expressions)
        self.assertIn("demo_ckc_commit_metadata_payload_raw_size_sum", size_expressions)
        self.assertIn("demo_ckc_commit_metadata_payload_encoded_size_sum", size_expressions)
        self.assertIn('included=\"true\"', size_expressions)

        compression = panels["CKC Commit Metadata Compression Ratio"]["targets"][0]["expr"]
        utilization = " ".join(
            target["expr"] for target in panels["CKC Commit Metadata Limit Utilization"]["targets"]
        )
        self.assertIn("demo_ckc_commit_metadata_payload_compression_ratio_sum", compression)
        self.assertIn("demo_ckc_commit_metadata_limit_utilization_sum", utilization)
        self.assertIn("demo_ckc_commit_metadata_limit_utilization_max", utilization)

    def test_dashboard_panel_ids_are_unique(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        panel_ids: list[int] = []

        def collect(items: list[dict]) -> None:
            for panel in items:
                if "id" in panel:
                    panel_ids.append(panel["id"])
                collect(panel.get("panels", []))

        collect(dashboard["panels"])
        self.assertEqual(len(panel_ids), len(set(panel_ids)))

    def test_metric_panels_keep_half_width_when_the_other_half_is_empty(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))

        def collect(items: list[dict]) -> list[dict]:
            return [
                child
                for panel in items
                for child in ([panel] + collect(panel.get("panels", [])))
                if child.get("type") not in {"row", "text"}
            ]

        full_width = [panel["title"] for panel in collect(dashboard["panels"]) if panel.get("gridPos", {}).get("w") == 24]
        self.assertEqual([], full_width)

    def test_parallel_consumer_state_and_partition_progress_are_readable(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        row = next(panel for panel in dashboard["panels"] if panel.get("title") == "Confluent Parallel Consumer")
        panels = {panel["title"]: panel for panel in row["panels"]}

        state = panels["PC Runtime State"]
        self.assertEqual("state-timeline", state["type"])
        mappings = state["fieldConfig"]["defaults"]["mappings"][0]["options"]
        self.assertEqual(
            ["UNUSED", "RUNNING", "PAUSED", "DRAINING", "CLOSING", "CLOSED"],
            [mappings[str(index)]["text"] for index in range(6)],
        )
        progress = panels["PC Partition Offset Progress"]
        for target in progress["targets"]:
            self.assertIn("partition, ${pod_grouping}", target["expr"])
            self.assertIn("p{{partition}}", target["legendFormat"])

    def test_application_context_switch_panel_uses_thread_stats_and_pod_filter(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        panels: list[dict] = []

        def collect(items: list[dict]) -> None:
            for panel in items:
                if panel.get("id") == 105:
                    panels.append(panel)
                collect(panel.get("panels", []))

        collect(dashboard["panels"])
        self.assertEqual(1, len(panels))
        self.assertEqual("Application Context Switches", panels[0]["title"])
        target = panels[0]["targets"][0]
        self.assertIn("thread_stats_context_switches_total", target["expr"])
        self.assertIn('job="ckc-demo"', target["expr"])
        self.assertIn('pod=~"$pod"', target["expr"])
        self.assertIn("sum by (type, ${pod_grouping})", target["expr"])
        self.assertEqual("{{type}} {{pod_legend}}", target["legendFormat"])
        self.assertNotIn("namedprocess_", target["expr"])

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

    def test_thread_stats_context_switch_panels_are_split_smooth_and_stacked(self) -> None:
        dashboard_path = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        panels: dict[int, dict] = {}

        def collect(items: list[dict]) -> None:
            for panel in items:
                if panel.get("id") in {119, 120}:
                    panels[panel["id"]] = panel
                collect(panel.get("panels", []))

        collect(dashboard["panels"])
        self.assertEqual({119, 120}, set(panels))
        expectations = {
            119: ("Voluntary Context Switches by Category", "voluntary"),
            120: ("Non-voluntary Context Switches by Category", "involuntary"),
        }
        for panel_id, (title, switch_type) in expectations.items():
            panel = panels[panel_id]
            self.assertEqual(title, panel["title"])
            custom = panel["fieldConfig"]["defaults"]["custom"]
            self.assertEqual("smooth", custom["lineInterpolation"])
            self.assertEqual("normal", custom["stacking"]["mode"])
            target = panel["targets"][0]
            self.assertIn("thread_stats_context_switches_total", target["expr"])
            self.assertIn(f'type="{switch_type}"', target["expr"])
            self.assertIn("sum by (category, ${pod_grouping})", target["expr"])
            self.assertIn('pod=~"$pod"', target["expr"])
            self.assertEqual("{{category}} {{pod_legend}}", target["legendFormat"])

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

    def test_materializes_live_dashboard_for_each_environment(self) -> None:
        source = Path(__file__).resolve().parents[1] / "grafana/dashboards/ckc-overview.json"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = [
                ("internal-lab", "kubernetes", True, False, 'namespace="ckc-perf"'),
                ("aws", "kubernetes", False, False, 'namespace="ckc-app"'),
                ("aws", "msk", False, True, 'namespace="ckc-app"'),
            ]
            for environment, kafka_mode, has_host_rows, has_msk, namespace in cases:
                target = root / f"{environment}-{kafka_mode}.json"
                materialize_environment_dashboard(
                    source,
                    target,
                    environment=environment,
                    kafka_mode=kafka_mode,
                )
                content = target.read_text(encoding="utf-8")
                decoded_content = str(json.loads(content))
                self.assertEqual(has_host_rows, "Host Services: Redis" in content)
                self.assertEqual(has_msk, "MSK CloudWatch Time Lag" in content)
                self.assertIn(namespace, decoded_content)

    def test_uses_latest_aws_lab_context_for_live_dashboard_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            older = root / "load-lab-old.json"
            newer = root / "load-lab-new.json"
            older.write_text(json.dumps({"kafka_mode": "kubernetes"}), encoding="utf-8")
            newer.write_text(json.dumps({"kafka_mode": "msk"}), encoding="utf-8")
            older.touch()
            newer.touch()
            older_time = newer.stat().st_mtime - 10
            os.utime(older, (older_time, older_time))

            self.assertEqual("msk", _latest_kafka_mode(root))

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
