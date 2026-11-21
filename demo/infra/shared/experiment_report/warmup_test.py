from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from .markdown import render_warmup_and_resources
from .prometheus import PrometheusClient
from .warmup import broker_requests, broker_usage, warmup_summary


START = datetime(2026, 11, 21, 10, tzinfo=timezone.utc)
END = START + timedelta(minutes=3)


class WarmupReportTest(unittest.TestCase):
    def test_broker_requests_use_each_targets_full_duration(self) -> None:
        config = {"enabled": True, "resources": {"requests": {"cpu": "500m", "memory": "768Mi"}}}
        self.assertEqual({"requested_cpu": 0.5, "requested_memory": 0.75}, broker_requests(config, 3600))
        self.assertEqual({"requested_cpu": 0.125, "requested_memory": 0.1875}, broker_requests(config, 900))
        self.assertEqual({"requested_cpu": None, "requested_memory": None}, broker_requests(config, None))
        self.assertEqual({"requested_cpu": 0.0, "requested_memory": 0.0}, broker_requests({}, None))

    def test_available_usage_is_reported_independently_for_each_metric(self) -> None:
        prometheus = mock.Mock(spec=PrometheusClient)
        self.assertEqual(
            {"used_cpu": 0.0, "average_memory": 0.0},
            broker_usage(prometheus, "run-a", False, START, END),
        )
        prometheus.query_scalar.assert_not_called()

        prometheus.query_scalar.side_effect = [None, 240.0]
        self.assertEqual(
            {"used_cpu": None, "average_memory": 240.0},
            broker_usage(prometheus, "run-a", True, START, END),
        )
        for query, instant in (call.args for call in prometheus.query_scalar.call_args_list):
            self.assertIn('label_ckc_run_id="run-a"', query)
            self.assertIn('container="kafka"', query)
            self.assertIn("180s", query)
            self.assertNotIn("count_over_time", query)
            self.assertNotIn("kube_pod_created", query)
            self.assertEqual(END, instant)

    def test_summary_counts_unique_successful_pods_and_keeps_failed_attempts(self) -> None:
        def measurement(pod: str, attempt: str, outcome: str, value: int) -> dict:
            return {
                "metric": {"pod": pod, "attempt": attempt, "outcome": outcome},
                "value": [0, str(value)],
            }

        prometheus = mock.Mock(spec=PrometheusClient)
        prometheus.query_vector.side_effect = [
            [
                measurement("pod-a", "one", "success", 100),
                measurement("pod-a", "two", "success", 50),
                measurement("pod-b", "one", "timeout", 10),
                measurement("pod-c", "one", "error", 3),
                measurement("pod-d", "one", "skipped", 0),
            ],
            [measurement("pod-a", "one", "success", 22), measurement("pod-a", "two", "success", 28)],
        ]
        summary = warmup_summary(prometheus, "run-a", True, START, END)
        self.assertEqual({
            "enabled": True, "pods": 1, "median_seconds": 25, "max_seconds": 28,
            "records": 163, "errors": 1, "timeouts": 1,
        }, summary)

    def test_totals_require_both_series(self) -> None:
        target = SimpleNamespace(
            name="warmup",
            warmup={"enabled": True, "broker_enabled": True},
            resource_windows=[],
            full_target_budget={
                "duration_seconds": 1200.0,
                "application": {"requested_cpu": 0.5, "used_cpu": 0.5},
                "broker": {"requested_cpu": 0.25},
            },
        )
        report = "\n".join(render_warmup_and_resources([target]))
        self.assertIn("Full target including preparation · 1,200.0 s", report)
        self.assertIn("| Reserved CPU requests (vCPU·h) | 0.5000 | 0.2500 | 0.7500 |", report)
        self.assertIn("| CPU used (core·h) | 0.5000 | no data | no data |", report)
