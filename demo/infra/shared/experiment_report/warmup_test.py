from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from .markdown import render_warmup_and_resources
from .prometheus import PrometheusClient
from .warmup import broker_budget, warmup_summary


START = datetime(2026, 11, 21, 10, tzinfo=timezone.utc)
END = START + timedelta(minutes=3)


class WarmupReportTest(unittest.TestCase):
    def test_disabled_broker_contributes_zero_and_unobserved_broker_is_missing(self) -> None:
        prometheus = mock.Mock(spec=PrometheusClient)
        off = broker_budget(prometheus, "run-a", False, START, END, END)
        prometheus.query_scalar.assert_not_called()
        self.assertEqual(set(off.values()), {0.0})

        prometheus.query_scalar.return_value = None
        missing = broker_budget(prometheus, "run-a", True, START, END, END)
        self.assertEqual(set(missing.values()), {None})
        query, instant = prometheus.query_scalar.call_args.args
        self.assertIn('label_ckc_run_id="run-a"', query)
        self.assertIn("kube_pod_created", query)
        self.assertEqual(END, instant)

    def test_budget_queries_use_the_same_target_at_both_time_scopes(self) -> None:
        prometheus = mock.Mock(spec=PrometheusClient)
        prometheus.query_scalar.side_effect = [12, 12, 0.025, 12, 0.05, 12, 240, 0.015]
        budget = broker_budget(prometheus, "run-a", True, START, END, END + timedelta(minutes=2))
        self.assertEqual({
            "requested_cpu": 0.025,
            "requested_memory": 0.05,
            "used_cpu": 0.015,
            "average_memory": 240,
        }, budget)
        query, instant = prometheus.query_scalar.call_args.args
        self.assertIn('[300s] @ ', query)
        self.assertIn('container="kafka"', query)
        self.assertIn('[180s]', query)
        self.assertNotIn('container="demo"', query)
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
            resource_windows=[
                {
                    "start_seconds": 5, "duration_seconds": 10,
                    "resource_budget": {
                        "application": {"requested_cpu": 0.5, "used_cpu": 0.5},
                        "broker": {"requested_cpu": 0.25},
                    },
                }
            ],
        )
        report = "\n".join(render_warmup_and_resources([target]))
        self.assertIn("| Reserved CPU requests (vCPU·h) | 0.5000 | 0.2500 | 0.7500 |", report)
        self.assertIn("| CPU used (core·h) | 0.5000 | unavailable | unavailable |", report)
