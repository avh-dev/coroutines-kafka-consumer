from __future__ import annotations

import unittest

from datetime import datetime, timezone

from .prometheus import STANDARD_MEASUREMENTS, collect_kafka_network_counters


class RecordingPrometheusClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query_scalar(self, query: str, _at: datetime) -> float:
        self.queries.append(query)
        if "rules_present" in query or "read_success" in query:
            return 1.0
        return 100.0


class LegacyAwsNetworkClient(RecordingPrometheusClient):
    def query_scalar(self, query: str, at: datetime) -> float | None:
        self.queries.append(query)
        if "rules_present" in query or "read_success" in query:
            return 1.0
        if "demo_ckc_poll_records_sum" in query and 'run_id="' in query:
            return None
        if 'role="' in query:
            return None
        return 100.0


class PrometheusMeasurementsTest(unittest.TestCase):
    def test_msk_credit_query_matches_cloudwatch_exporter_name(self) -> None:
        query = STANDARD_MEASUREMENTS["msk_cpu_credit_balance_min"]

        self.assertIn("aws_kafka_cpucredit_balance_minimum", query)
        self.assertNotIn("aws_kafka_cpu_credit_balance_minimum", query)

    def test_replica_queries_tolerate_short_cadvisor_scrape_gaps(self) -> None:
        for name in ("application_replicas_average", "application_replicas_min", "application_replicas_max"):
            self.assertIn("last_over_time(container_memory_working_set_bytes", STANDARD_MEASUREMENTS[name])
            self.assertIn("[1m]", STANDARD_MEASUREMENTS[name])

    def test_ckc_network_consumer_normalizes_by_polled_records(self) -> None:
        client = RecordingPrometheusClient()

        counters = collect_kafka_network_counters(
            client, datetime(2026, 10, 14, tzinfo=timezone.utc), 60, "run-1", "ckc"
        )

        self.assertEqual(100.0, counters["consumer"]["consumer_records"])
        self.assertEqual("success", counters["status"])
        self.assertEqual(2.0, counters["consumer"]["bytes_per_message"])
        self.assertTrue(any('demo_ckc_poll_records_sum{run_id="run-1"' in query for query in client.queries))
        self.assertFalse(any("demo_ckc_record_process_duration_seconds_count" in query for query in client.queries))

    def test_spring_network_consumer_uses_processed_plus_dropped_fallback(self) -> None:
        client = RecordingPrometheusClient()

        counters = collect_kafka_network_counters(
            client, datetime(2026, 10, 14, tzinfo=timezone.utc), 60, "run-1", "ckc-spring-boot"
        )

        self.assertEqual("processed_plus_dropped", counters["consumer"]["denominator_source"])
        fallback = next(query for query in client.queries if "demo_ckc_record_dropped_total" in query)
        self.assertIn('run_id="run-1"', fallback)
        self.assertIn("demo_ckc_record_process_duration_seconds_count", fallback)

    def test_network_counters_fall_back_to_legacy_aws_namespace_labels(self) -> None:
        client = LegacyAwsNetworkClient()

        counters = collect_kafka_network_counters(
            client, datetime(2026, 10, 14, tzinfo=timezone.utc), 60, "run-1", "ckc"
        )

        self.assertEqual("success", counters["status"])
        self.assertEqual(100.0, counters["producer"]["rx_bytes"])
        self.assertEqual(100.0, counters["consumer"]["tx_bytes"])
        self.assertEqual(100.0, counters["consumer"]["consumer_records"])
        self.assertTrue(any('namespace="ckc-loadtest"' in query for query in client.queries))
        self.assertTrue(any('namespace="ckc-app"' in query for query in client.queries))


if __name__ == "__main__":
    unittest.main()
