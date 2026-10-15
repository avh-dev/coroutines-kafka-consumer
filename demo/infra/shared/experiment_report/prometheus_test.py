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
        self.assertEqual(2.0, counters["consumer"]["bytes_per_message"])
        self.assertTrue(any("demo_ckc_poll_records_sum" in query for query in client.queries))
        self.assertFalse(any("demo_ckc_record_process_duration_seconds_count" in query for query in client.queries))

    def test_spring_network_consumer_uses_processed_plus_dropped_fallback(self) -> None:
        client = RecordingPrometheusClient()

        counters = collect_kafka_network_counters(
            client, datetime(2026, 10, 14, tzinfo=timezone.utc), 60, "run-1", "ckc-spring-boot"
        )

        self.assertEqual("processed_plus_dropped", counters["consumer"]["denominator_source"])
        fallback = next(query for query in client.queries if "demo_ckc_record_dropped_total" in query)
        self.assertIn("demo_ckc_record_process_duration_seconds_count", fallback)


if __name__ == "__main__":
    unittest.main()
