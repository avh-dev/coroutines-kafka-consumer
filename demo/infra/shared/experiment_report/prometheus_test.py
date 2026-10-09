from __future__ import annotations

import unittest

from .prometheus import STANDARD_MEASUREMENTS


class PrometheusMeasurementsTest(unittest.TestCase):
    def test_msk_credit_query_matches_cloudwatch_exporter_name(self) -> None:
        query = STANDARD_MEASUREMENTS["msk_cpu_credit_balance_min"]

        self.assertIn("aws_kafka_cpucredit_balance_minimum", query)
        self.assertNotIn("aws_kafka_cpu_credit_balance_minimum", query)

    def test_replica_queries_tolerate_short_cadvisor_scrape_gaps(self) -> None:
        for name in ("application_replicas_average", "application_replicas_min", "application_replicas_max"):
            self.assertIn("last_over_time(container_memory_working_set_bytes", STANDARD_MEASUREMENTS[name])
            self.assertIn("[1m]", STANDARD_MEASUREMENTS[name])


if __name__ == "__main__":
    unittest.main()
