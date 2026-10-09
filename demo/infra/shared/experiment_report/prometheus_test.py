from __future__ import annotations

import unittest

from .prometheus import STANDARD_MEASUREMENTS


class PrometheusMeasurementsTest(unittest.TestCase):
    def test_msk_credit_query_matches_cloudwatch_exporter_name(self) -> None:
        query = STANDARD_MEASUREMENTS["msk_cpu_credit_balance_min"]

        self.assertIn("aws_kafka_cpucredit_balance_minimum", query)
        self.assertNotIn("aws_kafka_cpu_credit_balance_minimum", query)


if __name__ == "__main__":
    unittest.main()
