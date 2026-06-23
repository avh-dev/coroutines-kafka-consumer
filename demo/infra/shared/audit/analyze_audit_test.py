#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


def load_analyzer():
    path = Path(__file__).with_name("analyze-audit.py")
    spec = importlib.util.spec_from_file_location("analyze_audit", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load analyzer module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyzer = load_analyzer()


def analyze(lines: list[str], open_record_ttl_ms: int | None = None) -> dict[str, object]:
    accumulator = analyzer.AuditAccumulator(open_record_ttl_ms=open_record_ttl_ms)
    for line in lines:
        accumulator.add(analyzer.parse_record(line))
    accumulator.finish()
    return analyzer.summary_document(accumulator, {})


class AuditAnalyzerFairnessTest(unittest.TestCase):
    def test_exact_matching_keeps_long_delayed_terminal_records(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|72000|order-a",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual("exact", document["audit"]["delivery_matching"]["mode"])
        self.assertIsNone(document["audit"]["delivery_matching"]["open_record_ttl_seconds"])
        self.assertEqual(0, totals["missing_terminal"])
        self.assertEqual(0, totals["without_publish"]["processed"])

    def test_bounded_matching_can_evict_long_delayed_terminal_records(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "C|1|0|1|72000|order-a",
            ],
            open_record_ttl_ms=60_000,
        )

        totals = document["audit"]["totals"]
        self.assertEqual("bounded_ttl", document["audit"]["delivery_matching"]["mode"])
        self.assertEqual(60.0, document["audit"]["delivery_matching"]["open_record_ttl_seconds"])
        self.assertEqual(1, totals["missing_terminal"])
        self.assertEqual(1, totals["without_publish"]["processed"])

    def test_retry_attempts_do_not_count_as_terminal_conflicts(self) -> None:
        document = analyze(
            [
                "P|1|0|1|1000|1000|order-a",
                "R|1|0|1|1100|order-a",
                "C|1|0|1|1200|order-a",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(1, totals["retry_attempts"])
        self.assertEqual(1, totals["processed"])
        self.assertEqual(1, totals["terminal"])
        self.assertEqual(0, totals["failed"])
        self.assertEqual(0, totals["conflicting_terminal_outcomes"])

    def test_counts_unique_drop_reasons(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1000|cauldron-a",
                "D|3|0|1|1100|cauldron-a|stale_age",
                "D|3|0|1|1200|cauldron-a|stale_age",
                "P|3|0|2|2000|2000|cauldron-b",
                "D|3|0|2|2100|cauldron-b",
            ]
        )

        totals = document["audit"]["totals"]
        self.assertEqual(2, totals["dropped"])
        self.assertEqual({"stale_age": 1, "unknown": 1}, totals["dropped_by_reason"])
        self.assertEqual(1, totals["duplicates"]["dropped"])

    def test_reports_skewed_cauldron_processing_fairness(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1010|cauldron-a",
                "C|3|0|1|1200|cauldron-a",
                "P|3|0|2|2000|2010|cauldron-a",
                "D|3|0|2|2020|cauldron-a",
                "P|3|0|3|3000|3010|cauldron-b",
                "D|3|0|3|3020|cauldron-b",
                "P|3|0|4|4000|4010|cauldron-b",
                "D|3|0|4|4020|cauldron-b",
                "P|3|0|5|5000|5010|cauldron-c",
                "C|3|0|5|5200|cauldron-c",
                "P|3|0|6|6000|6010|cauldron-c",
                "C|3|0|6|8200|cauldron-c",
            ]
        )

        fairness = document["audit"]["topics"]["cauldron.events.v1"]["key_fairness"]

        self.assertEqual(3, fairness["keys"])
        self.assertEqual(1, fairness["keys_without_processed"])
        self.assertEqual(0.444444, fairness["processed_ratio"]["gini"])
        self.assertEqual(0.816497, fairness["processed_ratio"]["coefficient_of_variation"])
        self.assertEqual(1, fairness["processed_max_gap_ms"]["keys_over_5s"])
        self.assertEqual(3, fairness["record_age"]["processed"]["count"])
        self.assertEqual(2200, fairness["record_age"]["processed"]["max_ms"])

    def test_reports_even_cauldron_processing_fairness(self) -> None:
        document = analyze(
            [
                "P|3|0|1|1000|1010|cauldron-a",
                "C|3|0|1|1100|cauldron-a",
                "P|3|0|2|2000|2010|cauldron-b",
                "C|3|0|2|2100|cauldron-b",
                "P|3|0|3|3000|3010|cauldron-c",
                "C|3|0|3|3100|cauldron-c",
            ]
        )

        fairness = document["audit"]["topics"]["cauldron.events.v1"]["key_fairness"]

        self.assertEqual(3, fairness["keys"])
        self.assertEqual(0, fairness["keys_without_processed"])
        self.assertEqual(0.0, fairness["processed_ratio"]["gini"])
        self.assertEqual(0.0, fairness["processed_ratio"]["coefficient_of_variation"])
        self.assertEqual(0, fairness["processed_max_gap_ms"]["keys_over_5s"])
        self.assertEqual(100, fairness["record_age"]["processed"]["max_ms"])


if __name__ == "__main__":
    unittest.main()
