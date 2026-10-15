from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("exporter", Path(__file__).with_name("exporter.py"))
exporter = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(exporter)


class ExporterTest(unittest.TestCase):
    def test_rules_include_both_directions_and_ports(self) -> None:
        text = exporter.ruleset({"kafka": [9092, 19092], "redis": [6379], "http": [8085]})
        self.assertIn("ckc-kafka-transmit", text)
        self.assertIn("ckc-kafka-receive", text)
        self.assertIn("hook input", text)
        self.assertIn("19092", text)
        self.assertIn("ckc-redis-transmit", text)
        self.assertIn("ckc-http-receive", text)

    @patch.object(exporter, "nft")
    def test_parses_json_counters(self, nft) -> None:
        nft.return_value.returncode = 0
        nft.return_value.stdout = json.dumps({"nftables": [
            {"rule": {"comment": "ckc-kafka-receive", "expr": [{"counter": {"bytes": 3, "packets": 2}}]}},
            {"rule": {"comment": "ckc-kafka-transmit", "expr": [{"counter": {"bytes": 8, "packets": 4}}]}},
            {"rule": {"comment": "ckc-redis-receive", "expr": [{"counter": {"bytes": 5, "packets": 1}}]}},
            {"rule": {"comment": "ckc-redis-transmit", "expr": [{"counter": {"bytes": 6, "packets": 1}}]}},
            {"rule": {"comment": "ckc-http-receive", "expr": [{"counter": {"bytes": 7, "packets": 1}}]}},
            {"rule": {"comment": "ckc-http-transmit", "expr": [{"counter": {"bytes": 9, "packets": 1}}]}},
        ]})
        self.assertEqual(6, len(exporter.counters({"kafka", "redis", "http"})[0]))
        self.assertTrue(exporter.counters({"kafka", "redis", "http"})[1])

    @patch.object(exporter, "nft")
    def test_read_failure_is_not_reported_as_success(self, nft) -> None:
        nft.return_value.returncode = 1
        self.assertEqual(({}, False), exporter.counters({"kafka", "redis", "http"}))
