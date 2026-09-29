from __future__ import annotations

import argparse
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


INTERNAL_LAB = Path(__file__).resolve().parents[1]
HELPERS = INTERNAL_LAB / "assets/helpers"
SHARED = INTERNAL_LAB.parent / "shared"
sys.path.insert(0, str(HELPERS))
sys.path.insert(0, str(SHARED))

SPEC = importlib.util.spec_from_file_location(
    "wait_consumer_drain_for_test",
    HELPERS / "wait-consumer-drain.py",
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load wait-consumer-drain.py")
DRAIN = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DRAIN
SPEC.loader.exec_module(DRAIN)


class ConsumerDrainHelperTest(unittest.TestCase):
    def args(self) -> argparse.Namespace:
        return argparse.Namespace(
            prometheus_url="http://prometheus:9090",
            group_regex="^ckc-demo$",
            groups="ckc-demo",
            kafka_implementation="apache-kafka",
        )

    def test_kafka_lag_is_used_when_prometheus_is_unavailable(self) -> None:
        with (
            patch.object(DRAIN, "query_lag", side_effect=OSError("connection refused")),
            patch.object(DRAIN, "query_apache_kafka_lag", return_value=17.0),
        ):
            lag, source = DRAIN.query_lag_with_fallback(self.args())

        self.assertEqual(17.0, lag)
        self.assertEqual("kafka-consumer-groups", source)

    def test_processing_total_is_optional_when_prometheus_is_unavailable(self) -> None:
        with patch.object(DRAIN, "query_processing_total", side_effect=OSError("connection refused")):
            self.assertIsNone(DRAIN.query_processing_total_optional("http://prometheus:9090"))


if __name__ == "__main__":
    unittest.main()
