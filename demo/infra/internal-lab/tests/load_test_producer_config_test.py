from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HELPERS = Path(__file__).resolve().parents[1] / "assets" / "helpers"


def load_helper(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HELPERS / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


definition_env = load_helper("producer_definition_env_for_test", "definition-env.py")


class LoadTestProducerConfigTest(unittest.TestCase):
    def test_reads_independent_topic_capacities(self) -> None:
        load_test = {
            "producer_capacity_tps": {
                "order": 700,
                "batch": 800,
                "telemetry": 900,
            }
        }

        self.assertEqual(700, definition_env.producer_capacity_tps(load_test, "order"))
        self.assertEqual(800, definition_env.producer_capacity_tps(load_test, "batch"))
        self.assertEqual(900, definition_env.producer_capacity_tps(load_test, "telemetry"))

    def test_defaults_each_topic_capacity(self) -> None:
        self.assertEqual(1000, definition_env.producer_capacity_tps({}, "order"))

    def test_rejects_non_positive_capacity(self) -> None:
        with self.assertRaisesRegex(ValueError, "producer_capacity_tps.order must be positive"):
            definition_env.producer_capacity_tps({"producer_capacity_tps": {"order": 0}}, "order")

    def test_rejects_non_object_capacity_config(self) -> None:
        with self.assertRaisesRegex(ValueError, "producer_capacity_tps must be an object"):
            definition_env.producer_capacity_tps({"producer_capacity_tps": 1000}, "order")

if __name__ == "__main__":
    unittest.main()
