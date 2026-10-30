from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HELPER = Path(__file__).resolve().parents[1] / "assets/helpers/definition-env.py"


def load_helper():
    spec = importlib.util.spec_from_file_location("definition_environment_topics_for_test", HELPER)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load definition-env.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


definition_env = load_helper()


class DefinitionEnvironmentTopicsTest(unittest.TestCase):
    def test_materialized_topics_override_planner_profile_topics(self) -> None:
        definition = {
            "deployment": {
                "kafka_topics": [
                    {"name": "order.events.v1", "partitions": 2},
                    {"name": "ckc.traffic-warmup.v1.order.events", "partitions": 1},
                ]
            }
        }
        deployment_profile = {
            "lab": {
                "kafkaTopics": [
                    {"name": "order.events.v1", "partitions": 2},
                ]
            }
        }

        self.assertEqual(
            ["order.events.v1:2", "ckc.traffic-warmup.v1.order.events:1"],
            definition_env.kafka_topic_specs(
                definition,
                deployment_profile,
                Path("run-plan-values.yaml"),
            ),
        )

    def test_legacy_definition_uses_deployment_profile_topics(self) -> None:
        self.assertEqual(
            ["order.events.v1:3"],
            definition_env.kafka_topic_specs(
                {},
                {"lab": {"kafkaTopics": [{"name": "order.events.v1", "partitions": 3}]}},
                Path("profile.yaml"),
            ),
        )


if __name__ == "__main__":
    unittest.main()
