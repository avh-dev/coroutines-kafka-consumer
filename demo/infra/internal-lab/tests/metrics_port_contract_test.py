from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[4]
COMPOSE = REPO_ROOT / "demo/infra/internal-lab/assets/compose/docker-compose.host-services.yml"
EXTERNAL_SERVICES = REPO_ROOT / "demo/infra/internal-lab/assets/k8s/external-services.yaml.tpl"
PROMETHEUS = REPO_ROOT / "demo/infra/internal-lab/assets/k8s/prometheus.yaml"


class MetricsPortContractTest(unittest.TestCase):
    def test_kafka_thread_stats_does_not_overlap_load_test_metrics(self) -> None:
        compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
        kafka_services = ("apache-kafka-1", "apache-kafka-2", "apache-kafka-3")
        kafka_host_ports = {
            int(str(port).split(":")[-2])
            for service in kafka_services
            for port in compose["services"][service]["ports"]
            if str(port).endswith(":9404")
        }
        self.assertEqual({9414, 9415, 9416}, kafka_host_ports)
        self.assertNotIn(9405, kafka_host_ports)

        documents = list(yaml.safe_load_all(EXTERNAL_SERVICES.read_text(encoding="utf-8")))
        services = {
            document["metadata"]["name"]: document
            for document in documents
            if document and document.get("kind") == "Service"
        }
        thread_stats_ports = {
            port["port"] for port in services["ckc-external-kafka-thread-stats"]["spec"]["ports"]
        }
        load_test_ports = {
            port["port"] for port in services["ckc-external-load-test"]["spec"]["ports"]
        }
        self.assertEqual({9414, 9415, 9416}, thread_stats_ports)
        self.assertEqual({9405}, load_test_ports)
        self.assertTrue(thread_stats_ports.isdisjoint(load_test_ports))

        prometheus_documents = list(yaml.safe_load_all(PROMETHEUS.read_text(encoding="utf-8")))
        config_map = next(document for document in prometheus_documents if document.get("kind") == "ConfigMap")
        prometheus_config = config_map["data"]["prometheus.yml"]
        for broker_id, port in enumerate((9414, 9415, 9416), start=1):
            self.assertIn(f"ckc-external-kafka-thread-stats.ckc-perf.svc.cluster.local:{port}", prometheus_config)
            self.assertIn(f'broker_id: "{broker_id}"', prometheus_config)
        self.assertIn("ckc-external-load-test.ckc-perf.svc.cluster.local:9405", prometheus_config)


if __name__ == "__main__":
    unittest.main()
