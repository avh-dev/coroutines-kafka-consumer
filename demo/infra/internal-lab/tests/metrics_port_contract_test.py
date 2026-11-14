from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[4]
COMPOSE = REPO_ROOT / "demo/infra/internal-lab/assets/compose/docker-compose.host-services.yml"
EXTERNAL_SERVICES = REPO_ROOT / "demo/infra/internal-lab/assets/k8s/external-services.yaml.tpl"
PROMETHEUS = REPO_ROOT / "demo/infra/internal-lab/assets/k8s/prometheus.yaml"
NODE_EXPORTER = REPO_ROOT / "demo/infra/internal-lab/assets/k8s/node-exporter.yaml"
DEPLOY_BASE = REPO_ROOT / "demo/infra/internal-lab/assets/libexec/deploy-base.sh"


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
        self.assertEqual({9414, 9415, 9416}, thread_stats_ports)
        self.assertNotIn("ckc-external-load-test", services)

        prometheus_documents = list(yaml.safe_load_all(PROMETHEUS.read_text(encoding="utf-8")))
        config_map = next(document for document in prometheus_documents if document.get("kind") == "ConfigMap")
        prometheus_config = config_map["data"]["prometheus.yml"]
        for broker_id, port in enumerate((9414, 9415, 9416), start=1):
            self.assertIn(f"ckc-external-kafka-thread-stats.ckc-perf.svc.cluster.local:{port}", prometheus_config)
            self.assertIn(f'broker_id: "{broker_id}"', prometheus_config)
        self.assertIn("regex: ckc-load-test", prometheus_config)
        self.assertIn('regex: "9405"', prometheus_config)

        deploy_base = DEPLOY_BASE.read_text(encoding="utf-8")
        self.assertIn("restart_prometheus", deploy_base)
        self.assertNotIn('curl -fsS -X POST "http://127.0.0.1:30090/-/reload"', deploy_base)

    def test_prometheus_has_capacity_for_long_high_churn_runs(self) -> None:
        documents = list(yaml.safe_load_all(PROMETHEUS.read_text(encoding="utf-8")))
        deployment = next(document for document in documents if document.get("kind") == "Deployment")
        prometheus = next(
            container
            for container in deployment["spec"]["template"]["spec"]["containers"]
            if container["name"] == "prometheus"
        )

        self.assertEqual("512Mi", prometheus["resources"]["requests"]["memory"])
        self.assertEqual("2Gi", prometheus["resources"]["limits"]["memory"])

    def test_node_exporter_collects_per_core_cpu_and_softnet_metrics(self) -> None:
        documents = list(yaml.safe_load_all(NODE_EXPORTER.read_text(encoding="utf-8")))
        daemon_set = next(document for document in documents if document.get("kind") == "DaemonSet")
        pod_spec = daemon_set["spec"]["template"]["spec"]
        exporter = next(container for container in pod_spec["containers"] if container["name"] == "node-exporter")

        self.assertIn("--collector.disable-defaults", exporter["args"])
        self.assertIn("--collector.cpu", exporter["args"])
        self.assertIn("--collector.softnet", exporter["args"])
        self.assertEqual("10m", exporter["resources"]["requests"]["cpu"])
        self.assertEqual("64Mi", exporter["resources"]["limits"]["memory"])
        self.assertTrue(exporter["securityContext"]["runAsNonRoot"])
        self.assertEqual(65534, exporter["securityContext"]["runAsUser"])
        self.assertEqual(65534, exporter["securityContext"]["runAsGroup"])

        prometheus_documents = list(yaml.safe_load_all(PROMETHEUS.read_text(encoding="utf-8")))
        config_map = next(document for document in prometheus_documents if document.get("kind") == "ConfigMap")
        prometheus_config = config_map["data"]["prometheus.yml"]
        self.assertIn("job_name: ckc-node-exporter", prometheus_config)
        self.assertIn("regex: ckc-node-exporter", prometheus_config)
        self.assertIn('regex: "9100"', prometheus_config)

        deploy_base = DEPLOY_BASE.read_text(encoding="utf-8")
        self.assertIn('kubectl apply -f "${K8S_DIR}/node-exporter.yaml"', deploy_base)
        self.assertIn("rollout status daemonset/ckc-node-exporter", deploy_base)


if __name__ == "__main__":
    unittest.main()
