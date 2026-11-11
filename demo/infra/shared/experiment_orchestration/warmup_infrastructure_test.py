from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from .contract import normalize_warmup_kafka, validate_canonical_experiment
from .definition import resolve_experiment_definition
from .deployment_plan import DeploymentBindings, render_project_manifests
from .materialize import materialize_experiment


ROOT = Path(__file__).resolve().parents[4]
EXAMPLE = ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"


class WarmupInfrastructureTest(unittest.TestCase):
    def test_topics_are_explicit_validated_and_isolated_from_broker_warmup(self) -> None:
        valid = {"topics": [{"name": "ckc.traffic-warmup.v1.control"}]}
        self.assertEqual(1, normalize_warmup_kafka(valid)["topics"][0]["partitions"])
        for topics in ([], [{"name": "order.events.v1"}], [{"name": "ckc.warmup.v1.control"}],
                       [{"name": "ckc.traffic-warmup.v1.control", "partitions": True}],
                       [{"name": "ckc.traffic-warmup.v1.control", "partitions": 0}],
                       valid["topics"] * 2):
            with self.subTest(topics=topics), self.assertRaises(ValueError):
                normalize_warmup_kafka({"topics": topics})
        self.assertFalse(normalize_warmup_kafka({"enabled": False})["enabled"])

    def test_broker_materializes_for_both_targets_without_demo_protocol_dependency(self) -> None:
        candidate = yaml.safe_load(EXAMPLE.read_text())
        candidate["environments"] = {"internal-lab": candidate["environments"]["internal-lab"]}
        topics = [{"name": "ckc.traffic-warmup.v1.control", "partitions": 1},
                  {"name": "ckc.traffic-warmup.v1.orders", "partitions": 2}]
        candidate["environments"]["internal-lab"]["warmup_kafka"] = {"topics": topics}
        candidate["targets"][0]["placement"] = {
            "application": "worker", "stubs": "worker", "generator": "controller",
        }
        cold = copy.deepcopy(candidate["targets"][0])
        cold["name"] = "cold"
        candidate["targets"].append(cold)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "experiment.yaml"
            source.write_text(yaml.safe_dump(candidate))
            resolved = resolve_experiment_definition(source, environment="internal-lab")
            targets = materialize_experiment(resolved, output_dir=Path(directory) / "out", repo_dir=ROOT)
            self.assertEqual(2, len(targets))
            for target in targets:
                definition = yaml.safe_load(target.definition_path.read_text())
                self.assertEqual(topics, definition["deployment"]["warmup_kafka"]["topics"])
                self.assertTrue({t["name"] for t in topics}.isdisjoint(
                    t["name"] for t in definition["deployment"]["kafka_topics"]))
                plan = yaml.safe_load(target.deployment_plan_path.read_text())
                self.assertEqual("worker", plan["placement"]["stubs"])
                manifests = render_project_manifests(plan, DeploymentBindings(
                    run_id="warmup-infra", application_image="demo:test", stubs_image="stubs:test",
                    load_test_image="load:test", kafka_bootstrap="primary:9094",
                    kafka_security_protocol="SSL", redis_host="redis", audit_host="audit",
                    application_node_selector={"kubernetes.io/hostname": "optilab2"},
                    support_node_selector={"kubernetes.io/hostname": "optilab2"},
                    load_test_node_selector={"kubernetes.io/hostname": "optilab"},
                ))
                deployments = {m["metadata"]["name"]: m for m in manifests if m["kind"] == "Deployment"}
                for name in ("ckc-demo", "ckc-demo-stubs", "ckc-warmup-kafka"):
                    self.assertEqual("optilab2", deployments[name]["spec"]["template"]["spec"]["nodeSelector"]["kubernetes.io/hostname"])
                broker = deployments["ckc-warmup-kafka"]["spec"]["template"]["spec"]
                self.assertEqual({}, broker["volumes"][0]["emptyDir"])
                env = {e["name"]: e["value"] for e in broker["containers"][0]["env"]}
                self.assertEqual("false", env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"])
                self.assertEqual("1", env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"])
                provisioner = broker["containers"][1]
                self.assertEqual(["ckc.traffic-warmup.v1.control", "1", "ckc.traffic-warmup.v1.orders", "2"], provisioner["args"][2:])
                self.assertEqual(["test", "-f", "/tmp/warmup-topics-ready"], provisioner["readinessProbe"]["exec"]["command"])
                service = next(m for m in manifests if m["kind"] == "Service" and m["metadata"]["name"] == "ckc-warmup-kafka")
                self.assertTrue(service["spec"]["publishNotReadyAddresses"])
                app = deployments["ckc-demo"]["spec"]["template"]["spec"]["containers"][0]
                app_env = {e["name"]: e["value"] for e in app["env"] if "value" in e}
                self.assertEqual("primary:9094", app_env["KAFKA_BOOTSTRAP_SERVERS"])
                self.assertEqual("ckc-warmup-kafka:9092", app_env["TRAFFIC_WARMUP_KAFKA_BOOTSTRAP_SERVERS"])
                self.assertEqual("PLAINTEXT", app_env["TRAFFIC_WARMUP_KAFKA_SECURITY_PROTOCOL"])

    def test_enabled_broker_is_rejected_for_unqualified_aws_adapter(self) -> None:
        candidate = yaml.safe_load(EXAMPLE.read_text())
        candidate["environments"]["aws"]["warmup_kafka"] = {
            "topics": [{"name": "ckc.traffic-warmup.v1.control"}],
        }
        with self.assertRaisesRegex(ValueError, "only by internal-lab"):
            validate_canonical_experiment(candidate, EXAMPLE, environment="aws")
