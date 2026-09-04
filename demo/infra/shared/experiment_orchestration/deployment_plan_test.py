from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition
from .deployment_plan import DeploymentBindings, render_project_manifests
from .materialize import materialize_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]
EXAMPLE = REPO_ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"


class DeploymentPlanTest(unittest.TestCase):
    def materialize(self, environment: str) -> tuple[Path, dict, object]:
        root = Path(self.temp.name)
        resolved = resolve_experiment_definition(EXAMPLE, None, environment=environment)
        targets = materialize_experiment(
            resolved,
            output_dir=root,
            consumer_profiles_path=root / "not-used.yaml",
            repo_dir=REPO_ROOT,
        )
        plan = yaml.safe_load(targets[0].deployment_plan_path.read_text(encoding="utf-8"))
        return root, plan, targets[0]

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_materializes_aws_terraform_inputs_without_profile_indirection(self) -> None:
        root, plan, target = self.materialize("aws")
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))

        self.assertEqual("aws", plan["experiment"]["environment"])
        self.assertEqual("ckc", plan["target"]["implementation"])
        self.assertEqual(target.plan["topics"], plan["application"]["planner"]["topics"])
        self.assertNotIn("values_path", plan["application"]["planner"])
        self.assertNotIn("runPlanPath", plan["application"]["generated_values"]["lab"])
        self.assertNotIn(str(root), target.deployment_plan_path.read_text(encoding="utf-8"))
        self.assertEqual("eu-central-1", variables["aws_region"])
        self.assertEqual(["eu-central-1a", "eu-central-1b", "eu-central-1c"], variables["availability_zones"])
        self.assertEqual(["m7i.large"], variables["node_instance_types"])
        self.assertEqual(3, variables["kubernetes_kafka_brokers"])
        self.assertNotIn("profile", variables)
        self.assertEqual("32.4.3", plan["third_party"][0]["version"])

    def test_renders_project_owned_resources_from_plan_and_runtime_bindings(self) -> None:
        _, plan, _ = self.materialize("internal-lab")
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="run-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9092",
            redis_host="redis.internal",
            audit_host="audit.internal",
            packet_capture_enabled=True,
        ))

        identities = {(item["kind"], item["metadata"]["name"]) for item in manifests}
        self.assertIn(("Deployment", "ckc-demo"), identities)
        self.assertIn(("Deployment", "ckc-demo-stubs"), identities)
        self.assertIn(("Job", "ckc-load-test-run-1"), identities)
        self.assertIn(("ConfigMap", "ckc-experiment-workload"), identities)
        application = next(item for item in manifests if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo")
        container = application["spec"]["template"]["spec"]["containers"][0]
        environment = {item["name"]: item["value"] for item in container["env"]}
        self.assertEqual("kafka.internal:9092", environment["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual("redis.internal", environment["SPRING_DATA_REDIS_HOST"])
        self.assertEqual("20", environment["TELEMETRY_WORKER_CONCURRENCY"])
        self.assertEqual("registry/demo@sha256:application", container["image"])
        self.assertEqual(["NET_RAW"], container["securityContext"]["capabilities"]["add"])


if __name__ == "__main__":
    unittest.main()
