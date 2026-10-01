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
        resolved = resolve_experiment_definition(EXAMPLE, environment=environment)
        targets = materialize_experiment(
            resolved,
            output_dir=root,
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

    def test_materializes_fixed_aws_spring_ckc_comparison_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-ckc-msk-sizing-50k.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        root = Path(self.temp.name) / "sizing"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)
        self.assertEqual(
            ["ckc.fixed-12", "spring-kafka.fixed-12"],
            [target.target.name for target in targets],
        )
        plans = [yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8")) for target in targets]
        definitions = [yaml.safe_load(target.definition_path.read_text(encoding="utf-8")) for target in targets]
        plan, spring_plan = plans
        definition = definitions[0]
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))

        self.assertEqual(50000, plan["workload"]["load"]["base_tps"])
        self.assertEqual(
            "0 -> (3m, warmup) -> 100 -> (20m, steady) -> 100",
            plan["workload"]["load"]["load_profile"],
        )
        self.assertEqual(
            {"name": "steady-state", "start_seconds": 180, "duration_seconds": 1200},
            definition["load_test"]["measurement_window"],
        )
        self.assertEqual(12, plan["application"]["configuration"]["replicas"])
        self.assertEqual(
            [12, 12, 12],
            [topic["partitions"] for topic in plan["application"]["planner"]["topics"]],
        )
        self.assertFalse(plan["application"]["configuration"]["hpa"]["enabled"])
        self.assertEqual(12, spring_plan["application"]["configuration"]["replicas"])
        self.assertEqual(
            [336, 156, 420],
            [topic["partitions"] for topic in spring_plan["application"]["planner"]["topics"]],
        )
        self.assertFalse(spring_plan["application"]["configuration"]["hpa"]["enabled"])
        self.assertEqual("kafka.m7g.large", variables["msk_broker_instance_type"])
        self.assertEqual(3, variables["msk_number_of_broker_nodes"])
        self.assertTrue(definition["load_test"]["audit_log_enabled"])
        self.assertEqual("FLEET", definition["load_test"]["telemetry_source_mode"])
        self.assertEqual(1, definition["load_test"]["telemetry_publish_interval_seconds"])
        self.assertEqual(10000, definition["load_test"]["cauldron_count"])
        self.assertEqual(
            {"order": 10000, "batch": 10000, "telemetry": 10000},
            definition["load_test"]["producer_capacity_tps"],
        )
        self.assertEqual(300, definition["load_test"]["kafka_producer_linger_ms"])
        self.assertEqual("lz4", definition["load_test"]["kafka_producer_compression_type"])
        self.assertEqual(
            {"order": 47, "batch": 33, "telemetry": 20},
            {
                "order": definition["load_test"]["order_event_percent"],
                "batch": definition["load_test"]["batch_event_percent"],
                "telemetry": definition["load_test"]["cauldron_telemetry_percent"],
            },
        )
        self.assertEqual(["m7i.xlarge"], variables["node_instance_types"])
        self.assertEqual(
            (8, 8, 8),
            (
                variables["node_desired_size"],
                variables["node_min_size"],
                variables["node_max_size"],
            ),
        )

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="sizing-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="msk:9092",
            redis_host="elasticache",
            audit_host="audit",
        ))
        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        application_resources = application["spec"]["template"]["spec"]["containers"][0]["resources"]
        application_environment = {
            item["name"]: item["value"]
            for item in application["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertEqual(12, application["spec"]["replicas"])
        self.assertEqual("500m", application_resources["requests"]["cpu"])
        self.assertNotIn("cpu", application_resources["limits"])
        self.assertEqual("65536", application_environment["KAFKA_CONSUMER_FETCH_MIN_BYTES"])
        self.assertEqual("350", application_environment["KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"])
        self.assertEqual("2000", application_environment["KAFKA_CONSUMER_MAX_POLL_RECORDS"])
        self.assertFalse(any(item["kind"] == "HorizontalPodAutoscaler" for item in manifests))

        load_job = next(item for item in manifests if item["kind"] == "Job")
        load_container = load_job["spec"]["template"]["spec"]["containers"][0]
        load_resources = load_container["resources"]
        load_environment = {item["name"]: item["value"] for item in load_container["env"]}
        self.assertEqual("1500m", load_resources["requests"]["cpu"])
        self.assertNotIn("cpu", load_resources["limits"])
        self.assertEqual(5, load_job["spec"]["completions"])
        self.assertEqual(5, load_job["spec"]["parallelism"])
        self.assertEqual("Indexed", load_job["spec"]["completionMode"])
        self.assertEqual("5", load_environment["TOTAL_SHARDS"])
        self.assertEqual("50000", load_environment["BASE_TPS"])
        self.assertEqual("10000", load_environment["ORDER_TPS_PER_PRODUCER"])
        self.assertEqual("10000", load_environment["BATCH_TPS_PER_PRODUCER"])
        self.assertEqual("10000", load_environment["CAULDRON_TELEMETRY_TPS_PER_PRODUCER"])
        self.assertEqual("300", load_environment["KAFKA_PRODUCER_LINGER_MS"])
        self.assertEqual("524288", load_environment["ORDER_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("131072", load_environment["TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE"])

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
            application_service_type="NodePort",
            application_node_port=30080,
            test_definition="smoke",
            application_node_selector={"ckc.dev/role": "application"},
            support_node_selector={"ckc.dev/role": "controller"},
            load_test_node_selector={"ckc.dev/role": "controller"},
        ))

        identities = {(item["kind"], item["metadata"]["name"]) for item in manifests}
        self.assertIn(("Deployment", "ckc-demo"), identities)
        self.assertIn(("Deployment", "ckc-demo-stubs"), identities)
        self.assertIn(("Job", "ckc-load-test-run-1"), identities)
        application = next(item for item in manifests if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo")
        service = next(item for item in manifests if item["kind"] == "Service" and item["metadata"]["name"] == "ckc-demo")
        labels = application["spec"]["template"]["metadata"]["labels"]
        self.assertEqual("run-1", labels["ckc_run_id"])
        self.assertEqual("smoke", labels["ckc_test_definition"])
        self.assertEqual("NodePort", service["spec"]["type"])
        self.assertEqual(30080, service["spec"]["ports"][0]["nodePort"])
        self.assertIn(("ConfigMap", "ckc-experiment-workload"), identities)
        application = next(item for item in manifests if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo")
        container = application["spec"]["template"]["spec"]["containers"][0]
        environment = {item["name"]: item["value"] for item in container["env"]}
        self.assertEqual("kafka.internal:9092", environment["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual("1800000", environment["KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS"])
        self.assertEqual("redis.internal", environment["SPRING_DATA_REDIS_HOST"])
        self.assertEqual("20", environment["TELEMETRY_WORKER_CONCURRENCY"])
        self.assertEqual("registry/demo@sha256:application", container["image"])
        self.assertEqual(["NET_RAW"], container["securityContext"]["capabilities"]["add"])
        self.assertEqual({"ckc.dev/role": "application"}, application["spec"]["template"]["spec"]["nodeSelector"])
        stubs = next(item for item in manifests if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo-stubs")
        load_test = next(item for item in manifests if item["kind"] == "Job")
        self.assertEqual({"ckc.dev/role": "controller"}, stubs["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual({"ckc.dev/role": "controller"}, load_test["spec"]["template"]["spec"]["nodeSelector"])


if __name__ == "__main__":
    unittest.main()
