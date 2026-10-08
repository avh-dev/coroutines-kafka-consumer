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

    def test_materializes_fixed_aws_ckc_baseline_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-ckc-msk-sizing-50k.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        root = Path(self.temp.name) / "sizing"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)
        self.assertEqual(["ckc.fixed-12"], [target.target.name for target in targets])
        plans = [yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8")) for target in targets]
        definitions = [yaml.safe_load(target.definition_path.read_text(encoding="utf-8")) for target in targets]
        plan = plans[0]
        definition = definitions[0]
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))

        self.assertEqual(50000, plan["workload"]["load"]["base_tps"])
        self.assertEqual(
            "0 -> (3m, warmup) -> 100 -> (20m, steady) -> 100",
            plan["workload"]["load"]["load_profile"],
        )
        self.assertEqual(
            [
                {"name": "steady-state", "start_seconds": 180, "duration_seconds": 1200},
                {"name": "steady-early", "start_seconds": 180, "duration_seconds": 300},
                {"name": "steady-middle", "start_seconds": 630, "duration_seconds": 300},
                {"name": "steady-late", "start_seconds": 1080, "duration_seconds": 300},
            ],
            definition["load_test"]["measurement_windows"],
        )
        self.assertEqual(12, plan["application"]["configuration"]["replicas"])
        self.assertEqual(
            [12, 12, 12],
            [topic["partitions"] for topic in plan["application"]["planner"]["topics"]],
        )
        self.assertEqual(
            [100, 100, 100],
            [topic["worker_concurrency"] for topic in plan["application"]["planner"]["topics"]],
        )
        self.assertEqual(
            [1, 1, 1],
            [topic["poll_loop_concurrency"] for topic in plan["application"]["planner"]["topics"]],
        )
        self.assertFalse(plan["application"]["configuration"]["hpa"]["enabled"])
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
            {"order": 40, "batch": 40, "telemetry": 20},
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
        plan["application"]["runtime"]["env"]["KAFKA_BOOTSTRAP_SERVERS"] = "runtime-override:9092"

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
        self.assertEqual("runtime-override:9092", application_environment["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertFalse(any(item["kind"] == "HorizontalPodAutoscaler" for item in manifests))

        load_job = next(item for item in manifests if item["kind"] == "Job")
        load_container = load_job["spec"]["template"]["spec"]["containers"][0]
        load_resources = load_container["resources"]
        load_environment = {item["name"]: item["value"] for item in load_container["env"]}
        self.assertEqual("1", load_resources["requests"]["cpu"])
        self.assertNotIn("cpu", load_resources["limits"])
        self.assertEqual(2, load_job["spec"]["completions"])
        self.assertEqual(2, load_job["spec"]["parallelism"])
        self.assertEqual("Indexed", load_job["spec"]["completionMode"])
        self.assertEqual("2", load_environment["TOTAL_SHARDS"])
        self.assertEqual("50000", load_environment["BASE_TPS"])
        self.assertEqual("10000", load_environment["ORDER_TPS_PER_PRODUCER"])
        self.assertEqual("10000", load_environment["BATCH_TPS_PER_PRODUCER"])
        self.assertEqual("10000", load_environment["CAULDRON_TELEMETRY_TPS_PER_PRODUCER"])
        self.assertEqual("300", load_environment["KAFKA_PRODUCER_LINGER_MS"])
        self.assertEqual("32768", load_environment["ORDER_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("32768", load_environment["TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE"])

    def test_materializes_dedicated_autoscaling_aws_ckc_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-ckc-hpa-50k-ramp.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        root = Path(self.temp.name) / "autoscaling"
        target = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)[0]
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
        definition = yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))

        self.assertTrue(variables["dedicated_node_groups"])
        self.assertEqual(["m7i.xlarge"], variables["support_node_instance_types"])
        self.assertEqual((2, 2, 2), (
            variables["support_node_desired_size"],
            variables["support_node_min_size"],
            variables["support_node_max_size"],
        ))
        self.assertEqual(["m7i.large"], variables["application_node_instance_types"])
        self.assertEqual(100, variables["msk_ebs_volume_size"])
        self.assertEqual((2, 2, 8), (
            variables["application_node_desired_size"],
            variables["application_node_min_size"],
            variables["application_node_max_size"],
        ))
        self.assertEqual(
            "0 -> (30m, ramp) -> 100 -> (20m, steady) -> 100 -> (10m, cool-down) -> 0",
            definition["load_test"]["load_profile"],
        )
        self.assertEqual([12, 12, 12], [topic["partitions"] for topic in plan["application"]["planner"]["topics"]])
        self.assertEqual("800m", plan["application"]["configuration"]["resources"]["requests"]["cpu"])
        self.assertEqual({
            "enabled": True,
            "min_replicas": 2,
            "max_replicas": 12,
            "target_cpu_utilization_percentage": 70,
            "scale_down_stabilization_window_seconds": 300,
        }, plan["application"]["configuration"]["hpa"])

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="autoscaling-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="msk:9092",
            redis_host="elasticache",
            audit_host="audit",
            application_node_selector={"ckc.dev/role": "application"},
            application_tolerations=({
                "key": "dedicated",
                "operator": "Equal",
                "value": "application",
                "effect": "NoSchedule",
            },),
            support_node_selector={"ckc.dev/role": "support"},
            load_test_node_selector={"ckc.dev/role": "support"},
        ))
        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        stubs = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo-stubs"
        )
        load_job = next(item for item in manifests if item["kind"] == "Job")
        hpa = next(item for item in manifests if item["kind"] == "HorizontalPodAutoscaler")

        self.assertEqual({"ckc.dev/role": "application"}, application["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual("application", application["spec"]["template"]["spec"]["tolerations"][0]["value"])
        self.assertEqual("800m", application["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"])
        self.assertNotIn("cpu", application["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"])
        self.assertEqual({"ckc.dev/role": "support"}, stubs["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual({"ckc.dev/role": "support"}, load_job["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual(2, hpa["spec"]["minReplicas"])
        self.assertEqual(12, hpa["spec"]["maxReplicas"])
        self.assertEqual(70, hpa["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"])

    def test_materializes_high_partition_internal_generator_heap(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/internal-generator-noop-50k.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "internal-generator"
        target = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)[0]
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="internal-generator-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9092",
            redis_host="redis.internal",
            audit_host="audit.internal",
        ))
        load_container = next(item for item in manifests if item["kind"] == "Job")["spec"]["template"]["spec"]["containers"][0]
        load_environment = {item["name"]: item["value"] for item in load_container["env"]}
        application_container = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )["spec"]["template"]["spec"]["containers"][0]
        application_environment = {item["name"]: item["value"] for item in application_container["env"]}

        self.assertEqual("-Xms256m -Xmx768m -XX:+UseG1GC", load_environment["JAVA_TOOL_OPTIONS"])
        self.assertEqual("1280Mi", load_container["resources"]["limits"]["memory"])
        self.assertEqual("32768", load_environment["ORDER_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("32768", load_environment["BATCH_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("32768", load_environment["TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("false", application_environment["DEMO_CONSUMER_PROCESSING_ENABLED"])
        self.assertNotIn("PROCESSING_ENABLED", application_environment)
        self.assertNotIn("PROCESSING_ENABLED", plan["application"]["runtime"]["env"])

    def test_materializes_independent_aws_spring_sizing_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-spring-msk-sizing-50k.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        ckc_resolved = resolve_experiment_definition(
            REPO_ROOT / "demo/infra/experiments/aws-ckc-msk-sizing-50k.yaml",
            environment="aws",
        )
        root = Path(self.temp.name) / "spring-sizing"
        ckc_root = Path(self.temp.name) / "ckc-sizing-comparison"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)
        materialize_experiment(ckc_resolved, output_dir=ckc_root, repo_dir=REPO_ROOT)

        self.assertEqual(["spring-kafka.fixed-12"], [target.target.name for target in targets])
        plan = yaml.safe_load(targets[0].deployment_plan_path.read_text(encoding="utf-8"))
        definition = yaml.safe_load(targets[0].definition_path.read_text(encoding="utf-8"))
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))
        ckc_variables = json.loads(
            (ckc_root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8")
        )

        self.assertEqual("aws-spring-msk-sizing-50k", plan["experiment"]["name"])
        self.assertEqual("kafka.m7g.xlarge", variables["msk_broker_instance_type"])
        self.assertEqual("kafka.m7g.large", ckc_variables["msk_broker_instance_type"])
        expected_differences = {"experiment_id", "msk_broker_instance_type"}
        self.assertEqual(
            {key: value for key, value in ckc_variables.items() if key not in expected_differences},
            {key: value for key, value in variables.items() if key not in expected_differences},
        )
        self.assertEqual(3, variables["msk_number_of_broker_nodes"])
        self.assertEqual(50000, plan["workload"]["load"]["base_tps"])
        self.assertEqual(2, definition["load_test"]["shards"])
        self.assertEqual(2, definition["load_test"]["dispatcher_threads"])
        self.assertEqual(10_000, definition["load_test"]["cauldron_count"])
        self.assertEqual(
            {"order": 40, "batch": 40, "telemetry": 20},
            {
                "order": definition["load_test"]["order_event_percent"],
                "batch": definition["load_test"]["batch_event_percent"],
                "telemetry": definition["load_test"]["cauldron_telemetry_percent"],
            },
        )
        self.assertEqual(12, plan["application"]["configuration"]["replicas"])
        self.assertEqual(
            ["steady-state", "steady-early", "steady-middle", "steady-late"],
            [window["name"] for window in definition["load_test"]["measurement_windows"]],
        )
        topics = plan["application"]["planner"]["topics"]
        self.assertEqual(
            [216, 192, 276],
            [topic["partitions"] for topic in topics],
        )
        self.assertEqual([18, 16, 23], [topic["poll_loop_concurrency"] for topic in topics])
        self.assertEqual([160, 140, 210], [topic["required_parallelism_without_headroom"] for topic in topics])
        self.assertEqual([208, 182, 273], [topic["required_parallelism"] for topic in topics])
        self.assertEqual([30.0, 30.0, 30.0], [topic["planning_headroom_percent"] for topic in topics])
        self.assertTrue(all(not topic["manual_overrides"] for topic in topics))
        self.assertFalse(plan["application"]["configuration"]["hpa"]["enabled"])

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="spring-sizing-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="msk:9092",
            redis_host="elasticache",
            audit_host="audit",
        ))
        load_job = next(item for item in manifests if item["kind"] == "Job")
        load_container = load_job["spec"]["template"]["spec"]["containers"][0]
        load_environment = {
            item["name"]: item["value"]
            for item in load_container["env"]
        }
        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        application_environment = {
            item["name"]: item["value"]
            for item in application["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        stubs = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo-stubs"
        )
        stubs_container = stubs["spec"]["template"]["spec"]["containers"][0]
        stubs_environment = {item["name"]: item["value"] for item in stubs_container["env"]}
        stubs_service = next(
            item for item in manifests
            if item["kind"] == "Service" and item["metadata"]["name"] == "ckc-demo-stubs"
        )
        self.assertEqual(2, load_job["spec"]["parallelism"])
        self.assertEqual("2", load_environment["TOTAL_SHARDS"])
        self.assertEqual("50000", load_environment["BASE_TPS"])
        self.assertEqual("2", load_environment["LOAD_TEST_DISPATCHER_THREADS"])
        self.assertEqual("32768", load_environment["ORDER_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("32768", load_environment["BATCH_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("32768", load_environment["TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE"])
        self.assertEqual("-Xms256m -Xmx768m -XX:+UseG1GC", load_environment["JAVA_TOOL_OPTIONS"])
        self.assertEqual({"cpu": "1", "memory": "1Gi"}, load_container["resources"]["requests"])
        self.assertEqual({"memory": "1280Mi"}, load_container["resources"]["limits"])
        self.assertEqual("true", application_environment["DEMO_CONSUMER_PROCESSING_ENABLED"])
        self.assertEqual("true", application_environment["AUDIT_LOG_ENABLED"])
        self.assertEqual("18", application_environment["ORDER_POLL_LOOP_CONCURRENCY"])
        self.assertEqual("16", application_environment["BATCH_POLL_LOOP_CONCURRENCY"])
        self.assertEqual("23", application_environment["TELEMETRY_POLL_LOOP_CONCURRENCY"])
        self.assertEqual(4, stubs["spec"]["replicas"])
        self.assertEqual("4", stubs_environment["STUB_WORKERS"])
        self.assertEqual({"cpu": "1", "memory": "1Gi"}, stubs_container["resources"]["requests"])
        self.assertEqual({"memory": "1536Mi"}, stubs_container["resources"]["limits"])
        self.assertEqual(
            {"app.kubernetes.io/name": "ckc-demo-stubs"},
            stubs_service["spec"]["selector"],
        )

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
            load_test_environment={"LOAD_TEST_DISPATCHER_THREADS": "2"},
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
        load_environment = {
            item["name"]: item["value"]
            for item in load_test["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertEqual("2", load_environment["LOAD_TEST_DISPATCHER_THREADS"])
        self.assertEqual(plan["target"]["implementation"], load_test["spec"]["template"]["metadata"]["labels"]["ckc.dev/profile"])
        load_pod_spec = load_test["spec"]["template"]["spec"]
        load_container = load_pod_spec["containers"][0]
        self.assertEqual(["NET_RAW"], load_container["securityContext"]["capabilities"]["add"])
        self.assertEqual(
            [{"name": "packet-captures", "mountPath": "/captures"}],
            load_container["volumeMounts"],
        )
        self.assertEqual(
            [{"name": "packet-captures", "emptyDir": {"sizeLimit": "256Mi"}}],
            load_pod_spec["volumes"],
        )

    def test_renders_independent_topic_deployments_and_native_kafka_lag_scalers(self) -> None:
        _, plan, _ = self.materialize("internal-lab")
        plan["application"]["configuration"] = {
            "deployment_mode": "per_topic",
            "resources": {"requests": {"cpu": "1", "memory": "1Gi"}},
            "workloads": {
                name: {
                    "replicas": 1,
                    "group_id": f"spring-{name}",
                    "hpa": {
                        "enabled": True,
                        "min_replicas": 1,
                        "max_replicas": 6,
                        "target_cpu_utilization_percentage": 80,
                        "kafka_lag": {
                            "enabled": True,
                            "lag_threshold": 300,
                        },
                    },
                }
                for name in ("order", "batch", "telemetry")
            },
        }

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="spring-scaling-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9092",
            redis_host="redis.internal",
            audit_host="audit.internal",
            application_service_type="NodePort",
            application_node_port=30080,
        ))

        deployments = {
            item["metadata"]["name"]: item
            for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"].startswith("ckc-demo-")
            and item["metadata"]["name"] != "ckc-demo-stubs"
        }
        self.assertEqual(
            {"ckc-demo-order", "ckc-demo-batch", "ckc-demo-telemetry"},
            set(deployments),
        )
        for workload, deployment in ((name, deployments[f"ckc-demo-{name}"]) for name in ("order", "batch", "telemetry")):
            environment = {
                item["name"]: item["value"]
                for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
            }
            self.assertEqual("true", environment[f"{workload.upper()}_CONSUMER_ENABLED"])
            self.assertEqual(f"spring-{workload}", environment[f"{workload.upper()}_CONSUMER_GROUP_ID"])
            self.assertEqual(workload, deployment["metadata"]["labels"]["ckc.dev/workload"])
        scalers = [item for item in manifests if item["kind"] == "ScaledObject"]
        self.assertEqual(3, len(scalers))
        order_scaler = next(item for item in scalers if item["metadata"]["name"] == "ckc-demo-order")
        self.assertEqual(6, order_scaler["spec"]["maxReplicaCount"])
        lag_trigger = order_scaler["spec"]["triggers"][1]
        self.assertEqual("kafka", lag_trigger["type"])
        self.assertEqual("AverageValue", lag_trigger["metricType"])
        self.assertEqual({
            "bootstrapServers": "kafka.internal:9092",
            "consumerGroup": "spring-order",
            "topic": "order.events.v1",
            "lagThreshold": "300",
            "activationLagThreshold": "0",
            "offsetResetPolicy": "latest",
            "allowIdleConsumers": "false",
            "fullMetadata": "false",
        }, lag_trigger["metadata"])
        alias = next(
            item for item in manifests if item["kind"] == "Service" and item["metadata"]["name"] == "ckc-demo"
        )
        self.assertEqual("ckc-demo-order", alias["spec"]["selector"]["app.kubernetes.io/name"])
        self.assertEqual(30080, alias["spec"]["ports"][0]["nodePort"])

    def test_materializes_local_spring_topic_autoscaling_qualification(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/spring-topic-autoscaling-5k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "spring-topic-autoscaling"

        target = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)[0]
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
        definition = yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))

        self.assertEqual("per_topic", plan["application"]["configuration"]["deployment_mode"])
        self.assertEqual(
            "0 -> (20m, ramp-to-3k) -> 60 -> (10m, steady-3k) -> 60 -> "
            "(5m, order-saturation) -> 60 -> (6m, order-recovery) -> 60 -> "
            "(20m, ramp-to-5k) -> 100 -> (10m, steady-5k) -> 100",
            plan["workload"]["load"]["load_profile"],
        )
        self.assertEqual(
            [{
                "at": "30m",
                "duration": "5m",
                "type": "stubs_degradation",
                "name": "order-downstream-saturation",
                "params": {"flavour": {"percentiles": {
                    "p90": 60, "p95": 500, "p99": 1000, "p100": 2000,
                }}},
            }],
            definition["chaos_steps"],
        )
        workloads = plan["application"]["configuration"]["workloads"]
        self.assertEqual(
            {"order": 400, "batch": 440, "telemetry": 440},
            {name: workload["hpa"]["kafka_lag"]["lag_threshold"] for name, workload in workloads.items()},
        )
        self.assertEqual(
            {"order": 5, "batch": 5, "telemetry": 5},
            {name: workload["hpa"]["max_replicas"] for name, workload in workloads.items()},
        )
        self.assertEqual(
            [(21, 4, 5), (21, 4, 5), (60, 12, 5)],
            [
                (topic["partitions"], topic["poll_loop_concurrency"], topic["capacity_replicas"])
                for topic in plan["application"]["planner"]["topics"]
            ],
        )


if __name__ == "__main__":
    unittest.main()
