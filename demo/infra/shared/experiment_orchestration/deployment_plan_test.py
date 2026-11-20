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

    def test_tls_bindings_pass_ca_to_application_and_generator(self) -> None:
        _, plan, _ = self.materialize("internal-lab")
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="tls-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka:9092",
            kafka_security_protocol="SSL",
            kafka_ssl_truststore_certificates="-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n",
            kafka_ssl_endpoint_identification_algorithm="",
            redis_host="redis",
            audit_host="audit",
        ))
        application = next(item for item in manifests if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo")
        load_job = next(item for item in manifests if item["kind"] == "Job")
        application_env = {item["name"]: item["value"] for item in application["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in item}
        load_env = {item["name"]: item["value"] for item in load_job["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in item}
        self.assertEqual("SSL", application_env["DEMO_KAFKA_SECURITY_PROTOCOL"])
        self.assertIn("BEGIN CERTIFICATE", application_env["DEMO_KAFKA_SSL_TRUSTSTORE_CERTIFICATES"])
        self.assertEqual("", application_env["DEMO_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"])
        self.assertEqual("SSL", load_env["KAFKA_SECURITY_PROTOCOL"])
        self.assertIn("BEGIN CERTIFICATE", load_env["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"])
        self.assertEqual("", load_env["KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"])

    def test_renders_one_autonomous_warmup_kafka_container_and_independent_connections(self) -> None:
        _, plan, _ = self.materialize("internal-lab")
        topics = [
            "ckc.traffic-warmup.v6.control",
            "ckc.traffic-warmup.v6.order.events",
        ]
        plan["environment"]["warmup_kafka"] = {
            "enabled": True,
            "placement": "application",
            "heap": "512Mi",
            "resources": {
                "requests": {"cpu": "500m", "memory": "768Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
            "topics": topics,
            "retention_ms": 600_000,
            "bootstrap_timeout_seconds": 120,
            "bootstrap_backoff_seconds": 2,
            "operation_timeout_seconds": 10,
        }
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="warmup-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            warmup_kafka_image="ckc-perf/warmup-kafka:test",
            kafka_bootstrap="main-kafka:9092",
            redis_host="redis",
            audit_host="audit",
        ))

        deployment = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-warmup-kafka"
        )
        service = next(
            item for item in manifests
            if item["kind"] == "Service" and item["metadata"]["name"] == "ckc-warmup-kafka"
        )
        containers = deployment["spec"]["template"]["spec"]["containers"]
        self.assertEqual(1, len(containers))
        self.assertEqual("ckc-perf/warmup-kafka:test", containers[0]["image"])
        environment = {item["name"]: item["value"] for item in containers[0]["env"]}
        self.assertEqual(",".join(topics), environment["KAFKA_BOOTSTRAP_TOPICS"])
        self.assertEqual("false", environment["KAFKA_AUTO_CREATE_TOPICS_ENABLE"])
        self.assertEqual(
            "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093",
            environment["KAFKA_LISTENERS"],
        )
        self.assertEqual(
            "INTERNAL://localhost:19092,EXTERNAL://ckc-warmup-kafka:9092",
            environment["KAFKA_ADVERTISED_LISTENERS"],
        )
        self.assertEqual("INTERNAL", environment["KAFKA_INTER_BROKER_LISTENER_NAME"])
        self.assertEqual("120", environment["KAFKA_BOOTSTRAP_TIMEOUT_SECONDS"])
        self.assertEqual("2", environment["KAFKA_BOOTSTRAP_BACKOFF_SECONDS"])
        self.assertEqual("180", environment["KAFKA_PREWARM_TIMEOUT_SECONDS"])
        self.assertNotIn("KAFKA_RECONCILE_INTERVAL_SECONDS", environment)
        self.assertNotIn("KAFKA_READY_MAX_AGE_SECONDS", environment)
        self.assertEqual(
            ["/opt/ckc-warmup-kafka/bin/readiness.sh"],
            containers[0]["readinessProbe"]["exec"]["command"],
        )
        self.assertNotIn("publishNotReadyAddresses", service["spec"])

        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        application_env = {
            item["name"]: item["value"]
            for item in application["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertEqual("main-kafka:9092", application_env["DEMO_KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual(
            "ckc-warmup-kafka:9092",
            application_env["TRAFFIC_WARMUP_CONTROL_KAFKA_BOOTSTRAP_SERVERS"],
        )
        self.assertEqual(
            "ckc-warmup-kafka:9092",
            application_env["TRAFFIC_WARMUP_DESTINATION_KAFKA_BOOTSTRAP_SERVERS"],
        )

    def test_materializes_internal_lab_warmup_transport_qualification(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-shared-partition-warmup-transport-qualification.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "warmup-qualification"
        target = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)[0]
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
        definition = yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))

        self.assertIn("warmup-kafka", plan["project_resources"])
        self.assertEqual(
            [
                "ckc.traffic-warmup.v6.control",
                "ckc.traffic-warmup.v6.order.events",
                "ckc.traffic-warmup.v6.batch.events",
                "ckc.traffic-warmup.v6.cauldron.events",
            ],
            plan["environment"]["warmup_kafka"]["topics"],
        )
        self.assertEqual(
            plan["environment"]["warmup_kafka"],
            definition["deployment"]["warmup_kafka"],
        )
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="qualification",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="main-kafka:9092",
            redis_host="redis",
            audit_host="audit",
        ))
        self.assertEqual(
            ["Deployment", "Service"],
            [
                item["kind"] for item in manifests
                if item["metadata"]["name"] == "ckc-warmup-kafka"
            ],
        )

    def test_materializes_shared_partition_fourth_pod_warmup_target(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-shared-partition-warmup-scale-up-14k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "warmup-ab"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)

        self.assertEqual(["ckc.shared-partition-warmup-1-to-100-180s"], [target.target.name for target in targets])
        plan, definition, environment = self._materialized_application(targets[0])
        self._assert_fourth_pod_scale_up(plan)
        self.assertEqual("true", environment["TRAFFIC_WARMUP_ENABLED"])
        for topic in ("ORDER", "BATCH", "CAULDRON"):
            self.assertEqual(
                "1 -> (180s, ramp) -> 100",
                environment[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
        self.assertEqual("180", environment["TRAFFIC_WARMUP_DURATION_SECONDS"])
        self.assertEqual("14m", definition["diagnostic_steps"][0]["at"])
        self.assertEqual("5m", definition["diagnostic_steps"][0]["duration"])

    def test_materializes_staged_and_linear_warmup_profile_comparison(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-staged-vs-linear-warmup-300-scale-up-14k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "staged-vs-linear-warmup"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)

        self.assertEqual(
            [
                "ckc.staged-step-warmup-1-to-300-120s",
                "ckc.linear-warmup-1-to-300-120s",
                "ckc.linear-control-warmup-1-to-100-180s",
            ],
            [target.target.name for target in targets],
        )
        materialized = [self._materialized_application(target) for target in targets]
        for plan, definition, _ in materialized:
            self._assert_fourth_pod_scale_up(plan)
            self.assertNotIn("diagnostic_steps", definition)
            self.assertEqual(
                [
                    {"name": "fourth-pod-transition", "start_seconds": 840, "duration_seconds": 360},
                    {"name": "settled-four-pod-plateau", "start_seconds": 1_260, "duration_seconds": 60},
                ],
                definition["load_test"]["measurement_windows"],
            )

        staged = materialized[0][2]
        linear = materialized[1][2]
        linear_control = materialized[2][2]
        self.assertEqual("true", staged["TRAFFIC_WARMUP_ENABLED"])
        self.assertEqual("true", linear["TRAFFIC_WARMUP_ENABLED"])
        self.assertEqual("true", linear_control["TRAFFIC_WARMUP_ENABLED"])
        for topic in ("ORDER", "BATCH", "CAULDRON"):
            self.assertEqual(
                "1 -> (30s, gentle start) -> 5 -> (0s, step) -> 50 -> (30s, hold) -> 50 -> "
                "(0s, step) -> 300 -> (60s, hold) -> 300",
                staged[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
            self.assertEqual(
                "1 -> (120s, direct linear) -> 300",
                linear[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
            self.assertEqual(
                "1 -> (180s, direct linear control) -> 100",
                linear_control[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
        for environment in (staged, linear):
            self.assertEqual("120", environment["TRAFFIC_WARMUP_DURATION_SECONDS"])
        self.assertEqual("180", linear_control["TRAFFIC_WARMUP_DURATION_SECONDS"])
        for environment in (staged, linear, linear_control):
            self.assertEqual("15", environment["TRAFFIC_WARMUP_PRODUCER_LINGER_MS"])
            self.assertEqual("32768", environment["TRAFFIC_WARMUP_PRODUCER_BATCH_SIZE"])

    def test_materializes_linear_warmup_reference_after_parallel_result_delivery(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-linear-warmup-reference-scale-up-14k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "linear-warmup-reference"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)

        self.assertEqual(
            ["ckc.linear-warmup-reference-1-to-100-180s"],
            [target.target.name for target in targets],
        )
        plan, definition, environment = self._materialized_application(targets[0])
        self._assert_fourth_pod_scale_up(plan)
        self.assertNotIn("diagnostic_steps", definition)
        self.assertEqual(
            [
                {"name": "fourth-pod-transition", "start_seconds": 840, "duration_seconds": 360},
                {"name": "settled-four-pod-plateau", "start_seconds": 1_260, "duration_seconds": 60},
            ],
            definition["load_test"]["measurement_windows"],
        )
        self.assertEqual("true", environment["TRAFFIC_WARMUP_ENABLED"])
        for topic in ("ORDER", "BATCH", "CAULDRON"):
            self.assertEqual(
                "1 -> (180s, linear reference) -> 100",
                environment[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
        self.assertEqual("180", environment["TRAFFIC_WARMUP_DURATION_SECONDS"])
        self.assertEqual("15", environment["TRAFFIC_WARMUP_PRODUCER_LINGER_MS"])
        self.assertEqual("32768", environment["TRAFFIC_WARMUP_PRODUCER_BATCH_SIZE"])

    def test_materializes_sequential_warmup_comparison_at_20k(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-sequential-warmup-scale-up-20k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "sequential-warmup-20k"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)

        self.assertEqual(
            [
                "ckc.cold-start-controller-stubs",
                "ckc.two-stage-linear-controller-stubs-1-to-100-to-300-150s",
            ],
            [target.target.name for target in targets],
        )
        cold_plan, cold_definition, cold_environment = self._materialized_application(targets[0])
        self._assert_controller_stubs_target(cold_plan, cold_definition)
        self.assertEqual("false", cold_environment["TRAFFIC_WARMUP_ENABLED"])
        self.assertEqual("5m", cold_environment["DEMO_REDIS_STATE_RETENTION"])
        for topic in ("ORDER", "BATCH", "CAULDRON"):
            self.assertNotIn(f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE", cold_environment)

        plan, definition, environment = self._materialized_application(targets[1])
        self._assert_controller_stubs_target(plan, definition)
        self.assertEqual("true", environment["TRAFFIC_WARMUP_ENABLED"])
        self.assertEqual("5m", environment["DEMO_REDIS_STATE_RETENTION"])
        for topic in ("ORDER", "BATCH", "CAULDRON"):
            self.assertEqual(
                "1 -> (60s, first linear) -> 100 -> (90s, second linear) -> 300",
                environment[f"TRAFFIC_WARMUP_{topic}_QUOTA_PROFILE"],
            )
        self.assertEqual("150", environment["TRAFFIC_WARMUP_DURATION_SECONDS"])
        self.assertEqual("15", environment["TRAFFIC_WARMUP_PRODUCER_LINGER_MS"])
        self.assertEqual("32768", environment["TRAFFIC_WARMUP_PRODUCER_BATCH_SIZE"])

    def _assert_controller_stubs_target(self, plan, definition) -> None:
        self.assertEqual(20_000, plan["workload"]["load"]["base_tps"])
        self.assertEqual(
            "0 -> (25m, ramp-to-20k) -> 100 -> (10m, hold-20k) -> 100 -> (10m, cool-down) -> 0",
            plan["workload"]["load"]["load_profile"],
        )
        application = plan["application"]["configuration"]
        self.assertEqual(1, application["replicas"])
        self.assertEqual("950m", application["resources"]["requests"]["cpu"])
        self.assertEqual(1, application["hpa"]["min_replicas"])
        self.assertEqual(4, application["hpa"]["max_replicas"])
        self.assertEqual(
            {"max_pods": 1, "period_seconds": 180, "tolerance": 0.1},
            application["hpa"]["hot_pod_cpu"]["scale_up"],
        )
        self.assertIn("jit+compilation=debug", application["java_options"])
        self.assertEqual(
            {"application": "worker", "stubs": "controller", "generator": "controller"},
            plan["placement"],
        )
        self.assertNotIn("diagnostic_steps", definition)
        self.assertEqual(
            [
                {"name": "sequential-scale-up", "start_seconds": 0, "duration_seconds": 1_500},
                {"name": "peak-plateau", "start_seconds": 1_500, "duration_seconds": 600},
                {"name": "cool-down", "start_seconds": 2_100, "duration_seconds": 600},
            ],
            definition["load_test"]["measurement_windows"],
        )

    def _materialized_application(self, target):
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
        definition = yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id=target.target.id,
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="main-kafka:9092",
            redis_host="redis",
            audit_host="audit",
            autoscaling_metrics_url="http://ckc-prometheus.ckc-perf.svc.cluster.local:9090",
        ))
        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        environment = {
            item["name"]: item["value"]
            for item in application["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        return plan, definition, environment

    def _assert_fourth_pod_scale_up(self, plan, expected_cpu_request: str = "775m") -> None:
        self.assertEqual(14_000, plan["workload"]["load"]["base_tps"])
        self.assertEqual(
            "0 -> (3m, ramp-to-12k) -> 86 -> (5m, stabilize-at-12k) -> 86 -> "
            "(10m, ramp-to-14k) -> 100 -> (4m, hold-14k) -> 100 -> (1m, cool-down) -> 0",
            plan["workload"]["load"]["load_profile"],
        )
        application = plan["application"]["configuration"]
        self.assertEqual(3, application["replicas"])
        self.assertEqual(expected_cpu_request, application["resources"]["requests"]["cpu"])
        self.assertEqual(3, application["hpa"]["min_replicas"])
        self.assertEqual(4, application["hpa"]["max_replicas"])
        self.assertEqual(
            {"max_pods": 1, "period_seconds": 180, "tolerance": 0.1},
            application["hpa"]["hot_pod_cpu"]["scale_up"],
        )
        self.assertIn("jit+compilation=debug", application["java_options"])

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
            if "value" in item
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
        load_environment = {item["name"]: item["value"] for item in load_container["env"] if "value" in item}
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
        self.assertEqual((1, 1, 8), (
            variables["application_node_desired_size"],
            variables["application_node_min_size"],
            variables["application_node_max_size"],
        ))
        self.assertEqual("kafka.t3.small", variables["msk_broker_instance_type"])
        self.assertEqual(
            "0 -> (20m, ramp) -> 100 -> (20m, steady) -> 100 -> (10m, cool-down) -> 0",
            definition["load_test"]["load_profile"],
        )
        self.assertEqual([12, 12, 12], [topic["partitions"] for topic in plan["application"]["planner"]["topics"]])
        self.assertEqual("850m", plan["application"]["configuration"]["resources"]["requests"]["cpu"])
        self.assertEqual({
            "enabled": True,
            "min_replicas": 1,
            "max_replicas": 12,
            "target_cpu_utilization_percentage": 75,
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
        self.assertEqual("850m", application["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"])
        self.assertNotIn("cpu", application["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"])
        self.assertEqual({"ckc.dev/role": "support"}, stubs["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual({"ckc.dev/role": "support"}, load_job["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual(1, hpa["spec"]["minReplicas"])
        self.assertEqual(12, hpa["spec"]["maxReplicas"])
        self.assertEqual(75, hpa["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"])

    def test_hourly_qualification_guards_ckc_against_a_sustained_hot_pod(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-hourly-50k-tls-qualification.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        root = Path(self.temp.name) / "hourly-qualification"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)
        ckc_target = next(target for target in targets if target.target.name == "ckc.hpa-1-12")
        plan = yaml.safe_load(ckc_target.deployment_plan_path.read_text(encoding="utf-8"))

        self.assertEqual(
            [500, 500, 500],
            [topic["worker_concurrency"] for topic in plan["application"]["planner"]["topics"]],
        )
        self.assertEqual(85, plan["application"]["configuration"]["hpa"]["target_cpu_utilization_percentage"])
        self.assertEqual(12, plan["application"]["configuration"]["hpa"]["max_replicas"])

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="hourly-ckc",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="msk:9094",
            kafka_security_protocol="SSL",
            redis_host="elasticache",
            audit_host="audit",
            autoscaling_metrics_url="http://10.0.0.10:8428",
        ))
        scaler = next(item for item in manifests if item["kind"] == "ScaledObject")
        self.assertEqual(1, scaler["spec"]["minReplicaCount"])
        self.assertEqual(12, scaler["spec"]["maxReplicaCount"])
        self.assertEqual("0", scaler["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]["scaleUp"]["tolerance"])
        cpu, hot_pod = scaler["spec"]["triggers"]
        self.assertEqual(("cpu", "85"), (cpu["type"], cpu["metadata"]["value"]))
        self.assertEqual(("prometheus", "Value"), (hot_pod["type"], hot_pod["metricType"]))
        self.assertEqual("100", hot_pod["metadata"]["threshold"])
        self.assertEqual("http://10.0.0.10:8428", hot_pod["metadata"]["serverAddress"])
        self.assertIn("[30s]", hot_pod["metadata"]["query"])
        self.assertIn("[2m:15s]", hot_pod["metadata"]["query"])
        self.assertIn("/ 0.85", hot_pod["metadata"]["query"])

    def test_thread_stats_replica_demand_excludes_jit_and_uses_requested_cpu(self) -> None:
        plan = {
            "target": {"name": "thread-stats-hot-pod", "implementation": "ckc"},
            "application": {
                "configuration": {
                    "replicas": 1,
                    "resources": {"requests": {"cpu": "400m", "memory": "512Mi"}},
                    "hpa": {
                        "enabled": True,
                        "min_replicas": 1,
                        "max_replicas": 12,
                        "hot_pod_cpu": {
                            "source": "thread_stats_excluding_jit",
                            "strategy": "replica_demand",
                            "replace_cpu_trigger": True,
                            "target_utilization_percentage": 75,
                            "averaging_window": "30s",
                            "observation_window": "1m",
                            "query_step": "15s",
                            "aggregation": "average",
                            "scale_up": {"max_pods": 2, "period_seconds": 60},
                        },
                    },
                },
                "runtime": {"env": {}},
                "planner": {"topics": []},
                "generated_values": {},
            },
            "workload": {"load": {}},
        }
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="thread-stats-hot-pod",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka:9092",
            redis_host="redis",
            audit_host="audit",
            application_namespace="ckc-perf",
            autoscaling_metrics_url="http://ckc-prometheus.ckc-perf.svc.cluster.local:9090",
        ))
        scaler = next(item for item in manifests if item["kind"] == "ScaledObject")
        self.assertEqual(1, len(scaler["spec"]["triggers"]))
        trigger = scaler["spec"]["triggers"][0]
        self.assertEqual(("prometheus", "AverageValue", "0.3"), (
            trigger["type"], trigger["metricType"], trigger["metadata"]["threshold"],
        ))
        query = trigger["metadata"]["query"]
        self.assertIn('group!="native-jit"', query)
        self.assertNotIn("or on (pod)", query)
        self.assertNotIn("count(", query)
        self.assertNotIn("vector(0)", query)
        self.assertNotIn("and on (pod)", query)
        self.assertNotIn("sum by (pod)", query)
        self.assertTrue(query.startswith("avg_over_time((sum (rate("))
        self.assertEqual("false", trigger["metadata"]["ignoreNullValues"])
        behavior = scaler["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]
        self.assertEqual({"type": "Pods", "value": 2, "periodSeconds": 60}, behavior["scaleUp"]["policies"][0])
        self.assertEqual("0", behavior["scaleUp"]["tolerance"])
        self.assertEqual(300, behavior["scaleDown"]["stabilizationWindowSeconds"])

    def test_materializes_local_thread_stats_autoscaling_comparison(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-thread-stats-autoscaling-10k-local.yaml"
        resolved = resolve_experiment_definition(source, environment="internal-lab")
        root = Path(self.temp.name) / "thread-stats-autoscaling"
        targets = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)

        self.assertEqual(
            ["ckc.thread-stats-no-jit", "ckc.cpu-hpa-85"],
            [target.target.name for target in targets],
        )
        plans = {
            target.target.name: yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
            for target in targets
        }
        self.assertEqual(10000, plans["ckc.thread-stats-no-jit"]["workload"]["load"]["base_tps"])
        self.assertEqual(
            "0 -> (5m, normal-ramp-1) -> 1 -> (5m, normal-ramp-2) -> 6 -> "
            "(5m, normal-ramp-3) -> 28 -> (5m, normal-ramp-4) -> 73 -> "
            "(5m, normal-ramp-5) -> 100 -> (10m, steady) -> 100 -> "
            "(5m, normal-cool-down-1) -> 73 -> (5m, normal-cool-down-2) -> 28 -> "
            "(5m, normal-cool-down-3) -> 6 -> (5m, normal-cool-down-4) -> 1 -> "
            "(5m, normal-cool-down-5) -> 0",
            plans["ckc.thread-stats-no-jit"]["workload"]["load"]["load_profile"],
        )
        self.assertEqual(
            {"cpu": "400m", "memory": "512Mi"},
            plans["ckc.thread-stats-no-jit"]["application"]["configuration"]["resources"]["requests"],
        )

        bindings = DeploymentBindings(
            run_id="thread-stats-comparison",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka:9094",
            kafka_security_protocol="SSL",
            redis_host="redis",
            audit_host="audit",
            application_namespace="ckc-perf",
            autoscaling_metrics_url="http://ckc-prometheus.ckc-perf.svc.cluster.local:9090",
        )
        classic = render_project_manifests(plans["ckc.cpu-hpa-85"], bindings)
        custom = render_project_manifests(plans["ckc.thread-stats-no-jit"], bindings)
        hpa = next(item for item in classic if item["kind"] == "HorizontalPodAutoscaler")
        scaler = next(item for item in custom if item["kind"] == "ScaledObject")
        self.assertEqual(85, hpa["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"])
        self.assertEqual(1, len(scaler["spec"]["triggers"]))
        self.assertIn(
            'group!="native-jit"',
            scaler["spec"]["triggers"][0]["metadata"]["query"],
        )
        self.assertEqual("0.34", scaler["spec"]["triggers"][0]["metadata"]["threshold"])
        self.assertEqual("AverageValue", scaler["spec"]["triggers"][0]["metricType"])
        self.assertTrue(scaler["spec"]["triggers"][0]["metadata"]["query"].startswith("avg_over_time((sum (rate("))
        scale_down = scaler["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]["scaleDown"]
        self.assertEqual("0.1", scaler["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]["scaleUp"]["tolerance"])
        self.assertEqual(60, scale_down["stabilizationWindowSeconds"])
        self.assertEqual({"type": "Pods", "value": 1, "periodSeconds": 60}, scale_down["policies"][0])

    def test_materializes_per_topic_autoscaling_aws_spring_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/aws-spring-keda-50k-ramp.yaml"
        resolved = resolve_experiment_definition(source, environment="aws")
        root = Path(self.temp.name) / "spring-autoscaling"
        target = materialize_experiment(resolved, output_dir=root, repo_dir=REPO_ROOT)[0]
        plan = yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
        variables = json.loads((root / "environment/terraform-lab-inputs.json").read_text(encoding="utf-8"))

        self.assertEqual("per_topic", plan["application"]["configuration"]["deployment_mode"])
        self.assertEqual("kafka.m7g.xlarge", variables["msk_broker_instance_type"])
        self.assertEqual((1, 1, 8), (
            variables["application_node_desired_size"],
            variables["application_node_min_size"],
            variables["application_node_max_size"],
        ))
        self.assertEqual(
            [(210, 42, 5), (195, 39, 5), (285, 57, 5)],
            [
                (topic["partitions"], topic["poll_loop_concurrency"], topic["capacity_replicas"])
                for topic in plan["application"]["planner"]["topics"]
            ],
        )
        workloads = plan["application"]["configuration"]["workloads"]
        self.assertEqual(
            {"order": 1500, "batch": 1500, "telemetry": 1250},
            {name: workload["hpa"]["kafka_lag"]["lag_threshold"] for name, workload in workloads.items()},
        )

        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="spring-autoscaling-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="msk:9092",
            redis_host="elasticache",
            audit_host="audit",
            application_node_selector={"ckc.dev/role": "application"},
            application_tolerations=({
                "key": "dedicated", "operator": "Equal", "value": "application", "effect": "NoSchedule",
            },),
            support_node_selector={"ckc.dev/role": "support"},
            load_test_node_selector={"ckc.dev/role": "support"},
        ))
        deployments = [
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"].startswith("ckc-demo-")
            and item["metadata"]["name"] != "ckc-demo-stubs"
        ]
        scaled_objects = [item for item in manifests if item["kind"] == "ScaledObject"]
        self.assertEqual(3, len(deployments))
        self.assertEqual(3, len(scaled_objects))
        self.assertTrue(all(item["spec"]["maxReplicaCount"] == 5 for item in scaled_objects))
        self.assertTrue(all(
            item["spec"]["template"]["spec"]["nodeSelector"] == {"ckc.dev/role": "application"}
            for item in deployments
        ))

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
        load_environment = {item["name"]: item["value"] for item in load_container["env"] if "value" in item}
        application_container = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )["spec"]["template"]["spec"]["containers"][0]
        application_environment = {item["name"]: item["value"] for item in application_container["env"] if "value" in item}

        self.assertEqual("-Xms256m -Xmx768m -XX:+UseSerialGC", load_environment["JAVA_TOOL_OPTIONS"])
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
            if "value" in item
        }
        application = next(
            item for item in manifests
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "ckc-demo"
        )
        application_environment = {
            item["name"]: item["value"]
            for item in application["spec"]["template"]["spec"]["containers"][0]["env"]
            if "value" in item
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
        self.assertEqual("-Xms256m -Xmx768m -XX:+UseSerialGC", load_environment["JAVA_TOOL_OPTIONS"])
        self.assertEqual({"cpu": "1", "memory": "1Gi"}, load_container["resources"]["requests"])
        self.assertEqual({"memory": "1280Mi"}, load_container["resources"]["limits"])
        self.assertEqual("true", application_environment["DEMO_CONSUMER_PROCESSING_ENABLED"])
        self.assertEqual("true", application_environment["AUDIT_LOG_ENABLED"])
        self.assertEqual("18", application_environment["ORDER_POLL_LOOP_CONCURRENCY"])
        self.assertEqual("16", application_environment["BATCH_POLL_LOOP_CONCURRENCY"])
        self.assertEqual("23", application_environment["TELEMETRY_POLL_LOOP_CONCURRENCY"])
        self.assertEqual(4, stubs["spec"]["replicas"])
        self.assertEqual("4", stubs_environment["STUB_WORKERS"])
        self.assertEqual([{"name": "http", "containerPort": 8085}], stubs_container["ports"])
        self.assertEqual(
            {"path": "/health", "port": 8085},
            stubs_container["startupProbe"]["httpGet"],
        )
        self.assertEqual(
            {"path": "/health", "port": 8085},
            stubs_container["readinessProbe"]["httpGet"],
        )
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
        stubs_labels = stubs["spec"]["template"]["metadata"]["labels"]
        self.assertEqual("shared", stubs_labels["ckc_run_id"])
        self.assertEqual("shared", stubs_labels["ckc_test_definition"])
        self.assertEqual("stubs", stubs_labels["ckc.dev/component"])
        self.assertEqual(
            {"app.kubernetes.io/name": "ckc-demo-stubs"},
            stubs["spec"]["selector"]["matchLabels"],
        )
        self.assertEqual({"ckc.dev/role": "controller"}, stubs["spec"]["template"]["spec"]["nodeSelector"])
        self.assertEqual({"ckc.dev/role": "controller"}, load_test["spec"]["template"]["spec"]["nodeSelector"])
        load_environment = {
            item["name"]: item["value"]
            for item in load_test["spec"]["template"]["spec"]["containers"][0]["env"]
            if "value" in item
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

    def test_bounds_indexed_load_job_name_without_losing_run_identity(self) -> None:
        _, plan, _ = self.materialize("internal-lab")
        run_id = "s-20261009-125002-9266d3-spring-kafka-per-topic-ke"
        manifests = render_project_manifests(plan, DeploymentBindings(
            run_id=run_id,
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9092",
            redis_host="redis.internal",
            audit_host="audit.internal",
        ))

        load_test = next(item for item in manifests if item["kind"] == "Job")
        name = load_test["metadata"]["name"]
        environment = {
            item["name"]: item["value"]
            for item in load_test["spec"]["template"]["spec"]["containers"][0]["env"]
            if "value" in item
        }
        self.assertLessEqual(len(name), 61)
        self.assertRegex(name, r"^ckc-load-test-[a-z0-9-]+-[0-9a-f]{8}$")
        self.assertEqual(run_id, environment["TEST_RUN_ID"])
        attempt = next(item for item in load_test["spec"]["template"]["spec"]["containers"][0]["env"] if item["name"] == "POD_UID")
        self.assertEqual("metadata.uid", attempt["valueFrom"]["fieldRef"]["fieldPath"])

        other_bindings = DeploymentBindings(
            run_id=run_id + "-other",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9092",
            redis_host="redis.internal",
            audit_host="audit.internal",
        )
        other_name = next(
            item for item in render_project_manifests(plan, other_bindings)
            if item["kind"] == "Job"
        )["metadata"]["name"]
        self.assertNotEqual(name, other_name)

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
                if "value" in item
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

        tls_manifests = render_project_manifests(plan, DeploymentBindings(
            run_id="spring-scaling-tls-1",
            application_image="registry/demo@sha256:application",
            stubs_image="registry/stubs@sha256:stubs",
            load_test_image="registry/load@sha256:load",
            kafka_bootstrap="kafka.internal:9094",
            kafka_security_protocol="SSL",
            redis_host="redis.internal",
            audit_host="audit.internal",
        ))
        tls_scaler = next(item for item in tls_manifests if item.get("kind") == "ScaledObject")
        self.assertEqual("enable", tls_scaler["spec"]["triggers"][1]["metadata"]["tls"])

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
            "(5m, order-saturation) -> 60 -> (20m, order-recovery) -> 60 -> "
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
                    "p90": 40, "p95": 70, "p99": 120, "p100": 250,
                }}},
            }],
            definition["chaos_steps"],
        )
        workloads = plan["application"]["configuration"]["workloads"]
        self.assertEqual(
            {"order": 1500, "batch": 1500, "telemetry": 1250},
            {name: workload["hpa"]["kafka_lag"]["lag_threshold"] for name, workload in workloads.items()},
        )
        self.assertEqual(
            {"order": 5, "batch": 5, "telemetry": 3},
            {name: workload["hpa"]["max_replicas"] for name, workload in workloads.items()},
        )
        self.assertEqual(
            [(27, 5, 5), (21, 4, 5), (30, 10, 3)],
            [
                (topic["partitions"], topic["poll_loop_concurrency"], topic["capacity_replicas"])
                for topic in plan["application"]["planner"]["topics"]
            ],
        )

    def test_provisioned_msk_experiments_use_4_1_except_t3_qualification(self) -> None:
        definitions = []
        for source in sorted((REPO_ROOT / "demo/infra/experiments").glob("*.yaml")):
            experiment = yaml.safe_load(source.read_text(encoding="utf-8"))
            kafka = ((experiment.get("environments") or {}).get("aws") or {}).get("lab", {}).get("kafka") or {}
            if kafka.get("mode") == "msk":
                definitions.append((source.name, kafka.get("msk_version")))

        self.assertTrue(definitions)
        expected = {
            name: ("3.9.x" if name in {
                "aws-eks-1-35-autoscaling-smoke.yaml",
                "aws-hourly-50k-tls-qualification.yaml",
                "aws-long-50k-tls-two-peaks.yaml",
            } else "4.1.x.kraft")
            for name, _ in definitions
        }
        self.assertEqual(expected, dict(definitions))
        variables = (REPO_ROOT / "demo/infra/aws/assets/terraform/load-lab/variables.tf").read_text(
            encoding="utf-8"
        )
        self.assertIn('variable "msk_kafka_version"', variables)
        self.assertIn('default     = "4.1.x.kraft"', variables)

    def test_maintained_aws_experiments_use_eks_1_35_standard_support(self) -> None:
        versions = {}
        for source in sorted((REPO_ROOT / "demo/infra/experiments").glob("*.yaml")):
            experiment = yaml.safe_load(source.read_text(encoding="utf-8"))
            aws = (experiment.get("environments") or {}).get("aws")
            if aws is not None:
                versions[source.name] = aws["lab"]["kubernetes_version"]

        self.assertTrue(versions)
        self.assertEqual({"1.35"}, set(versions.values()), versions)


if __name__ == "__main__":
    unittest.main()
