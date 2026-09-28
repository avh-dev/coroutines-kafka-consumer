from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition
from .materialize import materialize_experiment


REPO_ROOT = Path(__file__).resolve().parents[4]


class MaterializeTest(unittest.TestCase):
    def test_smoke_uses_two_minute_single_target_with_useful_traffic_windows(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/smoke.yaml"
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))
        resolve_experiment_definition(source, environment="internal-lab")

        workload = candidate["workload"]
        self.assertEqual(["ckc-smoke-1"], [target["name"] for target in candidate["targets"]])
        self.assertEqual(1000, workload["load"]["base_tps"])
        self.assertEqual(
            "0 -> (10s, warmup) -> 100 -> (100s, maximum) -> 100 -> (10s, cool-down) -> 0",
            workload["load"]["load_profile"],
        )
        self.assertEqual(
            [
                {"name": "max-load", "start": "20s", "duration": "80s"},
                {"name": "post-degradation", "start": "55s", "duration": "40s"},
            ],
            workload["measurement_windows"],
        )
        self.assertEqual("30s", workload["diagnostics"][0]["at"])
        self.assertEqual("10s", workload["diagnostics"][0]["duration"])
        self.assertEqual("25s", workload["chaos"][0]["at"])
        self.assertEqual("20s", workload["chaos"][0]["duration"])
        self.assertEqual(
            {"p99": 500, "p999": 1000, "p100": 2000},
            workload["chaos"][0]["params"]["eta"]["percentiles"],
        )

    def test_materializes_ckc_poller_and_partition_comparison_at_2k(self) -> None:
        baseline = yaml.safe_load(
            (REPO_ROOT / "demo/infra/experiments/spring-ckc-no-chaos-e2e-tuning-5k.yaml").read_text(
                encoding="utf-8"
            )
        )
        source = (
            REPO_ROOT
            / "demo/infra/experiments/spring-ckc-pollers-partitions-2k-comparison.yaml"
        )
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))

        expected_workload = baseline["workload"]
        expected_workload["load"]["base_tps"] = 2000
        expected_workload["load"]["producer_capacity_tps"] = {
            "order": 2000,
            "batch": 2000,
            "telemetry": 2000,
        }
        self.assertEqual(expected_workload, candidate["workload"])
        expected_spring = baseline["targets"][0]
        for topic, partitions in {"order": 24, "batch": 18, "telemetry": 75}.items():
            expected_spring["runtime"]["topics"][topic]["partitions"] = partitions
            expected_spring["runtime"]["topics"][topic]["pollers"] = partitions
        self.assertEqual(expected_spring, candidate["targets"][0])
        self.assertNotIn("chaos", candidate["workload"])

        self.assertEqual(
            {
                "implementation": "apache-kafka",
                "topology": "cluster",
                "brokers": 3,
                "replication_factor": 3,
                "min_insync_replicas": 2,
                "resources": {
                    "cpu_per_broker": 1,
                    "memory_per_broker": "2Gi",
                    "heap_per_broker": "1Gi",
                },
            },
            candidate["environments"]["internal-lab"]["lab"]["kafka"],
        )

        expected = {
            "spring-kafka.jdk-tuned": (
                [24, 18, 75],
                [24, 18, 75],
                [1, 1, 1],
            ),
            "ckc.fixed.1.spring-partitions.per-partition-pollers": (
                [24, 18, 75],
                [24, 18, 75],
                [500, 500, 500],
            ),
            "ckc.fixed.1.spring-partitions.single-poller": (
                [24, 18, 75],
                [1, 1, 1],
                [500, 500, 500],
            ),
            "ckc.fixed.1.min-partitions.single-poller": (
                [3, 3, 3],
                [1, 1, 1],
                [500, 500, 500],
            ),
        }
        for environment in ("internal-lab", "aws"):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as directory:
                experiment = resolve_experiment_definition(source, environment=environment)
                materialized = materialize_experiment(
                    experiment,
                    output_dir=Path(directory) / "out",
                    repo_dir=REPO_ROOT,
                )
                actual = {}
                for target in materialized:
                    definition = yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
                    topics = definition["deployment"]["run_plan"]["topics"]
                    actual[target.target.name] = (
                        [topic["partitions"] for topic in topics],
                        [topic["poll_loop_concurrency"] for topic in topics],
                        [topic["worker_concurrency"] for topic in topics],
                    )

                self.assertEqual(2000, definition["load_test"]["base_tps"])
                self.assertEqual(expected, actual)

    def test_10k_placement_comparison_uses_balanced_spring_partitions_and_measured_ckc_workers(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/application-placement-10k-comparison.yaml"
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))
        spring = candidate["targets"][0]["runtime"]["topics"]
        ckc = candidate["targets"][1]["runtime"]["topics"]

        for topic, partitions in {"order": 84, "batch": 57, "telemetry": 321}.items():
            self.assertEqual(partitions, spring[topic]["partitions"])
            self.assertEqual(partitions, spring[topic]["pollers"])
            self.assertEqual(0, partitions % 3)
        self.assertEqual(500, ckc["order"]["workers"])
        self.assertEqual(500, ckc["batch"]["workers"])
        self.assertEqual(900, ckc["telemetry"]["workers"])

        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            plans = {
                target.target.name: yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))[
                    "deployment"
                ]["run_plan"]["topics"]
                for target in materialized
            }

        for name in ("spring-kafka.jdk.same-host", "spring-kafka.jdk.split-host"):
            self.assertEqual([84, 57, 321], [topic["partitions"] for topic in plans[name]])
            self.assertEqual([84, 57, 321], [topic["poll_loop_concurrency"] for topic in plans[name]])
        for name in ("ckc.fixed.1.same-host", "ckc.fixed.1.split-host"):
            self.assertEqual([500, 500, 900], [topic["worker_concurrency"] for topic in plans[name]])

    def test_materializes_ckc_replica_scaling_comparison_at_10k(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-replica-scaling-10k-comparison.yaml"
        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            plans = {
                target.target.name: yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))[
                    "deployment"
                ]["run_plan"]
                for target in materialized
            }

        self.assertEqual(
            [
                "ckc.fixed.1.same-host",
                "ckc.fixed.2.same-host",
                "ckc.fixed.1.split-host",
                "ckc.fixed.2.split-host",
            ],
            list(plans),
        )
        self.assertEqual([1, 2, 1, 2], [plan["replica_count"] for plan in plans.values()])
        for plan in plans.values():
            self.assertEqual([3, 3, 3], [topic["partitions"] for topic in plan["topics"]])
            self.assertEqual([1, 1, 1], [topic["poll_loop_concurrency"] for topic in plan["topics"]])
            self.assertEqual([500, 500, 900], [topic["worker_concurrency"] for topic in plan["topics"]])

    def test_materializes_broker_aligned_split_host_failover_comparison_at_5k(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/kafka-cluster-failover-5k-comparison.yaml"
        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            definitions = {
                target.target.name: yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
                for target in materialized
            }
            deployment_plans = {
                target.target.name: yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
                for target in materialized
            }

        self.assertEqual(
            ["spring-kafka.jdk.3-cluster-failover", "ckc.fixed.3-cluster-failover"],
            [target.target.name for target in materialized],
        )
        self.assertEqual(
            {5000},
            {definition["load_test"]["base_tps"] for definition in definitions.values()},
        )
        partitions = {
            name: [topic["partitions"] for topic in definition["deployment"]["run_plan"]["topics"]]
            for name, definition in definitions.items()
        }
        self.assertEqual([3, 3, 3], partitions["ckc.fixed.3-cluster-failover"])
        self.assertEqual([60, 45, 189], partitions["spring-kafka.jdk.3-cluster-failover"])
        self.assertTrue(all(value % 3 == 0 for values in partitions.values() for value in values))
        plans = {
            name: definition["deployment"]["run_plan"]
            for name, definition in definitions.items()
        }
        self.assertEqual({3}, {plan["replica_count"] for plan in plans.values()})
        self.assertEqual(
            {"worker"},
            {
                plan["application"]["configuration"]["placement"]
                for plan in deployment_plans.values()
            },
        )
        self.assertEqual(
            [20, 15, 63],
            [topic["poll_loop_concurrency"] for topic in plans["spring-kafka.jdk.3-cluster-failover"]["topics"]],
        )
        self.assertEqual(
            [1, 1, 1],
            [topic["poll_loop_concurrency"] for topic in plans["ckc.fixed.3-cluster-failover"]["topics"]],
        )
        for plan in deployment_plans.values():
            self.assertNotIn("cpu", plan["application"]["configuration"]["resources"]["limits"])

    def test_materializes_split_host_replica_churn_comparison_at_5k(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/spring-ckc-replica-churn-5k-comparison.yaml"
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            definitions = {
                target.target.name: yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
                for target in materialized
            }
            deployment_plans = {
                target.target.name: yaml.safe_load(target.deployment_plan_path.read_text(encoding="utf-8"))
                for target in materialized
            }

        self.assertEqual(
            ["spring-kafka.jdk.2-replica-churn", "ckc.fixed.2-replica-churn"],
            [target.target.name for target in materialized],
        )
        load = candidate["workload"]["load"]
        self.assertEqual(5000, load["base_tps"])
        self.assertEqual(
            "0 -> (3m, warmup) -> 100 -> (3h55m, replica-churn) -> 100 -> (2m, cool-down) -> 0",
            load["load_profile"],
        )
        self.assertEqual(
            {"name": "replica-churn", "start": "3m", "duration": "3h55m"},
            candidate["workload"]["measurement_window"],
        )
        self.assertEqual("3h55m", candidate["workload"]["chaos"][0]["duration"])
        self.assertEqual(
            [
                {"type": "deployment_scale", "params": {"replicas": 3}},
                {"type": "delay", "duration": "45s"},
                {"type": "deployment_scale", "params": {"replicas": 2}},
                {"type": "delay", "duration": "45s"},
            ],
            candidate["workload"]["chaos"][0]["steps"],
        )
        self.assertEqual({2}, {
            definition["deployment"]["run_plan"]["replica_count"]
            for definition in definitions.values()
        })
        self.assertEqual(
            [60, 48, 192],
            [
                topic["partitions"]
                for topic in definitions["spring-kafka.jdk.2-replica-churn"]["deployment"]["run_plan"]["topics"]
            ],
        )
        self.assertEqual(
            [6, 6, 6],
            [
                topic["partitions"]
                for topic in definitions["ckc.fixed.2-replica-churn"]["deployment"]["run_plan"]["topics"]
            ],
        )
        for plan in deployment_plans.values():
            application = plan["application"]["configuration"]
            self.assertEqual("worker", application["placement"])
            self.assertEqual("1500m", application["resources"]["requests"]["cpu"])
            self.assertNotIn("cpu", application["resources"]["limits"])
        self.assertEqual(
            {False},
            {
                target.target.definition["env"]["AUDIT_LOG_ENABLED"]
                for target in materialized
            },
        )

    def test_materializes_canonical_snapshot_and_planner_capabilities(self) -> None:
        source = REPO_ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out"
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(experiment, output_dir=output, repo_dir=REPO_ROOT)
            snapshot = yaml.safe_load((output / "resolved-experiment.yaml").read_text(encoding="utf-8"))
            profiles = yaml.safe_load((output / "implementation-profiles.yaml").read_text(encoding="utf-8"))
            definition = yaml.safe_load(materialized[0].definition_path.read_text(encoding="utf-8"))

        self.assertEqual("internal-lab", snapshot["environment"]["name"])
        self.assertEqual(snapshot["implementations"], profiles)
        self.assertEqual("ckc", snapshot["targets"][0]["implementation"])
        self.assertEqual(20, definition["deployment"]["run_plan"]["topics"][2]["worker_concurrency"])

    def test_materializes_uniform_ckc_linger_fetch_wait_and_workers(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/spring-ckc-no-chaos-e2e-tuning-5k.yaml"
        for environment in ("internal-lab", "aws"):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as directory:
                experiment = resolve_experiment_definition(source, environment=environment)
                materialized = materialize_experiment(
                    experiment,
                    output_dir=Path(directory) / "out",
                    repo_dir=REPO_ROOT,
                )
                ckc = next(target for target in materialized if target.target.name == "ckc.fixed.1-tuned")
                definition = yaml.safe_load(ckc.definition_path.read_text(encoding="utf-8"))
                load = definition["load_test"]
                env = definition["deployment"]["run_plan"]

                self.assertEqual(300, load["kafka_producer_linger_ms"])
                self.assertEqual(300, load["telemetry_kafka_producer_linger_ms"])
                runtime_env = ckc.target.definition["env"]
                self.assertEqual(350, runtime_env["KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"])
                self.assertEqual(350, runtime_env["TELEMETRY_KAFKA_CONSUMER_FETCH_MAX_WAIT_MS"])
                self.assertEqual([500, 500, 500], [topic["worker_concurrency"] for topic in env["topics"]])

    def test_all_downstreams_tail_latency_comparison_only_adds_chaos_and_removes_armeria_target(self) -> None:
        baseline = yaml.safe_load(
            (REPO_ROOT / "demo/infra/experiments/spring-ckc-no-chaos-e2e-tuning-5k.yaml").read_text(
                encoding="utf-8"
            )
        )
        candidate_path = (
            REPO_ROOT / "demo/infra/experiments/spring-ckc-all-downstreams-500ms-5k-comparison.yaml"
        )
        candidate = yaml.safe_load(candidate_path.read_text(encoding="utf-8"))

        self.assertEqual([baseline["targets"][0], baseline["targets"][2]], candidate["targets"])
        self.assertEqual(baseline["environments"], candidate["environments"])

        candidate_workload = dict(candidate["workload"])
        chaos = candidate_workload.pop("chaos")
        self.assertEqual(baseline["workload"], candidate_workload)
        self.assertEqual("5m", chaos[0]["at"])
        self.assertEqual("3m", chaos[0]["duration"])
        self.assertEqual("stubs_degradation", chaos[0]["type"])
        self.assertEqual(0, chaos[0]["params"]["error_rate_percent"])
        for downstream in ("eta", "flavour", "registry"):
            with self.subTest(downstream=downstream):
                settings = chaos[0]["params"][downstream]
                self.assertEqual(500, settings["percentiles"]["p99"])
                self.assertEqual(2000, settings["percentiles"]["p100"])

        experiment = resolve_experiment_definition(candidate_path, environment="internal-lab")
        self.assertEqual(
            ["spring-kafka.jdk-tuned", "ckc.fixed.1-tuned"],
            [target.name for target in experiment.targets],
        )

    def test_materializes_bounded_tail_latency_comparison_at_5k(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/tail-latency-window-5k-comparison.yaml"
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))
        workload = candidate["workload"]

        self.assertEqual(5000, workload["load"]["base_tps"])
        self.assertEqual(
            {"order": 5000, "batch": 5000, "telemetry": 5000},
            workload["load"]["producer_capacity_tps"],
        )
        self.assertEqual(1800, workload["load"]["consumer_drain_timeout_seconds"])
        self.assertEqual(60, workload["load"]["consumer_drain_idle_seconds"])
        self.assertEqual(
            [
                {"name": "baseline", "start": "2m", "duration": "2m"},
                {"name": "degraded", "start": "5m", "duration": "5m"},
            ],
            workload["measurement_windows"],
        )
        chaos = workload["chaos"][0]
        self.assertEqual("5m", chaos["at"])
        self.assertEqual("5m", chaos["duration"])
        self.assertEqual(
            {"p995": 120, "p100": 60000},
            chaos["params"]["eta"]["percentiles"],
        )
        self.assertEqual(
            {"p995": 5, "p100": 60000},
            chaos["params"]["registry"]["percentiles"],
        )
        self.assertEqual(
            {"order": 35, "batch": 25, "telemetry": 40},
            {name: topic["traffic_percent"] for name, topic in workload["topics"].items()},
        )
        self.assertEqual(
            {1000},
            {topic["max_e2e_latency_ms"] for topic in workload["topics"].values()},
        )
        self.assertEqual(
            [("baseline", "3m"), ("degraded", "7m")],
            [(capture["name"], capture["at"]) for capture in workload["diagnostics"]],
        )

        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            definitions = {
                target.target.name: yaml.safe_load(target.definition_path.read_text(encoding="utf-8"))
                for target in materialized
            }

        self.assertEqual(
            ["spring-kafka.jdk", "ckc.fixed.1", "cpc-reactor.fixed.1"],
            [target.target.name for target in materialized],
        )
        for target in candidate["targets"]:
            self.assertIn(
                "-Xms512m -Xmx1536m -Xss256k -XX:+UseSerialGC",
                target["application"]["java_options"],
            )
        actual_partitions = {
            name: [topic["partitions"] for topic in definition["deployment"]["run_plan"]["topics"]]
            for name, definition in definitions.items()
        }
        spring_topics = definitions["spring-kafka.jdk"]["deployment"]["run_plan"]["topics"]
        self.assertEqual([14.0, 9.0, 41.0], [topic["average_processing_ms"] for topic in spring_topics])
        self.assertTrue(all(not any(topic["manual_overrides"].values()) for topic in spring_topics))
        self.assertEqual([3, 3, 3], actual_partitions["ckc.fixed.1"])
        self.assertEqual([3, 3, 3], actual_partitions["cpc-reactor.fixed.1"])
        self.assertEqual([25, 12, 82], actual_partitions["spring-kafka.jdk"])

    def test_materializes_two_hour_split_ckc_resilience_experiment(self) -> None:
        source = REPO_ROOT / "demo/infra/experiments/ckc-split-resilience-2h-7k.yaml"
        candidate = yaml.safe_load(source.read_text(encoding="utf-8"))
        workload = candidate["workload"]

        self.assertEqual(7000, workload["load"]["base_tps"])
        self.assertEqual(
            "0 -> (5m, warmup) -> 100 -> (110m, resilience) -> 100 -> (5m, cool-down) -> 0",
            workload["load"]["load_profile"],
        )
        self.assertEqual(10, len(workload["measurement_windows"]))
        self.assertEqual(12, len(workload["chaos"]))
        self.assertEqual(
            [3, 2],
            [step["params"]["replicas"] for step in workload["chaos"] if step["type"] == "deployment_scale"],
        )

        with tempfile.TemporaryDirectory() as directory:
            experiment = resolve_experiment_definition(source, environment="internal-lab")
            materialized = materialize_experiment(
                experiment,
                output_dir=Path(directory) / "out",
                repo_dir=REPO_ROOT,
            )
            definition = yaml.safe_load(materialized[0].definition_path.read_text(encoding="utf-8"))

        self.assertEqual(["ckc.fixed.2.split-resilience"], [target.target.name for target in materialized])
        self.assertEqual(2, materialized[0].plan["replica_count"])
        self.assertEqual("worker", materialized[0].target.definition["application"]["placement"])
        self.assertEqual(10, len(definition["load_test"]["measurement_windows"]))
        self.assertEqual(
            ["30m", "60m"],
            [
                step["at"]
                for step in definition["chaos_steps"]
                if step["type"] == "deployment_scale"
            ],
        )


if __name__ == "__main__":
    unittest.main()
