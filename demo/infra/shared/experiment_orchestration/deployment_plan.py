from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .definition import ResolvedExperiment, ResolvedTarget


DEPLOYMENT_PLAN_VERSION = 1
GENERATED_ENV_NAMES = {
    "experimentTargetName": "EXPERIMENT_TARGET_NAME",
    "springProfilesActive": "SPRING_PROFILES_ACTIVE",
    "processingEnabled": "DEMO_CONSUMER_PROCESSING_ENABLED",
    "processingDispatcherType": "PROCESSING_DISPATCHER_TYPE",
    "workerDispatcherThreads": "WORKER_DISPATCHER_THREADS",
    "modelHttpClient": "MODEL_HTTP_CLIENT",
    "modelSyncHttpClient": "MODEL_SYNC_HTTP_CLIENT",
    "jdkHttpClientExecutor": "JDK_HTTP_CLIENT_EXECUTOR",
    "jdkHttpClientVirtualThreadNamePrefix": "JDK_HTTP_CLIENT_VIRTUAL_THREAD_NAME_PREFIX",
    "processingDispatcherVirtualThreadNamePrefix": "PROCESSING_DISPATCHER_VIRTUAL_THREAD_NAME_PREFIX",
    "freshnessFirstMaxRecordAgeSeconds": "FRESHNESS_FIRST_MAX_RECORD_AGE_SECONDS",
    "consumerRetryMaxAttempts": "DEMO_CONSUMER_RETRY_MAX_ATTEMPTS",
    "consumerRetryMaxRetries": "DEMO_CONSUMER_RETRY_MAX_RETRIES",
    "consumerRetryBackoffMs": "DEMO_CONSUMER_RETRY_BACKOFF_MS",
    "kafkaConsumerFetchMinBytes": "KAFKA_CONSUMER_FETCH_MIN_BYTES",
    "kafkaConsumerFetchMaxWaitMs": "KAFKA_CONSUMER_FETCH_MAX_WAIT_MS",
    "kafkaConsumerMaxPollRecords": "KAFKA_CONSUMER_MAX_POLL_RECORDS",
    "kafkaConsumerFetchMaxBytes": "KAFKA_CONSUMER_FETCH_MAX_BYTES",
    "kafkaConsumerMaxPartitionFetchBytes": "KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES",
    "javaToolOptions": "JAVA_TOOL_OPTIONS",
    "orderProcessingMode": "ORDER_PROCESSING_MODE",
    "orderWorkerConcurrency": "ORDER_WORKER_CONCURRENCY",
    "orderPollLoopConcurrency": "ORDER_POLL_LOOP_CONCURRENCY",
    "orderWorkChannelCapacity": "ORDER_WORK_CHANNEL_CAPACITY",
    "batchProcessingMode": "BATCH_PROCESSING_MODE",
    "batchWorkerConcurrency": "BATCH_WORKER_CONCURRENCY",
    "batchPollLoopConcurrency": "BATCH_POLL_LOOP_CONCURRENCY",
    "batchWorkChannelCapacity": "BATCH_WORK_CHANNEL_CAPACITY",
    "telemetryProcessingMode": "TELEMETRY_PROCESSING_MODE",
    "telemetryWorkerConcurrency": "TELEMETRY_WORKER_CONCURRENCY",
    "telemetryPollLoopConcurrency": "TELEMETRY_POLL_LOOP_CONCURRENCY",
    "telemetryWorkChannelCapacity": "TELEMETRY_WORK_CHANNEL_CAPACITY",
}
for _topic in ("Order", "Batch", "Telemetry"):
    for _setting in ("FetchMinBytes", "FetchMaxWaitMs", "MaxPollRecords", "FetchMaxBytes", "MaxPartitionFetchBytes"):
        GENERATED_ENV_NAMES[f"{_topic.lower()}{_setting}"] = f"{_topic.upper()}_KAFKA_CONSUMER_{re.sub(r'(?<!^)(?=[A-Z])', '_', _setting).upper()}"
LOAD_ENV_NAMES = {
    "base_tps": "BASE_TPS",
    "order_event_percent": "ORDER_EVENT_PERCENT",
    "batch_event_percent": "BATCH_EVENT_PERCENT",
    "cauldron_telemetry_percent": "CAULDRON_TELEMETRY_PERCENT",
    "load_profile": "LOAD_PROFILE",
    "cauldron_count": "CAULDRON_COUNT",
    "min_orders_per_batch": "MIN_ORDERS_PER_BATCH",
    "max_orders_per_batch": "MAX_ORDERS_PER_BATCH",
    "min_brewing_steps": "MIN_BREWING_STEPS",
    "max_brewing_steps": "MAX_BREWING_STEPS",
    "brewing_step_burst_every": "BREWING_STEP_BURST_EVERY",
    "min_brewing_step_burst": "MIN_BREWING_STEP_BURST",
    "max_brewing_step_burst": "MAX_BREWING_STEP_BURST",
    "max_burst": "MAX_BURST",
    "stats_log_interval_seconds": "STATS_LOG_INTERVAL_SECONDS",
    "diagnostics_blob_size": "DIAGNOSTICS_BLOB_SIZE",
    "telemetry_source_mode": "TELEMETRY_SOURCE_MODE",
    "publish_enabled": "PUBLISH_ENABLED",
    "audit_log_enabled": "AUDIT_LOG_ENABLED",
    "workers": "LOAD_TEST_WORKERS",
    "kafka_producer_linger_ms": "KAFKA_PRODUCER_LINGER_MS",
    "kafka_producer_batch_size": "KAFKA_PRODUCER_BATCH_SIZE",
    "kafka_producer_compression_type": "KAFKA_PRODUCER_COMPRESSION_TYPE",
    "kafka_producer_buffer_memory": "KAFKA_PRODUCER_BUFFER_MEMORY",
    "order_kafka_producer_linger_ms": "ORDER_KAFKA_PRODUCER_LINGER_MS",
    "order_kafka_producer_batch_size": "ORDER_KAFKA_PRODUCER_BATCH_SIZE",
    "order_kafka_producer_compression_type": "ORDER_KAFKA_PRODUCER_COMPRESSION_TYPE",
    "order_kafka_producer_buffer_memory": "ORDER_KAFKA_PRODUCER_BUFFER_MEMORY",
    "batch_kafka_producer_linger_ms": "BATCH_KAFKA_PRODUCER_LINGER_MS",
    "batch_kafka_producer_batch_size": "BATCH_KAFKA_PRODUCER_BATCH_SIZE",
    "batch_kafka_producer_compression_type": "BATCH_KAFKA_PRODUCER_COMPRESSION_TYPE",
    "batch_kafka_producer_buffer_memory": "BATCH_KAFKA_PRODUCER_BUFFER_MEMORY",
    "telemetry_kafka_producer_linger_ms": "TELEMETRY_KAFKA_PRODUCER_LINGER_MS",
    "telemetry_kafka_producer_batch_size": "TELEMETRY_KAFKA_PRODUCER_BATCH_SIZE",
    "telemetry_kafka_producer_compression_type": "TELEMETRY_KAFKA_PRODUCER_COMPRESSION_TYPE",
    "telemetry_kafka_producer_buffer_memory": "TELEMETRY_KAFKA_PRODUCER_BUFFER_MEMORY",
}


@dataclass(frozen=True)
class DeploymentBindings:
    run_id: str
    application_image: str
    stubs_image: str
    load_test_image: str
    kafka_bootstrap: str
    redis_host: str
    audit_host: str
    audit_port: int = 5170
    image_pull_policy: str = "Always"
    application_namespace: str = "ckc-app"
    load_test_namespace: str = "ckc-loadtest"
    packet_capture_enabled: bool = False
    active_deadline_seconds: int = 3600
    application_service_type: str = "ClusterIP"
    application_node_port: int | None = None
    test_definition: str = "canonical"
    started_at: str | None = None


def build_deployment_plan(
    experiment: ResolvedExperiment,
    target: ResolvedTarget,
    planner_plan: Mapping[str, Any],
    planner_values: Mapping[str, Any],
) -> dict[str, Any]:
    if experiment.snapshot is None:
        raise ValueError("Canonical deployment plans require a resolved experiment snapshot")
    snapshot_targets = {
        str(item.get("id") or item.get("name")): item
        for item in experiment.snapshot["targets"]
    }
    target_snapshot = snapshot_targets.get(target.id) or snapshot_targets.get(target.name)
    if target_snapshot is None:
        raise ValueError(f"Resolved experiment snapshot does not contain target {target.id!r}")
    stable_planner = copy.deepcopy(dict(planner_plan))
    stable_planner.pop("test_definition", None)
    stable_planner.pop("values_path", None)
    stable_values = copy.deepcopy(dict(planner_values))
    if isinstance(stable_values.get("lab"), dict):
        stable_values["lab"].pop("runPlanPath", None)
    third_party = copy.deepcopy((experiment.environment_definition or {}).get("third_party") or [])
    if not isinstance(third_party, list):
        raise ValueError("Experiment environment third_party must be a list")
    for index, release in enumerate(third_party, start=1):
        if not isinstance(release, dict):
            raise ValueError(f"Experiment environment third_party[{index}] must be an object")
        missing = [key for key in ("name", "chart", "version") if not str(release.get(key) or "").strip()]
        if missing:
            raise ValueError(f"Experiment environment third_party[{index}] must define {', '.join(missing)}")
        if str(release["version"]).lower() == "latest":
            raise ValueError(f"Experiment environment third_party[{index}].version must be pinned")
    return {
        "schema_version": DEPLOYMENT_PLAN_VERSION,
        "experiment": {
            "name": experiment.name,
            "environment": experiment.environment,
        },
        "target": {
            "id": target.id,
            "name": target.name,
            "implementation": target.profile,
        },
        "environment": copy.deepcopy(experiment.environment_definition or {}),
        "application": {
            "configuration": copy.deepcopy(target_snapshot.get("application") or {}),
            "runtime": copy.deepcopy(target_snapshot.get("runtime") or {}),
            "planner": stable_planner,
            "generated_values": stable_values,
        },
        "workload": copy.deepcopy(target_snapshot["workload"]),
        "project_resources": ["application", "stubs", "load-test"],
        "third_party": third_party,
    }


def write_deployment_plan(path: Path, plan: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(dict(plan), sort_keys=False, allow_unicode=True), encoding="utf-8")


def aws_terraform_variables(
    environment: Mapping[str, Any],
    *,
    experiment_id: str,
    runtime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    region = str(environment.get("region") or "").strip()
    if not region:
        raise ValueError("AWS experiment environment must define region")
    lab = environment.get("lab")
    if not isinstance(lab, dict):
        raise ValueError("AWS experiment environment must define lab")
    allowed_lab = {"kubernetes_version", "network", "nodes", "kafka", "redis", "observability"}
    unknown = sorted(set(lab) - allowed_lab)
    if unknown:
        raise ValueError(f"AWS experiment environment.lab contains unknown fields: {', '.join(unknown)}")
    result: dict[str, Any] = {"aws_region": region, "experiment_id": experiment_id}
    if "kubernetes_version" in lab:
        result["kubernetes_version"] = str(lab["kubernetes_version"])
    sections = {
        "network": {
            "vpc_cidr": "vpc_cidr",
            "availability_zones": "availability_zones",
            "private_subnets": "private_subnets",
            "public_subnets": "public_subnets",
        },
        "nodes": {
            "instance_types": "node_instance_types",
            "desired_size": "node_desired_size",
            "min_size": "node_min_size",
            "max_size": "node_max_size",
            "disk_size_gib": "node_disk_size",
        },
        "kafka": {
            "mode": "kafka_mode",
            "kubernetes_brokers": "kubernetes_kafka_brokers",
            "msk_version": "msk_kafka_version",
            "msk_brokers": "msk_number_of_broker_nodes",
            "msk_instance_type": "msk_broker_instance_type",
            "msk_volume_size_gib": "msk_ebs_volume_size",
        },
        "redis": {
            "mode": "elasticache_mode",
            "kubernetes_architecture": "kubernetes_redis_architecture",
            "kubernetes_replicas": "kubernetes_redis_replica_count",
            "elasticache_node_type": "elasticache_node_type",
            "elasticache_engine_version": "elasticache_engine_version",
            "elasticache_nodes": "elasticache_num_cache_clusters",
        },
        "observability": {
            "runner_peering": "enable_runner_observability_peering",
            "runner_project": "runner_project",
            "remote_write_port": "runner_remote_write_port",
            "loki_port": "runner_loki_port",
            "audit_port": "runner_audit_port",
        },
    }
    for section, mapping in sections.items():
        raw = lab.get(section, {})
        if not isinstance(raw, dict):
            raise ValueError(f"AWS experiment environment.lab.{section} must be an object")
        unknown_fields = sorted(set(raw) - set(mapping))
        if unknown_fields:
            raise ValueError(
                f"AWS experiment environment.lab.{section} contains unknown fields: {', '.join(unknown_fields)}"
            )
        for source, destination in mapping.items():
            if source in raw:
                result[destination] = copy.deepcopy(raw[source])
    if runtime:
        result.update(copy.deepcopy(dict(runtime)))
    return result


def write_terraform_variables(path: Path, variables: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(variables), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _metadata(name: str, namespace: str, labels: Mapping[str, str] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "namespace": namespace}
    if labels:
        result["labels"] = dict(labels)
    return result


def _environment_entries(environment: Mapping[str, Any]) -> list[dict[str, str]]:
    def text(value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        return "" if value is None else str(value)

    return [{"name": key, "value": text(value)} for key, value in sorted(environment.items())]


def _hpa(name: str, namespace: str, values: Mapping[str, Any]) -> dict[str, Any] | None:
    if not values.get("enabled"):
        return None
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": _metadata(name, namespace),
        "spec": {
            "behavior": {"scaleDown": {"stabilizationWindowSeconds": int(values.get("scaleDownStabilizationWindowSeconds", 300))}},
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": name},
            "minReplicas": int(values.get("minReplicas", 1)),
            "maxReplicas": int(values.get("maxReplicas", 1)),
            "metrics": [{
                "type": "Resource",
                "resource": {
                    "name": "cpu",
                    "target": {"type": "Utilization", "averageUtilization": int(values.get("targetCPUUtilizationPercentage", 70))},
                },
            }],
        },
    }


def _deployment(
    *,
    name: str,
    namespace: str,
    container_name: str,
    image: str,
    pull_policy: str,
    replicas: int,
    run_id: str,
    profile: str,
    environment: Mapping[str, Any],
    resources: Mapping[str, Any] | None = None,
    packet_capture: bool = False,
    probes: Mapping[str, Any] | None = None,
    test_definition: str = "canonical",
) -> dict[str, Any]:
    labels = {"app.kubernetes.io/name": name}
    pod_labels = {
        **labels,
        "ckc.dev/test-run-id": run_id,
        "ckc.dev/profile": profile,
        "ckc_run_id": run_id,
        "ckc_profile": profile,
        "ckc_test_definition": test_definition,
    }
    container: dict[str, Any] = {
        "name": container_name,
        "image": image,
        "imagePullPolicy": pull_policy,
        "ports": [{"containerPort": 8080}],
        "env": _environment_entries(environment),
    }
    if resources:
        container["resources"] = copy.deepcopy(dict(resources))
    if probes:
        for probe_name in ("readiness", "liveness", "startup"):
            probe = probes.get(probe_name)
            if not isinstance(probe, dict) or (probe_name == "startup" and not probe.get("enabled")):
                continue
            container[f"{probe_name}Probe"] = {
                "httpGet": {"path": "/actuator/health", "port": 8080},
                **{
                    destination: int(probe[source])
                    for source, destination in (
                        ("initialDelaySeconds", "initialDelaySeconds"),
                        ("periodSeconds", "periodSeconds"),
                        ("timeoutSeconds", "timeoutSeconds"),
                        ("failureThreshold", "failureThreshold"),
                    )
                    if source in probe
                },
            }
    pod_spec: dict[str, Any] = {
        "topologySpreadConstraints": [{
            "maxSkew": 1,
            "topologyKey": "kubernetes.io/hostname",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": labels},
        }],
        "containers": [container],
    }
    if packet_capture:
        container["securityContext"] = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"add": ["NET_RAW"], "drop": ["ALL"]},
        }
        container["volumeMounts"] = [{"name": "packet-captures", "mountPath": "/captures"}]
        pod_spec["volumes"] = [{"name": "packet-captures", "emptyDir": {"sizeLimit": "256Mi"}}]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(name, namespace, labels),
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": labels},
            "template": {"metadata": {"labels": pod_labels}, "spec": pod_spec},
        },
    }


def render_project_manifests(plan: Mapping[str, Any], bindings: DeploymentBindings) -> list[dict[str, Any]]:
    application = plan["application"]
    values = application.get("generated_values") or {}
    configuration = application.get("configuration") or {}
    runtime = application.get("runtime") or {}
    profile = str(plan["target"]["implementation"])
    computed_env = {
        GENERATED_ENV_NAMES[key]: value
        for key, value in (values.get("env") or {}).items()
        if key in GENERATED_ENV_NAMES and value not in (None, "")
    }
    computed_env.update(copy.deepcopy(runtime.get("env") or {}))
    computed_env.update({
        "EXPERIMENT_TARGET_NAME": str(plan["target"]["name"]),
        "KAFKA_ENABLED": "true",
        "DEMO_KAFKA_ENABLED": "true",
        "DEMO_KAFKA_BOOTSTRAP_SERVERS": bindings.kafka_bootstrap,
        "KAFKA_BOOTSTRAP_SERVERS": bindings.kafka_bootstrap,
        "GROUP_ID": "ckc-demo",
        "SPRING_DATA_REDIS_HOST": bindings.redis_host,
        "SPRING_DATA_REDIS_PORT": 6379,
        "AUDIT_TCP_HOST": bindings.audit_host,
        "AUDIT_TCP_PORT": bindings.audit_port,
        "AUDIT_RUN_ID": bindings.run_id,
        "MODEL_BASE_URL": "http://ckc-demo-stubs:8080",
        "ETA_MODEL_BASE_URL": "http://ckc-demo-stubs:8080",
        "FLAVOUR_MODEL_BASE_URL": "http://ckc-demo-stubs:8080",
        "REGISTRY_BASE_URL": "http://ckc-demo-stubs:8080",
        "MODEL_HTTP_CLIENT": computed_env.get("MODEL_HTTP_CLIENT", "ARMERIA"),
        "MODEL_SYNC_HTTP_CLIENT": computed_env.get("MODEL_SYNC_HTTP_CLIENT", "ARMERIA"),
    })
    replicas = int(values.get("replicaCount", configuration.get("replicas", 1)))
    resources = values.get("resources") or configuration.get("resources") or {}
    manifests: list[dict[str, Any]] = [
        _deployment(
            name="ckc-demo-stubs",
            namespace=bindings.application_namespace,
            container_name="demo-stubs",
            image=bindings.stubs_image,
            pull_policy=bindings.image_pull_policy,
            replicas=1,
            run_id=bindings.run_id,
            profile="stubs",
            environment={"PORT": 8080, "REDIS_HOST": bindings.redis_host, "REDIS_PORT": 6379},
            test_definition=bindings.test_definition,
        ),
        {
            "apiVersion": "v1", "kind": "Service",
            "metadata": _metadata("ckc-demo-stubs", bindings.application_namespace),
            "spec": {"selector": {"app.kubernetes.io/name": "ckc-demo-stubs"}, "ports": [{"name": "http", "port": 8080, "targetPort": 8080}]},
        },
        _deployment(
            name="ckc-demo",
            namespace=bindings.application_namespace,
            container_name="demo",
            image=bindings.application_image,
            pull_policy=bindings.image_pull_policy,
            replicas=replicas,
            run_id=bindings.run_id,
            profile=profile,
            environment=computed_env,
            resources=resources,
            packet_capture=bindings.packet_capture_enabled,
            probes=values.get("probes") or {
                "readiness": {"initialDelaySeconds": 15, "periodSeconds": 10, "timeoutSeconds": 1, "failureThreshold": 3},
                "liveness": {"initialDelaySeconds": 30, "periodSeconds": 15, "timeoutSeconds": 1, "failureThreshold": 3},
            },
            test_definition=bindings.test_definition,
        ),
        {
            "apiVersion": "v1", "kind": "Service",
            "metadata": _metadata("ckc-demo", bindings.application_namespace),
            "spec": {
                "selector": {"app.kubernetes.io/name": "ckc-demo"},
                "type": bindings.application_service_type,
                "ports": [{
                    "name": "http", "port": 8080, "targetPort": 8080,
                    **({"nodePort": bindings.application_node_port} if bindings.application_node_port else {}),
                }],
            },
        },
    ]
    application_hpa = _hpa("ckc-demo", bindings.application_namespace, values.get("hpa") or configuration.get("hpa") or {})
    if application_hpa:
        manifests.append(application_hpa)
    workload = plan["workload"]
    manifests.extend([
        {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": _metadata("ckc-experiment-workload", bindings.load_test_namespace),
            "data": {"workload.yaml": yaml.safe_dump(workload, sort_keys=False)},
        },
        _load_test_job(plan, bindings),
    ])
    return manifests


def _load_test_job(plan: Mapping[str, Any], bindings: DeploymentBindings) -> dict[str, Any]:
    load = plan["workload"]["load"]
    shards = int(load.get("shards", 1))
    defaults = {
        "base_tps": 10000,
        "order_event_percent": 40,
        "batch_event_percent": 20,
        "cauldron_telemetry_percent": 40,
        "load_profile": "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0",
        "cauldron_count": 32,
        "min_orders_per_batch": 3,
        "max_orders_per_batch": 8,
        "min_brewing_steps": 5,
        "max_brewing_steps": 10,
        "brewing_step_burst_every": 1,
        "min_brewing_step_burst": 5,
        "max_brewing_step_burst": 10,
        "max_burst": 1000,
        "stats_log_interval_seconds": 30,
        "diagnostics_blob_size": 512,
        "telemetry_source_mode": "ACTIVE_BATCHES",
        "publish_enabled": True,
        "audit_log_enabled": True,
        "workers": "",
    }
    for key in LOAD_ENV_NAMES:
        defaults.setdefault(key, "")
    environment = {
        **{
            environment_name: load.get(key, defaults[key])
            for key, environment_name in LOAD_ENV_NAMES.items()
            if key in load or key in defaults
        },
        "BOOTSTRAP_SERVERS": bindings.kafka_bootstrap,
        "TOTAL_SHARDS": shards,
        "TEST_RUN_ID": bindings.run_id,
        "AUDIT_TCP_HOST": bindings.audit_host,
        "AUDIT_TCP_PORT": bindings.audit_port,
    }
    if bindings.started_at:
        environment["TEST_RUN_STARTED_AT"] = bindings.started_at
    container: dict[str, Any] = {
        "name": "load-test",
        "image": bindings.load_test_image,
        "imagePullPolicy": bindings.image_pull_policy,
        "ports": [{"name": "metrics", "containerPort": 9405}],
        "env": _environment_entries(environment),
    }
    resource_values: dict[str, dict[str, Any]] = {}
    if any(load.get(key) is not None for key in ("cpu_request", "memory_request", "cpu_limit", "memory_limit")):
        resource_values = {
            "requests": {
                "cpu": load.get("cpu_request", "500m"),
                "memory": load.get("memory_request", "512Mi"),
            },
            "limits": {
                "cpu": load.get("cpu_limit", "2"),
                "memory": load.get("memory_limit", "1Gi"),
            },
        }
    resources = {
        group: {key: value for key, value in entries.items() if value not in (None, "")}
        for group, entries in resource_values.items()
    }
    resources = {group: entries for group, entries in resources.items() if entries}
    if resources:
        container["resources"] = resources
    if bindings.packet_capture_enabled:
        container["securityContext"] = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"add": ["NET_RAW"], "drop": ["ALL"]},
        }
        container["volumeMounts"] = [{"name": "packet-captures", "mountPath": "/captures"}]
    pod_spec: dict[str, Any] = {"restartPolicy": "Never", "containers": [container]}
    if bindings.packet_capture_enabled:
        pod_spec["volumes"] = [{"name": "packet-captures", "emptyDir": {"sizeLimit": "256Mi"}}]
    name = f"ckc-load-test-{bindings.run_id}"
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": _metadata(name, bindings.load_test_namespace),
        "spec": {
            "activeDeadlineSeconds": bindings.active_deadline_seconds,
            "completions": shards,
            "parallelism": shards,
            "completionMode": "Indexed",
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": "ckc-load-test", "ckc.dev/test-run-id": bindings.run_id}},
                "spec": pod_spec,
            },
        },
    }


def write_project_manifests(path: Path, plan: Mapping[str, Any], bindings: DeploymentBindings) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump_all(render_project_manifests(plan, bindings), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
