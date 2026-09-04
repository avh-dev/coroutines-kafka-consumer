#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

PACKAGE_ROOT = Path(__file__).resolve().parent
if str(PACKAGE_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT.parent))

from experiment_orchestration.deployment_plan import DeploymentBindings, render_project_manifests  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Render project Kubernetes resources from a deployment plan.")
    parser.add_argument("plan", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--application-image", required=True)
    parser.add_argument("--stubs-image", required=True)
    parser.add_argument("--load-test-image", required=True)
    parser.add_argument("--kafka-bootstrap", required=True)
    parser.add_argument("--redis-host", required=True)
    parser.add_argument("--audit-host", required=True)
    parser.add_argument("--audit-port", type=int, default=5170)
    parser.add_argument("--namespace", default="ckc-app")
    parser.add_argument("--load-test-namespace", default="ckc-loadtest")
    parser.add_argument("--pull-policy", default="Always")
    parser.add_argument("--test-definition", default="canonical")
    parser.add_argument("--application-node-port", type=int)
    parser.add_argument("--packet-capture", action="store_true")
    parser.add_argument("--applications-only", action="store_true")
    parser.add_argument("--env", action="append", default=[])
    args = parser.parse_args()

    plan = yaml.safe_load(args.plan.read_text(encoding="utf-8"))
    runtime_environment = plan.setdefault("application", {}).setdefault("runtime", {}).setdefault("env", {})
    for value in args.env:
        key, separator, configured = value.partition("=")
        if not separator or not key:
            raise ValueError(f"--env must use KEY=VALUE: {value}")
        runtime_environment[key] = configured
    manifests = render_project_manifests(plan, DeploymentBindings(
        run_id=args.run_id,
        application_image=args.application_image,
        stubs_image=args.stubs_image,
        load_test_image=args.load_test_image,
        kafka_bootstrap=args.kafka_bootstrap,
        redis_host=args.redis_host,
        audit_host=args.audit_host,
        audit_port=args.audit_port,
        image_pull_policy=args.pull_policy,
        application_namespace=args.namespace,
        load_test_namespace=args.load_test_namespace,
        packet_capture_enabled=args.packet_capture,
        application_service_type="NodePort" if args.application_node_port else "ClusterIP",
        application_node_port=args.application_node_port,
        test_definition=args.test_definition,
    ))
    if args.applications_only:
        manifests = [item for item in manifests if item["kind"] not in {"ConfigMap", "Job"}]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(yaml.safe_dump_all(manifests, sort_keys=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
