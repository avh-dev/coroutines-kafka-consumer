#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write local Kubernetes runner context JSON.")
    parser.add_argument("--output", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--minikube-profile", required=True)
    parser.add_argument("--kafka-service", required=True)
    parser.add_argument("--redis-service", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    context = {
        "environment": args.environment,
        "provider": "local-k8s",
        "cluster_name": args.minikube_profile,
        "kube_context": args.minikube_profile,
        "aws_eks_update_kubeconfig": False,
        "aws_registry_fallback": False,
        "prometheus_bridge_enabled": False,
        "cleanup_workloads": False,
        "image_pull_policy": "IfNotPresent",
        "kafka_mode": "kubernetes",
        "kafka_bootstrap": f"{args.kafka_service}.ckc-app.svc.cluster.local:9092",
        "redis_mode": "kubernetes",
        "redis_host": f"{args.redis_service}.ckc-app.svc.cluster.local",
        "registry": "ckc-local",
        "local_log_archive_path": "/tmp/ckc-log-archive",
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
