#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WARMUP_DURATION_SECONDS = 180
WARMUP_RATE = 10_000
WARMUP_RECORD_SIZE = 1024
WARMUP_PARTITIONS = 12
WARMUP_IMAGE = "docker.io/apache/kafka:4.3.1"


def warmup_record_count() -> int:
    return WARMUP_DURATION_SECONDS * WARMUP_RATE


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fixed CKC Kafka broker warm-up workload.")
    parser.add_argument("--backend", required=True, choices=("docker", "kubernetes"))
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--replication-factor", type=int, required=True)
    parser.add_argument("--reason", default="Kafka runtime was redeployed")
    parser.add_argument("--experiment", default=os.environ.get("EXPERIMENT_NAME", "ckc experiment"))
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--notify-hook", type=Path)
    parser.add_argument("--notification-dir", type=Path)
    parser.add_argument("--grafana-url", default=os.environ.get("EXPERIMENT_GRAFANA_URL", "http://127.0.0.1:3000"))
    parser.add_argument("--docker-container")
    parser.add_argument("--namespace", default="ckc-loadtest")
    parser.add_argument("--kubeconfig")
    return parser.parse_args()


def notification_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "experiment": args.experiment,
        "duration_seconds": WARMUP_DURATION_SECONDS,
        "rate": WARMUP_RATE,
        "record_size_bytes": WARMUP_RECORD_SIZE,
        "partitions": WARMUP_PARTITIONS,
        "replication_factor": args.replication_factor,
        "reason": args.reason,
    }


def notify_started(args: argparse.Namespace) -> None:
    if args.notify_hook is None:
        return
    shared_root = Path(__file__).resolve().parents[1]
    if str(shared_root) not in sys.path:
        sys.path.insert(0, str(shared_root))
    from experiment_notifications import notify

    notify(
        args.notify_hook,
        "kafka_warmup_started",
        notification_payload(args),
        args.notification_dir or args.log_file.parent,
    )


def annotate_started(args: argparse.Namespace) -> None:
    if not args.grafana_url:
        return
    timestamp = datetime.now(timezone.utc)
    payload = {
        "dashboardUID": "ckc-overview",
        "time": int(timestamp.timestamp() * 1000),
        "tags": ["kafka-warmup", "phase:warmup"],
        "text": (
            f"Kafka warm-up started · {WARMUP_DURATION_SECONDS}s · {WARMUP_RATE} records/s · "
            f"{WARMUP_RECORD_SIZE} bytes · {args.reason}"
        ),
    }
    request = urllib.request.Request(
        f"{args.grafana_url.rstrip('/')}/api/annotations",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(b"admin:admin").decode("ascii"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5):
            pass
    except (OSError, urllib.error.URLError) as error:
        print(f"Kafka warm-up annotation failed: {error}", file=sys.stderr)


class KafkaExecutor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pod = f"ckc-kafka-warmup-{os.getpid()}"
        self.environment = dict(os.environ)
        if args.kubeconfig:
            self.environment["KUBECONFIG"] = args.kubeconfig

    def start(self) -> None:
        if self.args.backend == "docker":
            if not self.args.docker_container:
                raise ValueError("--docker-container is required for the docker backend")
            return
        subprocess.run([
            "kubectl", "-n", self.args.namespace, "run", self.pod,
            f"--image={WARMUP_IMAGE}", "--restart=Never", "--command", "--", "sleep", "600",
        ], check=True, env=self.environment)
        subprocess.run([
            "kubectl", "-n", self.args.namespace, "wait", "--for=condition=Ready",
            f"pod/{self.pod}", "--timeout=5m",
        ], check=True, env=self.environment)

    def stop(self) -> None:
        if self.args.backend == "kubernetes":
            subprocess.run([
                "kubectl", "-n", self.args.namespace, "delete", "pod", self.pod,
                "--ignore-not-found=true", "--wait=false",
            ], check=False, env=self.environment, stdout=subprocess.DEVNULL)

    def command(self, binary: str, *arguments: str) -> list[str]:
        if self.args.backend == "docker":
            prefix = ["docker", "exec", str(self.args.docker_container), "env", "KAFKA_OPTS="]
            binary_path = f"/opt/kafka/bin/{binary}"
        else:
            prefix = ["kubectl", "-n", self.args.namespace, "exec", self.pod, "--", "env", "KAFKA_OPTS="]
            binary_path = f"/opt/kafka/bin/{binary}"
        return [*prefix, binary_path, *arguments]

    def run(self, binary: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.command(binary, *arguments),
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.environment,
        )


def run_warmup(args: argparse.Namespace) -> None:
    if args.replication_factor < 1:
        raise ValueError("replication factor must be positive")
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    topic = f"ckc.warmup.v1.{int(time.time())}"
    group = f"ckc-warmup-v1-{int(time.time())}"
    messages = warmup_record_count()
    executor = KafkaExecutor(args)
    consumer: subprocess.Popen[str] | None = None
    with args.log_file.open("a", encoding="utf-8") as log:
        log.write(f"{datetime.now(timezone.utc).isoformat()} warm-up start topic={topic} reason={args.reason}\n")
        try:
            executor.start()
            notify_started(args)
            annotate_started(args)
            created = executor.run(
                "kafka-topics.sh", "--bootstrap-server", args.bootstrap_server,
                "--create", "--if-not-exists", "--topic", topic,
                "--partitions", str(WARMUP_PARTITIONS),
                "--replication-factor", str(args.replication_factor),
                "--config", "retention.ms=600000",
            )
            log.write(created.stdout)
            consumer = subprocess.Popen(
                executor.command(
                    "kafka-consumer-perf-test.sh", "--bootstrap-server", args.bootstrap_server,
                    "--topic", topic, "--group", group, "--num-records", str(messages),
                    "--timeout", str((WARMUP_DURATION_SECONDS + 120) * 1000), "--show-detailed-stats",
                ),
                text=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=executor.environment,
            )
            time.sleep(3)
            produced = executor.run(
                "kafka-producer-perf-test.sh", "--topic", topic,
                "--num-records", str(messages), "--record-size", str(WARMUP_RECORD_SIZE),
                "--throughput", str(WARMUP_RATE), "--producer-props",
                f"bootstrap.servers={args.bootstrap_server}", "acks=all", "compression.type=lz4",
            )
            log.write(produced.stdout)
            consumer_exit = consumer.wait(timeout=120)
            if consumer_exit != 0:
                raise RuntimeError(f"Kafka warm-up consumer exited with status {consumer_exit}")
            log.write(f"{datetime.now(timezone.utc).isoformat()} warm-up completed records={messages}\n")
        finally:
            if consumer is not None and consumer.poll() is None:
                consumer.send_signal(signal.SIGTERM)
                try:
                    consumer.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    consumer.kill()
                    consumer.wait()
            deleted = executor.run(
                "kafka-topics.sh", "--bootstrap-server", args.bootstrap_server,
                "--delete", "--if-exists", "--topic", topic,
                check=False,
            )
            log.write(deleted.stdout)
            executor.run(
                "kafka-consumer-groups.sh", "--bootstrap-server", args.bootstrap_server,
                "--delete", "--group", group,
                check=False,
            )
            executor.stop()


def main() -> int:
    run_warmup(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
