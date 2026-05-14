#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_TOPICS = [
    {"name": "potion.orders.lifecycle.v1", "partitions": 12},
    {"name": "potion.cauldrons.telemetry.v1", "partitions": 12},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recreate CKC Kafka topics for a lab test definition.")
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--replication-factor", type=int, required=True)
    parser.add_argument("--test-definition-path", required=True)
    parser.add_argument("--repo-dir", default=os.environ.get("CKC_RUNNER_REPO_DIR", "."))
    parser.add_argument("--namespace", default="ckc-app")
    parser.add_argument("--admin-image", default="docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10")
    parser.add_argument("--topics-bin", default="/opt/bitnami/kafka/bin/kafka-topics.sh")
    return parser.parse_args()


def run(command: list[str], *, cwd: Path | None = None, input_text: str | None = None, capture_output: bool = False, check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        input=input_text,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if check and result.returncode != 0:
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    if capture_output:
        return result.stdout
    return ""


def load_definition_from_yaml(test_definition_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="ckc-definition-") as temp_dir:
        tf_dir = Path(temp_dir)
        (tf_dir / "main.tf").write_text(
            """
variable "definition_path" {
  type = string
}

output "definition_json" {
  value = jsonencode(yamldecode(file(var.definition_path)))
}
""".strip()
            + "\n",
            encoding="utf-8",
        )

        run(["terraform", "-chdir=.", "init", "-backend=false"], cwd=tf_dir)
        run(
            [
                "terraform",
                "-chdir=.",
                "apply",
                "-auto-approve",
                "-input=false",
                f"-var=definition_path={test_definition_path}",
            ],
            cwd=tf_dir,
        )
        definition_json = run(["terraform", "-chdir=.", "output", "-raw", "definition_json"], cwd=tf_dir, capture_output=True)
    return json.loads(definition_json)


def load_definition(repo_dir: Path, test_definition_path: str) -> dict[str, Any]:
    path = Path(test_definition_path)
    if not path.is_absolute():
        path = repo_dir / path
    if not path.is_file():
        raise FileNotFoundError(f"Test definition file was not found: {path}")
    return load_definition_from_yaml(path)


def topic_specs(definition: dict[str, Any]) -> list[dict[str, int | str]]:
    deployment = definition.get("deployment")
    if not isinstance(deployment, dict):
        return DEFAULT_TOPICS
    topics = deployment.get("kafka_topics")
    if topics is None:
        return DEFAULT_TOPICS
    if not isinstance(topics, list) or not topics:
        raise ValueError("deployment.kafka_topics must be a non-empty list.")

    specs: list[dict[str, int | str]] = []
    for topic in topics:
        if not isinstance(topic, dict):
            raise ValueError("Each deployment.kafka_topics entry must be an object.")
        name = str(topic.get("name", "")).strip()
        if not name:
            raise ValueError("Each deployment.kafka_topics entry must define name.")
        partitions = int(topic.get("partitions", 1))
        if partitions < 1:
            raise ValueError(f"Topic {name} must define partitions >= 1.")
        specs.append({"name": name, "partitions": partitions})
    return specs


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def admin_script(bootstrap_server: str, replication_factor: int, topics_bin: str, topics: list[dict[str, int | str]]) -> str:
    lines = [
        "set -euo pipefail",
        f"BOOTSTRAP={shell_quote(bootstrap_server)}",
        f"TOPICS_BIN={shell_quote(topics_bin)}",
        "wait_topic_deleted() {",
        "  topic=\"$1\"",
        "  for attempt in $(seq 1 60); do",
        "    if ! \"${TOPICS_BIN}\" --bootstrap-server \"${BOOTSTRAP}\" --describe --topic \"${topic}\" >/dev/null 2>&1; then",
        "      return 0",
        "    fi",
        "    sleep 2",
        "  done",
        "  echo \"Topic ${topic} was not deleted in time.\" >&2",
        "  return 1",
        "}",
        "verify_partition_count() {",
        "  topic=\"$1\"",
        "  expected=\"$2\"",
        "  for attempt in $(seq 1 30); do",
        "    description=$(\"${TOPICS_BIN}\" --bootstrap-server \"${BOOTSTRAP}\" --describe --topic \"${topic}\" 2>/dev/null || true)",
        "    actual=$(printf '%s\\n' \"${description}\" | sed -n 's/.*PartitionCount: \\([0-9][0-9]*\\).*/\\1/p' | head -n 1)",
        "    if [ \"${actual}\" = \"${expected}\" ]; then",
        "      printf '%s\\n' \"${description}\"",
        "      return 0",
        "    fi",
        "    sleep 2",
        "  done",
        "  echo \"Topic ${topic} does not have expected partition count ${expected}.\" >&2",
        "  \"${TOPICS_BIN}\" --bootstrap-server \"${BOOTSTRAP}\" --describe --topic \"${topic}\" >&2 || true",
        "  return 1",
        "}",
    ]
    for topic in topics:
        name = str(topic["name"])
        partitions = int(topic["partitions"])
        quoted_name = shell_quote(name)
        lines.extend(
            [
                f'echo "Deleting topic {name} if it exists."',
                f'"${{TOPICS_BIN}}" --bootstrap-server "${{BOOTSTRAP}}" --delete --if-exists --topic {quoted_name} || true',
                f"wait_topic_deleted {quoted_name}",
                f'echo "Creating topic {name} with {partitions} partitions."',
                (
                    f'"${{TOPICS_BIN}}" --bootstrap-server "${{BOOTSTRAP}}" --create --topic {quoted_name} '
                    f"--partitions {partitions} --replication-factor {replication_factor}"
                ),
                f"verify_partition_count {quoted_name} {partitions}",
            ]
        )
    return "\n".join(lines) + "\n"


def indent_block(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else prefix for line in value.splitlines())


def recreate_topics(args: argparse.Namespace, topics: list[dict[str, int | str]]) -> None:
    script = admin_script(args.bootstrap_server, args.replication_factor, args.topics_bin, topics)
    manifest = f"""apiVersion: v1
kind: Pod
metadata:
  name: ckc-kafka-admin
  namespace: {args.namespace}
  labels:
    app.kubernetes.io/name: ckc-kafka-admin
spec:
  restartPolicy: Never
  containers:
    - name: kafka-admin
      image: {args.admin_image}
      command:
        - /bin/bash
        - -lc
        - |
{indent_block(script, 10)}
"""
    run(["kubectl", "-n", args.namespace, "delete", "pod", "ckc-kafka-admin", "--ignore-not-found=true"], check=False)
    run(["kubectl", "apply", "-f", "-"], input_text=manifest)
    run(["kubectl", "-n", args.namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/ckc-kafka-admin", "--timeout=10m"])
    run(["kubectl", "-n", args.namespace, "logs", "pod/ckc-kafka-admin"], check=False)
    run(["kubectl", "-n", args.namespace, "delete", "pod", "ckc-kafka-admin", "--ignore-not-found=true"], check=False)


def main() -> None:
    args = parse_args()
    repo_dir = Path(args.repo_dir)
    temp_dir = Path(os.environ.get("CKC_DEMO_INFRA_TMP_DIR", str(repo_dir / ".demo-infra" / "tmp")))
    temp_dir.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(temp_dir)
    definition = load_definition(repo_dir, args.test_definition_path)
    topics = topic_specs(definition)
    recreate_topics(args, topics)
    print(f"Kafka topics recreated for test definition '{definition.get('name', 'unnamed')}'.")


if __name__ == "__main__":
    main()
