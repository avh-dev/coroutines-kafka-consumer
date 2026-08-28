#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from experiment_events import append_event, publish_grafana_annotation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record and publish one Grafana annotation for a started test run.")
    parser.add_argument("--metadata", required=True)
    return parser.parse_args()


def compact(value: Any) -> str:
    return str(value) if value not in (None, "") else "-"


def effective_producer(metadata: dict[str, Any], topic: str) -> dict[str, Any]:
    load_test = metadata.get("load_test") if isinstance(metadata.get("load_test"), dict) else {}
    shared = load_test.get("kafka_producer") if isinstance(load_test.get("kafka_producer"), dict) else {}
    topics = load_test.get("topic_kafka_producers") if isinstance(load_test.get("topic_kafka_producers"), dict) else {}
    override = topics.get(topic) if isinstance(topics.get(topic), dict) else {}
    return {
        key: override.get(key) if override.get(key) not in (None, "") else shared.get(key)
        for key in ("linger_ms", "batch_size", "compression_type", "buffer_memory")
    }


def effective_consumer(metadata: dict[str, Any], topic: str) -> dict[str, Any]:
    kafka = metadata.get("kafka") if isinstance(metadata.get("kafka"), dict) else {}
    shared = kafka.get("consumer") if isinstance(kafka.get("consumer"), dict) else {}
    topics = kafka.get("topic_consumers") if isinstance(kafka.get("topic_consumers"), dict) else {}
    override = topics.get(topic) if isinstance(topics.get(topic), dict) else {}
    return {
        key: override.get(key) if override.get(key) not in (None, "") else shared.get(key)
        for key in (
            "fetch_min_bytes",
            "fetch_max_wait_ms",
            "max_poll_records",
            "fetch_max_bytes",
            "max_partition_fetch_bytes",
        )
    }


def active_topics(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    run_plan = metadata.get("run_plan") if isinstance(metadata.get("run_plan"), dict) else {}
    topics = run_plan.get("topics") if isinstance(run_plan.get("topics"), list) else []
    active = [topic for topic in topics if isinstance(topic, dict) and float(topic.get("target_tps") or 0) > 0]
    return active or [topic for topic in topics if isinstance(topic, dict)]


def run_started_event(metadata: dict[str, Any]) -> dict[str, Any]:
    experiment = metadata.get("experiment") if isinstance(metadata.get("experiment"), dict) else {}
    application = metadata.get("application") if isinstance(metadata.get("application"), dict) else {}
    load_test = metadata.get("load_test") if isinstance(metadata.get("load_test"), dict) else {}
    target = str(experiment.get("target") or metadata.get("test_definition") or metadata.get("deployment") or "test run")
    experiment_name = str(experiment.get("name") or "")
    profile = str(application.get("profile") or application.get("run_profile") or metadata.get("deployment") or "")
    topics = active_topics(metadata)
    topic_text = []
    for topic in topics:
        topic_text.append(
            f"{topic.get('name', 'topic')}:p{compact(topic.get('partitions'))}"
            f"c{compact(topic.get('poll_loop_concurrency'))}w{compact(topic.get('worker_concurrency'))}"
        )
    producer_topic = str(topics[0].get("name")) if len(topics) == 1 else ""
    producer = effective_producer(metadata, producer_topic)
    consumer = effective_consumer(metadata, producer_topic)
    parts = [
        f"Run started · {target}",
        f"run={metadata.get('run_id')}",
        f"profile={profile}",
        f"tps={compact(load_test.get('base_tps'))}",
        f"replicas={compact(application.get('replica_count'))}",
    ]
    if experiment_name:
        parts.insert(1, f"experiment={experiment_name}")
    if topic_text:
        parts.append("; ".join(topic_text))
    target_index = experiment.get("target_index")
    target_total = experiment.get("target_total")
    if target_index not in (None, "") and target_total not in (None, ""):
        parts.insert(2 if experiment_name else 1, f"target={target_index}/{target_total}")
    parts.extend(
        [
            f"compression={compact(producer.get('compression_type'))}",
            f"linger.ms={compact(producer.get('linger_ms'))}",
            f"batch.size={compact(producer.get('batch_size'))}",
            f"buffer.memory={compact(producer.get('buffer_memory'))}",
            f"max.poll.records={compact(consumer.get('max_poll_records'))}",
            f"fetch.min.bytes={compact(consumer.get('fetch_min_bytes'))}",
            f"fetch.max.wait.ms={compact(consumer.get('fetch_max_wait_ms'))}",
            f"load={compact(load_test.get('load_profile'))}",
        ]
    )
    tags = ["ckc-run", f"target:{target}", f"profile:{profile}"]
    if experiment_name:
        tags.append(f"experiment:{experiment_name}")
    if producer.get("compression_type") not in (None, ""):
        tags.append(f"compression:{producer['compression_type']}")
    if producer.get("linger_ms") not in (None, ""):
        tags.append(f"linger:{producer['linger_ms']}")
    return {
        "runId": str(metadata.get("run_id") or ""),
        "source": "orchestration",
        "type": "run_started",
        "status": "started",
        "title": f"Run started · {target}",
        "text": " · ".join(parts),
        "annotationTags": tags,
        "details": {
            "experiment": experiment_name,
            "target": target,
            "profile": profile,
            "baseTps": load_test.get("base_tps"),
            "replicas": application.get("replica_count"),
            "topics": topics,
            "producer": producer,
            "consumer": consumer,
            "loadProfile": load_test.get("load_profile"),
        },
    }


def main() -> int:
    args = parse_args()
    metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    event = append_event(run_started_event(metadata), publish_annotation=False)
    publish_grafana_annotation(event, "EXPERIMENT_GRAFANA_RUN_ANNOTATIONS_ENABLED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
