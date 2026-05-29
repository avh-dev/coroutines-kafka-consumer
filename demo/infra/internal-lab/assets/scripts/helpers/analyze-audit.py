#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RecordKey:
    topic_id: int
    partition: int
    offset: int


@dataclass(frozen=True)
class AuditRecord:
    record_type: str
    key: RecordKey
    kafka_timestamp_ms: int
    audit_timestamp_ms: int
    message_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze CKC demo audit TSV files.")
    parser.add_argument("--published-dir", required=True, type=Path)
    parser.add_argument("--processed-dir", required=True, type=Path)
    return parser.parse_args()


def read_records(directory: Path, expected_type: str) -> list[AuditRecord]:
    if not directory.is_dir():
        return []

    records: list[AuditRecord] = []
    for path in sorted(directory.glob("*.tsv")):
        with path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t", 6)
                if len(parts) != 7:
                    raise ValueError(f"{path}:{line_number}: expected 7 TSV fields, got {len(parts)}")
                record_type, topic_id, partition, offset, kafka_ts, audit_ts, message_key = parts
                if record_type != expected_type:
                    raise ValueError(f"{path}:{line_number}: expected record type {expected_type}, got {record_type}")
                records.append(
                    AuditRecord(
                        record_type=record_type,
                        key=RecordKey(int(topic_id), int(partition), int(offset)),
                        kafka_timestamp_ms=int(kafka_ts),
                        audit_timestamp_ms=int(audit_ts),
                        message_key=message_key,
                    )
                )
    return records


def percentile(values: list[int], percent: float) -> int:
    if not values:
        return 0
    index = round((len(values) - 1) * percent)
    return values[index]


def print_counter(name: str, counter: Counter[int]) -> None:
    parts = ", ".join(f"{topic_id}={count}" for topic_id, count in sorted(counter.items()))
    print(f"{name}: {parts if parts else '-'}")


def main() -> int:
    args = parse_args()
    published = read_records(args.published_dir, "P")
    processed = read_records(args.processed_dir, "C")

    published_by_key: dict[RecordKey, AuditRecord] = {}
    duplicate_published = 0
    for record in published:
        if record.key.partition < 0 or record.key.offset < 0:
            continue
        if record.key in published_by_key:
            duplicate_published += 1
        else:
            published_by_key[record.key] = record

    processed_counts = Counter(record.key for record in processed)
    processed_unique = set(processed_counts)
    published_keys = set(published_by_key)

    missing = published_keys - processed_unique
    unknown_processed = processed_unique - published_keys
    duplicate_processed = sum(count - 1 for count in processed_counts.values() if count > 1)

    latencies = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].audit_timestamp_ms
        for record in processed
        if record.key in published_by_key
    )
    kafka_age = sorted(
        record.audit_timestamp_ms - record.kafka_timestamp_ms
        for record in processed
        if record.kafka_timestamp_ms > 0
    )

    print("Audit summary")
    print(f"  published_records={len(published)}")
    print(f"  published_unique={len(published_keys)}")
    print(f"  processed_records={len(processed)}")
    print(f"  processed_unique={len(processed_unique)}")
    print(f"  missing_processed={len(missing)}")
    print(f"  duplicate_published={duplicate_published}")
    print(f"  duplicate_processed={duplicate_processed}")
    print(f"  processed_without_publish={len(unknown_processed)}")
    print_counter("  published_by_topic", Counter(record.key.topic_id for record in published_by_key.values()))
    print_counter("  processed_by_topic", Counter(record.key.topic_id for record in processed))
    print(
        "  publish_to_process_latency_ms="
        f"p50={percentile(latencies, 0.50)} p95={percentile(latencies, 0.95)} "
        f"p99={percentile(latencies, 0.99)} max={latencies[-1] if latencies else 0}"
    )
    print(
        "  kafka_to_process_age_ms="
        f"p50={percentile(kafka_age, 0.50)} p95={percentile(kafka_age, 0.95)} "
        f"p99={percentile(kafka_age, 0.99)} max={kafka_age[-1] if kafka_age else 0}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"audit analysis failed: {error}", file=sys.stderr)
        raise
