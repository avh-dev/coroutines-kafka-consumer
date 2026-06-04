#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

TOPIC_NAMES = {
    1: "order.events.v1",
    2: "batch.events.v1",
    3: "cauldron.events.v1",
}


@dataclass(frozen=True)
class RecordKey:
    topic_id: int
    partition: int
    offset: int


@dataclass(frozen=True)
class AuditRecord:
    record_type: str
    key: RecordKey
    audit_timestamp_ms: int
    message_key: str
    kafka_timestamp_ms: int | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze CKC demo audit records stored in archived audit files.")
    parser.add_argument("--input-file", action="append", required=True)
    return parser.parse_args()


def parse_record(value: str) -> AuditRecord:
    parts = value.rstrip("\n").split("|")
    record_type = parts[0] if parts else ""

    if record_type == "P":
        if len(parts) != 7:
            raise ValueError(f"expected 7 publish audit fields, got {len(parts)}: {value!r}")
        _, topic_id, partition, offset, kafka_ts, audit_ts, message_key = parts
        return AuditRecord(
            record_type=record_type,
            key=RecordKey(int(topic_id), int(partition), int(offset)),
            kafka_timestamp_ms=int(kafka_ts),
            audit_timestamp_ms=int(audit_ts),
            message_key=message_key,
        )

    if record_type in {"C", "F"}:
        if len(parts) != 6:
            raise ValueError(f"expected 6 consumer audit fields, got {len(parts)}: {value!r}")
        _, topic_id, partition, offset, audit_ts, message_key = parts
        return AuditRecord(
            record_type=record_type,
            key=RecordKey(int(topic_id), int(partition), int(offset)),
            kafka_timestamp_ms=None,
            audit_timestamp_ms=int(audit_ts),
            message_key=message_key,
        )

    raise ValueError(f"unexpected audit record type: {record_type}")


def read_records(paths: list[str]) -> list[AuditRecord]:
    records: list[AuditRecord] = []
    for path_value in paths:
        path = Path(path_value)
        if not path.is_file():
            raise ValueError(f"audit input file was not found: {path}")
        print(f"Reading audit records from {path}.", file=sys.stderr)
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                records.append(parse_record(line))
    print(f"Read {len(records)} audit records.", file=sys.stderr)
    return records


def percentile(values: list[int], percent: float) -> int:
    if not values:
        return 0
    index = round((len(values) - 1) * percent)
    return values[index]


def format_percentiles(values: list[int]) -> str:
    return (
        f"p50={percentile(values, 0.50)} p95={percentile(values, 0.95)} "
        f"p99={percentile(values, 0.99)} max={values[-1] if values else 0}"
    )


def print_summary(title: str, records: list[AuditRecord]) -> None:
    published = [record for record in records if record.record_type == "P"]
    processed = [record for record in records if record.record_type == "C"]
    failed = [record for record in records if record.record_type == "F"]
    terminal = processed + failed

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
    failed_counts = Counter(record.key for record in failed)
    processed_unique = set(processed_counts)
    failed_unique = set(failed_counts)
    terminal_unique = processed_unique | failed_unique
    published_keys = set(published_by_key)

    missing_terminal = published_keys - terminal_unique
    processed_without_publish = processed_unique - published_keys
    failed_without_publish = failed_unique - published_keys
    conflicting_terminal = processed_unique & failed_unique
    duplicate_processed = sum(count - 1 for count in processed_counts.values() if count > 1)
    duplicate_failed = sum(count - 1 for count in failed_counts.values() if count > 1)

    process_latency = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].audit_timestamp_ms
        for record in processed
        if record.key in published_by_key
    )
    failure_latency = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].audit_timestamp_ms
        for record in failed
        if record.key in published_by_key
    )
    terminal_latency = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].audit_timestamp_ms
        for record in terminal
        if record.key in published_by_key
    )
    kafka_to_process_age = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].kafka_timestamp_ms
        for record in processed
        if record.key in published_by_key and published_by_key[record.key].kafka_timestamp_ms is not None
    )
    kafka_to_failure_age = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].kafka_timestamp_ms
        for record in failed
        if record.key in published_by_key and published_by_key[record.key].kafka_timestamp_ms is not None
    )
    kafka_to_terminal_age = sorted(
        record.audit_timestamp_ms - published_by_key[record.key].kafka_timestamp_ms
        for record in terminal
        if record.key in published_by_key and published_by_key[record.key].kafka_timestamp_ms is not None
    )

    print(title)
    print(f"  published_records={len(published)}")
    print(f"  published_unique={len(published_keys)}")
    print(f"  processed_records={len(processed)}")
    print(f"  processed_unique={len(processed_unique)}")
    print(f"  failed_records={len(failed)}")
    print(f"  failed_unique={len(failed_unique)}")
    print(f"  terminal_unique={len(terminal_unique)}")
    print(f"  missing_terminal={len(missing_terminal)}")
    print(f"  duplicate_published={duplicate_published}")
    print(f"  duplicate_processed={duplicate_processed}")
    print(f"  duplicate_failed={duplicate_failed}")
    print(f"  processed_without_publish={len(processed_without_publish)}")
    print(f"  failed_without_publish={len(failed_without_publish)}")
    print(f"  conflicting_terminal_outcomes={len(conflicting_terminal)}")
    print(f"  publish_to_process_latency_ms={format_percentiles(process_latency)}")
    print(f"  publish_to_failure_latency_ms={format_percentiles(failure_latency)}")
    print(f"  publish_to_terminal_latency_ms={format_percentiles(terminal_latency)}")
    print(f"  kafka_to_process_age_ms={format_percentiles(kafka_to_process_age)}")
    print(f"  kafka_to_failure_age_ms={format_percentiles(kafka_to_failure_age)}")
    print(f"  kafka_to_terminal_age_ms={format_percentiles(kafka_to_terminal_age)}")


def main() -> int:
    args = parse_args()
    records = read_records(args.input_file)

    print_summary("Audit summary", records)
    for topic_id, topic_name in TOPIC_NAMES.items():
        print()
        print_summary(
            f"Topic summary: {topic_name}",
            [record for record in records if record.key.topic_id == topic_id],
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"audit analysis failed: {error}", file=sys.stderr)
        raise
