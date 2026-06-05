#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

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


@dataclass
class AuditStats:
    published_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    duplicate_published: int = 0
    published_by_key: dict[RecordKey, AuditRecord] = field(default_factory=dict)
    processed_counts: Counter[RecordKey] = field(default_factory=Counter)
    failed_counts: Counter[RecordKey] = field(default_factory=Counter)
    processed_by_key: dict[RecordKey, list[AuditRecord]] = field(default_factory=dict)
    failed_by_key: dict[RecordKey, list[AuditRecord]] = field(default_factory=dict)
    process_latency: list[int] = field(default_factory=list)
    failure_latency: list[int] = field(default_factory=list)
    terminal_latency: list[int] = field(default_factory=list)
    kafka_to_process_age: list[int] = field(default_factory=list)
    kafka_to_failure_age: list[int] = field(default_factory=list)
    kafka_to_terminal_age: list[int] = field(default_factory=list)

    def add(self, record: AuditRecord) -> None:
        if record.record_type == "P":
            self.published_records += 1
            if record.key.partition < 0 or record.key.offset < 0:
                return
            if record.key in self.published_by_key:
                self.duplicate_published += 1
            else:
                self.published_by_key[record.key] = record
                for processed in self.processed_by_key.get(record.key, []):
                    self._add_processed_latency(processed, record)
                for failed in self.failed_by_key.get(record.key, []):
                    self._add_failed_latency(failed, record)
            return

        if record.record_type == "C":
            self.processed_records += 1
            self.processed_counts[record.key] += 1
            self.processed_by_key.setdefault(record.key, []).append(record)
            published = self.published_by_key.get(record.key)
            if published is not None:
                self._add_processed_latency(record, published)
            return

        if record.record_type == "F":
            self.failed_records += 1
            self.failed_counts[record.key] += 1
            self.failed_by_key.setdefault(record.key, []).append(record)
            published = self.published_by_key.get(record.key)
            if published is not None:
                self._add_failed_latency(record, published)

    def _add_processed_latency(self, record: AuditRecord, published: AuditRecord) -> None:
        latency = record.audit_timestamp_ms - published.audit_timestamp_ms
        self.process_latency.append(latency)
        self.terminal_latency.append(latency)
        if published.kafka_timestamp_ms is not None:
            age = record.audit_timestamp_ms - published.kafka_timestamp_ms
            self.kafka_to_process_age.append(age)
            self.kafka_to_terminal_age.append(age)

    def _add_failed_latency(self, record: AuditRecord, published: AuditRecord) -> None:
        latency = record.audit_timestamp_ms - published.audit_timestamp_ms
        self.failure_latency.append(latency)
        self.terminal_latency.append(latency)
        if published.kafka_timestamp_ms is not None:
            age = record.audit_timestamp_ms - published.kafka_timestamp_ms
            self.kafka_to_failure_age.append(age)
            self.kafka_to_terminal_age.append(age)


class AuditAccumulator:
    def __init__(self) -> None:
        self.all = AuditStats()
        self.by_topic = {topic_id: AuditStats() for topic_id in TOPIC_NAMES}
        self.record_count = 0

    def add(self, record: AuditRecord) -> None:
        self.record_count += 1
        self.all.add(record)
        topic_stats = self.by_topic.get(record.key.topic_id)
        if topic_stats is not None:
            topic_stats.add(record)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze CKC demo audit records stored in archived audit files.")
    parser.add_argument("--input-file", action="append", default=[])
    parser.add_argument("--input-dir")
    parser.add_argument("--glob", default="*.log.gz")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--stop-file")
    parser.add_argument("--pending-dir")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--idle-seconds", type=float, default=5.0)
    parser.add_argument("--require-records", action="store_true")
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


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def read_path(path: Path, accumulator: AuditAccumulator) -> int:
    count = 0
    print(f"Reading audit records from {path}.", file=sys.stderr, flush=True)
    with open_text(path) as file:
        for line in file:
            if not line.strip():
                continue
            accumulator.add(parse_record(line))
            count += 1
    return count


def read_files(paths: list[str], accumulator: AuditAccumulator) -> None:
    for path_value in paths:
        path = Path(path_value)
        if not path.is_file():
            raise ValueError(f"audit input file was not found: {path}")
        read_path(path, accumulator)


def list_chunks(input_dir: Path, glob: str) -> list[Path]:
    if not input_dir.is_dir():
        return []
    return sorted(path for path in input_dir.glob(glob) if path.is_file())


def has_pending_parts(pending_dir: Path | None) -> bool:
    if pending_dir is None or not pending_dir.is_dir():
        return False
    return any(pending_dir.glob("*.part"))


def watch_chunks(args: argparse.Namespace, accumulator: AuditAccumulator) -> None:
    input_dir = Path(args.input_dir)
    stop_file = Path(args.stop_file) if args.stop_file else None
    pending_dir = Path(args.pending_dir) if args.pending_dir else input_dir
    seen: set[Path] = set()
    last_new_chunk = time.monotonic()

    while True:
        new_chunks = [path for path in list_chunks(input_dir, args.glob) if path not in seen]
        if new_chunks:
            for path in new_chunks:
                read_path(path, accumulator)
                seen.add(path)
            last_new_chunk = time.monotonic()
            print(
                f"Audit analyzer progress: chunks={len(seen)} records={accumulator.record_count}",
                file=sys.stderr,
                flush=True,
            )

        stopped = stop_file is not None and stop_file.exists()
        idle = time.monotonic() - last_new_chunk >= args.idle_seconds
        if stopped and idle and not has_pending_parts(pending_dir):
            break

        if not args.watch and not new_chunks:
            break
        time.sleep(args.poll_seconds)


def percentile(values: list[int], percent: float) -> int:
    if not values:
        return 0
    values.sort()
    index = round((len(values) - 1) * percent)
    return values[index]


def format_percentiles(values: list[int]) -> str:
    return (
        f"p50={percentile(values, 0.50)} p95={percentile(values, 0.95)} "
        f"p99={percentile(values, 0.99)} max={values[-1] if values else 0}"
    )


def print_summary(title: str, stats: AuditStats) -> None:
    processed_unique = set(stats.processed_counts)
    failed_unique = set(stats.failed_counts)
    terminal_unique = processed_unique | failed_unique
    published_keys = set(stats.published_by_key)

    missing_terminal = published_keys - terminal_unique
    processed_without_publish = processed_unique - published_keys
    failed_without_publish = failed_unique - published_keys
    conflicting_terminal = processed_unique & failed_unique
    duplicate_processed = sum(count - 1 for count in stats.processed_counts.values() if count > 1)
    duplicate_failed = sum(count - 1 for count in stats.failed_counts.values() if count > 1)

    print(title)
    print(f"  published_records={stats.published_records}")
    print(f"  published_unique={len(published_keys)}")
    print(f"  processed_records={stats.processed_records}")
    print(f"  processed_unique={len(processed_unique)}")
    print(f"  failed_records={stats.failed_records}")
    print(f"  failed_unique={len(failed_unique)}")
    print(f"  terminal_unique={len(terminal_unique)}")
    print(f"  missing_terminal={len(missing_terminal)}")
    print(f"  duplicate_published={stats.duplicate_published}")
    print(f"  duplicate_processed={duplicate_processed}")
    print(f"  duplicate_failed={duplicate_failed}")
    print(f"  processed_without_publish={len(processed_without_publish)}")
    print(f"  failed_without_publish={len(failed_without_publish)}")
    print(f"  conflicting_terminal_outcomes={len(conflicting_terminal)}")
    print(f"  publish_to_process_latency_ms={format_percentiles(stats.process_latency)}")
    print(f"  publish_to_failure_latency_ms={format_percentiles(stats.failure_latency)}")
    print(f"  publish_to_terminal_latency_ms={format_percentiles(stats.terminal_latency)}")
    print(f"  kafka_to_process_age_ms={format_percentiles(stats.kafka_to_process_age)}")
    print(f"  kafka_to_failure_age_ms={format_percentiles(stats.kafka_to_failure_age)}")
    print(f"  kafka_to_terminal_age_ms={format_percentiles(stats.kafka_to_terminal_age)}")


def main() -> int:
    args = parse_args()
    if not args.input_file and not args.input_dir:
        raise ValueError("at least one --input-file or --input-dir is required")
    if args.watch and not args.input_dir:
        raise ValueError("--watch requires --input-dir")
    if args.watch and not args.stop_file:
        raise ValueError("--watch requires --stop-file")

    accumulator = AuditAccumulator()
    read_files(args.input_file, accumulator)
    if args.input_dir:
        watch_chunks(args, accumulator)

    print(f"Read {accumulator.record_count} audit records.", file=sys.stderr, flush=True)
    if args.require_records and accumulator.record_count == 0:
        raise ValueError("no audit records were read")

    print_summary("Audit summary", accumulator.all)
    for topic_id, topic_name in TOPIC_NAMES.items():
        print()
        print_summary(f"Topic summary: {topic_name}", accumulator.by_topic[topic_id])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"audit analysis failed: {error}", file=sys.stderr)
        raise
