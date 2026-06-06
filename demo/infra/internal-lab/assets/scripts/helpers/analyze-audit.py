#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple, TextIO

TOPIC_NAMES = {
    1: "order.events.v1",
    2: "batch.events.v1",
    3: "cauldron.events.v1",
}

LATENCY_BUCKETS_MS = [
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    30_000,
    60_000,
    120_000,
    300_000,
    600_000,
]


class RecordKey(NamedTuple):
    topic_id: int
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class AuditRecord:
    record_type: str
    key: RecordKey
    audit_timestamp_ms: int
    message_key: str
    kafka_timestamp_ms: int | None = None


@dataclass(slots=True)
class LastOffsetState:
    offset: int
    updated_ms: int


@dataclass(slots=True)
class ProcessingOrderStats:
    terminal_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    out_of_order_terminal: int = 0
    out_of_order_processed: int = 0
    out_of_order_failed: int = 0
    last_offset_by_partition: dict[tuple[int, int], LastOffsetState] = field(default_factory=dict)
    last_offset_by_key: dict[tuple[int, int, str], LastOffsetState] = field(default_factory=dict)
    key_expiry_queue: deque[tuple[int, tuple[int, int, str]]] = field(default_factory=deque)

    def add_partition_order(self, record: AuditRecord) -> None:
        self._add(record, self.last_offset_by_partition, (record.key.topic_id, record.key.partition), False)

    def add_key_order(self, record: AuditRecord) -> None:
        self._add(record, self.last_offset_by_key, (record.key.topic_id, record.key.partition, record.message_key), True)

    def evict_before(self, cutoff_ms: int) -> None:
        self._evict_key_order(cutoff_ms)
        # Partition state is tiny and keeping it preserves full-run partition-order checks.

    def _add(
        self,
        record: AuditRecord,
        last_offsets: dict[object, LastOffsetState],
        group: object,
        track_expiry: bool,
    ) -> None:
        self.terminal_records += 1
        if record.record_type == "C":
            self.processed_records += 1
        elif record.record_type == "F":
            self.failed_records += 1

        last = last_offsets.get(group)
        if last is not None and record.key.offset < last.offset:
            self.out_of_order_terminal += 1
            if record.record_type == "C":
                self.out_of_order_processed += 1
            elif record.record_type == "F":
                self.out_of_order_failed += 1
            return

        if last is None or record.key.offset > last.offset:
            last_offsets[group] = LastOffsetState(record.key.offset, record.audit_timestamp_ms)
            if track_expiry:
                self.key_expiry_queue.append((record.audit_timestamp_ms, group))
        elif last is not None:
            last.updated_ms = record.audit_timestamp_ms
            if track_expiry:
                self.key_expiry_queue.append((record.audit_timestamp_ms, group))

    def _evict_key_order(self, cutoff_ms: int) -> None:
        while self.key_expiry_queue and self.key_expiry_queue[0][0] < cutoff_ms:
            updated_ms, group = self.key_expiry_queue.popleft()
            state = self.last_offset_by_key.get(group)
            if state is not None and state.updated_ms == updated_ms:
                del self.last_offset_by_key[group]


@dataclass(slots=True)
class LatencyHistogram:
    buckets: list[int] = field(default_factory=lambda: [0] * (len(LATENCY_BUCKETS_MS) + 1))
    count: int = 0
    max_value: int = 0

    def add(self, value: int) -> None:
        self.count += 1
        self.max_value = max(self.max_value, value)
        for index, bound in enumerate(LATENCY_BUCKETS_MS):
            if value <= bound:
                self.buckets[index] += 1
                return
        self.buckets[-1] += 1

    def percentile(self, percent: float) -> int:
        if self.count == 0:
            return 0
        target = max(1, round(self.count * percent))
        seen = 0
        for index, bucket_count in enumerate(self.buckets):
            seen += bucket_count
            if seen >= target:
                if index < len(LATENCY_BUCKETS_MS):
                    return min(LATENCY_BUCKETS_MS[index], self.max_value)
                return self.max_value
        return self.max_value

    def format(self) -> str:
        return (
            f"p50={self.percentile(0.50)} p95={self.percentile(0.95)} "
            f"p99={self.percentile(0.99)} max={self.max_value if self.count else 0} count={self.count}"
        )


@dataclass(slots=True)
class RecordState:
    first_seen_ms: int
    last_seen_ms: int
    published: AuditRecord | None = None
    processed: AuditRecord | None = None
    failed: AuditRecord | None = None
    processed_count: int = 0
    failed_count: int = 0
    conflict_counted: bool = False

    def has_terminal(self) -> bool:
        return self.processed is not None or self.failed is not None

    def is_complete(self) -> bool:
        return self.published is not None and self.has_terminal()


@dataclass(slots=True)
class ClosedRecordState:
    closed_ms: int
    published: AuditRecord | None
    processed_seen: bool
    failed_seen: bool


@dataclass(slots=True)
class AuditStats:
    open_record_ttl_ms: int
    published_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    published_unique: int = 0
    processed_unique: int = 0
    failed_unique: int = 0
    terminal_unique: int = 0
    missing_terminal: int = 0
    duplicate_published: int = 0
    duplicate_processed: int = 0
    duplicate_failed: int = 0
    processed_without_publish: int = 0
    failed_without_publish: int = 0
    conflicting_terminal_outcomes: int = 0
    evicted_open_records: int = 0
    max_open_records: int = 0
    max_recent_closed_records: int = 0
    open_by_key: dict[RecordKey, RecordState] = field(default_factory=dict)
    recent_closed_by_key: dict[RecordKey, ClosedRecordState] = field(default_factory=dict)
    open_expiry_queue: deque[tuple[int, RecordKey]] = field(default_factory=deque)
    recent_closed_expiry_queue: deque[tuple[int, RecordKey]] = field(default_factory=deque)
    partition_order: ProcessingOrderStats = field(default_factory=ProcessingOrderStats)
    key_order: ProcessingOrderStats = field(default_factory=ProcessingOrderStats)
    process_latency: LatencyHistogram = field(default_factory=LatencyHistogram)
    failure_latency: LatencyHistogram = field(default_factory=LatencyHistogram)
    terminal_latency: LatencyHistogram = field(default_factory=LatencyHistogram)
    kafka_to_process_age: LatencyHistogram = field(default_factory=LatencyHistogram)
    kafka_to_failure_age: LatencyHistogram = field(default_factory=LatencyHistogram)
    kafka_to_terminal_age: LatencyHistogram = field(default_factory=LatencyHistogram)
    watermark_ms: int = 0
    last_eviction_ms: int = 0
    eviction_interval_ms: int = 1_000

    @property
    def open_records(self) -> int:
        return len(self.open_by_key)

    @property
    def recent_closed_records(self) -> int:
        return len(self.recent_closed_by_key)

    def add(self, record: AuditRecord) -> None:
        self.watermark_ms = max(self.watermark_ms, record.audit_timestamp_ms)
        if self.watermark_ms - self.last_eviction_ms >= self.eviction_interval_ms:
            self._evict_expired(self.watermark_ms - self.open_record_ttl_ms)
            self.last_eviction_ms = self.watermark_ms

        if record.record_type == "P":
            self._add_published(record)
        elif record.record_type in {"C", "F"}:
            self._add_terminal(record)

        self.max_open_records = max(self.max_open_records, len(self.open_by_key))
        self.max_recent_closed_records = max(self.max_recent_closed_records, len(self.recent_closed_by_key))

    def finish(self) -> None:
        for key in list(self.open_by_key):
            self._evict_open_record(key)
        self.recent_closed_by_key.clear()

    def _add_published(self, record: AuditRecord) -> None:
        self.published_records += 1
        if not self._valid_key(record):
            return

        closed = self.recent_closed_by_key.get(record.key)
        if closed is not None:
            self.duplicate_published += 1
            return

        state = self._state(record)
        if state.published is not None:
            self.duplicate_published += 1
            return

        state.published = record
        state.last_seen_ms = record.audit_timestamp_ms
        self.open_expiry_queue.append((state.last_seen_ms, record.key))
        self.published_unique += 1
        if state.processed is not None:
            self._add_processed_latency(state.processed, record)
        if state.failed is not None:
            self._add_failed_latency(state.failed, record)
        self._close_if_complete(record.key)

    def _add_terminal(self, record: AuditRecord) -> None:
        if record.record_type == "C":
            self.processed_records += 1
        else:
            self.failed_records += 1
        if not self._valid_key(record):
            return

        closed = self.recent_closed_by_key.get(record.key)
        if closed is not None:
            self._add_terminal_to_closed(record, closed)
            return

        state = self._state(record)
        state.last_seen_ms = record.audit_timestamp_ms
        self.open_expiry_queue.append((state.last_seen_ms, record.key))

        if record.record_type == "C":
            if state.processed is None:
                state.processed = record
                state.processed_count = 1
                self.processed_unique += 1
                self._add_terminal_unique(record, state)
                if state.published is not None:
                    self._add_processed_latency(record, state.published)
            else:
                state.processed_count += 1
                self.duplicate_processed += 1
        else:
            if state.failed is None:
                state.failed = record
                state.failed_count = 1
                self.failed_unique += 1
                self._add_terminal_unique(record, state)
                if state.published is not None:
                    self._add_failed_latency(record, state.published)
            else:
                state.failed_count += 1
                self.duplicate_failed += 1

        if state.processed is not None and state.failed is not None and not state.conflict_counted:
            state.conflict_counted = True
            self.conflicting_terminal_outcomes += 1
        self._close_if_complete(record.key)

    def _add_terminal_to_closed(self, record: AuditRecord, closed: ClosedRecordState) -> None:
        closed.closed_ms = record.audit_timestamp_ms
        if record.record_type == "C":
            if closed.processed_seen:
                self.duplicate_processed += 1
            else:
                closed.processed_seen = True
                self.processed_unique += 1
                if closed.failed_seen:
                    self.conflicting_terminal_outcomes += 1
                if closed.published is not None:
                    self._add_processed_latency(record, closed.published)
        else:
            if closed.failed_seen:
                self.duplicate_failed += 1
            else:
                closed.failed_seen = True
                self.failed_unique += 1
                if closed.processed_seen:
                    self.conflicting_terminal_outcomes += 1
                if closed.published is not None:
                    self._add_failed_latency(record, closed.published)

    def _state(self, record: AuditRecord) -> RecordState:
        state = self.open_by_key.get(record.key)
        if state is None:
            state = RecordState(first_seen_ms=record.audit_timestamp_ms, last_seen_ms=record.audit_timestamp_ms)
            self.open_by_key[record.key] = state
            self.open_expiry_queue.append((state.last_seen_ms, record.key))
        return state

    def _add_terminal_unique(self, record: AuditRecord, state: RecordState) -> None:
        if state.processed is record or state.failed is record:
            if state.processed_count + state.failed_count == 1:
                self.terminal_unique += 1
                self.partition_order.add_partition_order(record)
                self.key_order.add_key_order(record)

    def _close_if_complete(self, key: RecordKey) -> None:
        state = self.open_by_key.get(key)
        if state is None or not state.is_complete():
            return
        del self.open_by_key[key]
        self.recent_closed_by_key[key] = ClosedRecordState(
            closed_ms=state.last_seen_ms,
            published=state.published,
            processed_seen=state.processed is not None,
            failed_seen=state.failed is not None,
        )
        self.recent_closed_expiry_queue.append((state.last_seen_ms, key))

    def _evict_expired(self, cutoff_ms: int) -> None:
        while self.open_expiry_queue and self.open_expiry_queue[0][0] < cutoff_ms:
            last_seen_ms, key = self.open_expiry_queue.popleft()
            state = self.open_by_key.get(key)
            if state is not None and state.last_seen_ms == last_seen_ms:
                self._evict_open_record(key)
        while self.recent_closed_expiry_queue and self.recent_closed_expiry_queue[0][0] < cutoff_ms:
            closed_ms, key = self.recent_closed_expiry_queue.popleft()
            state = self.recent_closed_by_key.get(key)
            if state is not None and state.closed_ms == closed_ms:
                del self.recent_closed_by_key[key]
        self.key_order.evict_before(cutoff_ms)

    def _evict_open_record(self, key: RecordKey) -> None:
        state = self.open_by_key.pop(key)
        self.evicted_open_records += 1
        if state.published is not None and not state.has_terminal():
            self.missing_terminal += 1
        if state.published is None:
            if state.processed is not None:
                self.processed_without_publish += 1
            if state.failed is not None:
                self.failed_without_publish += 1

    def _add_processed_latency(self, record: AuditRecord, published: AuditRecord) -> None:
        latency = record.audit_timestamp_ms - published.audit_timestamp_ms
        self.process_latency.add(latency)
        self.terminal_latency.add(latency)
        if published.kafka_timestamp_ms is not None:
            age = record.audit_timestamp_ms - published.kafka_timestamp_ms
            self.kafka_to_process_age.add(age)
            self.kafka_to_terminal_age.add(age)

    def _add_failed_latency(self, record: AuditRecord, published: AuditRecord) -> None:
        latency = record.audit_timestamp_ms - published.audit_timestamp_ms
        self.failure_latency.add(latency)
        self.terminal_latency.add(latency)
        if published.kafka_timestamp_ms is not None:
            age = record.audit_timestamp_ms - published.kafka_timestamp_ms
            self.kafka_to_failure_age.add(age)
            self.kafka_to_terminal_age.add(age)

    def _valid_key(self, record: AuditRecord) -> bool:
        return record.key.partition >= 0 and record.key.offset >= 0


class AuditAccumulator:
    def __init__(self, open_record_ttl_ms: int, topic_summaries: bool) -> None:
        self.all = AuditStats(open_record_ttl_ms=open_record_ttl_ms)
        self.by_topic = (
            {topic_id: AuditStats(open_record_ttl_ms=open_record_ttl_ms) for topic_id in TOPIC_NAMES}
            if topic_summaries
            else {}
        )
        self.record_count = 0

    def add(self, record: AuditRecord) -> None:
        self.record_count += 1
        self.all.add(record)
        topic_stats = self.by_topic.get(record.key.topic_id)
        if topic_stats is not None:
            topic_stats.add(record)

    def finish(self) -> None:
        self.all.finish()
        for stats in self.by_topic.values():
            stats.finish()


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
    parser.add_argument("--open-record-ttl-seconds", type=float, default=60.0)
    parser.add_argument("--no-topic-summaries", action="store_true")
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
                "Audit analyzer progress: "
                f"chunks={len(seen)} records={accumulator.record_count} "
                f"open_records={accumulator.all.open_records} "
                f"recent_closed_records={accumulator.all.recent_closed_records}",
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


def print_order_summary(label: str, stats: ProcessingOrderStats) -> None:
    print(
        f"  {label}_order_terminal_records={stats.terminal_records} "
        f"out_of_order_terminal={stats.out_of_order_terminal} "
        f"out_of_order_processed={stats.out_of_order_processed} "
        f"out_of_order_failed={stats.out_of_order_failed}"
    )


def print_summary(title: str, stats: AuditStats) -> None:
    print(title)
    print(f"  published_records={stats.published_records}")
    print(f"  published_unique={stats.published_unique}")
    print(f"  processed_records={stats.processed_records}")
    print(f"  processed_unique={stats.processed_unique}")
    print(f"  failed_records={stats.failed_records}")
    print(f"  failed_unique={stats.failed_unique}")
    print(f"  terminal_unique={stats.terminal_unique}")
    print(f"  missing_terminal={stats.missing_terminal}")
    print(f"  duplicate_published={stats.duplicate_published}")
    print(f"  duplicate_processed={stats.duplicate_processed}")
    print(f"  duplicate_failed={stats.duplicate_failed}")
    print(f"  processed_without_publish={stats.processed_without_publish}")
    print(f"  failed_without_publish={stats.failed_without_publish}")
    print(f"  conflicting_terminal_outcomes={stats.conflicting_terminal_outcomes}")
    print(f"  open_records={stats.open_records}")
    print(f"  recent_closed_records={stats.recent_closed_records}")
    print(f"  evicted_open_records={stats.evicted_open_records}")
    print(f"  max_open_records={stats.max_open_records}")
    print(f"  max_recent_closed_records={stats.max_recent_closed_records}")
    print("  processing_order_scope=topic_id+partition for partition order; topic_id+partition+message_key for key order")
    print_order_summary("partition", stats.partition_order)
    print_order_summary("key", stats.key_order)
    print(f"  publish_to_process_latency_ms={stats.process_latency.format()}")
    print(f"  publish_to_failure_latency_ms={stats.failure_latency.format()}")
    print(f"  publish_to_terminal_latency_ms={stats.terminal_latency.format()}")
    print(f"  kafka_to_process_age_ms={stats.kafka_to_process_age.format()}")
    print(f"  kafka_to_failure_age_ms={stats.kafka_to_failure_age.format()}")
    print(f"  kafka_to_terminal_age_ms={stats.kafka_to_terminal_age.format()}")


def main() -> int:
    args = parse_args()
    if not args.input_file and not args.input_dir:
        raise ValueError("at least one --input-file or --input-dir is required")
    if args.watch and not args.input_dir:
        raise ValueError("--watch requires --input-dir")
    if args.watch and not args.stop_file:
        raise ValueError("--watch requires --stop-file")
    if args.open_record_ttl_seconds <= 0:
        raise ValueError("--open-record-ttl-seconds must be positive")

    accumulator = AuditAccumulator(
        open_record_ttl_ms=round(args.open_record_ttl_seconds * 1000),
        topic_summaries=not args.no_topic_summaries,
    )
    read_files(args.input_file, accumulator)
    if args.input_dir:
        watch_chunks(args, accumulator)
    accumulator.finish()

    print(f"Read {accumulator.record_count} audit records.", file=sys.stderr, flush=True)
    if args.require_records and accumulator.record_count == 0:
        raise ValueError("no audit records were read")

    print_summary("Audit summary", accumulator.all)
    for topic_id, topic_stats in accumulator.by_topic.items():
        print()
        print_summary(f"Topic summary: {TOPIC_NAMES[topic_id]}", topic_stats)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"audit analysis failed: {error}", file=sys.stderr)
        raise
