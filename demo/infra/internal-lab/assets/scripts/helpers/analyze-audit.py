#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple, TextIO

TOPIC_NAMES = {
    1: "order.events.v1",
    2: "batch.events.v1",
    3: "cauldron.events.v1",
}

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
    out_of_order: int = 0
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
        last = last_offsets.get(group)
        out_of_order = last is not None and record.key.offset < last.offset

        if out_of_order:
            self.out_of_order += 1
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
    open_by_key: dict[RecordKey, RecordState] = field(default_factory=dict)
    recent_closed_by_key: dict[RecordKey, ClosedRecordState] = field(default_factory=dict)
    open_expiry_queue: deque[tuple[int, RecordKey]] = field(default_factory=deque)
    recent_closed_expiry_queue: deque[tuple[int, RecordKey]] = field(default_factory=deque)
    partition_order: ProcessingOrderStats = field(default_factory=ProcessingOrderStats)
    key_order: ProcessingOrderStats = field(default_factory=ProcessingOrderStats)
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
            else:
                state.processed_count += 1
                self.duplicate_processed += 1
        else:
            if state.failed is None:
                state.failed = record
                state.failed_count = 1
                self.failed_unique += 1
                self._add_terminal_unique(record, state)
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
        else:
            if closed.failed_seen:
                self.duplicate_failed += 1
            else:
                closed.failed_seen = True
                self.failed_unique += 1
                if closed.processed_seen:
                    self.conflicting_terminal_outcomes += 1

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
        if state.published is not None and not state.has_terminal():
            self.missing_terminal += 1
        if state.published is None:
            if state.processed is not None:
                self.processed_without_publish += 1
            if state.failed is not None:
                self.failed_without_publish += 1

    def _valid_key(self, record: AuditRecord) -> bool:
        return record.key.partition >= 0 and record.key.offset >= 0


class AuditAccumulator:
    def __init__(self, open_record_ttl_ms: int) -> None:
        self.all = AuditStats(open_record_ttl_ms=open_record_ttl_ms)
        self.by_topic = {topic_id: AuditStats(open_record_ttl_ms=open_record_ttl_ms) for topic_id in TOPIC_NAMES}
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
    parser.add_argument("--metadata-file")
    parser.add_argument("--open-record-ttl-seconds", type=float, default=60.0)
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


def read_chunks(args: argparse.Namespace, accumulator: AuditAccumulator) -> None:
    input_dir = Path(args.input_dir)
    chunks = list_chunks(input_dir, args.glob)
    for index, path in enumerate(chunks, start=1):
        read_path(path, accumulator)
        print_analysis_progress(accumulator, index, len(chunks))


def print_analysis_progress(accumulator: AuditAccumulator, processed_chunks: int, expected_chunks: int | None) -> None:
    chunk_progress = (
        f"chunks={processed_chunks}/{expected_chunks}"
        if expected_chunks is not None
        else f"chunks={processed_chunks}"
    )
    print(
        "Audit analyzer progress: "
        f"{chunk_progress} records={accumulator.record_count}",
        file=sys.stderr,
        flush=True,
    )


def ordering_summary(stats: ProcessingOrderStats) -> dict[str, int]:
    return {
        "out_of_order": stats.out_of_order,
        "processed": stats.out_of_order_processed,
        "failed": stats.out_of_order_failed,
    }


def stats_summary(stats: AuditStats, topic_id: int | None = None) -> dict[str, object]:
    summary: dict[str, object] = {}
    if topic_id is not None:
        summary["topic_id"] = topic_id
    summary.update(
        {
            "published": stats.published_unique,
            "processed": stats.processed_unique,
            "failed": stats.failed_unique,
            "terminal": stats.terminal_unique,
            "missing_terminal": stats.missing_terminal,
            "duplicates": {
                "published": stats.duplicate_published,
                "processed": stats.duplicate_processed,
                "failed": stats.duplicate_failed,
            },
            "without_publish": {
                "processed": stats.processed_without_publish,
                "failed": stats.failed_without_publish,
            },
            "conflicting_terminal_outcomes": stats.conflicting_terminal_outcomes,
            "ordering": {
                "by_partition": ordering_summary(stats.partition_order),
                "by_key": ordering_summary(stats.key_order),
            },
        }
    )
    return summary


def load_metadata(path_value: str | None) -> dict[str, object]:
    if not path_value:
        return {}
    path = Path(path_value)
    if not path.is_file():
        raise ValueError(f"metadata file was not found: {path}")
    with path.open("r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"metadata file must contain a JSON object: {path}")
    return value


def summary_document(accumulator: AuditAccumulator, metadata: dict[str, object]) -> dict[str, object]:
    audit = {
        "records_read": accumulator.record_count,
        "totals": stats_summary(accumulator.all),
        "topics": {
            TOPIC_NAMES.get(topic_id, f"unknown-topic-{topic_id}"): stats_summary(topic_stats, topic_id)
            for topic_id, topic_stats in accumulator.by_topic.items()
        },
    }
    document: dict[str, object] = {"audit": audit}
    if metadata:
        document = {"test": metadata, **document}
    return document


def yaml_scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, int | float):
        return str(value)
    text = str(value)
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def print_yaml(value: object, indent: int = 0) -> None:
    prefix = " " * indent
    if isinstance(value, dict):
        for key, nested in value.items():
            if isinstance(nested, dict):
                print(f"{prefix}{key}:")
                print_yaml(nested, indent + 2)
            elif isinstance(nested, list):
                print(f"{prefix}{key}:")
                print_yaml(nested, indent + 2)
            else:
                print(f"{prefix}{key}: {yaml_scalar(nested)}")
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                keys = list(item)
                if not keys:
                    print(f"{prefix}- {{}}")
                    continue
                first_key = keys[0]
                first_value = item[first_key]
                if isinstance(first_value, (dict, list)):
                    print(f"{prefix}- {first_key}:")
                    print_yaml(first_value, indent + 4)
                else:
                    print(f"{prefix}- {first_key}: {yaml_scalar(first_value)}")
                for key in keys[1:]:
                    nested = item[key]
                    if isinstance(nested, (dict, list)):
                        print(f"{prefix}  {key}:")
                        print_yaml(nested, indent + 4)
                    else:
                        print(f"{prefix}  {key}: {yaml_scalar(nested)}")
            elif isinstance(item, list):
                print(f"{prefix}-")
                print_yaml(item, indent + 2)
            else:
                print(f"{prefix}- {yaml_scalar(item)}")
    else:
        print(f"{prefix}{yaml_scalar(value)}")


def main() -> int:
    args = parse_args()
    if not args.input_file and not args.input_dir:
        raise ValueError("at least one --input-file or --input-dir is required")
    if args.open_record_ttl_seconds <= 0:
        raise ValueError("--open-record-ttl-seconds must be positive")

    accumulator = AuditAccumulator(
        open_record_ttl_ms=round(args.open_record_ttl_seconds * 1000),
    )
    read_files(args.input_file, accumulator)
    if args.input_dir:
        read_chunks(args, accumulator)
    accumulator.finish()

    print(f"Read {accumulator.record_count} audit records.", file=sys.stderr, flush=True)
    if args.require_records and accumulator.record_count == 0:
        raise ValueError("no audit records were read")

    print_yaml(summary_document(accumulator, load_metadata(args.metadata_file)))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"audit analysis failed: {error}", file=sys.stderr)
        raise
