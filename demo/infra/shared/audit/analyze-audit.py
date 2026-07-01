#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import json
import math
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
    drop_reason: str | None = None


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
class OnlineAgeStats:
    count: int = 0
    total_ms: int = 0
    min_ms: int | None = None
    max_ms: int | None = None

    def add(self, age_ms: int) -> None:
        self.count += 1
        self.total_ms += age_ms
        self.min_ms = age_ms if self.min_ms is None else min(self.min_ms, age_ms)
        self.max_ms = age_ms if self.max_ms is None else max(self.max_ms, age_ms)

    def summary(self) -> dict[str, int | float | None]:
        return {
            "count": self.count,
            "avg_ms": round(self.total_ms / self.count, 3) if self.count else None,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
        }


@dataclass(slots=True)
class FreshnessGapStats:
    processed_records: int = 0
    pending_drops_by_key: dict[str, int] = field(default_factory=dict)
    first_pending_drop_ms_by_key: dict[str, int] = field(default_factory=dict)
    last_pending_drop_ms_by_key: dict[str, int] = field(default_factory=dict)
    dropped_before_processed_histogram: dict[int, int] = field(default_factory=dict)
    first_drop_to_processed_ms: OnlineAgeStats = field(default_factory=OnlineAgeStats)
    last_drop_to_processed_ms: OnlineAgeStats = field(default_factory=OnlineAgeStats)

    def add_processed(self, record: AuditRecord) -> None:
        self.processed_records += 1
        dropped = self.pending_drops_by_key.pop(record.message_key, 0)
        self.dropped_before_processed_histogram[dropped] = (
            self.dropped_before_processed_histogram.get(dropped, 0) + 1
        )

        first_drop_ms = self.first_pending_drop_ms_by_key.pop(record.message_key, None)
        last_drop_ms = self.last_pending_drop_ms_by_key.pop(record.message_key, None)
        if first_drop_ms is not None:
            self.first_drop_to_processed_ms.add(max(0, record.audit_timestamp_ms - first_drop_ms))
        if last_drop_ms is not None:
            self.last_drop_to_processed_ms.add(max(0, record.audit_timestamp_ms - last_drop_ms))

    def add_dropped(self, record: AuditRecord) -> None:
        pending = self.pending_drops_by_key.get(record.message_key, 0)
        self.pending_drops_by_key[record.message_key] = pending + 1
        if pending == 0:
            self.first_pending_drop_ms_by_key[record.message_key] = record.audit_timestamp_ms
        self.last_pending_drop_ms_by_key[record.message_key] = record.audit_timestamp_ms

    def summary(self) -> dict[str, object]:
        return {
            "processed_records": self.processed_records,
            "dropped_before_processed": histogram_distribution_summary(self.dropped_before_processed_histogram),
            "dropped_before_processed_histogram": dict(sorted(self.dropped_before_processed_histogram.items())),
            "first_drop_to_processed_ms": self.first_drop_to_processed_ms.summary(),
            "last_drop_to_processed_ms": self.last_drop_to_processed_ms.summary(),
            "keys_with_pending_drops": len(self.pending_drops_by_key),
            "pending_dropped_records": sum(self.pending_drops_by_key.values()),
        }


@dataclass(slots=True)
class KeyOutcomeStats:
    published: int = 0
    processed: int = 0
    failed: int = 0
    dropped: int = 0
    last_processed_ms: int | None = None
    max_processed_gap_ms: int | None = None

    @property
    def terminal(self) -> int:
        return self.processed + self.failed + self.dropped


@dataclass(slots=True)
class KeyFairnessStats:
    by_message_key: dict[str, KeyOutcomeStats] = field(default_factory=dict)
    freshness_gap: FreshnessGapStats = field(default_factory=FreshnessGapStats)
    processed_age: OnlineAgeStats = field(default_factory=OnlineAgeStats)
    dropped_age: OnlineAgeStats = field(default_factory=OnlineAgeStats)
    failed_age: OnlineAgeStats = field(default_factory=OnlineAgeStats)
    finalized: bool = False

    def add_published(self, record: AuditRecord) -> None:
        self._stats(record.message_key).published += 1

    def add_terminal(self, record: AuditRecord) -> None:
        stats = self._stats(record.message_key)
        if record.record_type == "C":
            stats.processed += 1
            self.freshness_gap.add_processed(record)
            if stats.last_processed_ms is not None:
                gap_ms = record.audit_timestamp_ms - stats.last_processed_ms
                stats.max_processed_gap_ms = (
                    gap_ms
                    if stats.max_processed_gap_ms is None
                    else max(stats.max_processed_gap_ms, gap_ms)
                )
            stats.last_processed_ms = record.audit_timestamp_ms
        elif record.record_type == "F":
            stats.failed += 1
        elif record.record_type == "D":
            stats.dropped += 1
            self.freshness_gap.add_dropped(record)

    def add_terminal_age(self, published: AuditRecord, terminal: AuditRecord) -> None:
        if published.kafka_timestamp_ms is None:
            return
        age_ms = max(0, terminal.audit_timestamp_ms - published.kafka_timestamp_ms)
        if terminal.record_type == "C":
            self.processed_age.add(age_ms)
        elif terminal.record_type == "D":
            self.dropped_age.add(age_ms)
        elif terminal.record_type == "F":
            self.failed_age.add(age_ms)

    def finish(self, watermark_ms: int) -> None:
        if self.finalized:
            return
        for stats in self.by_message_key.values():
            if stats.last_processed_ms is None:
                continue
            trailing_gap_ms = watermark_ms - stats.last_processed_ms
            stats.max_processed_gap_ms = (
                trailing_gap_ms
                if stats.max_processed_gap_ms is None
                else max(stats.max_processed_gap_ms, trailing_gap_ms)
            )
        self.finalized = True

    def summary(self) -> dict[str, object]:
        key_stats = list(self.by_message_key.values())
        return {
            "keys": len(key_stats),
            "keys_without_processed": sum(1 for stats in key_stats if stats.processed == 0),
            "published_per_key": distribution_summary([stats.published for stats in key_stats]),
            "processed_per_key": distribution_summary([stats.processed for stats in key_stats]),
            "dropped_per_key": distribution_summary([stats.dropped for stats in key_stats]),
            "processed_ratio": distribution_summary([ratio(stats.processed, stats.published) for stats in key_stats]),
            "dropped_ratio": distribution_summary([ratio(stats.dropped, stats.published) for stats in key_stats]),
            "processed_max_gap_ms": gap_summary(
                [stats.max_processed_gap_ms for stats in key_stats if stats.max_processed_gap_ms is not None]
            ),
            "freshness_gap": self.freshness_gap.summary(),
            "record_age": {
                "processed": self.processed_age.summary(),
                "dropped": self.dropped_age.summary(),
                "failed": self.failed_age.summary(),
            },
        }

    def _stats(self, message_key: str) -> KeyOutcomeStats:
        stats = self.by_message_key.get(message_key)
        if stats is None:
            stats = KeyOutcomeStats()
            self.by_message_key[message_key] = stats
        return stats


def ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def distribution_summary(values: list[int | float]) -> dict[str, int | float | None]:
    if not values:
        return empty_distribution_summary()
    sorted_values = sorted(values)
    avg = sum(sorted_values) / len(sorted_values)
    return {
        "count": len(sorted_values),
        "avg": round(avg, 6),
        "min": rounded_number(sorted_values[0]),
        "p50": rounded_number(percentile(sorted_values, 0.50)),
        "p95": rounded_number(percentile(sorted_values, 0.95)),
        "p99": rounded_number(percentile(sorted_values, 0.99)),
        "max": rounded_number(sorted_values[-1]),
        "coefficient_of_variation": coefficient_of_variation(sorted_values, avg),
        "gini": gini(sorted_values),
        "max_to_min": max_to_min(sorted_values),
    }


def histogram_distribution_summary(histogram: dict[int, int]) -> dict[str, int | float | None]:
    items = [(value, count) for value, count in sorted(histogram.items()) if count > 0]
    if not items:
        return empty_distribution_summary()

    count = sum(bucket_count for _, bucket_count in items)
    total = sum(value * bucket_count for value, bucket_count in items)
    avg = total / count
    minimum = items[0][0]
    maximum = items[-1][0]
    return {
        "count": count,
        "avg": round(avg, 6),
        "min": minimum,
        "p50": rounded_number(histogram_percentile(items, count, 0.50)),
        "p95": rounded_number(histogram_percentile(items, count, 0.95)),
        "p99": rounded_number(histogram_percentile(items, count, 0.99)),
        "max": maximum,
        "coefficient_of_variation": histogram_coefficient_of_variation(items, count, avg),
        "gini": histogram_gini(items, count, total),
        "max_to_min": None if minimum == 0 else round(maximum / minimum, 6),
    }


def gap_summary(values: list[int]) -> dict[str, object]:
    summary = distribution_summary(values)
    summary["keys_over_1s"] = sum(1 for value in values if value > 1_000)
    summary["keys_over_5s"] = sum(1 for value in values if value > 5_000)
    summary["keys_over_10s"] = sum(1 for value in values if value > 10_000)
    summary["keys_over_30s"] = sum(1 for value in values if value > 30_000)
    return summary


def empty_distribution_summary() -> dict[str, None]:
    return {
        "count": None,
        "avg": None,
        "min": None,
        "p50": None,
        "p95": None,
        "p99": None,
        "max": None,
        "coefficient_of_variation": None,
        "gini": None,
        "max_to_min": None,
    }


def percentile(sorted_values: list[int | float], quantile: float) -> int | float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def histogram_percentile(items: list[tuple[int, int]], count: int, quantile: float) -> int | float:
    if count == 1:
        return items[0][0]
    position = (count - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return histogram_value_at(items, lower)
    lower_value = histogram_value_at(items, lower)
    upper_value = histogram_value_at(items, upper)
    return lower_value + (upper_value - lower_value) * (position - lower)


def histogram_value_at(items: list[tuple[int, int]], index: int) -> int:
    seen = 0
    for value, count in items:
        seen += count
        if index < seen:
            return value
    return items[-1][0]


def coefficient_of_variation(values: list[int | float], avg: float) -> float | None:
    if not values or avg == 0:
        return None
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return round(math.sqrt(variance) / avg, 6)


def histogram_coefficient_of_variation(
    items: list[tuple[int, int]],
    count: int,
    avg: float,
) -> float | None:
    if count == 0 or avg == 0:
        return None
    variance = sum(bucket_count * ((value - avg) ** 2) for value, bucket_count in items) / count
    return round(math.sqrt(variance) / avg, 6)


def gini(values: list[int | float]) -> float | None:
    if not values:
        return None
    total = sum(values)
    if total == 0:
        return None
    weighted_sum = sum((index + 1) * value for index, value in enumerate(sorted(values)))
    coefficient = (2 * weighted_sum) / (len(values) * total) - (len(values) + 1) / len(values)
    return round(coefficient, 6)


def histogram_gini(items: list[tuple[int, int]], count: int, total: int) -> float | None:
    if count == 0 or total == 0:
        return None
    weighted_sum = 0.0
    seen = 0
    for value, bucket_count in items:
        index_sum = bucket_count * (2 * seen + bucket_count + 1) / 2
        weighted_sum += value * index_sum
        seen += bucket_count
    coefficient = (2 * weighted_sum) / (count * total) - (count + 1) / count
    return round(coefficient, 6)


def max_to_min(values: list[int | float]) -> float | None:
    if not values:
        return None
    minimum = min(values)
    if minimum == 0:
        return None
    return round(max(values) / minimum, 6)


def rounded_number(value: int | float) -> int | float:
    if isinstance(value, int):
        return value
    return round(value, 6)

@dataclass(slots=True)
class RecordState:
    first_seen_ms: int
    last_seen_ms: int
    published: AuditRecord | None = None
    processed: AuditRecord | None = None
    failed: AuditRecord | None = None
    dropped: AuditRecord | None = None
    processed_count: int = 0
    failed_count: int = 0
    dropped_count: int = 0
    conflict_counted: bool = False

    def has_terminal(self) -> bool:
        return self.processed is not None or self.failed is not None or self.dropped is not None

    def is_complete(self) -> bool:
        return self.published is not None and self.has_terminal()


@dataclass(slots=True)
class ClosedRecordState:
    closed_ms: int
    processed_seen: bool
    failed_seen: bool
    dropped_seen: bool


@dataclass(slots=True)
class AuditStats:
    open_record_ttl_ms: int | None
    key_fairness: KeyFairnessStats | None = None
    published_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    dropped_records: int = 0
    retry_attempt_records: int = 0
    published_unique: int = 0
    processed_unique: int = 0
    failed_unique: int = 0
    dropped_unique: int = 0
    terminal_unique: int = 0
    missing_terminal: int = 0
    duplicate_published: int = 0
    duplicate_processed: int = 0
    duplicate_failed: int = 0
    duplicate_dropped: int = 0
    dropped_by_reason: dict[str, int] = field(default_factory=dict)
    processed_without_publish: int = 0
    failed_without_publish: int = 0
    dropped_without_publish: int = 0
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
        if (
            self.open_record_ttl_ms is not None
            and self.watermark_ms - self.last_eviction_ms >= self.eviction_interval_ms
        ):
            self._evict_expired(self.watermark_ms - self.open_record_ttl_ms)
            self.last_eviction_ms = self.watermark_ms

        if record.record_type == "P":
            self._add_published(record)
        elif record.record_type in {"C", "F", "D"}:
            self._add_terminal(record)
        elif record.record_type == "R":
            self.retry_attempt_records += 1

    def finish(self) -> None:
        for key in list(self.open_by_key):
            self._evict_open_record(key)
        self.recent_closed_by_key.clear()
        if self.key_fairness is not None:
            self.key_fairness.finish(self.watermark_ms)

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
        if self.open_record_ttl_ms is not None:
            self.open_expiry_queue.append((state.last_seen_ms, record.key))
        self.published_unique += 1
        if self.key_fairness is not None:
            self.key_fairness.add_published(record)
        self._close_if_complete(record.key)

    def _add_terminal(self, record: AuditRecord) -> None:
        if record.record_type == "C":
            self.processed_records += 1
        elif record.record_type == "F":
            self.failed_records += 1
        else:
            self.dropped_records += 1
        if not self._valid_key(record):
            return

        closed = self.recent_closed_by_key.get(record.key)
        if closed is not None:
            self._add_terminal_to_closed(record, closed)
            return

        state = self._state(record)
        state.last_seen_ms = record.audit_timestamp_ms
        if self.open_record_ttl_ms is not None:
            self.open_expiry_queue.append((state.last_seen_ms, record.key))

        if record.record_type == "C":
            if state.processed is None:
                state.processed = record
                state.processed_count = 1
                self.processed_unique += 1
                if self.key_fairness is not None:
                    self.key_fairness.add_terminal(record)
                self._add_terminal_unique(record, state)
            else:
                state.processed_count += 1
                self.duplicate_processed += 1
        elif record.record_type == "F":
            if state.failed is None:
                state.failed = record
                state.failed_count = 1
                self.failed_unique += 1
                if self.key_fairness is not None:
                    self.key_fairness.add_terminal(record)
                self._add_terminal_unique(record, state)
            else:
                state.failed_count += 1
                self.duplicate_failed += 1
        else:
            if state.dropped is None:
                state.dropped = record
                state.dropped_count = 1
                self.dropped_unique += 1
                self._add_drop_reason(record)
                if self.key_fairness is not None:
                    self.key_fairness.add_terminal(record)
                self._add_terminal_unique(record, state)
            else:
                state.dropped_count += 1
                self.duplicate_dropped += 1

        terminal_outcomes = sum(
            1 for outcome in (state.processed, state.failed, state.dropped)
            if outcome is not None
        )
        if terminal_outcomes > 1 and not state.conflict_counted:
            state.conflict_counted = True
            self.conflicting_terminal_outcomes += 1
        self._close_if_complete(record.key)

    def _add_terminal_to_closed(self, record: AuditRecord, closed: ClosedRecordState) -> None:
        closed.closed_ms = record.audit_timestamp_ms
        if record.record_type == "C":
            if closed.processed_seen:
                self.duplicate_processed += 1
            else:
                had_other_outcome = closed.failed_seen or closed.dropped_seen
                closed.processed_seen = True
                self.processed_unique += 1
                if had_other_outcome:
                    self.conflicting_terminal_outcomes += 1
        elif record.record_type == "F":
            if closed.failed_seen:
                self.duplicate_failed += 1
            else:
                had_other_outcome = closed.processed_seen or closed.dropped_seen
                closed.failed_seen = True
                self.failed_unique += 1
                if had_other_outcome:
                    self.conflicting_terminal_outcomes += 1
        else:
            if closed.dropped_seen:
                self.duplicate_dropped += 1
            else:
                had_other_outcome = closed.processed_seen or closed.failed_seen
                closed.dropped_seen = True
                self.dropped_unique += 1
                self._add_drop_reason(record)
                if had_other_outcome:
                    self.conflicting_terminal_outcomes += 1

    def _state(self, record: AuditRecord) -> RecordState:
        state = self.open_by_key.get(record.key)
        if state is None:
            state = RecordState(first_seen_ms=record.audit_timestamp_ms, last_seen_ms=record.audit_timestamp_ms)
            self.open_by_key[record.key] = state
            if self.open_record_ttl_ms is not None:
                self.open_expiry_queue.append((state.last_seen_ms, record.key))
        return state

    def _add_terminal_unique(self, record: AuditRecord, state: RecordState) -> None:
        if state.processed is record or state.failed is record or state.dropped is record:
            if state.processed_count + state.failed_count + state.dropped_count == 1:
                self.terminal_unique += 1
                if record.record_type in {"C", "F"}:
                    self.partition_order.add_partition_order(record)
                    self.key_order.add_key_order(record)

    def _add_drop_reason(self, record: AuditRecord) -> None:
        reason = record.drop_reason or "unknown"
        self.dropped_by_reason[reason] = self.dropped_by_reason.get(reason, 0) + 1

    def _close_if_complete(self, key: RecordKey) -> None:
        state = self.open_by_key.get(key)
        if state is None or not state.is_complete():
            return
        if self.key_fairness is not None and state.published is not None:
            terminal = state.processed or state.failed or state.dropped
            if terminal is not None:
                self.key_fairness.add_terminal_age(state.published, terminal)
        del self.open_by_key[key]
        self.recent_closed_by_key[key] = ClosedRecordState(
            closed_ms=state.last_seen_ms,
            processed_seen=state.processed is not None,
            failed_seen=state.failed is not None,
            dropped_seen=state.dropped is not None,
        )
        if self.open_record_ttl_ms is not None:
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
            if state.dropped is not None:
                self.dropped_without_publish += 1

    def _valid_key(self, record: AuditRecord) -> bool:
        return record.key.partition >= 0 and record.key.offset >= 0


class AuditAccumulator:
    def __init__(self, open_record_ttl_ms: int | None) -> None:
        self.open_record_ttl_ms = open_record_ttl_ms
        self.all = AuditStats(open_record_ttl_ms=open_record_ttl_ms)
        self.by_topic = {
            topic_id: AuditStats(
                open_record_ttl_ms=open_record_ttl_ms,
                key_fairness=KeyFairnessStats() if topic_id == 3 else None,
            )
            for topic_id in TOPIC_NAMES
        }
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
    parser.add_argument(
        "--open-record-ttl-seconds",
        type=float,
        help="Enable bounded-memory matching by evicting unmatched records after this many seconds. Omit for exact offline matching.",
    )
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

    if record_type in {"C", "F", "R"}:
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

    if record_type == "D":
        if len(parts) not in {6, 7}:
            raise ValueError(f"expected 6 or 7 drop audit fields, got {len(parts)}: {value!r}")
        _, topic_id, partition, offset, audit_ts, message_key, *detail = parts
        return AuditRecord(
            record_type=record_type,
            key=RecordKey(int(topic_id), int(partition), int(offset)),
            kafka_timestamp_ms=None,
            audit_timestamp_ms=int(audit_ts),
            message_key=message_key,
            drop_reason=detail[0] if detail else None,
        )

    raise ValueError(f"unexpected audit record type: {record_type}")


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def add_line(line: str, accumulator: AuditAccumulator) -> int:
    if not line.strip():
        return 0
    if line.startswith("S|"):
        return 0
    accumulator.add(parse_record(line))
    return 1


def read_gzip_path(path: Path, accumulator: AuditAccumulator) -> int:
    count = 0
    with open_text(path) as file:
        for line in file:
            count += add_line(line, accumulator)
    return count


def read_plain_path(
    path: Path,
    accumulator: AuditAccumulator,
    file_index: int,
    expected_files: int,
    progress_step_percent: int = 10,
) -> int:
    count = 0
    total_bytes = path.stat().st_size
    next_progress_percent = progress_step_percent

    with path.open("rb") as file:
        for line in file:
            count += add_line(line.decode("utf-8"), accumulator)
            if total_bytes <= 0:
                continue

            processed_bytes = file.tell()
            current_percent = min(100, int(processed_bytes * 100 / total_bytes))
            while current_percent >= next_progress_percent:
                print_analysis_progress(
                    accumulator,
                    file_index,
                    expected_files,
                    progress_percent=next_progress_percent,
                )
                next_progress_percent += progress_step_percent

    if total_bytes == 0:
        print_analysis_progress(accumulator, file_index, expected_files, progress_percent=100)
    return count


def read_path(path: Path, accumulator: AuditAccumulator, file_index: int, expected_files: int) -> int:
    print(f"Reading audit records from {path}.", file=sys.stderr, flush=True)
    if path.suffix == ".gz":
        count = read_gzip_path(path, accumulator)
        print_analysis_progress(accumulator, file_index, expected_files)
        return count
    return read_plain_path(path, accumulator, file_index, expected_files)


def read_files(paths: list[str], accumulator: AuditAccumulator) -> None:
    for index, path_value in enumerate(paths, start=1):
        path = Path(path_value)
        if not path.is_file():
            raise ValueError(f"audit input file was not found: {path}")
        read_path(path, accumulator, index, len(paths))


def list_chunks(input_dir: Path, glob: str) -> list[Path]:
    if not input_dir.is_dir():
        return []
    return sorted(path for path in input_dir.glob(glob) if path.is_file())


def read_chunks(args: argparse.Namespace, accumulator: AuditAccumulator) -> None:
    input_dir = Path(args.input_dir)
    chunks = list_chunks(input_dir, args.glob)
    for index, path in enumerate(chunks, start=1):
        read_path(path, accumulator, index, len(chunks))


def print_analysis_progress(
    accumulator: AuditAccumulator,
    processed_files: int,
    expected_files: int | None,
    progress_percent: int | None = None,
) -> None:
    file_progress = (
        f"files={processed_files}/{expected_files}"
        if expected_files is not None
        else f"files={processed_files}"
    )
    read_progress = f"{file_progress} {progress_percent}%" if progress_percent is not None else file_progress
    print(
        "Audit analyzer progress: "
        f"{read_progress} records={accumulator.record_count}",
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
            "dropped": stats.dropped_unique,
            "dropped_by_reason": stats.dropped_by_reason,
            "retry_attempts": stats.retry_attempt_records,
            "terminal": stats.terminal_unique,
            "missing_terminal": stats.missing_terminal,
            "duplicates": {
                "published": stats.duplicate_published,
                "processed": stats.duplicate_processed,
                "failed": stats.duplicate_failed,
                "dropped": stats.duplicate_dropped,
            },
            "without_publish": {
                "processed": stats.processed_without_publish,
                "failed": stats.failed_without_publish,
                "dropped": stats.dropped_without_publish,
            },
            "conflicting_terminal_outcomes": stats.conflicting_terminal_outcomes,
            "ordering": {
                "by_partition": ordering_summary(stats.partition_order),
                "by_key": ordering_summary(stats.key_order),
            },
        }
    )
    if stats.key_fairness is not None:
        summary["key_fairness"] = stats.key_fairness.summary()
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
        "delivery_matching": {
            "mode": "bounded_ttl" if accumulator.open_record_ttl_ms is not None else "exact",
            "open_record_ttl_seconds": (
                round(accumulator.open_record_ttl_ms / 1000, 3)
                if accumulator.open_record_ttl_ms is not None
                else None
            ),
        },
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
    if args.open_record_ttl_seconds is not None and args.open_record_ttl_seconds <= 0:
        raise ValueError("--open-record-ttl-seconds must be positive")

    accumulator = AuditAccumulator(
        open_record_ttl_ms=(
            round(args.open_record_ttl_seconds * 1000)
            if args.open_record_ttl_seconds is not None
            else None
        ),
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
