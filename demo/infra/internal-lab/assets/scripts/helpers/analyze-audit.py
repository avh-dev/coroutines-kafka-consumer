#!/usr/bin/env python3

from __future__ import annotations

import argparse
import socket
import sys
from collections import Counter
from dataclasses import dataclass
from typing import BinaryIO

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
    kafka_timestamp_ms: int
    audit_timestamp_ms: int
    message_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze CKC demo audit records stored in Redis.")
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--redis-key", default="audit")
    parser.add_argument("--batch-size", default=10_000, type=int)
    return parser.parse_args()


def encode_command(*parts: str | int) -> bytes:
    encoded = [str(part).encode("utf-8") for part in parts]
    return (
        f"*{len(encoded)}\r\n".encode("ascii")
        + b"".join(f"${len(part)}\r\n".encode("ascii") + part + b"\r\n" for part in encoded)
    )


def read_line(file: BinaryIO) -> bytes:
    line = file.readline()
    if not line.endswith(b"\r\n"):
        raise ValueError("Redis returned an incomplete response")
    return line[:-2]


def read_response(file: BinaryIO) -> object:
    prefix = file.read(1)
    if prefix == b"+":
        return read_line(file).decode("utf-8")
    if prefix == b"-":
        raise ValueError(f"Redis command failed: {read_line(file).decode('utf-8')}")
    if prefix == b":":
        return int(read_line(file))
    if prefix == b"$":
        size = int(read_line(file))
        if size < 0:
            return None
        data = file.read(size)
        if file.read(2) != b"\r\n":
            raise ValueError("Redis returned an incomplete bulk string")
        return data.decode("utf-8")
    if prefix == b"*":
        size = int(read_line(file))
        return [read_response(file) for _ in range(size)]
    raise ValueError(f"Unsupported Redis response prefix: {prefix!r}")


def parse_record(value: str) -> AuditRecord:
    parts = value.split("\t", 6)
    if len(parts) != 7:
        raise ValueError(f"expected 7 audit fields, got {len(parts)}: {value!r}")
    record_type, topic_id, partition, offset, kafka_ts, audit_ts, message_key = parts
    if record_type not in {"P", "C"}:
        raise ValueError(f"unexpected audit record type: {record_type}")
    return AuditRecord(
        record_type=record_type,
        key=RecordKey(int(topic_id), int(partition), int(offset)),
        kafka_timestamp_ms=int(kafka_ts),
        audit_timestamp_ms=int(audit_ts),
        message_key=message_key,
    )


def read_records(host: str, port: int, key: str, batch_size: int) -> list[AuditRecord]:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")

    records: list[AuditRecord] = []
    with socket.create_connection((host, port)) as connection:
        file = connection.makefile("rb")
        connection.sendall(encode_command("LLEN", key))
        total = read_response(file)
        if not isinstance(total, int):
            raise ValueError(f"Redis LLEN returned an unexpected response: {total!r}")
        print(f"Reading {total} Redis audit records.", file=sys.stderr)
        reported_percent = 0
        if total == 0:
            print("Read progress: 100%", file=sys.stderr)

        start = 0
        while True:
            connection.sendall(encode_command("LRANGE", key, start, start + batch_size - 1))
            values = read_response(file)
            if not isinstance(values, list):
                raise ValueError(f"Redis LRANGE returned an unexpected response: {values!r}")
            records.extend(parse_record(value) for value in values if isinstance(value, str))
            read_count = min(len(records), total)
            percent = 100 if total == 0 else read_count * 100 // total
            while reported_percent + 10 <= percent:
                reported_percent += 10
                print(f"Read progress: {reported_percent}%", file=sys.stderr)
            if len(values) < batch_size:
                return records
            start += batch_size


def percentile(values: list[int], percent: float) -> int:
    if not values:
        return 0
    index = round((len(values) - 1) * percent)
    return values[index]


def print_summary(title: str, records: list[AuditRecord]) -> None:
    published = [record for record in records if record.record_type == "P"]
    processed = [record for record in records if record.record_type == "C"]

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

    print(title)
    print(f"  published_records={len(published)}")
    print(f"  published_unique={len(published_keys)}")
    print(f"  processed_records={len(processed)}")
    print(f"  processed_unique={len(processed_unique)}")
    print(f"  missing_processed={len(missing)}")
    print(f"  duplicate_published={duplicate_published}")
    print(f"  duplicate_processed={duplicate_processed}")
    print(f"  processed_without_publish={len(unknown_processed)}")
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


def main() -> int:
    args = parse_args()
    records = read_records(args.redis_host, args.redis_port, args.redis_key, args.batch_size)

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
