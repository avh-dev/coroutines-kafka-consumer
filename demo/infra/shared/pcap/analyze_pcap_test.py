from __future__ import annotations

import importlib.util
import struct
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("analyze-pcap.py")
SPEC = importlib.util.spec_from_file_location("analyze_pcap", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
analyze_pcap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(analyze_pcap)


def signed_varint(value: int) -> bytes:
    unsigned = (value << 1) ^ (value >> 63)
    result = bytearray()
    while unsigned > 0x7F:
        result.append((unsigned & 0x7F) | 0x80)
        unsigned >>= 7
    result.append(unsigned)
    return bytes(result)


def compact(value: int) -> bytes:
    value += 1
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def record(key: bytes, value: bytes, header_key: bytes = b"", header_value: bytes = b"") -> bytes:
    body = b"".join(
        (
            b"\x00",
            signed_varint(0),
            signed_varint(0),
            signed_varint(len(key)), key,
            signed_varint(len(value)), value,
            signed_varint(1),
            signed_varint(len(header_key)), header_key,
            signed_varint(len(header_value)), header_value,
        )
    )
    return signed_varint(len(body)) + body


def batch(records: bytes, count: int) -> bytes:
    payload = bytearray(61)
    struct.pack_into(">q", payload, 0, 0)
    struct.pack_into(">i", payload, 8, 49 + len(records))
    payload[16] = 2
    struct.pack_into(">h", payload, 21, 0)
    struct.pack_into(">i", payload, 23, count - 1)
    struct.pack_into(">i", payload, 57, count)
    payload.extend(records)
    return bytes(payload)


def unsigned_varint(value: int) -> bytes:
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def kafka_string(value: str | None) -> bytes:
    if value is None:
        return struct.pack(">h", -1)
    encoded = value.encode()
    return struct.pack(">h", len(encoded)) + encoded


def kafka_bytes(value: bytes) -> bytes:
    return struct.pack(">i", len(value)) + value


class AnalyzePcapTest(unittest.TestCase):
    def test_tshark_rows_accepts_reassembled_fields_larger_than_csv_default(self) -> None:
        raw = "a" * (128 * 1024 + 1)
        columns = [""] * len(analyze_pcap.FIELDS)
        columns[analyze_pcap.FIELDS.index("frame.number")] = "1"
        columns[analyze_pcap.FIELDS.index("tcp.reassembled.data")] = raw
        stdout = "\t".join(analyze_pcap.FIELDS) + "\n" + "\t".join(columns) + "\n"
        completed = analyze_pcap.subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")
        with patch.object(analyze_pcap.subprocess, "run", return_value=completed):
            rows = analyze_pcap.tshark_rows(Path("large.pcap"), "tshark")
        self.assertEqual(raw, rows[0]["tcp.reassembled.data"])

    def test_record_breakdown_is_exhaustive(self) -> None:
        encoded = record(b"key", b"value", b"trace", b"abc")
        sizes = analyze_pcap.record_sizes(encoded)
        self.assertEqual(1, sizes["parsed_records"])
        self.assertEqual(3, sizes["key_bytes"])
        self.assertEqual(5, sizes["value_bytes"])
        self.assertEqual(8, sizes["header_bytes"])
        self.assertEqual(len(encoded), sum(sizes[key] for key in ("key_bytes", "value_bytes", "header_bytes", "record_overhead_bytes")))

    def test_finds_uncompressed_record_batch(self) -> None:
        records = record(b"a", b"one") + record(b"b", b"two")
        found = analyze_pcap.find_record_batches(b"\x00" * 12 + batch(records, 2), analyze_pcap.NativeCompression())
        self.assertEqual(1, len(found))
        self.assertEqual("none", found[0]["codec"])
        self.assertEqual(2, found[0]["parsed_records"])
        self.assertEqual(len(records), found[0]["compressed_record_bytes"])
        self.assertEqual(len(records), found[0]["uncompressed_record_bytes"])

    def test_parses_produce_v9_topic_blocks_without_tshark_topic_fields(self) -> None:
        records = batch(record(b"same-key", b"value"), 1)
        body = b"".join((
            struct.pack(">hhi", 0, 9, 7), kafka_string("load-test"), b"\x00", b"\x00",
            struct.pack(">hi", 1, 30_000), compact(1), compact(len("order.events.v1")), b"order.events.v1",
            compact(1), struct.pack(">i", 0), compact(len(records)), records, b"\x00", b"\x00", b"\x00",
        ))
        parsed = analyze_pcap.parse_produce_request_v9(struct.pack(">i", len(body)) + body)
        self.assertEqual([{"topic": "order.events.v1", "records": records}], parsed)

    def test_parses_fetch_v17_topic_uuid_and_records(self) -> None:
        records = batch(record(b"same-key", b"value"), 1)
        topic_uuid = bytes(range(16))
        body = b"".join((
            struct.pack(">i", 7), unsigned_varint(0), struct.pack(">i", 0), struct.pack(">h", 0), struct.pack(">i", 0),
            unsigned_varint(2), topic_uuid, unsigned_varint(2), struct.pack(">i", 0), struct.pack(">h", 0),
            struct.pack(">qqq", 1, 1, 0), unsigned_varint(0), struct.pack(">i", -1),
            unsigned_varint(len(records) + 1), records, unsigned_varint(0), unsigned_varint(0), unsigned_varint(0),
        ))
        parsed = analyze_pcap.parse_fetch_response_v17(struct.pack(">i", len(body)) + body)
        self.assertEqual(
            [{"topic_id": analyze_pcap.kafka_topic_id(topic_uuid), "records": records}],
            parsed,
        )

    def test_role_can_be_inferred_from_capture_filename(self) -> None:
        self.assertEqual("producer", analyze_pcap.expected_role(Path("sample-producer.pcap.gz")))
        self.assertEqual("consumer", analyze_pcap.expected_role(Path("sample-consumer.pcap")))

    def test_role_aggregation_preserves_topic_captured_wire_bytes(self) -> None:
        capture = {
            "role": "producer", "status": "success", "connections": {},
            "network": {"captured_wire_bytes": 1000},
            "protocol": {
                "tls_detected": False, "api_types": {}, "record_batches": {},
                "topics": {"order.events.v1": {"records": 4, "wire_bytes": 200, "captured_wire_bytes": 600}},
            },
        }
        summary = analyze_pcap.aggregate_role([capture], "producer")
        topic = summary["protocol"]["topics"]["order.events.v1"]
        self.assertEqual(4, topic["records"])
        self.assertEqual(600, topic["captured_wire_bytes"])


if __name__ == "__main__":
    unittest.main()
