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


class AnalyzePcapTest(unittest.TestCase):
    def test_tshark_rows_accepts_reassembled_fields_larger_than_csv_default(self) -> None:
        raw = "a" * (128 * 1024 + 1)
        stdout = "\t".join(analyze_pcap.FIELDS) + "\n" + "\t".join(["1", *([""] * 24), raw, *([""] * 6)]) + "\n"
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

    def test_role_can_be_inferred_from_capture_filename(self) -> None:
        self.assertEqual("producer", analyze_pcap.expected_role(Path("sample-producer.pcap.gz")))
        self.assertEqual("consumer", analyze_pcap.expected_role(Path("sample-consumer.pcap")))


if __name__ == "__main__":
    unittest.main()
