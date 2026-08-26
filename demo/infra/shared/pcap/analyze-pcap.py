#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import ctypes
import ctypes.util
import gzip
import json
import re
import shutil
import struct
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


API_NAMES = {
    0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata", 8: "OffsetCommit",
    9: "OffsetFetch", 10: "FindCoordinator", 11: "JoinGroup", 12: "Heartbeat",
    13: "LeaveGroup", 14: "SyncGroup", 18: "ApiVersions", 32: "DescribeConfigs",
}
CONSUMER_APIS = {1, 2, 8, 9, 10, 11, 12, 13, 14}
CODEC_NAMES = {0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}
FIELDS = [
    "frame.number", "frame.time_epoch", "frame.len", "frame.cap_len", "frame.protocols",
    "ip.len", "ip.hdr_len", "ipv6.plen", "tcp.stream", "tcp.srcport", "tcp.dstport",
    "tcp.len", "tcp.hdr_len", "tcp.flags.syn", "tcp.flags.ack", "tcp.flags.fin",
    "tcp.flags.reset", "tcp.analysis.retransmission", "kafka.len", "kafka.request_key",
    "kafka.response_key", "kafka.api_version", "kafka.correlation_id", "tls.record.length",
    "tcp.pdu.size", "tcp.reassembled.data", "tcp.payload", "ip.src", "ip.dst",
    "ipv6.src", "ipv6.dst", "_ws.col.Info",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Kafka packet captures with TShark.")
    parser.add_argument("path", help="Run directory, tcpdump diagnostics directory, or one pcap[.gz].")
    parser.add_argument("--output-dir", default="", help="Defaults to <run>/diagnostics/pcap-analysis.")
    parser.add_argument("--tshark", default="tshark")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def integers(value: str) -> list[int]:
    result = []
    for item in value.split(","):
        item = item.strip()
        if item and re.fullmatch(r"-?\d+", item):
            result.append(int(item))
    return result


def integer(value: str, default: int = 0) -> int:
    values = integers(value)
    return values[0] if values else default


def boolean(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def bytes_field(value: str) -> bytes:
    text = value.replace(":", "").replace(",", "").strip()
    if not text or not re.fullmatch(r"[0-9A-Fa-f]+", text) or len(text) % 2:
        return b""
    return bytes.fromhex(text)


def tshark_version(executable: str) -> str:
    result = subprocess.run([executable, "--version"], check=False, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"Could not run {executable}")
    return result.stdout.splitlines()[0].strip()


def tshark_rows(path: Path, executable: str) -> list[dict[str, str]]:
    command = [executable, "-r", str(path), "-T", "fields", "-E", "header=y", "-E", "separator=/t", "-E", "quote=d", "-E", "occurrence=a"]
    for field in FIELDS:
        command.extend(["-e", field])
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"TShark failed for {path}")
    csv.field_size_limit(sys.maxsize)
    return list(csv.DictReader(result.stdout.splitlines(), delimiter="\t", quotechar='"'))


class NativeCompression:
    def __init__(self) -> None:
        self._libraries: dict[str, Any] = {}

    def library(self, name: str) -> Any:
        if name not in self._libraries:
            path = ctypes.util.find_library(name)
            if not path:
                raise RuntimeError(f"system library lib{name} was not found")
            self._libraries[name] = ctypes.CDLL(path)
        return self._libraries[name]

    def lz4(self, data: bytes) -> bytes:
        lib = self.library("lz4")
        lib.LZ4F_createDecompressionContext.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint]
        lib.LZ4F_createDecompressionContext.restype = ctypes.c_size_t
        lib.LZ4F_freeDecompressionContext.argtypes = [ctypes.c_void_p]
        lib.LZ4F_decompress.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p]
        lib.LZ4F_decompress.restype = ctypes.c_size_t
        lib.LZ4F_isError.argtypes = [ctypes.c_size_t]
        lib.LZ4F_isError.restype = ctypes.c_uint
        context = ctypes.c_void_p()
        created = lib.LZ4F_createDecompressionContext(ctypes.byref(context), 100)
        if lib.LZ4F_isError(created):
            raise RuntimeError("could not create LZ4 frame decompression context")
        source = ctypes.create_string_buffer(data)
        position = 0
        output: list[bytes] = []
        try:
            while position < len(data):
                destination = ctypes.create_string_buffer(1024 * 1024)
                destination_size = ctypes.c_size_t(len(destination))
                source_size = ctypes.c_size_t(len(data) - position)
                remaining = lib.LZ4F_decompress(
                    context,
                    destination,
                    ctypes.byref(destination_size),
                    ctypes.cast(ctypes.byref(source, position), ctypes.c_void_p),
                    ctypes.byref(source_size),
                    None,
                )
                if lib.LZ4F_isError(remaining) or source_size.value == 0:
                    raise RuntimeError("invalid LZ4 frame")
                output.append(destination.raw[: destination_size.value])
                position += source_size.value
                if remaining == 0:
                    break
        finally:
            lib.LZ4F_freeDecompressionContext(context)
        return b"".join(output)

    def zstd(self, data: bytes) -> bytes:
        lib = self.library("zstd")
        lib.ZSTD_getFrameContentSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        lib.ZSTD_decompressBound.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        lib.ZSTD_decompressBound.restype = ctypes.c_ulonglong
        lib.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
        lib.ZSTD_decompress.restype = ctypes.c_size_t
        lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        lib.ZSTD_isError.restype = ctypes.c_uint
        source = ctypes.create_string_buffer(data)
        size = lib.ZSTD_getFrameContentSize(source, len(data))
        if size in {0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE}:
            size = lib.ZSTD_decompressBound(source, len(data))
        if size <= 0 or size > 1024 * 1024 * 1024:
            raise RuntimeError("invalid Zstandard frame size")
        destination = ctypes.create_string_buffer(size)
        written = lib.ZSTD_decompress(destination, size, source, len(data))
        if lib.ZSTD_isError(written):
            raise RuntimeError("invalid Zstandard frame")
        return destination.raw[:written]

    def snappy_block(self, data: bytes) -> bytes:
        lib = self.library("snappy")
        lib.snappy_uncompressed_length.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
        lib.snappy_uncompressed_length.restype = ctypes.c_int
        lib.snappy_uncompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
        lib.snappy_uncompress.restype = ctypes.c_int
        source = ctypes.create_string_buffer(data)
        size = ctypes.c_size_t()
        if lib.snappy_uncompressed_length(source, len(data), ctypes.byref(size)) != 0:
            raise RuntimeError("invalid Snappy block")
        destination = ctypes.create_string_buffer(size.value)
        if lib.snappy_uncompress(source, len(data), destination, ctypes.byref(size)) != 0:
            raise RuntimeError("invalid Snappy block")
        return destination.raw[: size.value]

    def snappy(self, data: bytes) -> bytes:
        if not data.startswith(b"\x82SNAPPY\x00"):
            return self.snappy_block(data)
        position = 16
        output: list[bytes] = []
        while position + 4 <= len(data):
            size = struct.unpack_from(">I", data, position)[0]
            position += 4
            if size <= 0 or position + size > len(data):
                raise RuntimeError("invalid Kafka Snappy framing")
            output.append(self.snappy_block(data[position : position + size]))
            position += size
        return b"".join(output)

    def decompress(self, codec: int, data: bytes) -> bytes:
        if codec == 0:
            return data
        if codec == 1:
            return gzip.decompress(data)
        if codec == 2:
            return self.snappy(data)
        if codec == 3:
            return self.lz4(data)
        if codec == 4:
            return self.zstd(data)
        raise RuntimeError(f"unsupported compression codec {codec}")


def signed_varint(data: bytes, position: int) -> tuple[int, int]:
    value = 0
    shift = 0
    for _ in range(10):
        if position >= len(data):
            raise ValueError("truncated varint")
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (value >> 1) ^ -(value & 1), position
        shift += 7
    raise ValueError("oversized varint")


def record_sizes(data: bytes) -> dict[str, int]:
    position = 0
    result = {"parsed_records": 0, "key_bytes": 0, "value_bytes": 0, "header_bytes": 0, "record_overhead_bytes": 0}
    while position < len(data):
        start = position
        header_payload_start = result["header_bytes"]
        length, position = signed_varint(data, position)
        end = position + length
        if length < 0 or end > len(data):
            raise ValueError("invalid record length")
        position += 1
        _, position = signed_varint(data, position)
        _, position = signed_varint(data, position)
        key_length, position = signed_varint(data, position)
        if key_length >= 0:
            result["key_bytes"] += key_length
            position += key_length
        value_length, position = signed_varint(data, position)
        if value_length >= 0:
            result["value_bytes"] += value_length
            position += value_length
        header_count, position = signed_varint(data, position)
        if header_count < 0:
            raise ValueError("invalid record header count")
        for _ in range(header_count):
            key_size, position = signed_varint(data, position)
            if key_size < 0:
                raise ValueError("invalid record header key")
            result["header_bytes"] += key_size
            position += key_size
            value_size, position = signed_varint(data, position)
            if value_size >= 0:
                result["header_bytes"] += value_size
                position += value_size
        if position != end:
            raise ValueError("record does not match its declared length")
        result["parsed_records"] += 1
        header_payload_bytes = result["header_bytes"] - header_payload_start
        result["record_overhead_bytes"] += (
            end - start - max(key_length, 0) - max(value_length, 0) - header_payload_bytes
        )
    return result


def find_record_batches(data: bytes, compression: NativeCompression) -> list[dict[str, Any]]:
    batches = []
    position = 16
    while position + 45 <= len(data):
        if data[position] != 2:
            position += 1
            continue
        start = position - 16
        batch_length = struct.unpack_from(">i", data, start + 8)[0]
        end = start + 12 + batch_length
        if batch_length < 49 or end > len(data):
            position += 1
            continue
        attributes = struct.unpack_from(">h", data, start + 21)[0]
        codec = attributes & 0x07
        last_offset_delta = struct.unpack_from(">i", data, start + 23)[0]
        record_count = struct.unpack_from(">i", data, start + 57)[0]
        if codec not in CODEC_NAMES or record_count < 0 or record_count > 10_000_000 or last_offset_delta < -1:
            position += 1
            continue
        compressed_records = data[start + 61 : end]
        batch: dict[str, Any] = {
            "codec": CODEC_NAMES[codec],
            "records": record_count,
            "batch_wire_bytes": end - start,
            "batch_header_bytes": 61,
            "compressed_record_bytes": len(compressed_records),
            "decompression_status": "success",
        }
        try:
            uncompressed = compression.decompress(codec, compressed_records)
            sizes = record_sizes(uncompressed)
            batch.update(sizes)
            batch["uncompressed_record_bytes"] = len(uncompressed)
            batch["compression_savings_bytes"] = len(uncompressed) - len(compressed_records)
            if sizes["parsed_records"] != record_count:
                batch["decompression_status"] = "record-count-mismatch"
        except Exception as error:
            batch["decompression_status"] = "unavailable"
            batch["decompression_error"] = str(error)
        batches.append(batch)
        position = end
    return batches


def expected_role(path: Path) -> str:
    parts = set(path.parts)
    if "application" in parts:
        return "consumer"
    if "load-test" in parts:
        return "producer"
    if "consumer" in path.name.lower():
        return "consumer"
    if "producer" in path.name.lower():
        return "producer"
    return "unknown"


def capture_metadata(path: Path) -> dict[str, Any]:
    name = path.name
    for suffix in (".pcap.gz", ".pcap"):
        if name.endswith(suffix):
            metadata_path = path.with_name(name[: -len(suffix)] + ".json")
            if metadata_path.is_file():
                try:
                    document = json.loads(metadata_path.read_text(encoding="utf-8"))
                    return document if isinstance(document, dict) else {}
                except (OSError, json.JSONDecodeError):
                    return {}
    return {}


def connection_role(request_apis: set[int]) -> str:
    producer = 0 in request_apis
    consumer = bool(request_apis & CONSUMER_APIS)
    if producer and consumer:
        return "mixed"
    if producer:
        return "producer"
    if consumer:
        return "consumer"
    return "unknown"


def add_numbers(target: dict[str, int], source: dict[str, Any], keys: list[str]) -> None:
    for key in keys:
        target[key] = target.get(key, 0) + int(source.get(key) or 0)


def analyze_capture(path: Path, executable: str, compression: NativeCompression) -> dict[str, Any]:
    rows = tshark_rows(path, executable)
    role = expected_role(path)
    metadata = capture_metadata(path)
    connections: dict[int, dict[str, Any]] = defaultdict(lambda: {"request_apis": set(), "syn": False, "fin": False, "reset": False})
    request_by_correlation: dict[tuple[int, int], int] = {}
    messages: list[dict[str, Any]] = []
    tls_detected = False
    for row in rows:
        stream = integer(row["tcp.stream"], -1)
        if stream < 0:
            continue
        connection = connections[stream]
        connection["syn"] = connection["syn"] or (boolean(row["tcp.flags.syn"]) and not boolean(row["tcp.flags.ack"]))
        connection["fin"] = connection["fin"] or boolean(row["tcp.flags.fin"])
        connection["reset"] = connection["reset"] or boolean(row["tcp.flags.reset"])
        protocols = row["frame.protocols"].split(":")
        tls_detected = tls_detected or "tls" in protocols
        request_api = integer(row["kafka.request_key"], -1)
        response_api = integer(row["kafka.response_key"], -1)
        correlation = integer(row["kafka.correlation_id"], -1)
        if request_api >= 0:
            connection["request_apis"].add(request_api)
            if correlation >= 0:
                request_by_correlation[(stream, correlation)] = request_api
        kafka_lengths = integers(row["kafka.len"])
        if kafka_lengths:
            direction = "request" if request_api >= 0 or integer(row["tcp.dstport"], -1) == 9092 else "response"
            api_key = request_api if direction == "request" else response_api
            messages.append(
                {
                    "stream": stream,
                    "direction": direction,
                    "api_key": api_key,
                    "correlation": correlation,
                    "bytes": sum(length + 4 for length in kafka_lengths),
                    "raw": bytes_field(row["tcp.reassembled.data"]) or bytes_field(row["tcp.payload"]),
                }
            )

    for message in messages:
        if message["direction"] == "response" and message["api_key"] < 0:
            message["api_key"] = request_by_correlation.get((message["stream"], message["correlation"]), -1)
    roles = {stream: connection_role(data["request_apis"]) for stream, data in connections.items()}
    selected = {stream for stream, stream_role in roles.items() if stream_role in {role, "mixed"}}
    warnings = []
    role_scoped_capture = metadata.get("backend") == "kubernetes" or (
        bool(metadata.get("host_address")) and bool(metadata.get("excluded_network"))
    )
    if role in {"producer", "consumer"} and role_scoped_capture:
        selected = set(connections)
    if not selected:
        selected = set(connections)
        warnings.append(f"No {role} Kafka connection could be classified; all observed TCP streams were used")
    if tls_detected:
        selected = set(connections)
        warnings.append("TLS traffic detected; Kafka message, record-batch, and compression details are unavailable")

    network = {
        "frames": 0, "captured_wire_bytes": 0, "captured_bytes": 0, "truncated_frames": 0,
        "link_layer_bytes": 0, "ip_header_bytes": 0, "tcp_header_bytes": 0,
        "tcp_payload_bytes": 0, "retransmitted_tcp_payload_bytes": 0, "tls_record_bytes": 0,
        "tls_record_header_bytes": 0,
    }
    for row in rows:
        stream = integer(row["tcp.stream"], -1)
        if stream not in selected:
            continue
        frame_length = integer(row["frame.len"])
        captured_length = integer(row["frame.cap_len"])
        ip_length = integer(row["ip.len"])
        ipv6_payload = integer(row["ipv6.plen"], -1)
        ip_header = integer(row["ip.hdr_len"], 40 if ipv6_payload >= 0 else 0)
        if not ip_length and ipv6_payload >= 0:
            ip_length = 40 + ipv6_payload
        tcp_header = integer(row["tcp.hdr_len"])
        tcp_payload = integer(row["tcp.len"])
        tls_lengths = integers(row["tls.record.length"])
        network["frames"] += 1
        network["captured_wire_bytes"] += frame_length
        network["captured_bytes"] += captured_length
        network["truncated_frames"] += int(captured_length < frame_length)
        network["link_layer_bytes"] += max(0, frame_length - ip_length)
        network["ip_header_bytes"] += ip_header
        network["tcp_header_bytes"] += tcp_header
        network["tcp_payload_bytes"] += tcp_payload
        network["retransmitted_tcp_payload_bytes"] += tcp_payload if row["tcp.analysis.retransmission"] else 0
        network["tls_record_bytes"] += sum(length + 5 for length in tls_lengths)
        network["tls_record_header_bytes"] += 5 * len(tls_lengths)
    network["network_header_bytes"] = network["link_layer_bytes"] + network["ip_header_bytes"] + network["tcp_header_bytes"]
    network["network_overhead_percent"] = round(network["network_header_bytes"] * 100 / network["captured_wire_bytes"], 3) if network["captured_wire_bytes"] else None

    selected_messages = [message for message in messages if message["stream"] in selected]
    api_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"requests": 0, "responses": 0, "request_bytes": 0, "response_bytes": 0})
    batch_totals: dict[str, Any] = {
        "batches": 0, "records": 0, "batch_wire_bytes": 0, "batch_header_bytes": 0,
        "compressed_record_bytes": 0, "uncompressed_record_bytes": 0, "compression_savings_bytes": 0,
        "parsed_records": 0, "key_bytes": 0, "value_bytes": 0, "header_bytes": 0,
        "record_overhead_bytes": 0, "decompression_unavailable_batches": 0,
    }
    codecs: Counter[str] = Counter()
    batch_keys = [
        "records", "batch_wire_bytes", "batch_header_bytes", "compressed_record_bytes",
        "uncompressed_record_bytes", "compression_savings_bytes", "parsed_records", "key_bytes",
        "value_bytes", "header_bytes", "record_overhead_bytes",
    ]
    for message in selected_messages:
        api_key = int(message["api_key"])
        api_name = API_NAMES.get(api_key, f"Unknown({api_key})")
        count = api_counts[api_name]
        count[f"{message['direction']}s"] += 1
        count[f"{message['direction']}_bytes"] += int(message["bytes"])
        carries_records = (
            role == "producer" and message["direction"] == "request" and api_key == 0
        ) or (
            role == "consumer" and message["direction"] == "response" and api_key == 1
        )
        if carries_records and message["raw"]:
            for batch in find_record_batches(message["raw"], compression):
                batch_totals["batches"] += 1
                codecs[batch["codec"]] += 1
                add_numbers(batch_totals, batch, batch_keys)
                if batch["decompression_status"] == "unavailable":
                    batch_totals["decompression_unavailable_batches"] += 1
    batch_totals["codecs"] = dict(sorted(codecs.items()))
    known_uncompressed = batch_totals["uncompressed_record_bytes"]
    compressed = batch_totals["compressed_record_bytes"]
    batch_totals["compression_ratio_percent"] = round(compressed * 100 / known_uncompressed, 3) if known_uncompressed else None
    batch_totals["space_saving_percent"] = round(batch_totals["compression_savings_bytes"] * 100 / known_uncompressed, 3) if known_uncompressed else None
    kafka_bytes = sum(message["bytes"] for message in selected_messages)
    protocol = {
        "tls_detected": tls_detected,
        "kafka_analysis_available": not tls_detected,
        "kafka_messages": len(selected_messages) if not tls_detected else None,
        "kafka_pdu_bytes": kafka_bytes if not tls_detected else None,
        "kafka_protocol_bytes_excluding_batches": max(0, kafka_bytes - batch_totals["batch_wire_bytes"]) if not tls_detected else None,
        "api_types": dict(sorted(api_counts.items())),
        "record_batches": batch_totals,
    }
    selected_connections = [connections[stream] for stream in selected]
    return {
        "path": str(path),
        "role": role,
        "capture_scope": "role" if role_scoped_capture else "classified-connections",
        "status": "partial" if warnings else "success",
        "warnings": warnings,
        "connections": {
            "observed": len(selected),
            "opened_during_capture": sum(bool(item["syn"]) for item in selected_connections),
            "active_at_capture_start": sum(not bool(item["syn"]) for item in selected_connections),
            "closed_during_capture": sum(bool(item["fin"] or item["reset"]) for item in selected_connections),
        },
        "network": network,
        "protocol": protocol,
    }


def capture_paths(source: Path) -> tuple[list[Path], Path]:
    if source.is_file():
        return [source], source.parent / "pcap-analysis"
    tcpdump = source / "diagnostics" / "tcpdump" if (source / "diagnostics" / "tcpdump").is_dir() else source
    captures = sorted([*tcpdump.rglob("*.pcap"), *tcpdump.rglob("*.pcap.gz")])
    default_output = tcpdump.parent / "pcap-analysis" if tcpdump.name == "tcpdump" else tcpdump / "pcap-analysis"
    return captures, default_output


def aggregate_role(captures: list[dict[str, Any]], role: str) -> dict[str, Any]:
    selected = [
        capture for capture in captures
        if capture["role"] == role and capture.get("status") != "failed"
    ]
    result: dict[str, Any] = {
        "capture_count": len(selected), "connections": {}, "network": {},
        "protocol": {"api_types": {}, "record_batches": {}},
    }
    for capture in selected:
        add_numbers(result["connections"], capture["connections"], list(capture["connections"]))
        add_numbers(result["network"], capture["network"], [key for key, value in capture["network"].items() if isinstance(value, int)])
        protocol = capture["protocol"]
        for key in ("kafka_messages", "kafka_pdu_bytes", "kafka_protocol_bytes_excluding_batches"):
            if protocol.get(key) is not None:
                result["protocol"][key] = result["protocol"].get(key, 0) + int(protocol[key])
        result["protocol"]["tls_detected"] = result["protocol"].get("tls_detected", False) or protocol["tls_detected"]
        for api_name, values in protocol["api_types"].items():
            target = result["protocol"]["api_types"].setdefault(api_name, {})
            add_numbers(target, values, list(values))
        batches = protocol["record_batches"]
        target_batches = result["protocol"]["record_batches"]
        add_numbers(target_batches, batches, [key for key, value in batches.items() if isinstance(value, int)])
        codecs = target_batches.setdefault("codecs", {})
        add_numbers(codecs, batches.get("codecs", {}), list(batches.get("codecs", {})))
    network = result["network"]
    network["network_overhead_percent"] = round(network.get("network_header_bytes", 0) * 100 / network.get("captured_wire_bytes", 0), 3) if network.get("captured_wire_bytes") else None
    batches = result["protocol"]["record_batches"]
    uncompressed = batches.get("uncompressed_record_bytes", 0)
    compressed = batches.get("compressed_record_bytes", 0)
    batches["compression_ratio_percent"] = round(compressed * 100 / uncompressed, 3) if uncompressed else None
    batches["space_saving_percent"] = round(batches.get("compression_savings_bytes", 0) * 100 / uncompressed, 3) if uncompressed else None
    return result


def human_bytes(value: Any) -> str:
    size = float(value or 0)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(size) < 1024 or unit == "GiB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} GiB"


def render_text(summary: dict[str, Any]) -> str:
    lines = ["Kafka packet capture analysis", f"Generated: {summary['generated_at']}", f"TShark: {summary['tshark_version']}", f"Status: {summary['status']}", ""]
    for role in ("producer", "consumer"):
        data = summary["roles"][role]
        network = data["network"]
        protocol = data["protocol"]
        batches = protocol["record_batches"]
        lines.extend(
            [
                role.upper(),
                f"  captures: {data['capture_count']}",
                f"  connections observed/opened/closed: {data['connections'].get('observed', 0)} / {data['connections'].get('opened_during_capture', 0)} / {data['connections'].get('closed_during_capture', 0)}",
                f"  captured wire bytes: {human_bytes(network.get('captured_wire_bytes'))}",
                f"  network headers: {human_bytes(network.get('network_header_bytes'))} ({network.get('network_overhead_percent')}%)",
                f"  TCP payload: {human_bytes(network.get('tcp_payload_bytes'))}",
                f"  Kafka messages / bytes: {protocol.get('kafka_messages', 'unavailable')} / {human_bytes(protocol.get('kafka_pdu_bytes')) if protocol.get('kafka_pdu_bytes') is not None else 'unavailable'}",
                f"  record batches / records: {batches.get('batches', 0)} / {batches.get('records', 0)}",
                f"  batch headers: {human_bytes(batches.get('batch_header_bytes'))}",
                f"  compressed / uncompressed records: {human_bytes(batches.get('compressed_record_bytes'))} / {human_bytes(batches.get('uncompressed_record_bytes'))}",
                f"  compression ratio / saving: {batches.get('compression_ratio_percent')}% / {batches.get('space_saving_percent')}%",
                f"  codecs: {', '.join(f'{name}={count}' for name, count in batches.get('codecs', {}).items()) or 'unavailable'}",
                f"  record values / keys / headers / metadata: {human_bytes(batches.get('value_bytes'))} / {human_bytes(batches.get('key_bytes'))} / {human_bytes(batches.get('header_bytes'))} / {human_bytes(batches.get('record_overhead_bytes'))}",
                "  API messages:",
            ]
        )
        if protocol["api_types"]:
            for name, values in protocol["api_types"].items():
                lines.append(f"    {name}: requests={values.get('requests', 0)}, responses={values.get('responses', 0)}, bytes={values.get('request_bytes', 0) + values.get('response_bytes', 0)}")
        else:
            lines.append("    unavailable")
        lines.append("")
    if summary["warnings"]:
        lines.append("WARNINGS")
        lines.extend(f"  - {warning}" for warning in summary["warnings"])
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    source = Path(args.path).resolve()
    captures, default_output = capture_paths(source)
    if not captures:
        raise FileNotFoundError(f"No pcap files were found under {source}")
    if shutil.which(args.tshark) is None:
        raise FileNotFoundError(f"TShark executable was not found: {args.tshark}")
    output = Path(args.output_dir).resolve() if args.output_dir else default_output
    output.mkdir(parents=True, exist_ok=True)
    version = tshark_version(args.tshark)
    compression = NativeCompression()
    results = []
    warnings = []
    for capture in captures:
        try:
            results.append(analyze_capture(capture, args.tshark, compression))
        except Exception as error:
            warning = f"{capture}: {error}"
            warnings.append(warning)
            results.append({"path": str(capture), "role": expected_role(capture), "status": "failed", "warnings": [str(error)]})
    warnings.extend(warning for capture in results for warning in capture.get("warnings", []))
    summary = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "status": "failed" if all(capture["status"] == "failed" for capture in results) else ("partial" if warnings else "success"),
        "tshark_version": version,
        "source": str(source),
        "captures": results,
        "roles": {
            "producer": aggregate_role(results, "producer"),
            "consumer": aggregate_role(results, "consumer"),
        },
        "warnings": warnings,
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "summary.txt").write_text(render_text(summary), encoding="utf-8")
    print(f"Packet capture analysis: {summary['status']}")
    print(f"  captures={len(captures)}")
    print(f"  output={output}")
    return 1 if summary["status"] == "failed" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Packet capture analysis failed: {error}", file=sys.stderr)
        raise SystemExit(1)
