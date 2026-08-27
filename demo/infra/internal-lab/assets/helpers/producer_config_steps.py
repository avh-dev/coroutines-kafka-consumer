from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def parse_duration_seconds(value: Any, context: str) -> int:
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{context} must be non-negative: {value}")
        return value
    text = str(value).strip()
    if not text:
        raise ValueError(f"{context} must not be empty")
    if text.isdigit():
        return int(text)
    compact = text.replace(" ", "")
    matches = list(re.finditer(r"(\d+)([hms])", compact))
    if not matches or "".join(match.group(0) for match in matches) != compact:
        raise ValueError(f"{context} must be a duration like 30s, 2m30s, or 1h")
    multipliers = {"h": 3600, "m": 60, "s": 1}
    return sum(int(match.group(1)) * multipliers[match.group(2)] for match in matches)


def positive_int(value: Any, context: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{context} must be positive")
    return parsed


def non_negative_int(value: Any, context: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{context} must be non-negative")
    return parsed


def normalize(load_test: dict[str, Any], definition_path: Path | str) -> list[dict[str, Any]]:
    raw_steps = load_test.get("producer_config_steps", [])
    if raw_steps in (None, ""):
        return []
    if not isinstance(raw_steps, list):
        raise ValueError(f"load_test.producer_config_steps must be a list: {definition_path}")

    field_mappings = {
        "linger_ms": ("lingerMs", non_negative_int),
        "batch_size": ("batchSize", positive_int),
        "buffer_memory": ("bufferMemory", positive_int),
    }
    supported_topics = {"all", "order", "batch", "telemetry"}
    result: list[dict[str, Any]] = []
    previous_at = -1
    for index, raw_step in enumerate(raw_steps, start=1):
        context = f"load_test.producer_config_steps[{index}]"
        if not isinstance(raw_step, dict):
            raise ValueError(f"{context} must be an object: {definition_path}")
        if "at" not in raw_step:
            raise ValueError(f"{context} must define at: {definition_path}")
        at_seconds = parse_duration_seconds(raw_step["at"], f"{context}.at")
        if at_seconds < previous_at:
            raise ValueError(f"load_test.producer_config_steps must be ordered by at: {definition_path}")
        previous_at = at_seconds

        topic = str(raw_step.get("topic", "all")).strip().lower()
        if topic not in supported_topics:
            raise ValueError(f"{context}.topic must be one of {sorted(supported_topics)}: {definition_path}")
        normalized: dict[str, Any] = {"atSeconds": at_seconds, "topic": topic}
        for source, (target, parser) in field_mappings.items():
            if source in raw_step and raw_step[source] not in (None, ""):
                normalized[target] = parser(raw_step[source], f"{context}.{source}")
        if "compression_type" in raw_step and raw_step["compression_type"] not in (None, ""):
            compression_type = str(raw_step["compression_type"]).strip().lower()
            if compression_type not in {"none", "gzip", "snappy", "lz4", "zstd"}:
                raise ValueError(f"{context}.compression_type is unsupported: {compression_type!r}")
            normalized["compressionType"] = compression_type
        if len(normalized) == 2:
            raise ValueError(f"{context} must change at least one producer setting: {definition_path}")
        unknown = sorted(
            set(raw_step)
            - {"at", "topic", "linger_ms", "batch_size", "compression_type", "buffer_memory"}
        )
        if unknown:
            raise ValueError(f"{context} contains unsupported fields {unknown}: {definition_path}")
        result.append(normalized)
    return result
