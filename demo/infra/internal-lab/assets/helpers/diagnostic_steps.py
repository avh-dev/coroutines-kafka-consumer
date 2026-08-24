from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def duration_seconds(value: Any, context: str, *, required: bool = False) -> int:
    if value in ("", None):
        if required:
            raise ValueError(f"{context} must be defined")
        return 0
    if isinstance(value, int):
        seconds = value
    else:
        text = str(value).strip()
        if text.isdigit():
            seconds = int(text)
        else:
            matches = list(re.finditer(r"(\d+)\s*([hms])", text))
            if not matches or "".join(match.group(0) for match in matches) != text.replace(" ", ""):
                raise ValueError(f"{context} must be a duration like 30s, 2m30s, or 1h")
            multipliers = {"h": 3600, "m": 60, "s": 1}
            seconds = sum(int(match.group(1)) * multipliers[match.group(2)] for match in matches)
    if seconds < 0 or (required and seconds == 0):
        qualifier = "positive" if required else "non-negative"
        raise ValueError(f"{context} must be {qualifier}")
    return seconds


def size_bytes(value: Any, context: str) -> int:
    if isinstance(value, int):
        amount, unit = value, "b"
    else:
        match = re.fullmatch(r"(\d+)\s*(B|Ki|Mi|Gi)?", str(value).strip(), re.IGNORECASE)
        if not match:
            raise ValueError(f"{context} must be bytes or a size like 256Mi")
        amount, unit = int(match.group(1)), (match.group(2) or "B").lower()
    if amount <= 0:
        raise ValueError(f"{context} must be positive")
    return amount * {"b": 1, "ki": 1024, "mi": 1024**2, "gi": 1024**3}[unit]


def normalize(definition: dict[str, Any], definition_path: Path) -> list[dict[str, Any]]:
    raw_steps = definition.get("diagnostic_steps", [])
    if raw_steps in ("", None):
        return []
    if not isinstance(raw_steps, list):
        raise ValueError(f"Test definition diagnostic_steps must be a list: {definition_path}")

    supported_targets = {"application", "load-test"}
    result: list[dict[str, Any]] = []
    names: set[str] = set()
    intervals_by_target: dict[str, list[tuple[int, int, int]]] = {}
    previous_at = -1
    for index, raw_step in enumerate(raw_steps, start=1):
        context = f"diagnostic_steps[{index}]"
        if not isinstance(raw_step, dict):
            raise ValueError(f"{context} must be an object: {definition_path}")
        if "at" not in raw_step:
            raise ValueError(f"{context} must define at: {definition_path}")
        if str(raw_step.get("type", "")).strip() != "tcpdump":
            raise ValueError(f"{context}.type must be 'tcpdump': {definition_path}")

        at_seconds = duration_seconds(raw_step["at"], f"{context}.at")
        if at_seconds < previous_at:
            raise ValueError(f"diagnostic_steps must be ordered by at: {definition_path}")
        previous_at = at_seconds
        name = str(raw_step.get("name", "")).strip()
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", name):
            raise ValueError(f"{context}.name must use lower-case letters, digits, and hyphens: {definition_path}")
        if name in names:
            raise ValueError(f"Duplicate diagnostic step name {name!r}: {definition_path}")
        names.add(name)

        capture_duration = duration_seconds(raw_step.get("duration"), f"{context}.duration", required=True)
        if capture_duration > 300:
            raise ValueError(f"{context}.duration must not exceed 5m: {definition_path}")
        targets = raw_step.get("targets")
        if not isinstance(targets, list) or not targets:
            raise ValueError(f"{context}.targets must be a non-empty list: {definition_path}")
        normalized_targets = [str(target).strip() for target in targets]
        if len(normalized_targets) != len(set(normalized_targets)):
            raise ValueError(f"{context}.targets must not contain duplicates: {definition_path}")
        unsupported = set(normalized_targets) - supported_targets
        if unsupported:
            raise ValueError(f"{context}.targets contains unsupported values {sorted(unsupported)}: {definition_path}")
        required = raw_step.get("required", False)
        if not isinstance(required, bool):
            raise ValueError(f"{context}.required must be true or false: {definition_path}")
        params = raw_step.get("params", {})
        if params in ("", None):
            params = {}
        if not isinstance(params, dict):
            raise ValueError(f"{context}.params must be an object: {definition_path}")
        interface = str(params.get("interface", "auto")).strip()
        if not re.fullmatch(r"[A-Za-z0-9_.:@-]{1,64}", interface):
            raise ValueError(f"{context}.params.interface is invalid: {definition_path}")
        snaplen = int(params.get("snaplen", 0))
        if snaplen < 0 or snaplen > 262144:
            raise ValueError(f"{context}.params.snaplen must be between 0 and 262144: {definition_path}")
        capture_filter = str(params.get("filter", "tcp port 9092")).strip()
        if not capture_filter or len(capture_filter) > 512 or any(char in capture_filter for char in "\r\n\0"):
            raise ValueError(f"{context}.params.filter is invalid: {definition_path}")
        max_size = size_bytes(params.get("max_file_size", "256Mi"), f"{context}.params.max_file_size")
        if max_size > 256 * 1024 * 1024:
            raise ValueError(f"{context}.params.max_file_size must not exceed 256Mi: {definition_path}")

        end_seconds = at_seconds + capture_duration
        for target in normalized_targets:
            for other_start, other_end, other_index in intervals_by_target.get(target, []):
                if at_seconds < other_end and other_start < end_seconds:
                    raise ValueError(f"{context} overlaps diagnostic_steps[{other_index}] for target {target!r}: {definition_path}")
            intervals_by_target.setdefault(target, []).append((at_seconds, end_seconds, index))
        result.append(
            {
                "atSeconds": at_seconds, "durationSeconds": capture_duration, "type": "tcpdump", "name": name,
                "targets": normalized_targets, "required": required,
                "params": {"interface": interface, "snaplen": snaplen, "filter": capture_filter, "maxFileSizeBytes": max_size},
            }
        )
    return result
