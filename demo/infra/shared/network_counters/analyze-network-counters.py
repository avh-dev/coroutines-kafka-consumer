#!/usr/bin/env python3
"""Reduce raw nftables Kafka network-counter snapshots into bytes/message evidence."""
from __future__ import annotations

import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path


def epoch_ms(value: str) -> int:
    return round(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshots-dir", required=True)
    parser.add_argument("--audit-dir", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def samples(root: Path) -> dict[str, list[list[dict]]]:
    grouped: dict[str, list[list[dict]]] = {}
    for path in root.glob("*/*/*/samples.json"):
        values = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(values, list) and values:
            grouped.setdefault(path.parents[2].name, []).append(values)
    return grouped


def stable(values: list[dict]) -> tuple[list[dict], str | None]:
    if len(values) < 2:
        return [], "fewer than two snapshots"
    uid = values[0].get("pod_uid")
    restart = values[0].get("restart_count")
    previous = epoch_ms(values[0]["timestamp"])
    for value in values[1:]:
        current = epoch_ms(value["timestamp"])
        if value.get("pod_uid") != uid or value.get("restart_count") != restart:
            return [], "pod UID or restart count changed"
        if current - previous > 7_500:
            return [], "snapshot gap exceeds 5 seconds"
        previous = current
    return values, None


def completed(audit_dir: Path, start: int, end: int) -> int:
    total = 0
    for path in [*audit_dir.glob("audit-*.log"), *audit_dir.glob("audit-*.log.gz")]:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as source:
            for line in source:
                parts = line.rstrip("\n").split("|")
                if len(parts) == 6 and parts[0] == "C" and start <= int(parts[4]) <= end:
                    total += 1
    return total


def main() -> None:
    options = args()
    groups = samples(Path(options.snapshots_dir))
    output: dict[str, object] = {"status": "unusable", "reason": None}
    all_values = [value for values in groups.values() for value in values]
    stable_values: list[list[dict]] = []
    for values in all_values:
        accepted, reason = stable(values)
        if reason:
            output["reason"] = reason
            Path(options.output).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
            return
        stable_values.append(accepted)
    if not stable_values:
        output["reason"] = "no counter snapshots"
    else:
        start = max(epoch_ms(values[0]["timestamp"]) for values in stable_values)
        end = min(epoch_ms(values[-1]["timestamp"]) for values in stable_values)
        selected = [[value for value in values if start <= epoch_ms(value["timestamp"]) <= end] for values in stable_values]
        if end <= start or any(len(values) < 2 for values in selected):
            output["reason"] = "no common stable interval"
        else:
            rx = sum(values[-1]["rx_bytes"] - values[0]["rx_bytes"] for values in selected)
            tx = sum(values[-1]["tx_bytes"] - values[0]["tx_bytes"] for values in selected)
            count = completed(Path(options.audit_dir), start, end)
            if count <= 0:
                output["reason"] = "no successful audit completions in interval"
            else:
                output = {"status": "success", "window": {"start_epoch_ms": start, "end_epoch_ms": end}, "successful_messages": count, "rx_bytes": rx, "tx_bytes": tx, "total_bytes": rx + tx, "rx_bytes_per_message": rx / count, "tx_bytes_per_message": tx / count, "bytes_per_message": (rx + tx) / count}
    Path(options.output).parent.mkdir(parents=True, exist_ok=True)
    Path(options.output).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
