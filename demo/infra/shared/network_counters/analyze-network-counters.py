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
            grouped.setdefault(path.parents[1].name, []).append(values)
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


def audit_counts(audit_dir: Path, start: int, end: int) -> dict[str, int]:
    counts = {"published": 0, "successful": 0}
    for path in [*audit_dir.glob("audit-*.log"), *audit_dir.glob("audit-*.log.gz")]:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as source:
            for line in source:
                parts = line.rstrip("\n").split("|")
                if len(parts) == 7 and parts[0] == "P" and start <= int(parts[5]) <= end:
                    counts["published"] += 1
                elif len(parts) == 6 and parts[0] == "C" and start <= int(parts[4]) <= end:
                    counts["successful"] += 1
    return counts


def counter_delta(values: list[list[dict]]) -> dict[str, int]:
    return {
        field: sum(int(samples[-1][field]) - int(samples[0][field]) for samples in values)
        for field in ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets")
    }


def with_totals(values: dict[str, int]) -> dict[str, int]:
    return {
        **values,
        "total_bytes": values["rx_bytes"] + values["tx_bytes"],
        "total_packets": values["rx_packets"] + values["tx_packets"],
    }


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
            by_target: dict[str, list[list[dict]]] = {}
            for target, target_values in groups.items():
                by_target[target] = [
                    [value for value in values if start <= epoch_ms(value["timestamp"]) <= end]
                    for values in target_values
                ]
            counts = audit_counts(Path(options.audit_dir), start, end)
            if counts["successful"] <= 0:
                output["reason"] = "no successful audit completions in interval"
            else:
                roles: dict[str, dict[str, int | float]] = {}
                if by_target.get("load-test"):
                    roles["producer"] = with_totals(counter_delta(by_target["load-test"]))
                    roles["producer"]["published_messages"] = counts["published"]
                if by_target.get("application"):
                    roles["consumer"] = with_totals(counter_delta(by_target["application"]))
                    roles["consumer"]["successful_messages"] = counts["successful"]
                total = with_totals({
                    field: sum(int(role[field]) for role in roles.values())
                    for field in ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets")
                })
                total["successful_messages"] = counts["successful"]
                total["published_messages"] = counts["published"]
                roles["total"] = total
                for role, denominator in (("producer", counts["published"]), ("consumer", counts["successful"]), ("total", counts["successful"])):
                    if role in roles and denominator > 0:
                        roles[role]["rx_bytes_per_message"] = roles[role]["rx_bytes"] / denominator
                        roles[role]["tx_bytes_per_message"] = roles[role]["tx_bytes"] / denominator
                        roles[role]["bytes_per_message"] = roles[role]["total_bytes"] / denominator
                output = {
                    "status": "success",
                    "window": {"start_epoch_ms": start, "end_epoch_ms": end},
                    "producer": roles.get("producer", {}),
                    "consumer": roles.get("consumer", {}),
                    "total": total,
                    # Compatibility for existing consumers of the initial artifact schema.
                    **total,
                }
    Path(options.output).parent.mkdir(parents=True, exist_ok=True)
    Path(options.output).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
