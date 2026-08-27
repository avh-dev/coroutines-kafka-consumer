#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import signal
import subprocess
from pathlib import Path
from typing import Any

from experiment_events import append_event


EVENT_PREFIX = "CKC_EXPERIMENT_EVENT "


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the load generator and persist its structured experiment events.")
    parser.add_argument("--log-file", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a load-test command is required after --")
    return args


def normalized_event(value: dict[str, Any]) -> dict[str, Any]:
    producer = value.get("producerConfig") if isinstance(value.get("producerConfig"), dict) else {}
    topic = str(value.get("topic") or producer.get("topic") or "all")
    return {
        **value,
        "source": "producer",
        "title": f"Producer config · {topic}",
        "details": producer,
    }


def main() -> int:
    args = parse_args()
    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(args.command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    def forward(signum: int, _frame: Any) -> None:
        if process.poll() is None:
            process.send_signal(signum)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    assert process.stdout is not None
    with log_path.open("a", encoding="utf-8") as output:
        for line in process.stdout:
            output.write(line)
            output.flush()
            if not line.startswith(EVENT_PREFIX):
                continue
            try:
                value = json.loads(line[len(EVENT_PREFIX) :])
                if isinstance(value, dict):
                    append_event(normalized_event(value))
            except (json.JSONDecodeError, OSError, ValueError):
                continue
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
