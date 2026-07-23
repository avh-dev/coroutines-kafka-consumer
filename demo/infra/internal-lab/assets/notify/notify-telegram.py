#!/usr/bin/env python3
"""
Telegram notification hook example for internal-lab experiment runs.

See README.md in this directory for setup instructions.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_EVENTS = {
    "experiment_started",
    "experiment_runs_finished",
    "audit_analysis_started",
    "audit_analysis_finished",
    "experiment_finished",
    "experiment_failed",
}


def configured_events() -> set[str]:
    raw = os.environ.get("TELEGRAM_EVENTS", "").strip()
    if not raw:
        return DEFAULT_EVENTS
    return {item.strip() for item in raw.split(",") if item.strip()}


def load_payload(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"payload must be a JSON object: {path}")
    return value


def short_status(payload: dict[str, Any]) -> str:
    exit_code = payload.get("exit_code")
    if exit_code is None:
        return ""
    return "ok" if exit_code == 0 else f"failed exit_code={exit_code}"


def message_for(event: str, payload: dict[str, Any]) -> str:
    experiment = payload.get("experiment") or payload.get("name") or "ckc experiment"
    if event == "experiment_started":
        return f"CKC experiment started: {experiment}\ntargets={payload.get('targets', '?')}"
    if event == "experiment_runs_finished":
        return (
            f"CKC experiment load phases finished: {experiment}\n"
            f"runs={payload.get('runs', '?')} auditable_runs={payload.get('auditable_runs', '?')}"
        )
    if event == "audit_analysis_started":
        return f"CKC audit analysis started: {experiment}\nauditable_runs={payload.get('auditable_runs', '?')}"
    if event == "audit_analysis_finished":
        analysis = payload.get("analysis", [])
        failures = sum(1 for item in analysis if isinstance(item, dict) and item.get("exit_code") != 0)
        return f"CKC audit analysis finished: {experiment}\nanalyses={len(analysis)} failures={failures}"
    if event in {"experiment_finished", "experiment_failed"}:
        targets = payload.get("targets", [])
        status = short_status(payload)
        return f"CKC experiment {event.removeprefix('experiment_')}: {experiment}\ntargets={len(targets)} {status}"
    return f"CKC event: {event}\n{json.dumps(payload, ensure_ascii=False, indent=2)[:3000]}"


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")

    data = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    thread_id = os.environ.get("TELEGRAM_THREAD_ID", "").strip()
    if thread_id:
        data["message_thread_id"] = thread_id

    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=urllib.parse.urlencode(data).encode("utf-8"),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: notify-telegram.py event-name payload.json", file=sys.stderr)
        return 2

    event = sys.argv[1]
    if event not in configured_events():
        return 0

    payload = load_payload(sys.argv[2])
    try:
        send_telegram(message_for(event, payload))
    except (OSError, RuntimeError, urllib.error.URLError) as error:
        print(f"Telegram notification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
