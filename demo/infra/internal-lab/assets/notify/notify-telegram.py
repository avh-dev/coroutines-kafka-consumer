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
    "report_ready",
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


def completion_message(phase: str, payload: dict[str, Any]) -> str:
    exit_code = payload.get("exit_code")
    if exit_code in (None, 0):
        return f"✅ {phase} completed"
    return f"❌ {phase} failed · exit {exit_code}"


def message_for(event: str, payload: dict[str, Any]) -> str:
    experiment = payload.get("experiment") or payload.get("name") or "ckc experiment"
    if event == "experiment_started":
        return f"🚀 CKC experiment started: {experiment}\nTargets: {payload.get('targets', '?')}"
    if event == "test_started":
        return "▶️ Test started"
    if event == "test_finished":
        return completion_message("Test", payload)
    if event == "experiment_runs_finished":
        return "✅ Load runs completed"
    if event == "audit_analysis_started":
        return "🔍 Audit analysis started"
    if event == "audit_analysis_finished":
        failures = [
            item
            for item in payload.get("analysis", [])
            if isinstance(item, dict) and item.get("exit_code") != 0
        ]
        return "❌ Audit analysis failed" if failures else "✅ Audit analysis completed"
    if event == "audit_run_analysis_started":
        return "🔍 Audit run analysis started"
    if event == "audit_run_analysis_finished":
        return completion_message("Audit run analysis", payload)
    if event == "experiment_finished":
        return "🏁 Experiment completed"
    if event == "experiment_failed":
        exit_code = payload.get("exit_code")
        if exit_code == 130:
            return "⏹️ Experiment stopped"
        suffix = "" if exit_code is None else f" · exit {exit_code}"
        return f"❌ Experiment failed{suffix}"
    if event == "report_ready":
        reports = payload.get("reports", [])
        report = reports[0] if reports else "unknown"
        return f"📊 CKC report ready: {experiment}\n{report}"
    return f"ℹ️ {event.replace('_', ' ').capitalize()}"


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
