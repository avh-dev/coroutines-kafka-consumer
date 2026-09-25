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
    "kafka_warmup_started",
    "measurements_finished",
    "report_ready",
    "bundle_ready",
    "experiment_completed",
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


def format_duration(seconds: Any) -> str:
    if seconds in (None, ""):
        return "unknown"
    value = max(0, int(float(seconds)))
    hours, remainder = divmod(value, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def environment_text(payload: dict[str, Any]) -> str:
    environment = payload.get("environment") or {}
    if isinstance(environment, str):
        return environment
    name = str(environment.get("name") or "unknown")
    detail = str(environment.get("detail") or "").strip()
    return f"{name} · {detail}" if detail else name


def kafka_text(payload: dict[str, Any]) -> str:
    kafka = payload.get("kafka") or {}
    if not isinstance(kafka, dict) or not kafka:
        return "unknown"
    implementation = str(kafka.get("implementation") or "Kafka")
    topology = str(kafka.get("topology") or "unknown")
    brokers = kafka.get("brokers")
    suffix = "" if brokers in (None, "") else f" · {brokers} broker(s)"
    return f"{implementation} · {topology}{suffix}"


def target_text(target: Any) -> str:
    if not isinstance(target, dict):
        return str(target)
    name = str(target.get("name") or target.get("id") or "target")
    details = []
    if target.get("profile"):
        details.append(str(target["profile"]))
    if target.get("replicas") not in (None, ""):
        details.append(f"{target['replicas']} replica(s)")
    if target.get("base_tps") not in (None, ""):
        details.append(f"{target['base_tps']} TPS")
    if target.get("duration_seconds") not in (None, ""):
        details.append(format_duration(target["duration_seconds"]))
    return f"{name} ({', '.join(details)})" if details else name


def artifact_lines(payload: dict[str, Any]) -> list[str]:
    artifacts = payload.get("artifacts") or {}
    if not isinstance(artifacts, dict):
        return []
    labels = {"report": "Report", "evidence": "Evidence", "audit": "Audit"}
    return [f"{labels[key]}: {artifacts[key]}" for key in labels if artifacts.get(key)]


def message_for(event: str, payload: dict[str, Any]) -> str:
    experiment = payload.get("experiment") or payload.get("name") or "ckc experiment"
    if event == "experiment_started":
        targets = payload.get("targets") or []
        lines = [
            f"🚀 CKC experiment started: {experiment}",
            f"Environment: {environment_text(payload)}",
            f"Kafka: {kafka_text(payload)}",
            f"Expected workload: {format_duration(payload.get('expected_duration_seconds'))}",
            f"Targets ({len(targets)}):",
        ]
        lines.extend(f"• {target_text(target)}" for target in targets)
        return "\n".join(lines)
    if event == "kafka_warmup_started":
        return (
            f"🔥 Kafka warm-up started: {format_duration(payload.get('duration_seconds'))}"
            f"\nReason: {payload.get('reason', 'Kafka runtime was redeployed')}"
        )
    if event in {"measurements_finished", "experiment_runs_finished"}:
        return "✅ Measurements completed · analysis started"
    if event == "report_ready":
        reports = payload.get("reports") or []
        report = reports[0] if reports else "unknown"
        return f"📊 CKC report ready: {experiment}\n{report}"
    if event == "bundle_ready":
        return "\n".join([f"📦 CKC evidence bundle ready: {experiment}", *artifact_lines(payload)])
    if event in {"experiment_completed", "experiment_finished"}:
        lines = [
            f"🏁 CKC experiment completed: {experiment}",
            f"Elapsed: {format_duration(payload.get('elapsed_seconds'))}",
        ]
        if payload.get("targets_succeeded") is not None:
            lines.append(f"Targets: {payload.get('targets_succeeded')}/{payload.get('targets_total')} succeeded")
        if payload.get("application_state"):
            lines.append(f"Application: {payload['application_state']}")
        if payload.get("cleanup_status"):
            lines.append(f"Cleanup: {payload['cleanup_status']}")
        return "\n".join(lines)
    if event == "experiment_failed":
        exit_code = payload.get("exit_code")
        suffix = "" if exit_code is None else f" · exit {exit_code}"
        lines = [f"❌ CKC experiment failed: {experiment}{suffix}"]
        if payload.get("cleanup_status"):
            lines.append(f"Cleanup: {payload['cleanup_status']}")
        if payload.get("error"):
            lines.append(str(payload["error"]))
        return "\n".join(lines)
    return f"ℹ️ {event.replace('_', ' ').capitalize()}"


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required")
    data = {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
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
