#!/usr/bin/env python3
from __future__ import annotations

import fcntl
import json
import os
import sys
import tempfile
import time
import urllib.error
import uuid
from pathlib import Path
from typing import Any, Callable


for candidate in (
    Path(__file__).resolve().parents[1] / "helpers",
    Path(__file__).resolve().parents[3] / "shared",
):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))

from experiment_notifications.telegram import (
    DEFAULT_EVENTS,
    configured_events,
    message_for,
    send_telegram,
)

__all__ = ["DEFAULT_EVENTS", "drain_outbox", "enqueue", "main", "message_for"]

SCHEMA_VERSION = 1
INITIAL_RETRY_SECONDS = 30
MAX_RETRY_SECONDS = 15 * 60


def outbox_root() -> Path:
    configured = os.environ.get("CKC_TELEGRAM_OUTBOX_DIR", "").strip()
    if configured:
        return Path(configured)
    return Path(os.environ.get("LAB_ROOT", "/opt/ckc-lab")) / "state/notifications/telegram"


def ensure_directories(root: Path) -> tuple[Path, Path]:
    pending = root / "pending"
    failed = root / "failed"
    pending.mkdir(parents=True, exist_ok=True, mode=0o700)
    failed.mkdir(parents=True, exist_ok=True, mode=0o700)
    return pending, failed


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    ) as file:
        json.dump(value, file, ensure_ascii=False, sort_keys=True)
        file.write("\n")
        temporary = Path(file.name)
    temporary.chmod(0o600)
    temporary.replace(path)


def enqueue(event: str, payload_path: Path, root: Path | None = None) -> Path | None:
    if event not in configured_events():
        return None
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Notification payload must be an object: {payload_path}")
    pending, _ = ensure_directories(root or outbox_root())
    created_ns = time.time_ns()
    path = pending / f"{created_ns:020d}-{uuid.uuid4().hex}.json"
    write_json_atomic(path, {
        "schema_version": SCHEMA_VERSION,
        "event": event,
        "payload": payload,
        "created_at_ns": created_ns,
        "attempts": 0,
        "next_attempt_at": 0,
    })
    return path


def retry_delay(attempts: int) -> int:
    return min(MAX_RETRY_SECONDS, INITIAL_RETRY_SECONDS * (2 ** max(0, attempts - 1)))


def permanent_http_error(error: urllib.error.HTTPError) -> bool:
    return 400 <= error.code < 500 and error.code != 429


def move_failed(path: Path, failed: Path, suffix: str) -> Path:
    destination = failed / f"{path.stem}.{suffix}.json"
    path.replace(destination)
    return destination


def drain_outbox(
    root: Path | None = None,
    *,
    sender: Callable[[str], None] = send_telegram,
    now: float | None = None,
) -> dict[str, int]:
    root = root or outbox_root()
    pending, failed = ensure_directories(root)
    lock_path = root / "dispatch.lock"
    delivered = 0
    deferred = 0
    rejected = 0
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        for path in sorted(pending.glob("*.json")):
            current_time = time.time() if now is None else now
            envelope: dict[str, Any] = {}
            try:
                envelope = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(envelope, dict) or envelope.get("schema_version") != SCHEMA_VERSION:
                    raise ValueError("unsupported outbox envelope")
                next_attempt_at = float(envelope.get("next_attempt_at") or 0)
                if next_attempt_at > current_time:
                    deferred += 1
                    break
                event = str(envelope["event"])
                payload = envelope["payload"]
                if not isinstance(payload, dict):
                    raise ValueError("notification payload is not an object")
                sender(message_for(event, payload))
            except urllib.error.HTTPError as error:
                if permanent_http_error(error):
                    move_failed(path, failed, f"http-{error.code}")
                    rejected += 1
                    print(f"Telegram notification rejected permanently: {path.name}: HTTP {error.code}", file=sys.stderr)
                    continue
                envelope["attempts"] = int(envelope.get("attempts") or 0) + 1
                envelope["last_error"] = f"HTTP {error.code}: {error.reason}"
                envelope["next_attempt_at"] = current_time + retry_delay(envelope["attempts"])
                write_json_atomic(path, envelope)
                deferred += 1
                break
            except (OSError, RuntimeError, urllib.error.URLError) as error:
                if not envelope:
                    print(f"Unable to read Telegram outbox entry: {path.name}: {error}", file=sys.stderr)
                    deferred += 1
                    break
                envelope["attempts"] = int(envelope.get("attempts") or 0) + 1
                envelope["last_error"] = str(error)
                envelope["next_attempt_at"] = current_time + retry_delay(envelope["attempts"])
                write_json_atomic(path, envelope)
                deferred += 1
                break
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                move_failed(path, failed, "invalid")
                rejected += 1
                print(f"Invalid Telegram outbox entry: {path.name}: {error}", file=sys.stderr)
                continue
            path.unlink()
            delivered += 1
    return {"delivered": delivered, "deferred": deferred, "rejected": rejected}


def main() -> int:
    arguments = sys.argv[1:]
    if len(arguments) == 3 and arguments[0] == "enqueue":
        enqueue(arguments[1], Path(arguments[2]))
        return 0
    if arguments == ["drain"]:
        result = drain_outbox()
        print(
            "Telegram outbox: "
            f"delivered={result['delivered']} deferred={result['deferred']} rejected={result['rejected']}"
        )
        return 0
    if len(arguments) == 2:
        enqueue(arguments[0], Path(arguments[1]))
        return 0
    print("Usage: notify-telegram.py [enqueue] event-name payload.json | drain", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
