from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def load_environment_file(path: Path) -> dict[str, str]:
    """Read the simple export KEY=value format used by the lab secret file."""
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for number, source in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = source.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, separator, raw = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"Invalid environment assignment at {path}:{number}")
        values = shlex.split(raw, posix=True)
        if len(values) != 1:
            raise ValueError(f"Environment value must be one shell word at {path}:{number}")
        result[key] = values[0]
    return result


def notify(
    hook: Path | None,
    event: str,
    payload: Mapping[str, Any],
    payload_dir: Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Persist and dispatch one best-effort lifecycle event with a delivery receipt."""
    if hook is None:
        return {"event": event, "status": "disabled"}
    payload_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        suffix=".json",
        prefix=f"notify-{event}-",
        dir=payload_dir,
        delete=False,
    ) as file:
        json.dump(dict(payload), file, indent=2)
        file.write("\n")
        payload_path = file.name
    receipt_path = Path(payload_path).with_suffix(".delivery.json")
    receipt: dict[str, Any] = {
        "event": event,
        "attempted_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "payload": str(payload_path),
        "hook": str(hook),
    }
    try:
        command = [sys.executable, str(hook), event, payload_path] if hook.suffix == ".py" else [str(hook), event, payload_path]
        completed = subprocess.run(
            command,
            check=False,
            env={**os.environ, **dict(environment or {})},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        receipt.update({
            "status": "delivered" if completed.returncode == 0 else "failed",
            "returncode": completed.returncode,
        })
        detail = (completed.stderr or completed.stdout or "").strip()
        if detail:
            receipt["detail"] = detail
        if completed.returncode != 0:
            print(f"Notification hook failed for {event}: {detail or f'exit {completed.returncode}'}", file=sys.stderr)
    except Exception as error:
        receipt.update({"status": "failed", "error": str(error)})
        print(f"Notification hook failed for {event}: {error}", file=sys.stderr)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt
