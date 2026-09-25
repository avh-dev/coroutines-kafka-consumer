from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
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
) -> None:
    """Persist and dispatch one best-effort lifecycle event."""
    if hook is None:
        return
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
    try:
        command = [sys.executable, str(hook), event, payload_path] if hook.suffix == ".py" else [str(hook), event, payload_path]
        subprocess.run(
            command,
            check=False,
            env={**os.environ, **dict(environment or {})},
        )
    except Exception as error:
        print(f"Notification hook failed for {event}: {error}", file=sys.stderr)
