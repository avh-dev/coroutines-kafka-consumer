#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


for candidate in (
    Path(__file__).resolve().parents[1] / "helpers",
    Path(__file__).resolve().parents[3] / "shared",
):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))

from experiment_notifications.telegram import DEFAULT_EVENTS, main, message_for

__all__ = ["DEFAULT_EVENTS", "main", "message_for"]


if __name__ == "__main__":
    raise SystemExit(main())
