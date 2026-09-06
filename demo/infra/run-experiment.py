#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent / "shared"
sys.path.insert(0, str(SHARED_ROOT))

from experiment_orchestration.runner import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
