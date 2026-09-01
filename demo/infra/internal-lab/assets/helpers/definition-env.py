#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path


SHARED_INFRA = Path(__file__).resolve().parents[3] / "shared"
if str(SHARED_INFRA) not in sys.path:
    sys.path.insert(0, str(SHARED_INFRA))

from experiment_orchestration.definition_environment import *  # noqa: F403,E402
from experiment_orchestration.definition_environment import main  # noqa: E402


if __name__ == "__main__":
    main()
