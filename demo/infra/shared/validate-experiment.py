#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

SHARED_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(SHARED_ROOT))

from experiment_orchestration import resolve_experiment_definition  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and resolve one canonical CKC experiment.")
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    resolved = resolve_experiment_definition(
        args.experiment.resolve(),
        environment=args.environment,
    )
    if resolved.snapshot is None:
        raise ValueError("Resolved canonical experiment snapshot is missing")
    rendered = yaml.safe_dump(resolved.snapshot, sort_keys=False, allow_unicode=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
