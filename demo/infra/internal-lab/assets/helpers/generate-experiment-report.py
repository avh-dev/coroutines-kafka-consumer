#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from experiment_report import generate_experiment_reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Markdown and SVG reports for an internal-lab experiment set.")
    parser.add_argument("summary", help="Experiment summary.json path or experiment result directory")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:30090")
    parser.add_argument(
        "--reanalyze-audit",
        action="store_true",
        help="Rebuild audit summaries with the current resolved SLA profile before rendering reports.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = Path(args.summary)
    if summary.is_dir():
        summary = summary / "summary.json"
    for report in generate_experiment_reports(
        summary,
        Path(args.lab_root),
        args.prometheus_url,
        reanalyze_audit=args.reanalyze_audit,
    ):
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
