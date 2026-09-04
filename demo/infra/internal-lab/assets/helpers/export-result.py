#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from result_bundle import collect, finalize, prepare


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and finalize a canonical internal-lab result.")
    parser.add_argument("target", nargs="?", help="Run id/path or experiment id/path; defaults to the latest experiment.")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--experiment", action="store_true")
    parser.add_argument("--latest-experiment", action="store_true")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--skip-loki", action="store_true")
    parser.add_argument("--loki-limit", type=int, default=5000, help=argparse.SUPPRESS)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:30090")
    parser.add_argument("--skip-prometheus", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.run and (args.experiment or args.latest_experiment):
        parser.error("--run cannot be combined with --experiment or --latest-experiment")
    if args.latest_experiment and args.target:
        parser.error("--latest-experiment does not accept a target")
    return args


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def latest(directory: Path, marker: str) -> Path:
    candidates = [path for path in directory.iterdir() if path.is_dir() and (path / marker).is_file()]
    if not candidates:
        raise FileNotFoundError(f"No results containing {marker} under {directory}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def resolve_result(args: argparse.Namespace, lab_root: Path) -> tuple[str, Path]:
    requested = "run" if args.run else "experiment" if args.experiment or args.latest_experiment else None
    roots = {
        "run": (lab_root / "results/runs", "run-metadata.json"),
        "experiment": (lab_root / "results/experiments", "summary.json"),
    }
    if args.target:
        direct = Path(args.target)
        candidates = [direct] if direct.is_dir() else []
        candidates.extend(root / args.target for root, _ in roots.values())
        for candidate in candidates:
            for kind, (_, marker) in roots.items():
                if (requested is None or requested == kind) and (candidate / marker).is_file():
                    return kind, candidate.resolve()
        raise FileNotFoundError(f"Result was not found: {args.target}")
    kind = requested or "experiment"
    root, marker = roots[kind]
    try:
        return kind, latest(root, marker)
    except FileNotFoundError:
        if requested:
            raise
        root, marker = roots["run"]
        return "run", latest(root, marker)


def run_dirs(kind: str, result: Path) -> list[Path]:
    if kind == "run":
        return [result]
    summary = load_json(result / "summary.json")
    return [
        Path(str(target["run_dir"]))
        for experiment in summary.get("experiments", [])
        for target in experiment.get("targets", [])
        if target.get("run_dir") and Path(str(target["run_dir"])).is_dir()
    ]


def experiment_name(kind: str, result: Path) -> str:
    if kind == "run":
        metadata = load_json(result / "run-metadata.json")
        return str(metadata.get("test_name") or result.name)
    summary = load_json(result / "summary.json")
    names = [str(item.get("experiment")) for item in summary.get("experiments", []) if item.get("experiment")]
    return names[0] if len(names) == 1 else result.name


def main() -> int:
    args = parse_args()
    lab_root = Path(args.lab_root)
    kind, result = resolve_result(args, lab_root)
    output_root = Path(args.output_dir) if args.output_dir else lab_root / "results/exports"
    output = output_root / result.name
    if output.exists():
        if not args.force:
            raise FileExistsError(f"Export already exists: {output}; use --force to replace it")
        shutil.rmtree(output)
    runs = run_dirs(kind, result)
    dashboard_dir = lab_root / "grafana/dashboards"
    collection = collect(
        result_root=result,
        run_dirs=runs,
        dashboard_dir=dashboard_dir,
        prometheus_url=None if args.skip_prometheus else args.prometheus_url,
        loki_url=None if args.skip_loki else args.loki_url,
    )
    source = dashboard_dir / "ckc-overview.json"
    if source.is_file():
        target = result / "config/ckc-overview.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    prepare(result, lab_root, "internal-lab")
    status = "failed" if collection["errors"] else "complete"
    report_candidates = [*result.glob("**/report.md"), *result.glob("**/experiment-report.md")]
    report_dir = report_candidates[0].parent if report_candidates else result / "missing-report"
    artifacts = finalize(
        result_root=result,
        report_dir=report_dir,
        output_dir=output,
        experiment=experiment_name(kind, result),
        environment="internal-lab",
        status=status,
        restore_sources=[lab_root / "helpers/result_bundle/restore"],
    )
    print(output)
    for name in ("report", "evidence", "audit"):
        print(f"  {name}: {artifacts[name]}")
    return 0 if not collection["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
