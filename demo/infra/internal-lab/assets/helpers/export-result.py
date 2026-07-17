#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export internal-lab run or bundle artifacts into a tar.gz archive.")
    parser.add_argument("target", nargs="?", help="Run id/path or bundle id/path. Defaults to the latest run.")
    parser.add_argument("--bundle", action="store_true", help="Interpret target as a bundle result.")
    parser.add_argument("--latest-bundle", action="store_true", help="Export the latest bundle result.")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--output-dir", default="", help="Archive directory. Defaults to /opt/ckc-lab/results/exports.")
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--skip-loki", action="store_true")
    parser.add_argument("--loki-limit", type=int, default=5000)
    parser.add_argument("--force", action="store_true", help="Overwrite an existing archive.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return data


def latest_child(directory: Path, marker: str) -> Path:
    candidates = []
    for path in directory.iterdir() if directory.is_dir() else []:
        if path.is_dir() and (path / marker).is_file():
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"No result directories with {marker} found in {directory}")
    return max(candidates, key=lambda item: item[0])[1]


def resolve_result(target: str | None, lab_root: Path, bundle: bool) -> tuple[str, Path]:
    result_type = "bundle" if bundle else "run"
    root = lab_root / "results" / ("bundles" if bundle else "runs")
    marker = "summary.json" if bundle else "run-metadata.json"
    if not target:
        return result_type, latest_child(root, marker)
    path = Path(target)
    if path.is_dir():
        return result_type, path.resolve()
    candidate = root / target
    if candidate.is_dir():
        return result_type, candidate
    raise FileNotFoundError(f"{result_type} result was not found: {target}")


def run_dirs_for_result(result_type: str, result_dir: Path) -> list[Path]:
    if result_type == "run":
        return [result_dir]
    summary = load_json(result_dir / "summary.json")
    runs: list[Path] = []
    for bundle in summary.get("bundles", []):
        for test in bundle.get("tests", []):
            run_dir = test.get("run_dir")
            if run_dir:
                runs.append(Path(run_dir))
    return sorted({path.resolve() for path in runs})


def copy_tree(source: Path, target: Path) -> None:
    if not source.exists():
        return
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def parse_instant(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def unix_ns(value: datetime) -> str:
    return str(int(value.timestamp() * 1_000_000_000))


def run_window(run_dir: Path) -> tuple[datetime | None, datetime | None]:
    status_path = run_dir / "run-status.json"
    metadata_path = run_dir / "run-metadata.json"
    status = load_json(status_path) if status_path.is_file() else {}
    metadata = load_json(metadata_path) if metadata_path.is_file() else {}
    start = parse_instant(status.get("started_at") or metadata.get("started_at"))
    end = parse_instant(status.get("ended_at")) or datetime.now(timezone.utc)
    if start:
        start -= timedelta(minutes=2)
    if end:
        end += timedelta(minutes=2)
    return start, end


def loki_query(loki_url: str, query: str, limit: int, start: datetime | None, end: datetime | None) -> dict[str, Any]:
    params = {"query": query, "limit": str(limit), "direction": "forward"}
    if start:
        params["start"] = unix_ns(start)
    if end:
        params["end"] = unix_ns(end)
    encoded = urllib.parse.urlencode(params)
    url = f"{loki_url.rstrip('/')}/loki/api/v1/query_range?{encoded}"
    with urllib.request.urlopen(url, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Loki response was not a JSON object")
    return data


def write_loki_logs(export_root: Path, run_dirs: list[Path], loki_url: str, limit: int) -> list[dict[str, Any]]:
    exported = []
    logs_dir = export_root / "loki"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for run_dir in run_dirs:
        run_id = run_dir.name
        query = f'{{namespace="ckc-perf", run_id="{run_id}"}}'
        output = logs_dir / f"{run_id}.jsonl"
        start, end = run_window(run_dir)
        response = loki_query(loki_url, query, limit, start, end)
        count = 0
        with output.open("w", encoding="utf-8") as file:
            for stream in response.get("data", {}).get("result", []):
                labels = stream.get("stream", {})
                for timestamp, line in stream.get("values", []):
                    file.write(json.dumps({"ts": timestamp, "labels": labels, "line": line}, ensure_ascii=False) + "\n")
                    count += 1
        exported.append(
            {
                "run_id": run_id,
                "query": query,
                "start": start.isoformat() if start else None,
                "end": end.isoformat() if end else None,
                "file": str(output.relative_to(export_root)),
                "entries": count,
            }
        )
    return exported


def write_manifest(export_root: Path, result_type: str, result_dir: Path, run_dirs: list[Path], loki_exports: list[dict[str, Any]]) -> None:
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "type": result_type,
        "source": str(result_dir),
        "runs": [path.name for path in run_dirs],
        "loki": loki_exports,
    }
    (export_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def create_archive(source_dir: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source_dir, arcname=source_dir.name)


def main() -> int:
    args = parse_args()
    lab_root = Path(args.lab_root)
    result_type, result_dir = resolve_result(args.target, lab_root, args.bundle or args.latest_bundle)
    if args.latest_bundle:
        result_type, result_dir = resolve_result(None, lab_root, True)

    output_dir = Path(args.output_dir) if args.output_dir else lab_root / "results" / "exports"
    export_name = f"{result_type}-{result_dir.name}"
    archive_path = output_dir / f"{export_name}.tar.gz"
    if archive_path.exists() and not args.force:
        raise FileExistsError(f"Archive already exists: {archive_path}. Use --force to overwrite it.")
    run_dirs = run_dirs_for_result(result_type, result_dir)

    with tempfile.TemporaryDirectory(prefix="ckc-result-export-") as temp_dir:
        export_root = Path(temp_dir) / export_name
        export_root.mkdir(parents=True)
        if result_type == "bundle":
            copy_tree(result_dir, export_root / "bundle")
        for run_dir in run_dirs:
            copy_tree(run_dir, export_root / "runs" / run_dir.name)
        copy_tree(lab_root / "grafana" / "dashboards", export_root / "grafana" / "dashboards")
        copy_tree(lab_root / "grafana" / "provisioning", export_root / "grafana" / "provisioning")
        loki_exports = [] if args.skip_loki else write_loki_logs(export_root, run_dirs, args.loki_url, args.loki_limit)
        write_manifest(export_root, result_type, result_dir, run_dirs, loki_exports)
        create_archive(export_root, archive_path)

    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
