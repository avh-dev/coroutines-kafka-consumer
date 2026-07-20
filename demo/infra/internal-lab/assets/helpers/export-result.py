#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export internal-lab run or bundle artifacts into a result directory.")
    parser.add_argument("target", nargs="?", help="Run id/path or bundle id/path. Defaults to the latest run.")
    parser.add_argument("--bundle", action="store_true", help="Interpret target as a bundle result.")
    parser.add_argument("--latest-bundle", action="store_true", help="Export the latest bundle result.")
    parser.add_argument("--lab-root", default="/opt/ckc-lab")
    parser.add_argument("--output-dir", default="", help="Export root directory. Defaults to /opt/ckc-lab/results/exports.")
    parser.add_argument("--loki-url", default="http://127.0.0.1:3100")
    parser.add_argument("--skip-loki", action="store_true")
    parser.add_argument("--loki-limit", type=int, default=5000)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:30090")
    parser.add_argument("--skip-prometheus", action="store_true")
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


def result_window(run_dirs: list[Path]) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for run_dir in run_dirs:
        start, end = run_window(run_dir)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


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


def prometheus_snapshot(prometheus_url: str) -> str:
    url = f"{prometheus_url.rstrip('/')}/api/v1/admin/tsdb/snapshot"
    request = urllib.request.Request(url, data=b"", method="POST")
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.loads(response.read().decode("utf-8"))
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus snapshot failed: {data}")
    snapshot_name = data.get("data", {}).get("name")
    if not snapshot_name:
        raise RuntimeError(f"Prometheus snapshot response did not include a snapshot name: {data}")
    return str(snapshot_name)


def block_overlaps_window(block_dir: Path, start: datetime | None, end: datetime | None) -> bool:
    meta_path = block_dir / "meta.json"
    if not meta_path.is_file():
        return False
    meta = load_json(meta_path)
    min_time = int(meta.get("minTime", 0))
    max_time = int(meta.get("maxTime", 0))
    start_ms = int(start.timestamp() * 1000) if start else None
    end_ms = int(end.timestamp() * 1000) if end else None
    if start_ms is not None and max_time < start_ms:
        return False
    if end_ms is not None and min_time > end_ms:
        return False
    return True


def metric_names_from_dashboard(dashboard_dir: Path) -> list[str]:
    names: set[str] = set()
    for dashboard in dashboard_dir.glob("*.json"):
        try:
            data = json.loads(dashboard.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for expression in iter_dashboard_expressions(data):
            names.update(referenced_metric_names(expression))
    return sorted(names)


def iter_dashboard_expressions(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        expression = value.get("expr")
        if isinstance(expression, str):
            yield expression
        query = value.get("query")
        if isinstance(query, dict):
            nested = query.get("query")
            if isinstance(nested, str):
                yield nested
        for child in value.values():
            yield from iter_dashboard_expressions(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dashboard_expressions(child)


def referenced_metric_names(expression: str) -> set[str]:
    names = set()
    names.update(match.group(1) for match in re.finditer(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{|\[)", expression))
    for match in re.finditer(r"label_values\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*,", expression):
        names.add(match.group(1))
    return names


def export_prometheus_snapshot(
    export_root: Path,
    lab_root: Path,
    prometheus_url: str,
    start: datetime | None,
    end: datetime | None,
) -> dict[str, Any]:
    snapshot_name = prometheus_snapshot(prometheus_url)
    snapshot_dir = lab_root / "prometheus" / "snapshots" / snapshot_name
    output_dir = export_root / "metrics" / "prometheus"
    if not snapshot_dir.is_dir():
        raise FileNotFoundError(f"Prometheus snapshot directory was not found: {snapshot_dir}")
    block_count = 0
    for block_dir in sorted(path for path in snapshot_dir.iterdir() if path.is_dir()):
        if block_overlaps_window(block_dir, start, end):
            copy_tree(block_dir, output_dir / block_dir.name)
            block_count += 1
    shutil.rmtree(snapshot_dir, ignore_errors=True)
    return {
        "type": "prometheus_tsdb_snapshot",
        "source": str(snapshot_dir),
        "path": str(output_dir.relative_to(export_root)),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "blocks": block_count,
    }


def create_contents_archive(source_dir: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        for child in sorted(source_dir.iterdir()):
            archive.add(child, arcname=child.name)


def copy_audit_files(export_dir: Path, run_dirs: list[Path]) -> list[dict[str, Any]]:
    audit_exports = []
    for run_dir in run_dirs:
        source = run_dir / "audit"
        if not source.exists():
            audit_exports.append({"run_id": run_dir.name, "path": "", "files": 0})
            continue
        target = export_dir / "audit" / run_dir.name
        copy_tree(source, target)
        file_count = sum(1 for path in target.rglob("*") if path.is_file())
        audit_exports.append({"run_id": run_dir.name, "path": str(target.relative_to(export_dir)), "files": file_count})
    return audit_exports


def write_summary(export_dir: Path, result_type: str, result_dir: Path, run_dirs: list[Path], manifest: dict[str, Any]) -> None:
    lines = [
        f"# CKC Internal Lab Export: {result_type}-{result_dir.name}",
        "",
        f"- Type: `{result_type}`",
        f"- Source: `{result_dir}`",
        f"- Exported at: `{manifest['exported_at']}`",
        f"- Restore archive: `restore.tar.gz`",
        "",
        "## Runs",
        "",
    ]
    for run_dir in run_dirs:
        status = load_json(run_dir / "run-status.json") if (run_dir / "run-status.json").is_file() else {}
        metadata = load_json(run_dir / "run-metadata.json") if (run_dir / "run-metadata.json").is_file() else {}
        lines.extend(
            [
                f"### {run_dir.name}",
                "",
                f"- Status: `{status.get('status', 'unknown')}`",
                f"- Exit code: `{status.get('exit_code', '')}`",
                f"- Profile: `{metadata.get('application', {}).get('profile', '')}`",
                f"- Test definition: `{metadata.get('test_definition', '')}`",
                f"- Started: `{status.get('started_at') or metadata.get('started_at', '')}`",
                f"- Ended: `{status.get('ended_at', '')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Exported Data",
            "",
            f"- Loki files: `{len(manifest.get('loki', []))}`",
            f"- Prometheus blocks: `{(manifest.get('metrics') or {}).get('blocks', 0)}`",
            f"- Metrics window: `{(manifest.get('metrics') or {}).get('start', '')}` .. `{(manifest.get('metrics') or {}).get('end', '')}`",
            "",
        ]
    )
    (export_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    lab_root = Path(args.lab_root)
    result_type, result_dir = resolve_result(args.target, lab_root, args.bundle or args.latest_bundle)
    if args.latest_bundle:
        result_type, result_dir = resolve_result(None, lab_root, True)

    output_dir = Path(args.output_dir) if args.output_dir else lab_root / "results" / "exports"
    export_name = f"{result_type}-{result_dir.name}"
    export_dir = output_dir / export_name
    if export_dir.exists():
        if not args.force:
            raise FileExistsError(f"Export directory already exists: {export_dir}. Use --force to overwrite it.")
        shutil.rmtree(export_dir)
    run_dirs = run_dirs_for_result(result_type, result_dir)
    start, end = result_window(run_dirs)
    export_dir.mkdir(parents=True)

    with tempfile.TemporaryDirectory(prefix="ckc-result-export-") as temp_dir:
        restore_root = Path(temp_dir) / export_name
        restore_root.mkdir(parents=True)
        if result_type == "bundle":
            copy_tree(result_dir, export_dir / "bundle")
        for run_dir in run_dirs:
            copy_tree(run_dir / "run-metadata.json", export_dir / "runs" / run_dir.name / "run-metadata.json")
            copy_tree(run_dir / "run-status.json", export_dir / "runs" / run_dir.name / "run-status.json")
        copy_tree(lab_root / "grafana" / "dashboards", restore_root / "grafana" / "dashboards")
        copy_tree(lab_root / "grafana" / "provisioning", restore_root / "grafana" / "provisioning")
        copy_tree(lab_root / "restore", restore_root / "restore")
        loki_exports = [] if args.skip_loki else write_loki_logs(restore_root, run_dirs, args.loki_url, args.loki_limit)
        metrics_export = None if args.skip_prometheus else export_prometheus_snapshot(restore_root, lab_root, args.prometheus_url, start, end)
        audit_exports = copy_audit_files(export_dir, run_dirs)
        manifest = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "type": result_type,
            "source": str(result_dir),
            "runs": [path.name for path in run_dirs],
            "audit": audit_exports,
            "loki": loki_exports,
            "metrics": metrics_export,
            "dashboard_metric_names": metric_names_from_dashboard(restore_root / "grafana" / "dashboards"),
        }
        (restore_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        (export_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        write_summary(export_dir, result_type, result_dir, run_dirs, manifest)
        create_contents_archive(restore_root, export_dir / "restore.tar.gz")

    print(export_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
