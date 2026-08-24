from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .analyze import analyze_experiment, load_json, load_sla_profile, load_yaml
from .markdown import render_markdown
from .svg import comparison_bar_svg, comparison_values_svg, load_profile_svg


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip().lower())
    return re.sub(r"-+", "-", slug).strip("-") or "experiment"


def write_report(report_dir: Path, report: Any) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    model = report.to_dict()
    (report_dir / "report-model.yaml").write_text(
        yaml.safe_dump(model, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    (report_dir / "load-profile.svg").write_text(load_profile_svg(report), encoding="utf-8")
    latency_misses = []
    for target in report.targets:
        measured = sum(rule.measured for rule in target.latency_sla)
        exceeded = sum(rule.exceeded for rule in target.latency_sla)
        latency_misses.append((target.name, 100 * exceeded / measured if measured else None))
    (report_dir / "latency-sla-misses.svg").write_text(
        comparison_values_svg(latency_misses, "Processed records above latency SLA", "%"),
        encoding="utf-8",
    )
    (report_dir / "latency-p95.svg").write_text(
        comparison_bar_svg(report, "latency_p95_ms", "End-to-end latency p95", "ms"), encoding="utf-8"
    )
    (report_dir / "cpu-average.svg").write_text(
        comparison_bar_svg(report, "cpu_average_cores", "Average application CPU", "cores"), encoding="utf-8"
    )
    (report_dir / "throughput-average.svg").write_text(
        comparison_bar_svg(report, "throughput_average_rps", "Average processed throughput", "records/s"), encoding="utf-8"
    )
    (report_dir / "poll-batch-average.svg").write_text(
        comparison_bar_svg(
            report,
            "telemetry_poll_batch_average_records",
            "Average telemetry poll batch",
            "records",
        ),
        encoding="utf-8",
    )
    (report_dir / "active-workers-max.svg").write_text(
        comparison_bar_svg(
            report,
            "telemetry_active_workers_max",
            "Maximum sampled active telemetry workers",
            "workers",
        ),
        encoding="utf-8",
    )
    (report_dir / "worker-allocation-average.svg").write_text(
        comparison_bar_svg(
            report,
            "processing_worker_allocation_average_bytes_per_second",
            "Average processing-worker allocation",
            "bytes/s",
        ),
        encoding="utf-8",
    )
    (report_dir / "worker-cpu-average.svg").write_text(
        comparison_bar_svg(
            report,
            "processing_worker_cpu_average_cores",
            "Average processing-worker CPU",
            "cores",
        ),
        encoding="utf-8",
    )
    (report_dir / "context-switches-average.svg").write_text(
        comparison_bar_svg(
            report,
            "context_switches_average_per_second",
            "Average demo process context switches",
            "switches/s",
        ),
        encoding="utf-8",
    )
    (report_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")


def copy_report_sources(
    report_dir: Path,
    summary_path: Path,
    experiment_summary: dict[str, Any],
    lab_root: Path,
    report: Any,
) -> None:
    raw_dir = report_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    sources = {
        summary_path: raw_dir / "experiment-set-summary.json",
        Path(str(experiment_summary["experiment_file"])): raw_dir / "experiment.yaml",
        lab_root
        / "workloads"
        / "test-definitions"
        / f"{experiment_summary['test_definition']}.yaml": raw_dir / "test-definition.yaml",
    }
    for source, target in sources.items():
        if source.is_file():
            shutil.copy2(source, target)
    if report.sla_profile:
        (raw_dir / "sla-profile.yaml").write_text(
            yaml.safe_dump(report.sla_profile, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
    for target_report in report.targets:
        run_dir = Path(target_report.run_dir)
        run_sources = {
            run_dir / "run-metadata.json": raw_dir / f"{target_report.run_id}-metadata.json",
            run_dir / "audit" / "summary.yaml": raw_dir / f"{target_report.run_id}-audit-summary.yaml",
            run_dir / "diagnostics" / "thread-stats" / "summary.json": raw_dir / f"{target_report.run_id}-thread-stats-summary.json",
            run_dir / "diagnostics" / "thread-stats" / "index.jsonl": raw_dir / f"{target_report.run_id}-thread-stats-index.jsonl",
            run_dir / "diagnostics" / "thread-stats" / "collector.log": raw_dir / f"{target_report.run_id}-thread-stats-collector.log",
        }
        for source, target in run_sources.items():
            if source.is_file():
                shutil.copy2(source, target)


def audit_input_file(audit_dir: Path) -> Path | None:
    plain = sorted(audit_dir.glob("audit-*.log"))
    if plain:
        return plain[0]
    compressed = sorted(audit_dir.glob("audit-*.log.gz"))
    return compressed[0] if compressed else None


def reanalyze_experiment_audits(
    experiment_summary: dict[str, Any],
    lab_root: Path,
    result_dir: Path,
) -> None:
    experiment = load_yaml(Path(str(experiment_summary["experiment_file"])))
    sla_profile = load_sla_profile(lab_root, experiment)
    if sla_profile is None:
        return
    analyzer = lab_root / "helpers" / "audit" / "analyze-audit.py"
    if not analyzer.is_file():
        raise FileNotFoundError(f"Audit analyzer was not found: {analyzer}")
    result_dir.mkdir(parents=True, exist_ok=True)
    profile_file = result_dir / f"{experiment_summary['experiment']}-sla-profile.json"
    profile_file.write_text(json.dumps(sla_profile, indent=2), encoding="utf-8")
    experiment_summary["sla_profile_file"] = str(profile_file)
    for target in experiment_summary.get("targets", []):
        if not isinstance(target, dict) or not target.get("run_dir"):
            continue
        run_dir = Path(str(target["run_dir"]))
        audit_dir = run_dir / "audit"
        input_file = audit_input_file(audit_dir)
        metadata_file = run_dir / "run-metadata.json"
        if input_file is None or not metadata_file.is_file():
            raise FileNotFoundError(f"Complete audit inputs were not found for run: {run_dir}")
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=audit_dir, delete=False) as summary:
            summary_path = Path(summary.name)
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=audit_dir, delete=False) as progress:
                progress_path = Path(progress.name)
                process = subprocess.run(
                    [
                        sys.executable,
                        str(analyzer),
                        "--input-file",
                        str(input_file),
                        "--metadata-file",
                        str(metadata_file),
                        "--sla-profile-file",
                        str(profile_file),
                        "--require-records",
                    ],
                    stdout=summary,
                    stderr=progress,
                    check=False,
                )
        if process.returncode != 0:
            summary_path.unlink(missing_ok=True)
            error = progress_path.read_text(encoding="utf-8")
            progress_path.unlink(missing_ok=True)
            raise RuntimeError(f"Audit reanalysis failed for {run_dir}: {error.strip()}")
        final_summary = audit_dir / "summary.yaml"
        final_progress = audit_dir / "analyzer-progress.log"
        summary_path.replace(final_summary)
        progress_path.replace(final_progress)
        final_summary.chmod(0o644)
        final_progress.chmod(0o644)


def generate_experiment_reports(
    summary_path: Path,
    lab_root: Path,
    prometheus_url: str = "http://127.0.0.1:30090",
    generated_at: datetime | None = None,
    reanalyze_audit: bool = False,
) -> list[Path]:
    document = load_json(summary_path)
    experiment_set_id = str(document.get("experiment_set_id") or summary_path.parent.name)
    output_root = summary_path.parent / "reports"
    outputs = []
    for experiment_summary in document.get("experiments", []):
        if not isinstance(experiment_summary, dict):
            continue
        if reanalyze_audit:
            reanalyze_experiment_audits(experiment_summary, lab_root, summary_path.parent)
        report = analyze_experiment(
            experiment_set_id,
            experiment_summary,
            lab_root,
            prometheus_url,
            generated_at=generated_at or datetime.now(timezone.utc),
        )
        report_dir = output_root / slugify(report.name)
        write_report(report_dir, report)
        copy_report_sources(report_dir, summary_path, experiment_summary, lab_root, report)
        outputs.append(report_dir / "report.md")
    index = ["# Experiment Reports", ""]
    for path in outputs:
        index.append(f"- [{path.parent.name}]({path.relative_to(output_root)})")
    index.append("")
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "README.md").write_text("\n".join(index), encoding="utf-8")
    return outputs
