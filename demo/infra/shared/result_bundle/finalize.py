from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml


BUNDLE_VERSION = 1
FINAL_STATUSES = {"complete", "failed", "interrupted"}
SENSITIVE_NAMES = {".terraform", "terraform.tfstate", "terraform.tfstate.backup", "kubeconfig", "secrets"}
AUDIT_RAW_NAMES = {"chunks"}
SENSITIVE_KEY = re.compile(r"(?:password|passwd|secret|token|credential|private[_-]?key|access[_-]?key)", re.I)
TEXT_SECRET = re.compile(
    r"(?im)(\b(?:password|passwd|secret|token|credential|private[_-]?key|access[_-]?key)\b\s*[:=]\s*)([^\s,;]+)"
)
AWS_ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def files_manifest(root: Path) -> list[dict[str, Any]]:
    return [
        {"path": path.relative_to(root).as_posix(), "size": path.stat().st_size, "sha256": digest(path)}
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]


def write_manifest(root: Path, kind: str, metadata: dict[str, Any]) -> None:
    document = {
        "schema_version": BUNDLE_VERSION,
        "kind": kind,
        "created_at": datetime.now(timezone.utc).isoformat(),
        **metadata,
        "files": files_manifest(root),
    }
    (root / "manifest.json").write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "<redacted>" if SENSITIVE_KEY.search(str(key)) else redact_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return value


def copy_evidence_file(source: Path, target: Path) -> None:
    if source.suffix.lower() == ".json":
        try:
            document = json.loads(source.read_text(encoding="utf-8"))
            target.write_text(json.dumps(redact_value(document), indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
    if source.suffix.lower() in {".yaml", ".yml"}:
        try:
            document = yaml.safe_load(source.read_text(encoding="utf-8"))
            target.write_text(yaml.safe_dump(redact_value(document), sort_keys=False), encoding="utf-8")
            return
        except (UnicodeDecodeError, yaml.YAMLError):
            pass
    if source.suffix.lower() in {".env", ".log", ".txt", ".properties"} or source.name in {"stdout", "stderr"}:
        try:
            content = source.read_text(encoding="utf-8")
            content = TEXT_SECRET.sub(r"\1<redacted>", content)
            content = AWS_ACCESS_KEY.sub("<redacted-aws-access-key>", content)
            target.write_text(content, encoding="utf-8")
            shutil.copystat(source, target)
            return
        except UnicodeDecodeError:
            pass
    shutil.copy2(source, target)


def is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def copy_tree_filtered(
    source: Path,
    destination: Path,
    *,
    evidence: bool,
    excluded_roots: Iterable[Path] = (),
) -> None:
    for path in sorted(source.rglob("*")):
        if any(is_within(path, excluded) for excluded in excluded_roots):
            continue
        relative = path.relative_to(source)
        if any(part in SENSITIVE_NAMES for part in relative.parts):
            continue
        if path.name in {"evidence.tar.gz", "audit.tar.gz"}:
            continue
        if evidence and "audit" in relative.parts and (
            any(part in AUDIT_RAW_NAMES for part in relative.parts)
            or path.name.startswith("audit-")
        ):
            continue
        if path.is_dir():
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if evidence:
            copy_evidence_file(path, target)
        else:
            shutil.copy2(path, target)


def run_directories(result_root: Path) -> list[Path]:
    summary = result_root / "summary.json"
    if summary.is_file():
        value = json.loads(summary.read_text(encoding="utf-8"))
        paths = []
        for experiment in value.get("experiments", []):
            for target in experiment.get("targets", []):
                configured = Path(str(target.get("run_dir") or ""))
                if configured.is_dir():
                    paths.append(configured)
        if paths:
            return paths
    nested = result_root / "runs"
    if nested.is_dir():
        return sorted(path for path in nested.iterdir() if path.is_dir())
    return [result_root] if (result_root / "run-metadata.json").is_file() else []


def create_archive(source: Path, target: Path, root_name: str) -> None:
    partial = target.with_suffix(target.suffix + ".partial")
    partial.unlink(missing_ok=True)
    with tarfile.open(partial, "w:gz") as archive:
        archive.add(source, arcname=root_name, recursive=True)
    os.replace(partial, target)


def copy_report(report_dir: Path, output_dir: Path, *, experiment: str, environment: str, status: str) -> None:
    report = report_dir / "report.md"
    temporary = output_dir / "report.md.partial"
    markdown = (
        report.read_text(encoding="utf-8")
        if report.is_file()
        else f"# Experiment {experiment}\n\n- Environment: `{environment}`\n- Status: `{status}`\n\nNo generated metrics report was available. See `evidence.tar.gz` for collected diagnostics.\n"
    )
    if report.is_file():
        lines = markdown.splitlines(keepends=True)
        insertion = 1 if lines and lines[0].startswith("#") else 0
        lines[insertion:insertion] = [f"\n> Environment: `{environment}` · Status: `{status}`\n"]
        markdown = "".join(lines)
    asset_names = {
        path.name for path in report_dir.iterdir()
        if report_dir.is_dir() and path.is_file() and path.suffix.lower() in {".svg", ".png", ".webp", ".jpg", ".jpeg"}
    } if report_dir.is_dir() else set()
    for name in asset_names:
        markdown = re.sub(rf"\((?:\./)?{re.escape(name)}\)", f"(report-assets/{name})", markdown)
    temporary.write_text(markdown, encoding="utf-8")
    os.replace(temporary, output_dir / "report.md")
    assets_partial = output_dir / "report-assets.partial"
    if assets_partial.exists():
        shutil.rmtree(assets_partial)
    assets_partial.mkdir()
    for path in sorted(report_dir.iterdir()) if report_dir.is_dir() else []:
        if path.is_file() and path.suffix.lower() in {".svg", ".png", ".webp", ".jpg", ".jpeg"}:
            shutil.copy2(path, assets_partial / path.name)
    assets = output_dir / "report-assets"
    if assets.exists():
        shutil.rmtree(assets)
    os.replace(assets_partial, assets)


def finalize(
    *,
    result_root: Path,
    report_dir: Path,
    output_dir: Path,
    experiment: str,
    environment: str,
    status: str,
    restore_sources: Iterable[Path] = (),
) -> dict[str, Path]:
    if status not in FINAL_STATUSES:
        raise ValueError(f"Unsupported final artifact status: {status}")
    result_root = result_root.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ckc-finalize-", dir=output_dir) as temporary:
        staging = Path(temporary)
        publish_root = staging / "publish"
        audit_root = staging / "audit"
        evidence_root = staging / "evidence"
        publish_root.mkdir()
        audit_root.mkdir()
        evidence_root.mkdir()
        for run_dir in run_directories(result_root):
            audit = run_dir / "audit"
            if audit.is_dir():
                copy_tree_filtered(audit, audit_root / "runs" / run_dir.name / "audit", evidence=False)
        write_manifest(audit_root, "ckc-audit", {"experiment": experiment, "status": status})
        audit_target = publish_root / "audit.tar.gz"
        create_archive(audit_root, audit_target, "audit")

        copy_tree_filtered(
            result_root,
            evidence_root / "result",
            evidence=True,
            excluded_roots=[output_dir],
        )
        for run_dir in run_directories(result_root):
            try:
                run_dir.resolve().relative_to(result_root)
            except ValueError:
                copy_tree_filtered(run_dir, evidence_root / "result" / "runs" / run_dir.name, evidence=True)
        for source in restore_sources:
            if source.is_dir():
                copy_tree_filtered(source, evidence_root / "restore", evidence=False)
        write_manifest(evidence_root, "ckc-evidence", {
            "experiment": experiment,
            "environment": environment,
            "status": status,
            "audit": {"file": "audit.tar.gz", "sha256": digest(audit_target)},
        })
        evidence_target = publish_root / "evidence.tar.gz"
        create_archive(evidence_root, evidence_target, "evidence")
        copy_report(report_dir, publish_root, experiment=experiment, environment=environment, status=status)
        for name in ("report.md", "evidence.tar.gz", "audit.tar.gz"):
            os.replace(publish_root / name, output_dir / name)
        assets = output_dir / "report-assets"
        if assets.exists():
            shutil.rmtree(assets)
        os.replace(publish_root / "report-assets", assets)
    return {
        "report": output_dir / "report.md",
        "report_assets": output_dir / "report-assets",
        "evidence": output_dir / "evidence.tar.gz",
        "audit": output_dir / "audit.tar.gz",
    }
