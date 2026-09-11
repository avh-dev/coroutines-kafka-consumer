from __future__ import annotations

import json
import gzip
import os
import re
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml


FINAL_STATUSES = {"complete", "failed", "interrupted"}
SENSITIVE_NAMES = {".terraform", "terraform.tfstate", "terraform.tfstate.backup", "kubeconfig", "secrets"}
SENSITIVE_KEY = re.compile(r"(?:password|passwd|secret|token|credential|private[_-]?key|access[_-]?key)", re.I)
TEXT_SECRET = re.compile(
    r"(?im)(\b(?:password|passwd|secret|token|credential|private[_-]?key|access[_-]?key)\b\s*[:=]\s*)([^\s,;]+)"
)
AWS_ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
RESTORE_FILES = {
    "docker-compose.yml",
    "import-grafana-annotations.py",
    "import-loki.py",
    "loki.yaml",
}


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "<redacted>" if SENSITIVE_KEY.search(str(key)) else redact_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return value


def portable_value(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: portable_value(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [portable_value(item, replacements) for item in value]
    if isinstance(value, str):
        for source, target in replacements.items():
            value = value.replace(source, target)
    return value


def copy_evidence_file(source: Path, target: Path, replacements: dict[str, str] | None = None) -> None:
    replacements = replacements or {}
    if source.suffix.lower() == ".json":
        try:
            document = json.loads(source.read_text(encoding="utf-8"))
            target.write_text(
                json.dumps(portable_value(redact_value(document), replacements), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
    if source.suffix.lower() in {".yaml", ".yml"}:
        try:
            document = yaml.safe_load(source.read_text(encoding="utf-8"))
            target.write_text(
                yaml.safe_dump(portable_value(redact_value(document), replacements), sort_keys=False),
                encoding="utf-8",
            )
            return
        except (UnicodeDecodeError, yaml.YAMLError):
            pass
    if source.suffix.lower() in {".env", ".log", ".txt", ".properties", ".sh", ".tftpl", ".jsonl"} or source.name in {"stdout", "stderr"}:
        try:
            content = source.read_text(encoding="utf-8")
            content = TEXT_SECRET.sub(r"\1<redacted>", content)
            content = AWS_ACCESS_KEY.sub("<redacted-aws-access-key>", content)
            for source_path, portable_path in replacements.items():
                content = content.replace(source_path, portable_path)
            target.write_text(content, encoding="utf-8")
            shutil.copystat(source, target)
            return
        except UnicodeDecodeError:
            pass
    shutil.copy2(source, target)


def copy_portable(source: Path, target: Path, replacements: dict[str, str]) -> bool:
    if not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    copy_evidence_file(source, target, replacements)
    return True


def copy_portable_tree(
    source: Path,
    target: Path,
    replacements: dict[str, str],
    *,
    suffixes: set[str] | None = None,
) -> None:
    if not source.is_dir():
        return
    for path in sorted(source.rglob("*")):
        if not path.is_file() or any(part in SENSITIVE_NAMES for part in path.relative_to(source).parts):
            continue
        if suffixes is not None and path.suffix.lower() not in suffixes and path.name not in suffixes:
            continue
        copy_portable(path, target / path.relative_to(source), replacements)


def copy_audit_log(source: Path, destination: Path) -> bool:
    sources = sorted(source.glob("chunks/audit-*.log.gz"))
    sources += sorted(source.glob("chunks/audit-*.log"))
    sources += sorted(source.glob("audit-*.log.gz"))
    sources += sorted(source.glob("audit-*.log"))
    if not sources:
        return False
    with destination.open("wb") as output:
        for path in sources:
            if path.name.endswith(".gz"):
                with gzip.open(path, "rb") as input_stream:
                    shutil.copyfileobj(input_stream, output)
            else:
                with path.open("rb") as input_stream:
                    shutil.copyfileobj(input_stream, output)
    return True


def run_directories(result_root: Path) -> list[Path]:
    summary = result_root / "summary.json"
    if summary.is_file():
        value = json.loads(summary.read_text(encoding="utf-8"))
        paths = []
        for experiment in value.get("experiments", []):
            for target in experiment.get("targets", []):
                configured_value = str(target.get("run_dir") or "").strip()
                if not configured_value:
                    continue
                configured = Path(configured_value)
                if not configured.is_absolute():
                    configured = result_root / configured
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


def copy_report(
    report_dir: Path,
    output_dir: Path,
    *,
    experiment: str,
    environment: str,
    status: str,
    evidence_name: str | None = None,
    audit_name: str | None = None,
) -> None:
    report = report_dir / "report.md"
    output_dir.mkdir(parents=True, exist_ok=True)
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
    if evidence_name and audit_name:
        links = (
            f"[Evidence bundle](../{evidence_name}) · "
            f"[Audit archive](../{audit_name})"
        )
        placeholder = "Evidence bundle and audit archive links are added when the result is finalized."
        if placeholder in markdown:
            markdown = markdown.replace(placeholder, links)
        elif links not in markdown:
            markdown = markdown.rstrip() + f"\n\n## Full evidence\n\n{links}\n"
    asset_names = {
        path.name for path in report_dir.iterdir()
        if report_dir.is_dir() and path.is_file() and path.suffix.lower() in {".svg", ".png", ".webp", ".jpg", ".jpeg"}
    } if report_dir.is_dir() else set()
    for name in asset_names:
        markdown = re.sub(rf"\((?:\./)?{re.escape(name)}\)", f"(assets/{name})", markdown)
    temporary.write_text(markdown, encoding="utf-8")
    os.replace(temporary, output_dir / "report.md")
    assets_partial = output_dir / "assets.partial"
    if assets_partial.exists():
        shutil.rmtree(assets_partial)
    assets_partial.mkdir()
    for path in sorted(report_dir.iterdir()) if report_dir.is_dir() else []:
        if path.is_file() and path.suffix.lower() in {".svg", ".png", ".webp", ".jpg", ".jpeg"}:
            shutil.copy2(path, assets_partial / path.name)
    assets = output_dir / "assets"
    if assets.exists():
        shutil.rmtree(assets)
    os.replace(assets_partial, assets)


def timestamp_from(value: str) -> str | None:
    match = re.search(r"(20\d{6})[T-]?(\d{6})Z?", value)
    return f"{match.group(1)}T{match.group(2)[:4]}Z" if match else None


def result_identity(experiment: str, result_root: Path) -> str:
    timestamp = timestamp_from(result_root.name) or timestamp_from(result_root.parent.name)
    summary = result_root / "summary.json"
    document = json.loads(summary.read_text(encoding="utf-8")) if summary.is_file() else {}
    timestamp = timestamp or timestamp_from(str(document.get("experiment_set_id") or ""))
    if timestamp is None:
        starts = [
            str(target.get("started_at") or "")
            for item in document.get("experiments", [])
            for target in item.get("targets", [])
            if target.get("started_at")
        ]
        if starts:
            parsed = datetime.fromisoformat(min(starts).replace("Z", "+00:00"))
            timestamp = parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    timestamp = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = re.sub(r"[^a-zA-Z0-9._-]+", "-", experiment.strip().lower()).strip("-") or "experiment"
    return f"ckc-experiment-{name}-{timestamp}"


def evidence_readme(identity: str, experiment: str, environment: str, status: str) -> str:
    return f"""# Experiment evidence: {identity}

| Field | Value |
| --- | --- |
| Experiment | `{experiment}` |
| Environment | `{environment}` |
| Status | `{status}` |
| Result | `{identity}` |

Run `./run-grafana.sh` from an interactive terminal to open the preserved report, metrics, and logs. Docker with Compose, Python 3, and curl are required. The command stays attached; press `q` or `Ctrl-C` to stop and remove the containers.

## Contents

- `report/` — the Markdown report and its images.
- `restore/` — only the dashboard, Loki records, metrics snapshot, and private restore implementation.
- `deployment/` — resolved experiment inputs, generated Kubernetes resources, and execution logs.
- `lab/` — environment construction commands and, for AWS, the exact Terraform sources and resolved inputs.
- `diagnostics/` — raw Thread Stats snapshots, Kafka packet captures, capture metadata, and packet-analysis outputs.
"""


def audit_readme(identity: str, experiment: str, status: str) -> str:
    return f"""# Experiment audit: {identity}

Independent raw audit evidence for experiment `{experiment}` with final status `{status}`.
Each target is stored below `targetN.<name>/`. A target contains its summary, analyzer progress, and one uncompressed `audit.log`; `summary.yaml` at this level combines the target summaries.
"""


def repository_root() -> Path | None:
    source = Path(__file__).resolve()
    relative = Path("demo/infra/shared/result_bundle/finalize.py")
    for candidate in source.parents:
        if (candidate / relative).resolve() == source:
            return candidate
    return None


def portable_replacements(result_root: Path) -> dict[str, str]:
    result = result_root.resolve()
    values = {
        str(result): "$RESULT_DIR",
        "/opt/ckc-lab": "$LAB_ROOT",
        "/opt/ckc-runner": "$RUNNER_ROOT",
    }
    repository = repository_root()
    if repository is not None:
        values[str(repository)] = "$REPOSITORY"
    if result.name == "result":
        values[str(result.parent)] = "$SESSION_DIR"
    return dict(sorted(values.items(), key=lambda item: len(item[0]), reverse=True))


def build_restore(
    result_root: Path,
    destination: Path,
    restore_sources: Iterable[Path],
    replacements: dict[str, str],
) -> None:
    implementation = destination / "_implementation"
    for source in restore_sources:
        if not source.is_dir():
            continue
        for name in RESTORE_FILES:
            copy_portable(source / name, implementation / name, replacements)
        copy_portable_tree(source / "provisioning", implementation / "provisioning", replacements)
        copy_portable(source / "run-grafana.sh", destination.parent / "run-grafana.sh", replacements)
    dashboard = result_root / "config/ckc-experiment.json"
    copy_portable(dashboard, destination / "dashboard/ckc-experiment.json", replacements)
    root_loki = sorted((result_root / "logs/loki").glob("*.jsonl"))
    if root_loki:
        for source in root_loki:
            copy_portable(source, destination / "loki" / source.name, replacements)
    else:
        for run_dir in run_directories(result_root):
            for source in sorted((run_dir / "logs/loki").glob("*.jsonl")):
                copy_portable(source, destination / "loki" / f"{run_dir.name}-{source.name}", replacements)
    metrics_archive = result_root / "metrics/victoriametrics-data.tar.gz"
    if metrics_archive.is_file():
        copy_portable(metrics_archive, destination / "victoriametrics-data.tar.gz", replacements)
    else:
        copy_portable_tree(result_root / "metrics/prometheus", destination / "prometheus", replacements)


def materialized_roots(result_root: Path) -> list[Path]:
    roots = sorted(path for path in result_root.glob("*-materialized") if path.is_dir())
    session_materialized = (result_root.parent if result_root.name == "result" else result_root) / "materialized"
    if session_materialized.is_dir():
        roots.append(session_materialized)
    return roots


def target_names_by_run(result_root: Path) -> dict[str, str]:
    summary = result_root / "summary.json"
    document = json.loads(summary.read_text(encoding="utf-8")) if summary.is_file() else {}
    return {
        Path(str(target.get("run_dir"))).name: str(target.get("target") or target.get("name"))
        for experiment in document.get("experiments", [])
        for target in experiment.get("targets", [])
        if target.get("run_dir") and (target.get("target") or target.get("name"))
    }


def build_deployment(result_root: Path, report_dir: Path, destination: Path, replacements: dict[str, str]) -> None:
    raw = report_dir / "raw"
    for name in ("experiment.yaml", "resolved-experiment.yaml", "acceptance.yaml"):
        copy_portable(raw / name, destination / name, replacements)
    for root in materialized_roots(result_root):
        copy_portable(root / "implementation-profiles.yaml", destination / "implementation-profiles.yaml", replacements)
        if not (destination / "resolved-experiment.yaml").is_file():
            copy_portable(root / "resolved-experiment.yaml", destination / "resolved-experiment.yaml", replacements)
        for target_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            target = destination / "targets" / target_dir.name
            for source in sorted(target_dir.glob("*.yaml")):
                section = "kubernetes" if source.name in {"load-test.yaml", "project-deployment.yaml"} else "definition"
                copy_portable(source, target / section / source.name, replacements)
    target_names = target_names_by_run(result_root)
    for run_dir in run_directories(result_root):
        target = destination / "targets" / target_names.get(run_dir.name, run_dir.name)
        copy_portable_tree(run_dir / "generated", target / "kubernetes", replacements, suffixes={".yaml", ".yml"})
        copy_portable(run_dir / "logs/runner/session.log", target / "execution.log", replacements)
    logs = sorted(result_root.glob("*.log"))
    if logs:
        copy_portable(logs[0], destination / "commands.log", replacements)


def build_diagnostics(result_root: Path, destination: Path, replacements: dict[str, str]) -> None:
    target_names = target_names_by_run(result_root)
    for run_dir in run_directories(result_root):
        target = destination / "targets" / target_names.get(run_dir.name, run_dir.name)
        diagnostics = run_dir / "diagnostics"
        copy_portable(diagnostics / "kafka-metadata.json", target / "kafka-metadata.json", replacements)
        copy_portable_tree(diagnostics / "thread-stats", target / "thread-stats", replacements)
        copy_portable_tree(diagnostics / "tcpdump", target / "tcpdump", replacements)
        copy_portable_tree(diagnostics / "pcap-analysis", target / "pcap-analysis", replacements)


def build_lab(result_root: Path, destination: Path, environment: str, replacements: dict[str, str]) -> None:
    if environment == "aws":
        session = result_root.parent if result_root.name == "result" else result_root
        copy_portable(session / "commands.log", destination / "commands.log", replacements)
        repository = repository_root()
        if repository is None:
            raise RuntimeError("AWS evidence finalization requires a repository checkout")
        copy_portable(
            repository / "demo/infra/aws/runner-assets/bin/create-lab.sh",
            destination / "scripts/create-lab.sh",
            replacements,
        )
        terraform_root = destination / "terraform"
        modules = {
            "load-lab": repository / "demo/infra/aws/assets/terraform/load-lab",
            "runner": repository / "demo/infra/aws/terraform/runner",
            "session-artifacts": repository / "demo/infra/aws/terraform/session-artifacts",
        }
        for name, source in modules.items():
            copy_portable_tree(
                source,
                terraform_root / name,
                replacements,
                suffixes={".tf", ".hcl", ".tftpl"},
            )
        state_path = session / "session.json"
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.is_file() else {}
        stack_names = {"lab": "load-lab", "runner": "runner", "artifacts": "session-artifacts"}
        for stack, target_name in stack_names.items():
            variables = (state.get("terraform", {}).get(stack, {}) or {}).get("variables")
            if isinstance(variables, dict):
                target = terraform_root / target_name / "resolved.auto.tfvars.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(
                    json.dumps(portable_value(redact_value(variables), replacements), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
        for run_dir in run_directories(result_root):
            copy_portable_tree(run_dir / "config/lab-evidence/helm", destination / "helm", replacements)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "README.md").write_text(
            "# AWS lab construction\n\n`commands.log` preserves the executed controller commands and Terraform apply output. `terraform/` contains the exact module sources and resolved variables used for the disposable lab, runner, and artifact transport. `scripts/create-lab.sh` is the runner-side lab configuration entrypoint; generated third-party chart values are under `helm/` when Helm was used. Terraform state and credentials are intentionally excluded.\n",
            encoding="utf-8",
        )
    else:
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "README.md").write_text(
            "# Internal lab\n\nThe experiment used the pre-existing internal lab; it did not provision or destroy the lab. Deployment commands and generated Kubernetes inputs are preserved under `../deployment/`.\n",
            encoding="utf-8",
        )


def build_audit(
    result_root: Path,
    destination: Path,
    identity: str,
    experiment: str,
    status: str,
    replacements: dict[str, str],
) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "README.md").write_text(audit_readme(identity, experiment, status), encoding="utf-8")
    summaries: list[dict[str, Any]] = []
    target_names = target_names_by_run(result_root)
    for index, run_dir in enumerate(run_directories(result_root), start=1):
        audit = run_dir / "audit"
        if audit.is_dir():
            target_name = re.sub(r"[^A-Za-z0-9._-]+", "-", target_names.get(run_dir.name, run_dir.name)).strip("-") or run_dir.name
            target = destination / f"target{index}.{target_name}"
            target.mkdir(parents=True, exist_ok=True)
            for name in ("summary.yaml", "analyzer-progress.log", "acceptance.json"):
                source = audit / name
                if source.is_file():
                    copy_portable(source, target / name, replacements)
            copy_audit_log(audit, target / "audit.log")
            summary = audit / "summary.yaml"
            if summary.is_file():
                summaries.append({
                    "target": target.name,
                    "summary": portable_value(
                        redact_value(yaml.safe_load(summary.read_text(encoding="utf-8"))),
                        replacements,
                    ),
                })
    if summaries:
        (destination / "summary.yaml").write_text(
            yaml.safe_dump({"targets": summaries}, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )


def finalize(
    *,
    result_root: Path,
    report_dir: Path,
    output_dir: Path,
    experiment: str,
    environment: str,
    status: str,
    restore_sources: Iterable[Path] = (),
    replace: bool = False,
) -> dict[str, Path]:
    if status not in FINAL_STATUSES:
        raise ValueError(f"Unsupported final artifact status: {status}")
    result_root = result_root.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    identity = result_identity(experiment, result_root)
    final_dir = output_dir / identity
    if final_dir.exists() and not replace:
        raise FileExistsError(f"Final experiment result already exists: {final_dir}")
    replacements = portable_replacements(result_root)
    with tempfile.TemporaryDirectory(prefix="ckc-finalize-", dir=output_dir) as temporary:
        staging = Path(temporary)
        publish_root = staging / identity
        evidence_root = staging / "evidence"
        audit_root = staging / "audit"
        publish_root.mkdir()
        evidence_root.mkdir()
        build_audit(result_root, audit_root / "audit", identity, experiment, status, replacements)
        audit_name = f"{identity}-audit.tar.gz"
        audit_target = publish_root / audit_name
        create_archive(audit_root, audit_target, identity)
        (evidence_root / "README.md").write_text(
            evidence_readme(identity, experiment, environment, status), encoding="utf-8"
        )
        copy_report(report_dir, evidence_root / "report", experiment=experiment, environment=environment, status=status)
        build_restore(result_root, evidence_root / "restore", restore_sources, replacements)
        build_deployment(result_root, report_dir, evidence_root / "deployment", replacements)
        build_lab(result_root, evidence_root / "lab", environment, replacements)
        build_diagnostics(result_root, evidence_root / "diagnostics", replacements)
        evidence_name = f"{identity}-evidence.tar.gz"
        evidence_target = publish_root / evidence_name
        create_archive(evidence_root, evidence_target, identity)
        copy_report(
            report_dir,
            publish_root / "report",
            experiment=experiment,
            environment=environment,
            status=status,
            evidence_name=evidence_name,
            audit_name=audit_name,
        )
        if final_dir.exists():
            shutil.rmtree(final_dir)
        os.replace(publish_root, final_dir)
    return {
        "root": final_dir,
        "report": final_dir / "report/report.md",
        "report_assets": final_dir / "report/assets",
        "evidence": final_dir / f"{identity}-evidence.tar.gz",
        "audit": final_dir / f"{identity}-audit.tar.gz",
    }
