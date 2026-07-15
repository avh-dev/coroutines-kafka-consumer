#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required. Install python3-yaml on the lab host.") from error


LEGACY_ENV_ARGS = {
    "LAB_KAFKA_IMPLEMENTATION": "--kafka-implementation",
    "PROCESSING_DISPATCHER_TYPE": "--processing-dispatcher-type",
    "PROCESSING_ENABLED": "--processing-enabled",
    "AUDIT_LOG_ENABLED": "--audit-log-enabled",
    "METRICS_IMPLEMENTATION": "--metrics-implementation",
    "WORKER_DISPATCHER_THREADS": "--worker-dispatcher-threads",
}


def parse_args() -> argparse.Namespace:
    lab_root = os.environ.get("LAB_ROOT", "/opt/ckc-lab")
    parser = argparse.ArgumentParser(description="Run internal-lab test bundles sequentially.")
    parser.add_argument("bundle", nargs="?", help="Bundle name/path, or 'all'. Omit for interactive selection.")
    parser.add_argument("--all", action="store_true", help="Run all bundle definitions sequentially.")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE", help="Global env override for all bundle tests.")
    parser.add_argument("--lab-root", default=lab_root)
    parser.add_argument("--run-test", default=f"{lab_root}/bin/run-test.sh")
    parser.add_argument("--bundle-dir", default=f"{lab_root}/test-bundles")
    parser.add_argument("--log-dir", default=f"{lab_root}/logs/bundles")
    parser.add_argument("--notify-hook", default=os.environ.get("CKC_NOTIFY_HOOK", ""))
    return parser.parse_args()


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return data


def bundle_files(bundle_dir: Path) -> list[Path]:
    return sorted(bundle_dir.glob("*.yaml"))


def resolve_named_yaml(directory: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() and path.is_file():
        return path
    if path.is_file():
        return path.resolve()
    if path.suffix != ".yaml":
        path = path.with_suffix(".yaml")
    candidate = directory / path
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(f"YAML file was not found: {value}")


def resolve_bundle(bundle: str, bundle_dir: Path) -> Path:
    return resolve_named_yaml(bundle_dir, bundle)


def select_bundles(bundle_dir: Path) -> list[Path]:
    bundles = bundle_files(bundle_dir)
    if not bundles:
        raise FileNotFoundError(f"No bundle definitions were found in {bundle_dir}")

    print("Available test bundles:", file=sys.stderr)
    for index, path in enumerate(bundles, start=1):
        bundle = load_yaml(path)
        description = bundle.get("description", "")
        suffix = f" - {description}" if description else ""
        print(f"  {index:2d}) {path.stem}{suffix}", file=sys.stderr)
    print("   A) all", file=sys.stderr)
    choice = input("Select number or A: ").strip().lower()
    if choice in {"a", "all"}:
        return bundles
    if not choice.isdigit() or not (1 <= int(choice) <= len(bundles)):
        raise ValueError(f"Invalid selection: {choice}")
    return [bundles[int(choice) - 1]]


def env_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def parse_env_override(value: str) -> tuple[str, str]:
    key, separator, raw = value.partition("=")
    key = key.strip()
    if not separator or not key:
        raise ValueError(f"Environment override must use KEY=VALUE: {value!r}")
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
        raise ValueError(f"Environment override key must be uppercase snake case: {key!r}")
    return key, raw


def env_overrides(values: list[str]) -> dict[str, str]:
    return dict(parse_env_override(value) for value in values)


def merge_env(defaults: dict[str, Any], global_env: dict[str, str], test: dict[str, Any]) -> dict[str, str]:
    legacy_env_keys = {
        "processing_enabled": "PROCESSING_ENABLED",
        "audit_log_enabled": "AUDIT_LOG_ENABLED",
        "metrics_implementation": "METRICS_IMPLEMENTATION",
        "worker_dispatcher_threads": "WORKER_DISPATCHER_THREADS",
    }
    default_env = defaults.get("env", {})
    test_env = test.get("env", {})
    if default_env in ("", None):
        default_env = {}
    if test_env in ("", None):
        test_env = {}
    if not isinstance(default_env, dict):
        raise ValueError("bundle defaults.env must be an object")
    if not isinstance(test_env, dict):
        raise ValueError("bundle tests[].env must be an object")

    result = {str(key): env_value(value) for key, value in default_env.items()}
    for source_key, env_key in legacy_env_keys.items():
        if source_key in defaults:
            result[env_key] = env_value(defaults[source_key])
    result.update(global_env)
    result.update({str(key): env_value(value) for key, value in test_env.items()})
    for source_key, env_key in legacy_env_keys.items():
        if source_key in test:
            result[env_key] = env_value(test[source_key])
    for key in result:
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
            raise ValueError(f"Environment override key must be uppercase snake case: {key!r}")
    return result


def normalize_tests(bundle: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    tests = bundle.get("tests")
    if tests is None:
        tests = bundle.get("runs")
    if not isinstance(tests, list) or not tests:
        raise ValueError(f"Bundle must define a non-empty tests list: {path}")
    for index, test in enumerate(tests, start=1):
        if not isinstance(test, dict):
            raise ValueError(f"Bundle tests[{index}] must be an object: {path}")
        if "deployment" not in test and "profile" not in test:
            raise ValueError(f"Bundle tests[{index}] must define profile or deployment: {path}")
        if "test_definition" not in test:
            raise ValueError(f"Bundle tests[{index}] must define test_definition: {path}")
    return tests


def select_bool(title: str, default: str) -> str:
    choice = input(f"{title} [{default}]: ").strip().lower()
    if not choice:
        return default
    if choice in {"true", "t", "yes", "y", "1"}:
        return "true"
    if choice in {"false", "f", "no", "n", "0"}:
        return "false"
    raise ValueError(f"Expected true or false, got {choice!r}")


def select_choice(title: str, default: str, choices: list[str]) -> str:
    choice_text = "/".join(choices)
    choice = input(f"{title} ({choice_text}) [{default}]: ").strip().upper()
    if not choice:
        return default
    if choice not in choices:
        raise ValueError(f"Expected one of {choices}, got {choice!r}")
    return choice


def interactive_global_env(current: dict[str, str]) -> dict[str, str]:
    if not sys.stdin.isatty():
        return {}
    if {"PROCESSING_ENABLED", "AUDIT_LOG_ENABLED", "METRICS_IMPLEMENTATION"}.issubset(current):
        return {}
    print("Bundle-wide settings:", file=sys.stderr)
    try:
        result = {}
        if "PROCESSING_ENABLED" not in current:
            result["PROCESSING_ENABLED"] = select_bool("Processing enabled", current.get("PROCESSING_ENABLED", "true"))
        if "AUDIT_LOG_ENABLED" not in current:
            result["AUDIT_LOG_ENABLED"] = select_bool("Audit logging enabled", current.get("AUDIT_LOG_ENABLED", "true"))
        if "METRICS_IMPLEMENTATION" not in current:
            result["METRICS_IMPLEMENTATION"] = select_choice("Metrics implementation", current.get("METRICS_IMPLEMENTATION", "MICROMETER"), ["MICROMETER", "NOOP"])
        return result
    except EOFError:
        return {}


def duration_seconds(text: str) -> int:
    total = 0
    for number, unit in re.findall(r"(\d+)\s*([hms])", text):
        total += int(number) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total


def load_profile_seconds(profile: str) -> int:
    return sum(duration_seconds(match) for match in re.findall(r"\(([^)]*)\)", profile))


def test_expected_seconds(lab_root: Path, test_definition: str) -> int | None:
    try:
        path = resolve_named_yaml(lab_root / "test-definitions", test_definition)
        definition = load_yaml(path)
        load_test = definition.get("load_test", {})
        if not isinstance(load_test, dict):
            return None
        profile = str(load_test.get("load_profile", ""))
        seconds = load_profile_seconds(profile)
        return seconds if seconds > 0 else None
    except Exception:
        return None


def format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "unknown"
    value = max(0, int(seconds))
    hours, remainder = divmod(value, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    return f"{minutes}m{secs:02d}s"


def audit_dirs(lab_root: Path) -> set[str]:
    audit_dir = lab_root / "audit"
    if not audit_dir.is_dir():
        return set()
    return {path.name for path in audit_dir.iterdir() if path.is_dir()}


def newest_audit_dir(lab_root: Path, before: set[str]) -> str:
    audit_dir = lab_root / "audit"
    if not audit_dir.is_dir():
        return ""
    candidates = []
    for path in audit_dir.iterdir():
        if path.name == "live" or not path.is_dir() or path.name in before:
            continue
        metadata = path / "run-metadata.json"
        if metadata.is_file():
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        return ""
    return str(max(candidates, key=lambda item: item[0])[1])


def run_status(audit_dir_value: str) -> dict[str, Any]:
    if not audit_dir_value:
        return {}
    path = Path(audit_dir_value) / "run-status.json"
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


class TtyCommandReader:
    def __init__(self, stop_queue: queue.Queue[str]) -> None:
        self._stop_queue = stop_queue
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "TtyCommandReader":
        if not sys.stdin.isatty():
            return self
        self._thread = threading.Thread(target=self._read_lines, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        return

    def _read_lines(self) -> None:
        while True:
            try:
                line = sys.stdin.readline()
            except OSError:
                return
            if line == "":
                return
            if line.strip().lower() == "q":
                self._stop_queue.put("q")
                return


def stdout_reader(process: subprocess.Popen[str], output: queue.Queue[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        output.put(line)


def request_graceful_stop(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "posix":
        process.send_signal(signal.SIGINT)
    else:
        process.terminate()


def force_stop_if_needed(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def notify_hook_path(lab_root: Path, configured: str) -> Path | None:
    candidates = []
    if configured:
        candidates.append(Path(configured))
    candidates.extend(
        [
            lab_root / "notify" / "notify.py",
            lab_root / "notify" / "notify.sh",
            lab_root / "config" / "notify.py",
            lab_root / "config" / "notify.sh",
        ]
    )
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return path
    return None


def notify(hook: Path | None, event: str, payload: dict[str, Any], log_dir: Path) -> None:
    if hook is None:
        return
    log_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", prefix=f"notify-{event}-", dir=log_dir, delete=False) as file:
        json.dump(payload, file, indent=2)
        file.write("\n")
        payload_path = file.name
    try:
        subprocess.run([str(hook), event, payload_path], check=False)
    except Exception as error:
        print(f"Notification hook failed for {event}: {error}", file=sys.stderr)


def command_for_run(run_test: Path, test: dict[str, Any], test_definition: str, env: dict[str, str]) -> list[str]:
    command = [str(run_test), "--skip-analysis"]
    if "profile" in test:
        command.extend(["--profile", str(test["profile"])])
        if "base_rate" in test:
            command.extend(["--base-rate", env_value(test["base_rate"])])
        if "capacity_factor" in test:
            command.extend(["--capacity-factor", env_value(test["capacity_factor"])])
        if "replicas" in test:
            command.extend(["--replicas", env_value(test["replicas"])])
        for topic in ("order", "batch", "telemetry"):
            mode_key = f"{topic}_processing_mode"
            if mode_key in test:
                command.extend([f"--{topic}-processing-mode", env_value(test[mode_key])])
            for knob in ("partitions", "workers", "pollers"):
                key = f"{topic}_{knob}"
                if key in test:
                    command.extend([f"--{topic}-{knob}", env_value(test[key])])
    else:
        command.extend(["--deployment", str(test["deployment"])])
    for key, flag in LEGACY_ENV_ARGS.items():
        if key in env:
            command.extend([flag, env[key]])
    for key in sorted(env):
        command.extend(["--env", f"{key}={env[key]}"])
    command.append(test_definition)
    return command


def run_one(
    run_test: Path,
    lab_root: Path,
    defaults: dict[str, Any],
    global_env: dict[str, str],
    test: dict[str, Any],
    index: int,
    total: int,
    log_file,
    hook: Path | None,
    log_dir: Path,
) -> dict[str, Any]:
    name = str(test.get("name") or test.get("profile") or test.get("deployment"))
    profile = str(test.get("profile", ""))
    deployment = str(test.get("deployment", ""))
    test_definition = str(test["test_definition"])
    env = merge_env(defaults, global_env, test)
    command = command_for_run(run_test, test, test_definition, env)
    expected_seconds = test_expected_seconds(lab_root, test_definition)

    before = audit_dirs(lab_root)
    started_at = datetime.now(timezone.utc)
    log_file.write(f"\n=== {name} started at {started_at.isoformat()} ===\n")
    log_file.write("command: " + " ".join(command) + "\n")
    log_file.flush()
    print(f"\n=== Running bundle entry {index}/{total}: {name} ===", flush=True)
    print(f"Expected load phase duration: {format_duration(expected_seconds)}", flush=True)
    notify(hook, "test_started", {"name": name, "profile": profile, "deployment": deployment, "test_definition": test_definition, "index": index, "total": total}, log_dir)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        env={**os.environ, "LAB_ROOT": str(lab_root)},
    )
    output: queue.Queue[str] = queue.Queue()
    stop_queue: queue.Queue[str] = queue.Queue()
    reader = threading.Thread(target=stdout_reader, args=(process, output), daemon=True)
    reader.start()
    stop_requested = False
    last_progress_at = 0.0
    if sys.stdin.isatty():
        print("Type q and press Enter to stop the bundle after current test cleanup.", flush=True)
    with TtyCommandReader(stop_queue):
        while process.poll() is None or not output.empty():
            while True:
                try:
                    line = output.get_nowait()
                except queue.Empty:
                    break
                print(line, end="")
                log_file.write(line)
            now = time.monotonic()
            if now - last_progress_at >= 30:
                elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                eta = None if expected_seconds is None else max(0, expected_seconds - elapsed)
                print(f"Bundle progress: test {index}/{total} {name}, elapsed {format_duration(elapsed)}, eta {format_duration(eta)}", flush=True)
                last_progress_at = now
            try:
                stop_queue.get_nowait()
            except queue.Empty:
                pass
            else:
                stop_requested = True
                print("Stopping bundle by user request. Waiting for current test cleanup.", flush=True)
                log_file.write("Stopping bundle by user request.\n")
                request_graceful_stop(process)
            time.sleep(0.2)
    reader.join(timeout=1)
    while not output.empty():
        line = output.get_nowait()
        print(line, end="")
        log_file.write(line)
    if stop_requested and process.returncode is None:
        force_stop_if_needed(process)

    ended_at = datetime.now(timezone.utc)
    audit_dir = newest_audit_dir(lab_root, before)
    status = run_status(audit_dir)
    child_exit_code = int(process.returncode or 0)
    interrupted = stop_requested or status.get("status") == "interrupted" or child_exit_code == 130
    exit_code = 130 if stop_requested else int(status.get("exit_code", child_exit_code))
    result = {
        "name": name,
        "profile": profile,
        "deployment": deployment,
        "test_definition": test_definition,
        "env": env,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "exit_code": exit_code,
        "interrupted": interrupted,
        "stop_requested": stop_requested,
        "run_status": status,
        "audit_dir": audit_dir,
    }
    log_file.write(f"=== {name} finished with exit_code={exit_code} at {ended_at.isoformat()} ===\n")
    if audit_dir:
        log_file.write(f"audit_dir: {audit_dir}\n")
    log_file.flush()
    notify(hook, "test_finished", result, log_dir)
    return result


def audit_input_file(audit_dir: Path) -> Path | None:
    logs = sorted(audit_dir.glob("audit-*.log"))
    if logs:
        return logs[0]
    gz_logs = sorted(audit_dir.glob("audit-*.log.gz"))
    return gz_logs[0] if gz_logs else None


def analyze_one(lab_root: Path, audit_dir_value: str, log_file, hook: Path | None, log_dir: Path) -> dict[str, Any]:
    audit_dir = Path(audit_dir_value)
    input_file = audit_input_file(audit_dir)
    metadata_file = audit_dir / "run-metadata.json"
    progress_file = audit_dir / "analyzer-progress.log"
    summary_file = audit_dir / "summary.yaml"
    if input_file is None:
        return {"audit_dir": str(audit_dir), "exit_code": 1, "error": "raw audit log was not found"}

    print(f"\n=== Analyzing audit: {audit_dir.name} ===", flush=True)
    notify(hook, "audit_run_analysis_started", {"audit_dir": str(audit_dir)}, log_dir)
    started_at = datetime.now(timezone.utc)
    command = [
        sys.executable,
        str(lab_root / "helpers" / "audit" / "analyze-audit.py"),
        "--input-file",
        str(input_file),
        "--metadata-file",
        str(metadata_file),
        "--require-records",
    ]
    with summary_file.open("w", encoding="utf-8") as summary, progress_file.open("w", encoding="utf-8") as progress:
        process = subprocess.Popen(command, stdout=summary, stderr=subprocess.PIPE, text=True, bufsize=1)
        assert process.stderr is not None
        for line in process.stderr:
            print(line, end="")
            progress.write(line)
            log_file.write(line)
        exit_code = process.wait()
    if exit_code == 0 and input_file.suffix != ".gz":
        subprocess.run(["gzip", "-1", str(input_file)], check=False)
    result = {
        "audit_dir": str(audit_dir),
        "summary": str(summary_file),
        "progress": str(progress_file),
        "started_at": started_at.isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "exit_code": exit_code,
    }
    notify(hook, "audit_run_analysis_finished", result, log_dir)
    return result


def run_bundle(
    bundle_path: Path,
    run_test: Path,
    lab_root: Path,
    log_dir: Path,
    bundle_set_id: str,
    global_env: dict[str, str],
    hook: Path | None,
) -> dict[str, Any]:
    bundle = load_yaml(bundle_path)
    defaults = bundle.get("defaults", {})
    if defaults in ("", None):
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError(f"Bundle defaults must be an object: {bundle_path}")
    tests = normalize_tests(bundle, bundle_path)
    bundle_name = str(bundle.get("name") or bundle_path.stem)
    log_path = log_dir / f"{bundle_name}-{bundle_set_id}.log"

    results: list[dict[str, Any]] = []
    analysis_results: list[dict[str, Any]] = []
    notify(hook, "bundle_started", {"bundle": bundle_name, "bundle_file": str(bundle_path), "tests": len(tests)}, log_dir)
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"bundle: {bundle_name}\n")
        log_file.write(f"bundle_file: {bundle_path}\n")
        description = bundle.get("description")
        if description:
            log_file.write(f"description: {description}\n")
        for index, test in enumerate(tests, start=1):
            result = run_one(run_test, lab_root, defaults, global_env, test, index, len(tests), log_file, hook, log_dir)
            results.append(result)
            if result["exit_code"] != 0:
                break

        runs_exit_code = next((test["exit_code"] for test in results if test["exit_code"] != 0), 0)
        if runs_exit_code == 0:
            auditable_runs = [result for result in results if result.get("audit_dir") and result["env"].get("AUDIT_LOG_ENABLED", "true") == "true"]
            notify(hook, "bundle_runs_finished", {"bundle": bundle_name, "runs": len(results), "auditable_runs": len(auditable_runs)}, log_dir)
            if auditable_runs:
                print(f"\n=== Bundle load phases finished. Starting audit analysis for {len(auditable_runs)} run(s). ===", flush=True)
                notify(hook, "audit_analysis_started", {"bundle": bundle_name, "auditable_runs": len(auditable_runs)}, log_dir)
                for result in auditable_runs:
                    analysis_results.append(analyze_one(lab_root, str(result["audit_dir"]), log_file, hook, log_dir))
                notify(hook, "audit_analysis_finished", {"bundle": bundle_name, "analysis": analysis_results}, log_dir)

    analysis_exit_code = next((analysis["exit_code"] for analysis in analysis_results if analysis["exit_code"] != 0), 0)
    exit_code = runs_exit_code or analysis_exit_code
    summary = {
        "bundle": bundle_name,
        "description": bundle.get("description", ""),
        "bundle_file": str(bundle_path),
        "log_file": str(log_path),
        "tests": results,
        "analysis": analysis_results,
        "exit_code": exit_code,
    }
    notify(hook, "bundle_finished" if exit_code == 0 else "bundle_failed", summary, log_dir)
    return summary


def main() -> int:
    args = parse_args()
    lab_root = Path(args.lab_root)
    bundle_dir = Path(args.bundle_dir)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.all or args.bundle == "all":
        selected = bundle_files(bundle_dir)
        if not selected:
            raise FileNotFoundError(f"No bundle definitions were found in {bundle_dir}")
    elif args.bundle:
        selected = [resolve_bundle(args.bundle, bundle_dir)]
    elif sys.stdin.isatty():
        selected = select_bundles(bundle_dir)
    else:
        raise ValueError("bundle is required without interactive input")

    cli_env = env_overrides(args.env)
    global_env = {**interactive_global_env(cli_env), **cli_env}
    hook = notify_hook_path(lab_root, args.notify_hook)
    bundle_set_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summaries = []
    for bundle_path in selected:
        summary = run_bundle(bundle_path, Path(args.run_test), lab_root, log_dir, bundle_set_id, global_env, hook)
        summaries.append(summary)
        if summary["exit_code"] != 0:
            break

    summary_path = log_dir / f"bundle-run-{bundle_set_id}.json"
    document = {
        "bundle_set_id": bundle_set_id,
        "bundles": summaries,
        "exit_code": next((bundle["exit_code"] for bundle in summaries if bundle["exit_code"] != 0), 0),
    }
    summary_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    print(f"\nBundle summary: {summary_path}")
    return int(document["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
