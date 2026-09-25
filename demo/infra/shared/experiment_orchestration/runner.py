from __future__ import annotations

import argparse
import shlex
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

import yaml

from .definition import ResolvedExperiment, resolve_experiment_definition


@dataclass(frozen=True)
class RunRequest:
    repo_root: Path
    experiment: Path
    environment: str
    work_dir: Path
    lab_root: Path
    env: tuple[str, ...]
    build_images: bool
    internal_lab_host: str
    internal_lab_user: str
    notify_hook: str
    telegram_env: Path


class EnvironmentAdapter(Protocol):
    name: str

    def command(self, request: RunRequest, experiment: ResolvedExperiment) -> list[str]: ...


class AwsAdapter:
    name = "aws"

    def command(self, request: RunRequest, experiment: ResolvedExperiment) -> list[str]:
        configuration = experiment.environment_definition or {}
        region = str(configuration.get("region") or "").strip()
        if not region:
            raise ValueError("AWS experiment environment must define region")
        try:
            source = request.experiment.relative_to(request.repo_root)
        except ValueError as error:
            raise ValueError("AWS experiment must be inside the repository checkout") from error
        command = [
            "python3",
            str(request.repo_root / "demo/infra/aws/scripts/run-experiment.py"),
            "--work-dir",
            str(request.work_dir / "aws"),
            "run",
            "--experiment",
            source.as_posix(),
            "--region",
            region,
        ]
        command.append("--build-images" if request.build_images else "--skip-build-images")
        if request.notify_hook:
            command.extend(["--notify-hook", request.notify_hook])
        command.extend(["--telegram-env", str(request.telegram_env)])
        return command


class InternalLabAdapter:
    name = "internal-lab"

    def command(self, request: RunRequest, experiment: ResolvedExperiment) -> list[str]:
        helper = request.lab_root / "helpers/run-experiment.py"
        installed_experiment = request.lab_root / "experiments" / request.experiment.name
        arguments = [
            "python3", str(helper), str(installed_experiment),
            "--lab-root", str(request.lab_root),
        ]
        for value in request.env:
            arguments.extend(["--env", value])
        if request.notify_hook:
            arguments.extend(["--notify-hook", request.notify_hook])
        remote = " ".join(shlex.quote(value) for value in arguments)
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", f"{request.internal_lab_user}@{request.internal_lab_host}", remote]


ADAPTERS: dict[str, EnvironmentAdapter] = {
    "aws": AwsAdapter(),
    "internal-lab": InternalLabAdapter(),
}


def execute(command: Sequence[str]) -> int:
    process = subprocess.Popen(list(command))
    previous: dict[int, signal.Handlers] = {}

    def forward(signum: int, _frame: object) -> None:
        if process.poll() is None:
            process.send_signal(signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, forward)
    try:
        return process.wait()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def run(request: RunRequest) -> int:
    adapter = ADAPTERS.get(request.environment)
    if adapter is None:
        raise ValueError(f"Unsupported experiment environment: {request.environment}")
    resolved = resolve_experiment_definition(
        request.experiment,
        environment=request.environment,
    )
    return execute(adapter.command(request, resolved))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one canonical CKC experiment in a configured environment.")
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--environment", required=True, choices=sorted(ADAPTERS))
    parser.add_argument("--work-dir", type=Path, default=Path(".demo-infra/experiments"))
    parser.add_argument("--lab-root", type=Path)
    parser.add_argument("--internal-lab-host")
    parser.add_argument("--internal-lab-user")
    parser.add_argument("--env", action="append", default=[])
    parser.add_argument("--notify-hook", default="")
    parser.add_argument("--telegram-env", type=Path, default=Path("~/.config/ckc-lab/telegram.env"))
    images = parser.add_mutually_exclusive_group()
    images.add_argument("--build-images", dest="build_images", action="store_true")
    images.add_argument("--skip-build-images", dest="build_images", action="store_false")
    parser.set_defaults(build_images=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[4]
    experiment = args.experiment.resolve()
    if not experiment.is_file():
        raise FileNotFoundError(f"Experiment was not found: {experiment}")
    lab_root = args.lab_root or Path("/opt/ckc-lab")
    internal_lab_host = args.internal_lab_host or "optilab"
    internal_lab_user = args.internal_lab_user or "ckc-lab"
    lab_config_path = repo_root / ".demo-infra/internal-lab/lab.yaml"
    if args.environment == "internal-lab" and lab_config_path.is_file():
        lab_config = yaml.safe_load(lab_config_path.read_text(encoding="utf-8")) or {}
        runtime = lab_config.get("runtime") or {}
        nodes = lab_config.get("nodes") or {}
        controller = next((node for node in nodes.values() if "controller" in (node.get("roles") or [])), {})
        lab_root = args.lab_root or Path(runtime.get("root") or "/opt/ckc-lab")
        internal_lab_user = args.internal_lab_user or str(runtime.get("user") or "ckc-lab")
        internal_lab_host = args.internal_lab_host or str(controller.get("host") or "optilab")
        if internal_lab_host == "local":
            internal_lab_host = "127.0.0.1"
    request = RunRequest(
        repo_root=repo_root,
        experiment=experiment,
        environment=args.environment,
        work_dir=args.work_dir.resolve(),
        lab_root=lab_root,
        env=tuple(args.env),
        build_images=bool(args.build_images),
        internal_lab_host=internal_lab_host,
        internal_lab_user=internal_lab_user,
        notify_hook=args.notify_hook,
        telegram_env=args.telegram_env.expanduser().resolve(),
    )
    return run(request)
