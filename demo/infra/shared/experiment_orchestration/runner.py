from __future__ import annotations

import argparse
import os
import shlex
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

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
        return command


class InternalLabAdapter:
    name = "internal-lab"

    def command(self, request: RunRequest, experiment: ResolvedExperiment) -> list[str]:
        helper = request.repo_root / "demo/infra/internal-lab/assets/helpers/run-experiment.py"
        arguments = [
            "python3", str(helper), str(request.experiment),
            "--lab-root", str(request.lab_root),
        ]
        for value in request.env:
            arguments.extend(["--env", value])
        if os.geteuid() == 0:
            return arguments
        remote = " ".join(shlex.quote(value) for value in arguments)
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", f"root@{request.internal_lab_host}", remote]


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
        None,
        environment=request.environment,
    )
    if resolved.legacy:
        raise ValueError("The shared experiment runner accepts only schema_version: 1 experiments")
    return execute(adapter.command(request, resolved))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one canonical CKC experiment in a configured environment.")
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--environment", required=True, choices=sorted(ADAPTERS))
    parser.add_argument("--work-dir", type=Path, default=Path(".demo-infra/experiments"))
    parser.add_argument("--lab-root", type=Path, default=Path("/opt/ckc-lab"))
    parser.add_argument("--internal-lab-host", default="optilab")
    parser.add_argument("--env", action="append", default=[])
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
    request = RunRequest(
        repo_root=repo_root,
        experiment=experiment,
        environment=args.environment,
        work_dir=args.work_dir.resolve(),
        lab_root=args.lab_root,
        env=tuple(args.env),
        build_images=bool(args.build_images),
        internal_lab_host=args.internal_lab_host,
    )
    return run(request)
