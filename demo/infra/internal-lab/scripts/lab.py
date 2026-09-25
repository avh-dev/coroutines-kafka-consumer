#!/usr/bin/env python3

from __future__ import annotations

import argparse
import getpass
import os
import re
import shlex
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

try:
    import yaml
except ImportError as error:
    raise SystemExit("PyYAML is required on the operator machine (apt install python3-yaml).") from error


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[3]
DEFAULT_CONFIG = REPO_ROOT / ".demo-infra/internal-lab/lab.yaml"
BOOTSTRAP_SCRIPT = SCRIPT_DIR / "bootstrap-host.sh"
UPDATE_SCRIPT = SCRIPT_DIR / "update-lab.sh"
USER_PATTERN = re.compile(r"^[a-z_][a-z0-9_-]*[$]?$")
ADDRESS_PATTERN = re.compile(r"^[A-Za-z0-9._:-]+$")
PATH_PATTERN = re.compile(r"^/[A-Za-z0-9._/-]+$")


@dataclass(frozen=True)
class Node:
    name: str
    host: str
    admin_user: str
    lab_address: str
    roles: tuple[str, ...]

    @property
    def local(self) -> bool:
        return self.host == "local"

    @property
    def ssh_host(self) -> str:
        return "127.0.0.1" if self.local else self.host


@dataclass(frozen=True)
class LabConfig:
    path: Path
    topology: str
    runtime_user: str
    lab_root: Path
    public_key: Path
    nodes: tuple[Node, ...]

    @property
    def infra(self) -> Node:
        return next(node for node in self.nodes if "controller" in node.roles)


def _mapping(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def _non_empty(value: Any, context: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{context} is required")
    return text


def load_config(path: Path) -> LabConfig:
    if not path.is_file():
        raise FileNotFoundError(f"Lab configuration was not found: {path}. Run 'lab.sh init' first.")
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    root = _mapping(document, "lab configuration")
    if root.get("version") != 1:
        raise ValueError("lab configuration version must be 1")
    topology = _non_empty(root.get("topology"), "topology")
    if topology != "single-host":
        raise ValueError(f"INFRA-210 supports topology=single-host; got {topology!r}")
    runtime = _mapping(root.get("runtime"), "runtime")
    runtime_user = _non_empty(runtime.get("user"), "runtime.user")
    if not USER_PATTERN.fullmatch(runtime_user):
        raise ValueError(f"runtime.user is not a valid Linux user: {runtime_user!r}")
    lab_root = Path(_non_empty(runtime.get("root"), "runtime.root"))
    if not lab_root.is_absolute() or lab_root == Path("/") or not PATH_PATTERN.fullmatch(str(lab_root)):
        raise ValueError("runtime.root must be a non-root absolute path")
    operator = _mapping(root.get("operator"), "operator")
    public_key = Path(_non_empty(operator.get("public_key"), "operator.public_key")).expanduser()
    nodes_value = _mapping(root.get("nodes"), "nodes")
    nodes: list[Node] = []
    for name, raw_node in nodes_value.items():
        node = _mapping(raw_node, f"nodes.{name}")
        host = _non_empty(node.get("host"), f"nodes.{name}.host")
        admin_user = _non_empty(node.get("admin_user"), f"nodes.{name}.admin_user")
        if not USER_PATTERN.fullmatch(admin_user):
            raise ValueError(f"nodes.{name}.admin_user is not a valid Linux user: {admin_user!r}")
        lab_address = _non_empty(node.get("lab_address"), f"nodes.{name}.lab_address")
        if not ADDRESS_PATTERN.fullmatch(host) or not ADDRESS_PATTERN.fullmatch(lab_address):
            raise ValueError(f"nodes.{name} host and lab_address must be host names or IP addresses")
        roles_value = node.get("roles")
        if not isinstance(roles_value, list) or not roles_value:
            raise ValueError(f"nodes.{name}.roles must be a non-empty list")
        nodes.append(Node(str(name), host, admin_user, lab_address, tuple(str(role) for role in roles_value)))
    controllers = [node for node in nodes if "controller" in node.roles]
    if len(nodes) != 1 or len(controllers) != 1:
        raise ValueError("single-host topology requires exactly one controller node")
    required_roles = {"controller", "k3s-server", "services", "application"}
    missing = required_roles - set(controllers[0].roles)
    if missing:
        raise ValueError(f"single-host controller is missing roles: {sorted(missing)}")
    return LabConfig(path, topology, runtime_user, lab_root, public_key, tuple(nodes))


def default_public_key() -> Path:
    ssh_dir = Path.home() / ".ssh"
    for name in ("id_ed25519.pub", "id_ecdsa.pub", "id_rsa.pub"):
        candidate = ssh_dir / name
        if candidate.is_file():
            return candidate
    return ssh_dir / "id_ed25519.pub"


def default_lab_address(host: str) -> str:
    lookup = socket.gethostname() if host == "local" else host
    try:
        addresses = socket.getaddrinfo(lookup, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        return ""
    values = [entry[4][0] for entry in addresses if entry[4][0] != "127.0.0.1"]
    return values[0] if values else "127.0.0.1"


def prompt(label: str, default: str) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default


def init_command(args: argparse.Namespace) -> int:
    path = args.config.resolve()
    if path.exists() and not args.force:
        raise FileExistsError(f"Lab configuration already exists: {path}. Use --force to replace it.")
    interactive = not args.non_interactive
    host = args.host
    admin_user = args.admin_user
    lab_address = args.lab_address
    runtime_user = args.runtime_user
    lab_root = args.lab_root
    public_key = args.public_key
    if interactive:
        print("Internal lab setup\n")
        print("INFRA-210 configures the single-host topology. Split SUT support follows in a separate task.")
        location = prompt("Run the lab on this machine? (yes/no)", "yes").lower()
        host = "local" if location in {"y", "yes"} else prompt("Lab SSH host", socket.gethostname())
        admin_user = prompt("Administrative SSH user", getpass.getuser())
        lab_address = prompt("Address used by lab services", default_lab_address(host))
        runtime_user = prompt("Lab runtime user", runtime_user)
        lab_root = prompt("Installed lab root", lab_root)
        public_key = prompt("Operator SSH public key", public_key)
    if not lab_address:
        lab_address = default_lab_address(host)
    document = {
        "version": 1,
        "topology": "single-host",
        "operator": {"public_key": str(Path(public_key).expanduser())},
        "runtime": {"user": runtime_user, "root": lab_root},
        "nodes": {
            "infra": {
                "host": host,
                "admin_user": admin_user,
                "lab_address": lab_address,
                "roles": ["controller", "k3s-server", "services", "application"],
            }
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    config = load_config(path)
    print(f"Lab configuration written: {path}")
    print_plan(config)
    return 0


def print_plan(config: LabConfig) -> None:
    node = config.infra
    print("\nResolved lab plan:")
    print(f"  topology:     {config.topology}")
    print(f"  node:         {node.name} ({node.host}, {node.lab_address})")
    print(f"  roles:        {', '.join(node.roles)}")
    print(f"  runtime user: {config.runtime_user}")
    print(f"  lab root:     {config.lab_root}")


def run(command: Sequence[str], *, dry_run: bool = False) -> None:
    print("+ " + shlex.join(str(value) for value in command))
    if not dry_run:
        subprocess.run([str(value) for value in command], check=True)


def bootstrap_arguments(config: LabConfig, public_key: Path) -> list[str]:
    node = config.infra
    return [
        "--runtime-user", config.runtime_user,
        "--lab-root", str(config.lab_root),
        "--node-ip", node.lab_address,
        "--authorized-key-file", str(public_key),
    ]


def bootstrap_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    public_key = config.public_key.resolve()
    if not public_key.is_file():
        raise FileNotFoundError(f"Operator public key was not found: {public_key}")
    node = config.infra
    print_plan(config)
    if node.local:
        run(["sudo", str(BOOTSTRAP_SCRIPT), *bootstrap_arguments(config, public_key)], dry_run=args.dry_run)
        return 0
    target = f"{node.admin_user}@{node.ssh_host}"
    remote_dir = f"/tmp/ckc-lab-bootstrap-{os.getuid()}"
    remote_script = f"{remote_dir}/bootstrap-host.sh"
    remote_key = f"{remote_dir}/operator.pub"
    run(["ssh", target, "mkdir", "-p", remote_dir], dry_run=args.dry_run)
    run(["scp", str(BOOTSTRAP_SCRIPT), f"{target}:{remote_script}"], dry_run=args.dry_run)
    run(["scp", str(public_key), f"{target}:{remote_key}"], dry_run=args.dry_run)
    arguments = bootstrap_arguments(config, Path(remote_key))
    privileged_command = [remote_script, *arguments] if node.admin_user == "root" else ["sudo", remote_script, *arguments]
    remote_command = " ".join(shlex.quote(value) for value in privileged_command)
    ssh_command = ["ssh", target, remote_command]
    if node.admin_user != "root":
        ssh_command.insert(1, "-t")
    run(ssh_command, dry_run=args.dry_run)
    run(["ssh", target, "rm", "-r", "--", remote_dir], dry_run=args.dry_run)
    return 0


def write_compatibility_environment(config: LabConfig) -> Path:
    node = config.infra
    state_dir = config.path.parent
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "lab.env"
    values = {
        "LAB_HOST": node.lab_address,
        "LAB_SSH_HOST": node.ssh_host,
        "LAB_USER": config.runtime_user,
        "LAB_NODE_IP": node.lab_address,
        "LAB_ROOT": str(config.lab_root),
    }
    path.write_text("".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()), encoding="utf-8")
    return path


def up_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    environment_path = write_compatibility_environment(config)
    node = config.infra
    target = f"{config.runtime_user}@{node.ssh_host}"
    run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", target, "true"], dry_run=args.dry_run)
    command = [str(UPDATE_SCRIPT)]
    if args.force_rebuild:
        command.append("--force-rebuild")
    environment = {**os.environ, "CKC_LAB_ENV": str(environment_path)}
    print("+ " + shlex.join(command))
    if not args.dry_run:
        subprocess.run(command, check=True, env=environment)
    return 0


def setup_command(args: argparse.Namespace) -> int:
    if not args.config.exists():
        init_args = argparse.Namespace(
            config=args.config, force=False, non_interactive=False, host="local",
            admin_user=getpass.getuser(), lab_address="", runtime_user="ckc-lab",
            lab_root="/opt/ckc-lab", public_key=str(default_public_key()),
        )
        init_command(init_args)
    bootstrap_command(argparse.Namespace(config=args.config, dry_run=args.dry_run))
    return up_command(argparse.Namespace(config=args.config, dry_run=args.dry_run, force_rebuild=False))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Configure and deploy the CKC internal lab.")
    root.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    commands = root.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("init", help="Create a validated local lab configuration.")
    initialize.add_argument("--force", action="store_true")
    initialize.add_argument("--non-interactive", action="store_true")
    initialize.add_argument("--host", default="local")
    initialize.add_argument("--admin-user", default=getpass.getuser())
    initialize.add_argument("--lab-address", default="")
    initialize.add_argument("--runtime-user", default="ckc-lab")
    initialize.add_argument("--lab-root", default="/opt/ckc-lab")
    initialize.add_argument("--public-key", default=str(default_public_key()))
    initialize.set_defaults(handler=init_command)
    bootstrap = commands.add_parser("bootstrap", help="Prepare the host through one privileged operation.")
    bootstrap.add_argument("--dry-run", action="store_true")
    bootstrap.set_defaults(handler=bootstrap_command)
    up = commands.add_parser("up", help="Build, synchronize, and deploy the configured lab as the runtime user.")
    up.add_argument("--force-rebuild", action="store_true")
    up.add_argument("--dry-run", action="store_true")
    up.set_defaults(handler=up_command)
    setup = commands.add_parser("setup", help="Run init when needed, then bootstrap and deploy.")
    setup.add_argument("--dry-run", action="store_true")
    setup.set_defaults(handler=setup_command)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (FileNotFoundError, FileExistsError, ValueError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
