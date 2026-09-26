#!/usr/bin/env python3

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
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
ENV_OVERRIDE_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*=.*$")


@dataclass(frozen=True)
class Node:
    name: str
    host: str
    admin_user: str
    lab_address: str
    k3s_name: str
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
    telegram_env: Path
    performance_cpu_khz: int
    application_link: str
    nodes: tuple[Node, ...]

    @property
    def infra(self) -> Node:
        return next(node for node in self.nodes if "controller" in node.roles)

    @property
    def application(self) -> Node:
        return next(node for node in self.nodes if "application" in node.roles)

    @property
    def split(self) -> bool:
        return self.topology == "split-application"


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
    if topology not in {"single-host", "split-application"}:
        raise ValueError(f"topology must be single-host or split-application; got {topology!r}")
    runtime = _mapping(root.get("runtime"), "runtime")
    runtime_user = _non_empty(runtime.get("user"), "runtime.user")
    if not USER_PATTERN.fullmatch(runtime_user):
        raise ValueError(f"runtime.user is not a valid Linux user: {runtime_user!r}")
    lab_root = Path(_non_empty(runtime.get("root"), "runtime.root"))
    if not lab_root.is_absolute() or lab_root == Path("/") or not PATH_PATTERN.fullmatch(str(lab_root)):
        raise ValueError("runtime.root must be a non-root absolute path")
    operator = _mapping(root.get("operator"), "operator")
    public_key = Path(_non_empty(operator.get("public_key"), "operator.public_key")).expanduser()
    telegram_env = Path(str(operator.get("telegram_env") or "~/.config/ckc-lab/telegram.env")).expanduser()
    performance_cpu_khz = int(runtime.get("performance_cpu_khz", 2_000_000))
    if performance_cpu_khz < 0:
        raise ValueError("runtime.performance_cpu_khz must be zero or a positive integer")
    network = root.get("network") or {}
    network = _mapping(network, "network")
    application_link = str(network.get("application_link") or ("lan" if topology == "split-application" else "local"))
    if application_link not in {"local", "direct", "lan"}:
        raise ValueError("network.application_link must be local, direct, or lan")
    if topology == "single-host" and application_link != "local":
        raise ValueError("single-host topology requires network.application_link=local")
    if topology == "split-application" and application_link == "local":
        raise ValueError("split-application topology requires network.application_link=direct or lan")
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
        default_k3s_name = socket.gethostname() if host == "local" else host
        k3s_name = _non_empty(node.get("k3s_name") or default_k3s_name, f"nodes.{name}.k3s_name")
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", k3s_name):
            raise ValueError(f"nodes.{name}.k3s_name is not a valid Kubernetes node name: {k3s_name!r}")
        nodes.append(Node(str(name), host, admin_user, lab_address, k3s_name, tuple(str(role) for role in roles_value)))
    controllers = [node for node in nodes if "controller" in node.roles]
    applications = [node for node in nodes if "application" in node.roles]
    if len(controllers) != 1 or len(applications) != 1:
        raise ValueError("lab topology requires exactly one controller and one application role")
    if (
        len({node.name for node in nodes}) != len(nodes)
        or len({node.lab_address for node in nodes}) != len(nodes)
        or len({node.k3s_name for node in nodes}) != len(nodes)
    ):
        raise ValueError("lab nodes must have unique names, k3s_name values, and lab_address values")
    controller = controllers[0]
    application = applications[0]
    if topology == "single-host":
        if len(nodes) != 1 or controller != application:
            raise ValueError("single-host topology requires one node with controller and application roles")
        missing = {"controller", "k3s-server", "services", "application"} - set(controller.roles)
        if missing:
            raise ValueError(f"single-host controller is missing roles: {sorted(missing)}")
    else:
        if len(nodes) != 2 or controller == application:
            raise ValueError("split-application topology requires separate controller and application nodes")
        controller_missing = {"controller", "k3s-server", "services"} - set(controller.roles)
        application_missing = {"application", "k3s-agent"} - set(application.roles)
        if controller_missing or application_missing:
            raise ValueError(
                "split-application roles are incomplete: "
                f"controller missing {sorted(controller_missing)}, application missing {sorted(application_missing)}"
            )
        if {"application", "k3s-agent"} & set(controller.roles):
            raise ValueError("split-application controller cannot have application or k3s-agent roles")
        if {"controller", "k3s-server", "services"} & set(application.roles):
            raise ValueError("split-application worker may only host the application k3s agent")
    return LabConfig(
        path, topology, runtime_user, lab_root, public_key, telegram_env,
        performance_cpu_khz, application_link, tuple(nodes),
    )


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
    telegram_env = args.telegram_env
    performance_cpu_khz = args.performance_cpu_khz
    topology = args.topology
    application_host = args.application_host
    application_admin_user = args.application_admin_user
    application_lab_address = args.application_lab_address
    application_k3s_name = args.application_k3s_name
    application_link = args.application_link
    if interactive:
        print("Internal lab setup\n")
        topology_choice = prompt("Topology (single-host/split-application)", topology)
        topology = topology_choice
        location = prompt("Run the lab on this machine? (yes/no)", "yes").lower()
        host = "local" if location in {"y", "yes"} else prompt("Lab SSH host", socket.gethostname())
        admin_user = prompt("Administrative SSH user", getpass.getuser())
        lab_address = prompt("Address used by lab services", default_lab_address(host))
        if topology == "split-application":
            application_host = prompt("Application worker SSH host", application_host or "optilab2")
            application_admin_user = prompt("Application worker administrative SSH user", application_admin_user or admin_user)
            application_lab_address = prompt(
                "Application worker address reachable from the controller",
                application_lab_address or default_lab_address(application_host),
            )
            application_k3s_name = prompt("Application worker Kubernetes node name", application_k3s_name or application_host)
            application_link = prompt("Application worker connection (direct/lan)", application_link)
        runtime_user = prompt("Lab runtime user", runtime_user)
        lab_root = prompt("Installed lab root", lab_root)
        public_key = prompt("Operator SSH public key", public_key)
        telegram_env = prompt("Optional Telegram environment file", telegram_env)
        performance_cpu_khz = int(prompt("Experiment CPU frequency in kHz (0 disables tuning)", str(performance_cpu_khz)))
    if not lab_address:
        lab_address = default_lab_address(host)
    controller_roles = ["controller", "k3s-server", "services"]
    if topology == "single-host":
        controller_roles.append("application")
    nodes: dict[str, Any] = {
        "infra": {
            "host": host,
            "admin_user": admin_user,
            "lab_address": lab_address,
            "roles": controller_roles,
        }
    }
    if topology == "split-application":
        if not application_lab_address:
            application_lab_address = default_lab_address(application_host)
        nodes["application"] = {
            "host": application_host,
            "admin_user": application_admin_user,
            "lab_address": application_lab_address,
            "k3s_name": application_k3s_name or application_host,
            "roles": ["k3s-agent", "application"],
        }
    document = {
        "version": 1,
        "topology": topology,
        "operator": {
            "public_key": str(Path(public_key).expanduser()),
            "telegram_env": str(Path(telegram_env).expanduser()),
        },
        "runtime": {
            "user": runtime_user,
            "root": lab_root,
            "performance_cpu_khz": performance_cpu_khz,
        },
        "network": {
            "application_link": "local" if topology == "single-host" else application_link,
        },
        "nodes": nodes,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    config = load_config(path)
    print(f"Lab configuration written: {path}")
    print_plan(config)
    return 0


def print_plan(config: LabConfig) -> None:
    print("\nResolved lab plan:")
    print(f"  topology:     {config.topology}")
    for node in config.nodes:
        print(f"  node:         {node.name} ({node.host}, {node.lab_address}, k3s={node.k3s_name})")
        print(f"  roles:        {', '.join(node.roles)}")
    print(f"  runtime user: {config.runtime_user}")
    print(f"  lab root:     {config.lab_root}")
    frequency = "disabled" if config.performance_cpu_khz == 0 else f"{config.performance_cpu_khz} kHz"
    print(f"  experiment CPU: {frequency}")
    if config.split:
        print(f"  application link: {config.application_link}")


def run(command: Sequence[str], *, dry_run: bool = False) -> None:
    print("+ " + shlex.join(str(value) for value in command))
    if not dry_run:
        subprocess.run([str(value) for value in command], check=True)


def capture(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(value) for value in command],
        check=check,
        text=True,
        capture_output=True,
    )


def bootstrap_arguments(
    config: LabConfig,
    node: Node,
    public_key: Path,
    *,
    node_role: str,
    token_file: Path | None = None,
) -> list[str]:
    arguments = [
        "--node-role", node_role,
        "--node-name", node.k3s_name,
        "--server-address", config.infra.lab_address,
    ]
    if config.split:
        peer = config.application if node.name == config.infra.name else config.infra
        arguments.extend(["--network-peer-address", peer.lab_address])
    if token_file is not None:
        arguments.extend(["--k3s-token-file", str(token_file)])
    arguments.extend([
        "--runtime-user", config.runtime_user,
        "--lab-root", str(config.lab_root),
        "--node-ip", node.lab_address,
        "--authorized-key-file", str(public_key),
        "--performance-cpu-khz", str(config.performance_cpu_khz),
    ])
    return arguments


def bootstrap_node(
    config: LabConfig,
    node: Node,
    public_key: Path,
    *,
    node_role: str,
    token_file: Path | None,
    dry_run: bool,
) -> None:
    arguments = bootstrap_arguments(config, node, public_key, node_role=node_role, token_file=token_file)
    if node.local:
        run(["sudo", str(BOOTSTRAP_SCRIPT), *arguments], dry_run=dry_run)
        return
    target = f"{node.admin_user}@{node.ssh_host}"
    remote_dir = f"/tmp/ckc-lab-bootstrap-{os.getuid()}"
    remote_script = f"{remote_dir}/bootstrap-host.sh"
    remote_key = f"{remote_dir}/operator.pub"
    run(["ssh", target, "mkdir", "-p", remote_dir], dry_run=dry_run)
    run(["scp", str(BOOTSTRAP_SCRIPT), f"{target}:{remote_script}"], dry_run=dry_run)
    run(["scp", str(public_key), f"{target}:{remote_key}"], dry_run=dry_run)
    remote_token: Path | None = None
    if token_file is not None:
        remote_token = Path(f"{remote_dir}/k3s-token")
        run(["scp", str(token_file), f"{target}:{remote_token}"], dry_run=dry_run)
    remote_arguments = bootstrap_arguments(
        config, node, Path(remote_key), node_role=node_role, token_file=remote_token,
    )
    privileged_command = [remote_script, *remote_arguments] if node.admin_user == "root" else ["sudo", remote_script, *remote_arguments]
    ssh_command = ["ssh", target, " ".join(shlex.quote(value) for value in privileged_command)]
    if node.admin_user != "root":
        ssh_command.insert(1, "-t")
    run(ssh_command, dry_run=dry_run)
    run(["ssh", target, "rm", "-r", "--", remote_dir], dry_run=dry_run)


def fetch_controller_bootstrap_material(config: LabConfig, directory: Path, *, dry_run: bool) -> tuple[Path, Path]:
    token = directory / "k3s-token"
    controller_key = directory / "controller-runtime.pub"
    controller = runtime_target(config)
    sources = (
        (config.lab_root / "config/k3s-join-token", token),
        (Path(f"/var/lib/{config.runtime_user}/.ssh/id_ed25519.pub"), controller_key),
    )
    for remote_path, local_path in sources:
        command = ["scp", f"{controller}:{remote_path}", str(local_path)]
        run(command, dry_run=dry_run)
        if dry_run:
            local_path.write_text("dry-run\n", encoding="utf-8")
            os.chmod(local_path, 0o600)
    return token, controller_key


def combined_authorized_keys(operator_key: Path, controller_key: Path, directory: Path) -> Path:
    path = directory / "worker-authorized-keys"
    values = [operator_key.read_text(encoding="utf-8").strip(), controller_key.read_text(encoding="utf-8").strip()]
    path.write_text("\n".join(value for value in values if value) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def bootstrap_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    public_key = config.public_key.resolve()
    if not public_key.is_file():
        raise FileNotFoundError(f"Operator public key was not found: {public_key}")
    print_plan(config)
    bootstrap_node(config, config.infra, public_key, node_role="server", token_file=None, dry_run=args.dry_run)
    if config.split:
        with tempfile.TemporaryDirectory(prefix="ckc-lab-bootstrap-") as temporary:
            directory = Path(temporary)
            token, controller_key = fetch_controller_bootstrap_material(config, directory, dry_run=args.dry_run)
            worker_keys = combined_authorized_keys(public_key, controller_key, directory)
            bootstrap_node(
                config, config.application, worker_keys,
                node_role="agent", token_file=token, dry_run=args.dry_run,
            )
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
        "LAB_TOPOLOGY": config.topology,
        "LAB_APPLICATION_LINK": config.application_link,
        "LAB_APPLICATION_HOST": config.application.lab_address,
        "LAB_APPLICATION_SSH_HOST": config.application.ssh_host,
        "LAB_APPLICATION_TARGET": "" if not config.split else f"{config.runtime_user}@{config.application.lab_address}",
        "LAB_APPLICATION_NODE_SELECTOR": "" if not config.split else "ckc.dev/role=application",
        "LAB_CONTROLLER_NODE_SELECTOR": "" if not config.split else "ckc.dev/role=controller",
    }
    path.write_text("".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()), encoding="utf-8")
    return path


def runtime_target(config: LabConfig) -> str:
    node = config.infra
    return f"{config.runtime_user}@{node.ssh_host}"


def install_telegram_environment(config: LabConfig, *, dry_run: bool) -> None:
    source = config.telegram_env.resolve()
    if not source.is_file():
        print(f"Telegram notifications are not configured; file not found: {source}")
        return
    target = runtime_target(config)
    destination = config.lab_root / "config/telegram.env"
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    if not dry_run:
        remote_digest = capture([
            "ssh", "-o", "BatchMode=yes", target,
            f"sha256sum '{destination}' 2>/dev/null | awk '{{print $1}}'",
        ], check=False).stdout.strip()
        if remote_digest == digest:
            print("Telegram environment is already current; no secret transferred.")
            return
    command = [
        "ssh", target,
        f"umask 077; cat > '{destination}.tmp' && mv '{destination}.tmp' '{destination}'",
    ]
    print("+ " + shlex.join(command) + " < [telegram environment redacted]")
    if not dry_run:
        subprocess.run(command, input=source.read_text(encoding="utf-8"), text=True, check=True)


def up_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    environment_path = write_compatibility_environment(config)
    command = [str(UPDATE_SCRIPT)]
    if args.force_rebuild:
        command.append("--force-rebuild")
    environment = {**os.environ, "CKC_LAB_ENV": str(environment_path)}
    print("+ " + shlex.join(command))
    if not args.dry_run:
        subprocess.run(command, check=True, env=environment)
    install_telegram_environment(config, dry_run=args.dry_run)
    return 0


def resolve_repository_experiment(value: Path) -> Path:
    catalog = (REPO_ROOT / "demo/infra/experiments").resolve()
    candidate = value.resolve()
    if not candidate.is_file():
        candidate = (catalog / value).resolve()
    if candidate.suffix != ".yaml" or not candidate.is_file() or candidate.parent != catalog:
        raise ValueError(f"experiment must be a YAML file from {catalog}: {value}")
    return candidate


def remote_json(config: LabConfig, path: Path) -> dict[str, Any]:
    result = capture([
        "ssh", "-o", "BatchMode=yes", runtime_target(config),
        f"cat '{path}' 2>/dev/null || true",
    ], check=False)
    try:
        value = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        return {"error": f"{path.name} is not valid JSON"}
    return value if isinstance(value, dict) else {}


def managed_unit_properties(config: LabConfig) -> tuple[dict[str, str], dict[str, Any], dict[str, Any]]:
    target = runtime_target(config)
    result = capture([
        "ssh", "-o", "BatchMode=yes", target,
        "systemctl --user show ckc-experiment.service "
        "--property=ActiveState --property=SubState --property=Result "
        "--property=ExecMainStatus --property=ExecMainStartTimestamp --property=ExecMainExitTimestamp",
    ])
    properties = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    request = remote_json(config, config.lab_root / "state/experiment/request.json")
    progress = remote_json(config, config.lab_root / "state/experiment/progress.json")
    return properties, request, progress


def parse_utc(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def compact_duration(seconds: float) -> str:
    value = max(0, int(seconds))
    hours, remainder = divmod(value, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes:02d}m" if hours else f"{minutes}m {secs:02d}s"


def print_managed_status(config: LabConfig, *, json_output: bool = False) -> int:
    properties, request, progress = managed_unit_properties(config)
    systemd_result = properties.get("Result", "unknown")
    result = "cancelled" if progress.get("status") == "cancelled" else systemd_result
    document = {
        "host": config.infra.host,
        "runtime_user": config.runtime_user,
        "experiment": request.get("experiment"),
        "requested_at": request.get("requested_at"),
        "env": request.get("env", []),
        "active_state": properties.get("ActiveState", "unknown"),
        "sub_state": properties.get("SubState", "unknown"),
        "result": result,
        "systemd_result": systemd_result,
        "exit_code": properties.get("ExecMainStatus", ""),
        "started_at": properties.get("ExecMainStartTimestamp", ""),
        "ended_at": properties.get("ExecMainExitTimestamp", ""),
        "progress": progress,
    }
    if json_output:
        print(json.dumps(document, indent=2))
        return 0
    print(f"experiment:   {document['experiment'] or 'none'}")
    print(f"environment:  internal-lab on {document['host']} as {document['runtime_user']}")
    print(f"state:        {document['active_state']} ({document['sub_state']})")
    print(f"result:       {document['result']}")
    if progress.get("label"):
        print(f"step:         {progress['label']}")
    target = progress.get("target")
    if isinstance(target, dict) and target:
        position = f"{target.get('index', '?')}/{target.get('total', '?')}"
        print(f"target:       {position} — {target.get('name') or 'unknown'}")
        target_started = parse_utc(target.get("started_at"))
        if target_started:
            target_end = (
                parse_utc(progress.get("ended_at"))
                if progress.get("status") != "active"
                else None
            ) or datetime.now(timezone.utc)
            elapsed = (target_end - target_started).total_seconds()
            print(f"elapsed:      {compact_duration(elapsed)}")
            expected = target.get("expected_duration_seconds")
            if progress.get("step") == "running_target" and expected is not None:
                print(f"target ETA:   {compact_duration(max(0, float(expected) - elapsed))}")
    elif (progress_started := parse_utc(progress.get("started_at"))):
        progress_end = parse_utc(progress.get("ended_at")) or datetime.now(timezone.utc)
        print(f"elapsed:      {compact_duration((progress_end - progress_started).total_seconds())}")
    details = progress.get("details")
    if progress.get("step") == "draining" and isinstance(details, dict):
        lag = details.get("lag")
        print(f"consumer lag: {'unknown' if lag is None else lag}")
    if document["exit_code"]:
        print(f"exit code:    {document['exit_code']}")
    if document["started_at"]:
        print(f"started:      {document['started_at']}")
    if document["ended_at"]:
        print(f"ended:        {document['ended_at']}")
    return 0


def experiment_start_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    experiment = resolve_repository_experiment(args.experiment)
    invalid_env = next((value for value in args.env if not ENV_OVERRIDE_PATTERN.fullmatch(value)), None)
    if invalid_env is not None:
        raise ValueError(f"experiment environment override must use UPPER_SNAKE_CASE=value: {invalid_env!r}")
    if not args.no_update:
        up_command(argparse.Namespace(config=args.config, dry_run=args.dry_run, force_rebuild=False))
    target = runtime_target(config)
    active = capture(["ssh", "-o", "BatchMode=yes", target, "systemctl --user is-active --quiet ckc-experiment.service"], check=False)
    if active.returncode == 0:
        raise ValueError("a managed experiment is already active; inspect it with 'lab.sh experiment status'")
    request = {
        "version": 1,
        "experiment": experiment.name,
        "environment": "internal-lab",
        "host": config.infra.host,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "requested_by": getpass.getuser(),
        "env": list(args.env),
        "skip_archives": bool(args.skip_archives),
    }
    local_request = config.path.parent / "experiment-request.json"
    local_request.write_text(json.dumps(request, indent=2) + "\n", encoding="utf-8")
    os.chmod(local_request, 0o600)
    remote_request = config.lab_root / "state/experiment/request.json"
    run(["scp", str(local_request), f"{target}:{remote_request}.tmp"], dry_run=args.dry_run)
    run([
        "ssh", target,
        f"set -e; chmod 600 '{remote_request}.tmp'; mv '{remote_request}.tmp' '{remote_request}'; "
        f"rm -f '{config.lab_root}/state/experiment/progress.json'; "
        "systemctl --user reset-failed ckc-experiment.service >/dev/null 2>&1 || true; "
        "systemctl --user start ckc-experiment.service",
    ], dry_run=args.dry_run)
    if args.dry_run:
        return 0
    return print_managed_status(config)


def experiment_status_command(args: argparse.Namespace) -> int:
    return print_managed_status(load_config(args.config.resolve()), json_output=args.json)


def experiment_logs_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    target = runtime_target(config)
    command = ["ssh", "-o", "BatchMode=yes", target, "tail"]
    if args.follow:
        command.append("-f")
    command.extend(["-n", str(args.lines), str(config.lab_root / "logs/managed-experiment.log")])
    run(command)
    return 0


def experiment_stop_command(args: argparse.Namespace) -> int:
    config = load_config(args.config.resolve())
    run(["ssh", "-o", "BatchMode=yes", runtime_target(config), "systemctl", "--user", "stop", "ckc-experiment.service"])
    return print_managed_status(config)


def setup_command(args: argparse.Namespace) -> int:
    if not args.config.exists():
        init_args = argparse.Namespace(
            config=args.config, force=False, non_interactive=False, host="local",
            admin_user=getpass.getuser(), lab_address="", runtime_user="ckc-lab",
            lab_root="/opt/ckc-lab", public_key=str(default_public_key()),
            telegram_env=str(Path.home() / ".config/ckc-lab/telegram.env"),
            performance_cpu_khz=2_000_000,
            topology="single-host", application_host="", application_admin_user=getpass.getuser(),
            application_lab_address="", application_k3s_name="", application_link="lan",
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
    initialize.add_argument("--telegram-env", default=str(Path.home() / ".config/ckc-lab/telegram.env"))
    initialize.add_argument("--performance-cpu-khz", type=int, default=2_000_000)
    initialize.add_argument("--topology", choices=("single-host", "split-application"), default="single-host")
    initialize.add_argument("--application-host", default="")
    initialize.add_argument("--application-admin-user", default=getpass.getuser())
    initialize.add_argument("--application-lab-address", default="")
    initialize.add_argument("--application-k3s-name", default="")
    initialize.add_argument("--application-link", choices=("direct", "lan"), default="lan")
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
    experiment = commands.add_parser("experiment", help="Manage the detached internal-lab experiment service.")
    experiment_commands = experiment.add_subparsers(dest="experiment_command", required=True)
    start = experiment_commands.add_parser("start", help="Update the lab and start one detached experiment.")
    start.add_argument("experiment", type=Path)
    start.add_argument("--env", action="append", default=[])
    start.add_argument("--skip-archives", action="store_true")
    start.add_argument("--no-update", action="store_true")
    start.add_argument("--dry-run", action="store_true")
    start.set_defaults(handler=experiment_start_command)
    status = experiment_commands.add_parser("status", help="Show the current or most recent experiment state.")
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=experiment_status_command)
    logs = experiment_commands.add_parser("logs", help="Read the managed experiment service log.")
    logs.add_argument("-n", "--lines", type=int, default=100)
    logs.add_argument("-f", "--follow", action="store_true")
    logs.set_defaults(handler=experiment_logs_command)
    stop = experiment_commands.add_parser("stop", help="Gracefully stop the active experiment.")
    stop.set_defaults(handler=experiment_stop_command)
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
