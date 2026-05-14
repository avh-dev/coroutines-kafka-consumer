#!/usr/bin/env python3
"""Wake the internal lab host using Wake-on-LAN.

This helper intentionally uses only the Python standard library so it works on
Windows, macOS, Linux, and Git Bash without installing wakeonlan/netcat tools.
"""

from __future__ import annotations

import argparse
import re
import socket
import sys
import time
from pathlib import Path


DEFAULT_BROADCAST = "255.255.255.255"
DEFAULT_WOL_PORT = 9
DEFAULT_WAIT_PORT = 22


def repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "settings.gradle.kts").exists():
            return parent
    raise RuntimeError("Could not find repository root from helper path.")


def default_state_file() -> Path:
    return repo_root() / ".demo-infra" / "internal-lab" / "lab.env"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def normalize_mac(mac: str) -> bytes:
    cleaned = re.sub(r"[^0-9A-Fa-f]", "", mac)
    if len(cleaned) != 12 or not re.fullmatch(r"[0-9A-Fa-f]{12}", cleaned):
        raise ValueError(
            "MAC address must contain 12 hex digits, for example "
            "aa:bb:cc:dd:ee:ff"
        )
    return bytes.fromhex(cleaned)


def send_magic_packet(mac: bytes, broadcast: str, port: int) -> None:
    packet = b"\xff" * 6 + mac * 16
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(packet, (broadcast, port))


def wait_for_port(host: str, port: int, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except OSError:
            time.sleep(2.0)
    return False


def parse_args() -> argparse.Namespace:
    default_state = default_state_file()
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument(
        "--state-file",
        default=str(default_state),
        help="Path to lab.env. Defaults to .demo-infra/internal-lab/lab.env.",
    )
    pre_args, _ = pre_parser.parse_known_args()
    env = read_env_file(Path(pre_args.state_file))

    parser = argparse.ArgumentParser(
        description="Wake the CKC internal lab host with a Wake-on-LAN magic packet.",
        parents=[pre_parser],
    )
    parser.add_argument(
        "mac",
        nargs="?",
        default=env.get("LAB_HOST_MAC"),
        help="Lab host MAC address. Defaults to LAB_HOST_MAC from lab.env.",
    )
    parser.add_argument(
        "--host",
        default=env.get("LAB_HOST_IP"),
        help="Lab host IP or DNS name used only for optional readiness waiting.",
    )
    parser.add_argument(
        "--broadcast",
        default=env.get("LAB_WAKE_BROADCAST", DEFAULT_BROADCAST),
        help=f"Broadcast address for the magic packet. Default: {DEFAULT_BROADCAST}",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(env.get("LAB_WAKE_PORT", DEFAULT_WOL_PORT)),
        help=f"UDP port for the magic packet. Default: {DEFAULT_WOL_PORT}",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=int(env.get("LAB_WAKE_REPEAT", "3")),
        help="How many magic packets to send. Default: 3",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=float(env.get("LAB_WAKE_DELAY_SECONDS", "1")),
        help="Delay between repeated magic packets. Default: 1",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=float(env.get("LAB_WAKE_WAIT_SECONDS", "0")),
        help="Wait for the lab host after sending packets. Default: 0",
    )
    parser.add_argument(
        "--wait-port",
        type=int,
        default=int(env.get("LAB_WAKE_WAIT_PORT", DEFAULT_WAIT_PORT)),
        help=f"TCP port used for readiness waiting. Default: {DEFAULT_WAIT_PORT}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.mac:
        print(
            "MAC address is required. Pass it as an argument or set LAB_HOST_MAC "
            "in .demo-infra/internal-lab/lab.env.",
            file=sys.stderr,
        )
        return 2
    if args.repeat < 1:
        print("--repeat must be >= 1.", file=sys.stderr)
        return 2
    if args.delay_seconds < 0:
        print("--delay-seconds must be >= 0.", file=sys.stderr)
        return 2
    if args.wait_seconds < 0:
        print("--wait-seconds must be >= 0.", file=sys.stderr)
        return 2

    try:
        mac = normalize_mac(args.mac)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    for attempt in range(1, args.repeat + 1):
        send_magic_packet(mac, args.broadcast, args.port)
        print(
            f"Sent Wake-on-LAN packet {attempt}/{args.repeat} "
            f"to {args.mac} via {args.broadcast}:{args.port}."
        )
        if attempt < args.repeat:
            time.sleep(args.delay_seconds)

    if args.wait_seconds == 0:
        return 0
    if not args.host:
        print("--host is required when --wait-seconds is greater than 0.", file=sys.stderr)
        return 2

    print(
        f"Waiting up to {args.wait_seconds:g}s for {args.host}:{args.wait_port}..."
    )
    if wait_for_port(args.host, args.wait_port, args.wait_seconds):
        print(f"Lab host is reachable at {args.host}:{args.wait_port}.")
        return 0

    print(
        f"Timed out waiting for {args.host}:{args.wait_port}. "
        "The wake packet was still sent.",
        file=sys.stderr,
    )
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
