#!/usr/bin/env python3

"""Expose the active AWS runner Grafana through a specific local interface."""

from __future__ import annotations

import argparse
import json
import os
import select
import signal
import socket
import socketserver
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


TERMINAL_PHASES = {
    "COMPLETED",
    "FAILED",
    "FAILED_CLEANUP_INCOMPLETE",
    "CLEANING_UP",
}


def repository_root() -> Path:
    return Path(__file__).resolve().parents[4]


def load_session(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read AWS session {path}: {error}") from error
    if not isinstance(payload, dict):
        raise RuntimeError(f"AWS session is not an object: {path}")
    payload["_path"] = path
    return payload


def session_sort_key(session: dict[str, Any]) -> tuple[str, int]:
    path = session["_path"]
    return str(session.get("updated_at") or ""), path.stat().st_mtime_ns


def resolve_session(work_dir: Path, requested: str | None) -> dict[str, Any]:
    if requested:
        requested_path = Path(requested).expanduser()
        if requested_path.is_dir():
            requested_path = requested_path / "session.json"
        elif not requested_path.exists():
            requested_path = work_dir / requested / "session.json"
        session = load_session(requested_path)
        if not session.get("runner_instance_id"):
            raise RuntimeError(f"session has no runner instance yet: {requested_path}")
        return session

    candidates: list[dict[str, Any]] = []
    for path in work_dir.glob("s-*/session.json"):
        session = load_session(path)
        if session.get("runner_instance_id") and session.get("phase") not in TERMINAL_PHASES:
            candidates.append(session)
    if not candidates:
        raise RuntimeError(f"no active AWS session with a runner found below {work_dir}")
    return max(candidates, key=session_sort_key)


def wait_for_listener(host: str, port: int, process: subprocess.Popen[bytes], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(f"SSM port-forward exited before becoming ready (exit code {return_code})")
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"SSM port-forward did not listen on {host}:{port} within {timeout:.0f}s")


def available_bind_port(host: str, preferred_port: int, attempts: int = 100) -> int:
    for port in range(preferred_port, preferred_port + attempts):
        with socket.socket() as candidate:
            try:
                candidate.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(
        f"no free TCP port on {host} between {preferred_port} and {preferred_port + attempts - 1}"
    )


class GrafanaProxyHandler(socketserver.BaseRequestHandler):
    upstream_host = "127.0.0.1"
    upstream_port = 13002

    def handle(self) -> None:
        try:
            upstream = socket.create_connection((self.upstream_host, self.upstream_port), timeout=10)
        except OSError as error:
            print(f"Grafana tunnel upstream connection failed: {error}", file=sys.stderr)
            return
        with upstream:
            sockets = (self.request, upstream)
            while True:
                try:
                    readable, _, _ = select.select(sockets, (), (), 60)
                except OSError:
                    return
                if not readable:
                    continue
                for source in readable:
                    try:
                        data = source.recv(65536)
                        if not data:
                            return
                        destination = upstream if source is self.request else self.request
                        destination.sendall(data)
                    except OSError:
                        return


class ThreadingProxyServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=10)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Forward active AWS runner Grafana to the optilab VPN address.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=repository_root() / ".demo-infra/experiments/aws",
        help="AWS session directory (default: checkout-local session root)",
    )
    parser.add_argument("--session", help="session id, session directory, or session.json path")
    parser.add_argument(
        "--bind-address",
        default=os.environ.get("CKC_GRAFANA_BIND_ADDRESS", "192.168.6.8"),
    )
    parser.add_argument("--port", type=int, default=3002, help="VPN-facing Grafana port")
    parser.add_argument("--local-tunnel-port", type=int, default=13002)
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session = resolve_session(args.work_dir.expanduser().resolve(), args.session)
    instance_id = str(session["runner_instance_id"])
    region = str(session.get("config", {}).get("region") or session.get("region") or "eu-central-1")
    bind_port = available_bind_port(args.bind_address, args.port)
    command = ["aws"]
    if args.profile:
        command.extend(("--profile", args.profile))
    command.extend((
        "ssm",
        "start-session",
        "--target",
        instance_id,
        "--document-name",
        "AWS-StartPortForwardingSession",
        "--parameters",
        f"portNumber=3000,localPortNumber={args.local_tunnel_port}",
        "--region",
        region,
    ))

    GrafanaProxyHandler.upstream_port = args.local_tunnel_port
    print(f"AWS session: {session['_path'].parent.name} ({session.get('phase', 'unknown')})")
    print(f"Runner:      {instance_id} in {region}")
    print(f"Starting SSM tunnel on 127.0.0.1:{args.local_tunnel_port} ...")
    tunnel = subprocess.Popen(command, start_new_session=True)
    try:
        wait_for_listener("127.0.0.1", args.local_tunnel_port, tunnel)
        with ThreadingProxyServer((args.bind_address, bind_port), GrafanaProxyHandler) as proxy:
            proxy.timeout = 1
            if bind_port != args.port:
                print(f"Port {args.port} is occupied; selected {bind_port}.")
            print(f"Grafana:     http://{args.bind_address}:{bind_port}")
            print("Press Ctrl+C to stop the tunnel.")
            while tunnel.poll() is None:
                proxy.handle_request()
            raise RuntimeError(f"SSM port-forward exited (exit code {tunnel.returncode})")
    except KeyboardInterrupt:
        print("\nStopping Grafana tunnel ...")
        return 0
    finally:
        terminate_process_group(tunnel)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
