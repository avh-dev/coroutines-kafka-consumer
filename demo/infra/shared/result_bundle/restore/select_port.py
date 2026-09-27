#!/usr/bin/env python3

from __future__ import annotations

import argparse
import socket
from collections.abc import Callable, Collection


class PortSelectionError(ValueError):
    pass


def parse_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise PortSelectionError(f"port must be an integer: {value}") from error
    if not 1 <= port <= 65535:
        raise PortSelectionError(f"port must be between 1 and 65535: {value}")
    return port


def port_available(bind_address: str, port: int) -> bool:
    try:
        addresses = socket.getaddrinfo(
            bind_address,
            port,
            type=socket.SOCK_STREAM,
            flags=socket.AI_PASSIVE,
        )
    except socket.gaierror as error:
        raise PortSelectionError(f"cannot resolve restore bind address {bind_address}: {error}") from error

    checked = False
    for family, socktype, protocol, _, address in addresses:
        try:
            with socket.socket(family, socktype, protocol) as listener:
                if family == socket.AF_INET6:
                    listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                listener.bind(address)
            checked = True
        except OSError:
            return False
    return checked


def select_port(
    bind_address: str,
    preferred_port: int,
    *,
    explicit: bool,
    excluded: Collection[int] = (),
    available: Callable[[str, int], bool] = port_available,
) -> int:
    if explicit:
        if preferred_port in excluded or not available(bind_address, preferred_port):
            raise PortSelectionError(
                f"requested port {preferred_port} is already in use on {bind_address}"
            )
        return preferred_port

    for port in range(preferred_port, 65536):
        if port not in excluded and available(bind_address, port):
            return port
    raise PortSelectionError(
        f"no free port is available on {bind_address} from {preferred_port} through 65535"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Select a host port for the portable CKC restore stack.")
    parser.add_argument("--bind-address", required=True)
    parser.add_argument("--preferred-port", required=True)
    parser.add_argument("--explicit", action="store_true")
    parser.add_argument("--exclude-port", action="append", default=[])
    parser.add_argument("--service", default="restore service")
    args = parser.parse_args()

    try:
        preferred = parse_port(args.preferred_port)
        excluded = {parse_port(value) for value in args.exclude_port}
        selected = select_port(
            args.bind_address,
            preferred,
            explicit=args.explicit,
            excluded=excluded,
        )
    except PortSelectionError as error:
        parser.exit(1, f"{args.service}: {error}\n")
    print(selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
