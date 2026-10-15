#!/usr/bin/env python3
"""Small, dependency-free nftables Kafka TCP counter exporter.

The rules intentionally live in the pod network namespace.  The table is
owned by this exporter; a table with a different shape is never replaced.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TABLE = "ckc_client_network"
CHAIN = "output"


def nft(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["nft", *args], text=True, capture_output=True, check=False)


def ruleset(service_ports: dict[str, list[int]]) -> str:
    rules = []
    for service, ports in service_ports.items():
        port_set = ", ".join(str(port) for port in ports)
        rules.extend((
            f'  tcp dport {{ {port_set} }} counter comment "ckc-{service}-transmit"',
            f'  tcp sport {{ {port_set} }} counter comment "ckc-{service}-receive"',
        ))
    output_rules = "\n".join(rule for index, rule in enumerate(rules) if index % 2 == 0)
    input_rules = "\n".join(rule for index, rule in enumerate(rules) if index % 2 == 1)
    return f'''table inet {TABLE} {{
 chain {CHAIN} {{
  type filter hook output priority filter; policy accept;
{output_rules}
 }}
 chain input {{
  type filter hook input priority filter; policy accept;
{input_rules}
 }}
}}'''


def ensure_rules(service_ports: dict[str, list[int]]) -> None:
    existing = nft("list", "table", "inet", TABLE)
    if existing.returncode == 0:
        text = existing.stdout
        expected = {str(port) for ports in service_ports.values() for port in ports}
        comments = {f"ckc-{service}-{direction}" for service in service_ports for direction in ("receive", "transmit")}
        if "chain input" not in text or not all(comment in text for comment in comments) or not expected.issubset(set(text.replace("{", " ").replace("}", " ").replace(",", " ").split())):
            raise RuntimeError("existing ckc Kafka nftables table is incompatible; refusing to replace it")
        return
    created = subprocess.run(["nft", "-f", "-"], input=ruleset(service_ports), text=True, capture_output=True, check=False)
    if created.returncode:
        raise RuntimeError(created.stderr.strip() or "could not create nftables rules")


def counters(services: set[str]) -> tuple[dict[str, tuple[int, int]], bool]:
    result = nft("-j", "list", "table", "inet", TABLE)
    if result.returncode:
        return {}, False
    try:
        data = json.loads(result.stdout)
        found: dict[str, tuple[int, int]] = {}
        for item in data.get("nftables", []):
            rule = item.get("rule", {})
            comment = rule.get("comment")
            counter = next((entry["counter"] for entry in rule.get("expr", []) if "counter" in entry), None)
            if comment in {f"ckc-{service}-{direction}" for service in services for direction in ("receive", "transmit")} and counter:
                found[comment] = (int(counter.get("bytes", 0)), int(counter.get("packets", 0)))
        return found, len(found) == 2 * len(services)
    except (TypeError, ValueError, KeyError):
        return {}, False


class Handler(BaseHTTPRequestHandler):
    exporter: "Exporter"
    def log_message(self, *_: object) -> None: pass
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_response(200 if self.exporter.ready else 503); self.end_headers(); return
        if self.path != "/metrics": self.send_response(404); self.end_headers(); return
        values, ok = counters(set(self.exporter.service_ports))
        lines = ["# TYPE ckc_kafka_network_rules_present gauge", f"ckc_kafka_network_rules_present {1 if self.exporter.ready else 0}", "# TYPE ckc_kafka_network_counter_read_success gauge", f"ckc_kafka_network_counter_read_success {1 if ok else 0}"]
        for service in self.exporter.service_ports:
            for direction in ("receive", "transmit"):
                bytes_, packets = values.get(f"ckc-{service}-{direction}", (0, 0))
                labels = f'service="{service}",direction="{direction}"'
                lines += [f"ckc_client_network_bytes_total{{{labels}}} {bytes_}", f"ckc_client_network_packets_total{{{labels}}} {packets}"]
                if service == "kafka":
                    lines += [f'ckc_kafka_client_network_bytes_total{{direction="{direction}"}} {bytes_}', f'ckc_kafka_client_network_packets_total{{direction="{direction}"}} {packets}']
        payload = ("\n".join(lines) + "\n").encode()
        self.send_response(200); self.send_header("Content-Type", "text/plain; version=0.0.4"); self.send_header("Content-Length", str(len(payload))); self.end_headers(); self.wfile.write(payload)


class Exporter:
    def __init__(self, service_ports: dict[str, list[int]]): self.service_ports, self.ready = service_ports, False
    def run(self) -> None:
        ensure_rules(self.service_ports); self.ready = True
        Handler.exporter = self
        ThreadingHTTPServer(("0.0.0.0", 9406), Handler).serve_forever()


def parse_ports(value: str, argument: str) -> list[int]:
    ports = sorted({int(port) for port in value.split(",") if port})
    if not ports or any(port < 1 or port > 65535 for port in ports): raise SystemExit(f"{argument} must contain TCP ports")
    return ports


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--ports", required=True); parser.add_argument("--redis-ports", default="6379"); parser.add_argument("--http-ports", default="8085")
    args = parser.parse_args()
    Exporter({"kafka": parse_ports(args.ports, "--ports"), "redis": parse_ports(args.redis_ports, "--redis-ports"), "http": parse_ports(args.http_ports, "--http-ports")}).run()

if __name__ == "__main__": main()
