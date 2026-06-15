#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import socketserver
import sys
import threading
import time
from pathlib import Path


class ChunkWriter:
    def __init__(self, output_dir: Path, max_bytes: int, max_seconds: int, idle_seconds: int) -> None:
        self.output_dir = output_dir
        self.max_bytes = max_bytes
        self.max_seconds = max_seconds
        self.idle_seconds = idle_seconds
        self.lock = threading.Lock()
        self.active_path: Path | None = None
        self.active_file = None
        self.active_started_at = 0.0
        self.active_bytes = 0
        self.last_write_at = 0.0
        self.sequence = 0
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, line: str) -> None:
        if not line.endswith("\n"):
            line += "\n"
        data = line.encode("utf-8")
        now = time.time()
        with self.lock:
            self._ensure_active(now)
            assert self.active_file is not None
            self.active_file.write(data)
            self.active_file.flush()
            self.active_bytes += len(data)
            self.last_write_at = now
            if self.active_bytes >= self.max_bytes or now - self.active_started_at >= self.max_seconds:
                self._finalize_locked()

    def finalize_idle(self) -> None:
        now = time.time()
        with self.lock:
            if self.active_file is not None and now - self.last_write_at >= self.idle_seconds:
                self._finalize_locked()

    def close(self) -> None:
        with self.lock:
            if self.active_file is not None:
                self._finalize_locked()

    def _ensure_active(self, now: float) -> None:
        if self.active_file is not None:
            return
        self.sequence += 1
        timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now))
        self.active_path = self.output_dir / f"audit-{timestamp}-{self.sequence:06d}.log.part"
        self.active_file = self.active_path.open("ab")
        self.active_started_at = now
        self.active_bytes = 0
        self.last_write_at = now

    def _finalize_locked(self) -> None:
        assert self.active_path is not None
        assert self.active_file is not None
        part_path = self.active_path
        self.active_file.close()
        self.active_file = None
        self.active_path = None
        if self.active_bytes == 0:
            part_path.unlink(missing_ok=True)
            return
        final_path = part_path.with_suffix("").with_suffix(".log.gz")
        with part_path.open("rb") as source, gzip.open(final_path, "wb", compresslevel=1) as target:
            shutil.copyfileobj(source, target)
        part_path.unlink()
        print(f"Finalized audit chunk {final_path}", file=sys.stderr, flush=True)


class AuditTcpHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        writer: ChunkWriter = self.server.writer  # type: ignore[attr-defined]
        for raw_line in self.rfile:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            writer.write(extract_message(line))


class ThreadingTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def extract_message(line: str) -> str:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return line
    if isinstance(value, dict):
        message = value.get("message")
        if isinstance(message, str):
            return message
    return line


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive Fluent Bit audit records into completed gzip chunks.")
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5171)
    parser.add_argument("--output-dir", default="/audit/chunks")
    parser.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--max-seconds", type=int, default=30)
    parser.add_argument("--idle-seconds", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    writer = ChunkWriter(Path(args.output_dir), args.max_bytes, args.max_seconds, args.idle_seconds)
    server = ThreadingTcpServer((args.listen, args.port), AuditTcpHandler)
    server.writer = writer  # type: ignore[attr-defined]

    def idle_loop() -> None:
        while True:
            time.sleep(1)
            writer.finalize_idle()

    threading.Thread(target=idle_loop, daemon=True).start()
    print(f"Audit archiver listening on {args.listen}:{args.port}", file=sys.stderr, flush=True)
    try:
        server.serve_forever()
    finally:
        writer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
