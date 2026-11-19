from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import ANY, patch


HELPERS = Path(__file__).resolve().parents[1] / "assets/helpers"
SPEC = importlib.util.spec_from_file_location("prepare_stubs_for_test", HELPERS / "prepare-stubs.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load prepare-stubs.py")
PREPARE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PREPARE
SPEC.loader.exec_module(PREPARE)


class _PrewarmHandler(BaseHTTPRequestHandler):
    paths: list[str] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        if self.headers.get("X-CKC-Prewarm") != "true":
            self.send_error(400)
            return
        type(self).paths.append(self.path)
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class _Forward:
    ports = {"stub-a": 10001, "stub-b": 10002}

    def __init__(self, _namespace: str, pod: str) -> None:
        self.port = self.ports[pod]

    def wait_ready(self, _deadline: float) -> None:
        return

    def __enter__(self) -> "_Forward":
        return self

    def __exit__(self, *_args: object) -> None:
        return


class StubsPreparationTest(unittest.TestCase):
    def test_prewarm_exercises_every_endpoint_with_bounded_workers(self) -> None:
        _PrewarmHandler.paths = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), _PrewarmHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = PREPARE.prewarm(server.server_port, 7, 3, PREPARE.time.monotonic() + 10)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(21, result["requests"])
        self.assertEqual(3, result["concurrency"])
        self.assertEqual(
            {"/eta": 7, "/flavour": 7, "/brewing-registry/steps": 7},
            {path: _PrewarmHandler.paths.count(path) for path in set(_PrewarmHandler.paths)},
        )

    def test_prepare_rewarms_only_new_processes_and_records_identity(self) -> None:
        settings = {
            "eta": {"percentiles": {"p100": 1}},
            "flavour": {"percentiles": {"p100": 1}},
            "registry": {"percentiles": {"p100": 1}},
            "errorRatePercent": 0,
        }
        pods = [
            {"name": "stub-a", "uid": "uid-a", "image": "stubs:v1", "image_id": "sha256:a", "restart_count": 0},
            {"name": "stub-b", "uid": "uid-b", "image": "stubs:v1", "image_id": "sha256:a", "restart_count": 1},
        ]
        statuses = {
            10001: {"processId": "process-a", "startedAt": "2026-01-01T00:00:00Z", "settingsVersion": "version-1", "settings": settings},
            10002: {"processId": "process-b", "startedAt": "2026-01-01T00:01:00Z", "settingsVersion": "version-1", "settings": settings},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            state.write_text(json.dumps({"processes": {"process-a": {"prewarmed_at": "earlier"}}}), encoding="utf-8")
            args = PREPARE.argparse.Namespace(
                settings_json=json.dumps(settings), namespace="ckc-perf", selector="app=stubs",
                container="demo-stubs", state=state, evidence=root / "evidence.json",
                requests_per_endpoint=10, concurrency=2, timeout_seconds=30,
            )
            with (
                patch.object(PREPARE, "ready_pods", return_value=pods),
                patch.object(PREPARE, "PortForward", _Forward),
                patch.object(PREPARE, "request_json", return_value=settings) as request,
                patch.object(PREPARE, "await_settings", side_effect=lambda port, _expected, _deadline: statuses[port]),
                patch.object(PREPARE, "prewarm", return_value={"requests": 30}) as prewarm,
            ):
                result = PREPARE.prepare(args)

            saved = json.loads(state.read_text(encoding="utf-8"))

        self.assertEqual("ready", result["status"])
        self.assertFalse(result["pods"][0]["prewarm"]["performed"])
        self.assertTrue(result["pods"][1]["prewarm"]["performed"])
        prewarm.assert_called_once_with(10002, 10, 2, ANY)
        self.assertEqual({"process-a", "process-b"}, set(saved["processes"]))
        self.assertEqual("sha256:a", result["pods"][1]["image_id"])
        self.assertEqual("POST", request.call_args_list[0].kwargs["method"])


if __name__ == "__main__":
    unittest.main()
