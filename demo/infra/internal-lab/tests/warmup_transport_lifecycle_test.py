from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUIESCE = ROOT / "assets/libexec/quiesce-application.sh"


class WarmupTransportLifecycleTest(unittest.TestCase):
    def test_preparation_waits_for_topics_before_applying_application_manifest(self) -> None:
        script = (ROOT / "assets/libexec/prepare-test.sh").read_text()
        broker_apply = script.index('kubectl apply -f "${PROJECT_MANIFEST}" -l ckc.dev/component=warmup-kafka')
        ready = script.index("rollout status deployment/ckc-warmup-kafka")
        full_apply = script.index('kubectl apply -f "${PROJECT_MANIFEST}"\n')
        self.assertLess(broker_apply, ready)
        self.assertLess(ready, full_apply)

    def run_cleanup(self, mode: str) -> tuple[subprocess.CompletedProcess, list[str]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / "kubectl"
            fake.write_text('''#!/usr/bin/env python3
import os
import sys
from pathlib import Path
args = " ".join(sys.argv[1:])
root = Path(os.environ["FAKE_ROOT"])
with (root / "calls").open("a") as log:
    log.write(args + "\\n")
if "delete deployment,service" in args:
    sys.exit(1 if os.environ["FAKE_MODE"] == "delete-failure" else 0)
if "get deployment,service,pods" in args:
    if os.environ["FAKE_MODE"] == "read-failure":
        sys.exit(1)
    if not (root / "checked").exists():
        (root / "checked").touch()
        print("service/ckc-warmup-kafka")
''')
            fake.chmod(0o755)
            result = subprocess.run(["sh", str(QUIESCE), "--timeout-seconds", "5"],
                                    env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                                         "FAKE_ROOT": str(root), "FAKE_MODE": mode},
                                    text=True, capture_output=True, timeout=10)
            return result, (root / "calls").read_text().splitlines()

    def test_cleanup_waits_for_service_even_after_broker_pods_are_gone(self) -> None:
        result, calls = self.run_cleanup("success")
        self.assertEqual(0, result.returncode, result.stderr)
        delete = next(i for i, call in enumerate(calls) if "delete deployment,service" in call)
        app_checks = [i for i, call in enumerate(calls) if "get pods -l app.kubernetes.io/name=" in call]
        self.assertTrue(app_checks)
        self.assertLess(max(app_checks), delete)
        self.assertEqual(2, sum("get deployment,service,pods" in call for call in calls))

    def test_cleanup_does_not_report_success_after_delete_or_verification_failure(self) -> None:
        for mode in ("delete-failure", "read-failure"):
            with self.subTest(mode=mode):
                result, _ = self.run_cleanup(mode)
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("workloads are stopped", result.stdout)
