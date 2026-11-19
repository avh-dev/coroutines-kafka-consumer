from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREPARE = ROOT / "assets/libexec/prepare-test.sh"
QUIESCE = ROOT / "assets/libexec/quiesce-application.sh"
RUN_TARGET = ROOT / "assets/libexec/run-target.sh"


class WarmupTransportLifecycleTest(unittest.TestCase):
    def test_both_preparation_paths_write_target_specific_stub_evidence(self) -> None:
        script = RUN_TARGET.read_text(encoding="utf-8")

        self.assertEqual(
            2,
            script.count(
                'STUB_PREPARATION_EVIDENCE_FILE="${RESULTS_DIR}/runs/${RUN_ID}/diagnostics/stubs-preparation.json"'
            ),
        )

    def test_preparation_bounds_transport_wait_before_applying_application_manifest(self) -> None:
        script = PREPARE.read_text(encoding="utf-8")
        broker_apply = script.index('kubectl apply -f "${PROJECT_MANIFEST}" -l ckc.dev/component=warmup-kafka')
        ready = script.index("rollout status deployment/ckc-warmup-kafka --timeout=5m")
        stubs_apply = script.index('kubectl apply -f "${PROJECT_MANIFEST}" -l ckc.dev/component=stubs')
        stubs_prepare = script.index('"${LAB_ROOT}/libexec/configure-stubs.sh" "${STUB_SETTINGS_JSON}"')
        fallback = script.index("applications will start and use cold fallback")
        application_apply = script.index('kubectl apply -f "${PROJECT_MANIFEST}" -l ckc.dev/component=application')

        self.assertLess(broker_apply, ready)
        self.assertLess(ready, stubs_apply)
        self.assertLess(stubs_apply, stubs_prepare)
        self.assertLess(ready, fallback)
        self.assertLess(stubs_prepare, fallback)
        self.assertLess(fallback, application_apply)

    def run_cleanup(self, mode: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / "kubectl"
            fake.write_text(
                """#!/usr/bin/env python3
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
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            result = subprocess.run(
                ["sh", str(QUIESCE), "--timeout-seconds", "5"],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "FAKE_ROOT": str(root),
                    "FAKE_MODE": mode,
                },
                text=True,
                capture_output=True,
                timeout=10,
            )
            return result, (root / "calls").read_text(encoding="utf-8").splitlines()

    def test_cleanup_removes_transport_after_application_workloads(self) -> None:
        result, calls = self.run_cleanup("success")

        self.assertEqual(0, result.returncode, result.stderr)
        delete = next(i for i, call in enumerate(calls) if "delete deployment,service" in call)
        application_lookup = next(
            i for i, call in enumerate(calls)
            if "get deployment -l ckc.dev/component=application" in call
        )
        self.assertLess(application_lookup, delete)
        self.assertFalse(any("ckc-demo-stubs" in call for call in calls))
        self.assertIn("persistent demo stubs remain available", result.stdout)
        self.assertEqual(2, sum("get deployment,service,pods" in call for call in calls))

    def test_cleanup_reports_delete_and_verification_failures(self) -> None:
        for mode in ("delete-failure", "read-failure"):
            with self.subTest(mode=mode):
                result, _ = self.run_cleanup(mode)
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("workloads are stopped", result.stdout)


if __name__ == "__main__":
    unittest.main()
