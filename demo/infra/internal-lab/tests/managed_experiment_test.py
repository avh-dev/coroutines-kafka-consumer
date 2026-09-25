from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT / "assets/libexec/managed-experiment.py"
UNIT = ROOT / "assets/systemd/ckc-experiment.service.in"
SPEC = importlib.util.spec_from_file_location("managed_experiment_for_test", ENTRYPOINT)
assert SPEC and SPEC.loader
MANAGED = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MANAGED
SPEC.loader.exec_module(MANAGED)


class ManagedExperimentTest(unittest.TestCase):
    def test_unit_wraps_the_run_in_cpu_acquire_and_release(self) -> None:
        unit = UNIT.read_text(encoding="utf-8")

        self.assertIn("ExecStartPre=@LAB_ROOT@/libexec/cluster-performance-acquire.sh", unit)
        self.assertIn("ExecStopPost=@LAB_ROOT@/libexec/cluster-performance-release.sh", unit)
        self.assertIn("KillSignal=SIGINT", unit)
        self.assertIn("KillMode=mixed", unit)
        self.assertIn("StandardOutput=append:@LAB_ROOT@/logs/managed-experiment.log", unit)

    @mock.patch.object(os, "execve")
    def test_entrypoint_executes_only_an_installed_catalog_experiment(self, execve: mock.Mock) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lab_root = Path(directory)
            (lab_root / "state/experiment").mkdir(parents=True)
            (lab_root / "experiments").mkdir()
            (lab_root / "bin").mkdir()
            experiment = lab_root / "experiments/smoke.yaml"
            experiment.write_text("name: smoke\n", encoding="utf-8")
            (lab_root / "state/experiment/request.json").write_text(json.dumps({
                "experiment": "smoke.yaml",
                "env": ["AUDIT_LOG_ENABLED=true"],
                "skip_archives": True,
            }), encoding="utf-8")

            with mock.patch.dict(os.environ, {"LAB_ROOT": str(lab_root)}, clear=False):
                self.assertEqual(127, MANAGED.main())

            command = execve.call_args.args[1]
            self.assertEqual(str(lab_root / "bin/run-experiment.sh"), command[0])
            self.assertIn(str(experiment), command)
            self.assertIn("AUDIT_LOG_ENABLED=true", command)
            self.assertIn("--skip-archives", command)

    def test_entrypoint_rejects_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lab_root = Path(directory)
            (lab_root / "state/experiment").mkdir(parents=True)
            (lab_root / "state/experiment/request.json").write_text(
                json.dumps({"experiment": "../smoke.yaml"}), encoding="utf-8"
            )
            with mock.patch.dict(os.environ, {"LAB_ROOT": str(lab_root)}, clear=False):
                with self.assertRaisesRegex(ValueError, "invalid managed experiment"):
                    MANAGED.main()


if __name__ == "__main__":
    unittest.main()
