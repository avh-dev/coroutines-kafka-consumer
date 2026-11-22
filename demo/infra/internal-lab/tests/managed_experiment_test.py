from __future__ import annotations

import importlib.util
import io
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT / "assets/libexec/experiment-service.py"
UNIT = ROOT / "assets/systemd/ckc-experiment.service.in"
SPEC = importlib.util.spec_from_file_location("managed_experiment_for_test", ENTRYPOINT)
assert SPEC and SPEC.loader
MANAGED = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MANAGED
SPEC.loader.exec_module(MANAGED)


class ManagedExperimentTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.manager = MANAGED.ManagedExperiment(self.root)
        self.manager.state_dir.mkdir(parents=True)
        (self.root / "helpers").symlink_to(ROOT / "assets/helpers", target_is_directory=True)
        catalog = self.root / "experiments"
        catalog.mkdir()
        self.paths = [catalog / name for name in ("baseline.yaml", "ckc.yaml", "repeat.yaml")]
        for path in self.paths:
            shutil.copyfile(ROOT.parent / "experiments/smoke.yaml", path)
        signals = mock.patch.object(MANAGED.signal, "signal")
        signals.start()
        self.addCleanup(signals.stop)

    def request(self, *, legacy=False) -> dict:
        request = {
            "experiment": self.paths[0].name,
            "env": ["AUDIT_LOG_ENABLED=true"],
            "skip_archives": True,
        }
        if not legacy:
            request["experiments"] = [path.name for path in self.paths]
            request["queue"] = MANAGED.new_queue(self.paths)
        self.manager.save(request)
        return request

    def queue(self) -> dict:
        return MANAGED.load_document(self.manager.request_path)["queue"]

    def run_queue(self, *codes: int) -> tuple[mock.Mock, int, str]:
        expected = iter(codes)

        def launch(command, **_kwargs):
            self.assertFalse(self.manager.progress_path.exists())
            self.manager.progress_path.write_text(
                json.dumps({"experiment_set_id": Path(command[1]).stem + "-run"}), encoding="utf-8",
            )
            child = mock.Mock()
            child.wait.return_value = next(expected)
            return child

        with mock.patch.object(MANAGED.subprocess, "Popen", side_effect=launch) as popen:
            with redirect_stdout(io.StringIO()) as output:
                result = self.manager.run()
        return popen, result, output.getvalue()

    def test_unit_wraps_the_run_in_cpu_acquire_and_release(self) -> None:
        unit = UNIT.read_text(encoding="utf-8")

        self.assertIn("ExecStartPre=@LAB_ROOT@/libexec/cluster-performance-acquire.sh", unit)
        self.assertIn("ExecStart=@LAB_ROOT@/libexec/experiment-service.py run", unit)
        self.assertIn("ExecStopPost=-@LAB_ROOT@/libexec/quiesce-application.sh --timeout-seconds 30", unit)
        self.assertIn("ExecStopPost=@LAB_ROOT@/libexec/cluster-performance-release.sh", unit)
        self.assertIn("ExecStopPost=@LAB_ROOT@/libexec/experiment-service.py finalize", unit)
        self.assertIn("KillSignal=SIGINT", unit)
        self.assertIn("KillMode=mixed", unit)
        self.assertIn("StandardOutput=append:@LAB_ROOT@/logs/managed-experiment.log", unit)

    @mock.patch.object(MANAGED, "systemctl")
    def test_start_validates_installed_files_and_starts_the_unit_once(self, systemctl: mock.Mock) -> None:
        systemctl.return_value = "inactive"
        self.manager.start({"experiments": [path.name for path in reversed(self.paths)]})
        self.assertEqual(
            [
                mock.call("show", "--property=ActiveState", "--value", MANAGED.UNIT, capture=True),
                mock.call("reset-failed", MANAGED.UNIT, capture=True, check=False),
                mock.call("start", MANAGED.UNIT),
            ],
            systemctl.call_args_list,
        )
        self.assertEqual(
            [path.name for path in reversed(self.paths)],
            [entry["experiment"] for entry in self.queue()["experiments"]],
        )

        systemctl.reset_mock()
        broken = yaml.safe_load(self.paths[1].read_text(encoding="utf-8"))
        broken["targets"][0]["annotation_label"] = ""
        self.paths[1].write_text(yaml.safe_dump(broken), encoding="utf-8")
        previous = self.manager.request_path.read_bytes()
        with self.assertRaisesRegex(ValueError, r"ckc\.yaml: .*annotation_label must not be blank"):
            self.manager.start({"experiments": [path.name for path in self.paths]})
        systemctl.assert_not_called()
        self.assertEqual(previous, self.manager.request_path.read_bytes())

    def test_legacy_request_executes_one_experiment_with_existing_options(self) -> None:
        self.request(legacy=True)
        popen, result, _ = self.run_queue(0)
        self.assertEqual(0, result)

        command = popen.call_args.args[0]
        self.assertEqual(str(self.root / "bin/run-experiment.sh"), command[0])
        self.assertIn(str(self.paths[0]), command)
        self.assertIn("AUDIT_LOG_ENABLED=true", command)
        self.assertIn("--skip-archives", command)
        self.assertEqual("succeeded", self.queue()["status"])

    def test_queue_runs_all_experiments_in_order(self) -> None:
        self.request()
        popen, result, output = self.run_queue(0, 0, 0)

        self.assertEqual(0, result)
        self.assertEqual([str(path) for path in self.paths], [call.args[0][1] for call in popen.call_args_list])
        self.assertEqual("0", popen.call_args_list[0].kwargs["env"]["CKC_EXPERIMENT_REACQUIRE_CPU"])
        self.assertEqual("1", popen.call_args_list[1].kwargs["env"]["CKC_EXPERIMENT_REACQUIRE_CPU"])
        self.assertLess(output.index("baseline.yaml succeeded"), output.index("ckc.yaml starting"))
        queue = self.queue()
        self.assertEqual("succeeded", queue["status"])
        self.assertEqual(["baseline-run", "ckc-run", "repeat-run"], [entry["run_id"] for entry in queue["experiments"]])

    def test_queue_preserves_failure_code_and_leaves_later_experiments_unstarted(self) -> None:
        self.request()
        popen, result, _ = self.run_queue(0, 7)

        self.assertEqual(7, result)
        self.assertEqual(2, popen.call_count)
        queue = self.queue()
        self.assertEqual("failed", queue["status"])
        self.assertEqual(7, queue["exit_code"])
        self.assertEqual(
            [
                ("baseline.yaml", "succeeded", 0, "baseline-run"),
                ("ckc.yaml", "failed", 7, "ckc-run"),
                ("repeat.yaml", "not_started", None, None),
            ],
            [tuple(entry[key] for key in ("experiment", "status", "exit_code", "run_id"))
             for entry in queue["experiments"]],
        )

    @mock.patch.object(MANAGED, "systemctl")
    def test_stop_accepted_between_experiments_preserves_success_without_launching_next(self, systemctl: mock.Mock) -> None:
        self.request()
        lock = self.manager.lock
        controller = MANAGED.ManagedExperiment(self.root)

        @contextmanager
        def stop_after_first_completion(name):
            with lock(name):
                yield
            queue = self.queue()
            if queue["status"] == "running" and queue["current_index"] is None:
                self.assertEqual("succeeded", queue["experiments"][0]["status"])
                controller.stop()

        with mock.patch.object(self.manager, "lock", side_effect=stop_after_first_completion):
            popen, result, _ = self.run_queue(0)

        self.assertEqual(130, result)
        popen.assert_called_once()
        systemctl.assert_called_once_with("stop", MANAGED.UNIT)
        queue = self.queue()
        self.assertEqual("cancelled", queue["status"])
        self.assertEqual(["succeeded", "not_started", "not_started"], [e["status"] for e in queue["experiments"]])
        self.assertEqual([0, None, None], [e["exit_code"] for e in queue["experiments"]])
        self.assertEqual(["baseline-run", None, None], [e["run_id"] for e in queue["experiments"]])

    @mock.patch.object(MANAGED, "systemctl")
    @mock.patch.object(MANAGED.subprocess, "Popen")
    def test_stop_after_launch_waits_until_the_runner_can_handle_sigint(
        self, popen: mock.Mock, systemctl: mock.Mock,
    ) -> None:
        self.request()
        child = popen.return_value
        waits = iter(range(3))
        controller = MANAGED.ManagedExperiment(self.root)

        def wait(timeout):
            attempt = next(waits)
            if attempt == 0:
                self.assertEqual(1, self.queue()["current_index"])
                controller.stop()
            elif attempt == 1:
                child.send_signal.assert_not_called()
                self.manager.progress_path.write_text('{"experiment_set_id": "baseline-run"}', encoding="utf-8")
            else:
                child.send_signal.assert_called_once_with(signal.SIGINT)
                return 130
            raise subprocess.TimeoutExpired("stub runner", timeout)

        child.wait.side_effect = wait
        with redirect_stdout(io.StringIO()):
            self.assertEqual(130, self.manager.run())

        popen.assert_called_once()
        systemctl.assert_called_once_with("stop", MANAGED.UNIT)
        queue = self.queue()
        self.assertEqual("cancelled", queue["status"])
        self.assertEqual(["cancelled", "not_started", "not_started"], [e["status"] for e in queue["experiments"]])

    def test_entrypoint_rejects_path_traversal(self) -> None:
        self.manager.save({"experiment": "../smoke.yaml"})
        with self.assertRaisesRegex(ValueError, "invalid managed experiment"):
            self.manager.run()


if __name__ == "__main__":
    unittest.main()
