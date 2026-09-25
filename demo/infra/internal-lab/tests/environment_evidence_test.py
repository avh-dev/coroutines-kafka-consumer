from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "assets/helpers/capture-environment-evidence.py"
SPEC = importlib.util.spec_from_file_location("capture_environment_evidence", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class EnvironmentEvidenceTest(unittest.TestCase):
    def test_resolved_hosts_separates_controller_and_application_worker(self) -> None:
        nodes = [
            {"name": "optilab", "cpu": "6", "memory": "16Gi"},
            {"name": "optilab2", "cpu": "6", "memory": "8Gi"},
        ]
        workloads = {
            "application": ["optilab2"],
            "stubs": ["optilab"],
            "producer": ["optilab"],
        }
        controller_hardware = {"cpu_model": "Controller CPU", "logical_cpus": "6"}
        worker_hardware = {"cpu_model": "Worker CPU", "logical_cpus": "6"}

        with patch.dict(MODULE.os.environ, {"LAB_APPLICATION_TARGET": "ckc-lab@10.10.20.3"}), patch.object(
            MODULE, "hardware_evidence", return_value=worker_hardware
        ) as remote_hardware:
            hosts = MODULE.resolved_hosts("optilab", nodes, workloads, controller_hardware)

        self.assertEqual(["optilab", "optilab2"], [host["name"] for host in hosts])
        self.assertEqual("controller", hosts[0]["role"])
        self.assertEqual(controller_hardware, hosts[0]["hardware"])
        self.assertEqual("application-worker", hosts[1]["role"])
        self.assertEqual(worker_hardware, hosts[1]["hardware"])
        remote_hardware.assert_called_once_with("ckc-lab@10.10.20.3")

    def test_resolved_hosts_keeps_single_host_without_remote_probe(self) -> None:
        nodes = [{"name": "laptop", "cpu": "8", "memory": "16Gi"}]
        controller_hardware = {"cpu_model": "Laptop CPU", "logical_cpus": "8"}

        with patch.dict(MODULE.os.environ, {"LAB_APPLICATION_TARGET": ""}), patch.object(
            MODULE, "hardware_evidence"
        ) as remote_hardware:
            hosts = MODULE.resolved_hosts(
                "laptop",
                nodes,
                {"application": ["laptop"], "producer": ["laptop"]},
                controller_hardware,
            )

        self.assertEqual(1, len(hosts))
        self.assertEqual("controller", hosts[0]["role"])
        remote_hardware.assert_not_called()


if __name__ == "__main__":
    unittest.main()
