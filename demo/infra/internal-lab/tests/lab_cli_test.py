from __future__ import annotations

import importlib.util
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


LAB_SCRIPT = Path(__file__).resolve().parents[1] / "scripts/lab.py"
BOOTSTRAP_SCRIPT = Path(__file__).resolve().parents[1] / "scripts/bootstrap-host.sh"
SPEC = importlib.util.spec_from_file_location("ckc_lab_cli", LAB_SCRIPT)
assert SPEC and SPEC.loader
LAB = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LAB
SPEC.loader.exec_module(LAB)


class LabCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.public_key = self.root / "operator.pub"
        self.public_key.write_text("ssh-ed25519 test-key operator\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def write_config(self, *, topology: str = "single-host") -> Path:
        application_node = ""
        controller_roles = "[controller, k3s-server, services, application]"
        if topology == "split-application":
            controller_roles = "[controller, k3s-server, services]"
            application_node = """
  application:
    host: 192.0.2.11
    admin_user: alexey
    lab_address: 198.51.100.11
    k3s_name: app-node
    roles: [k3s-agent, application]
"""
        path = self.root / "lab.yaml"
        path.write_text(
            f"""version: 1
topology: {topology}
operator:
  public_key: {self.public_key}
runtime:
  user: ckc-lab
  root: /opt/ckc-lab
nodes:
  infra:
    host: 192.0.2.10
    admin_user: alexey
    lab_address: 192.0.2.10
    roles: {controller_roles}
{application_node}
""",
            encoding="utf-8",
        )
        return path

    def test_loads_single_host_configuration(self) -> None:
        config = LAB.load_config(self.write_config())

        self.assertEqual("ckc-lab", config.runtime_user)
        self.assertEqual("192.0.2.10", config.infra.lab_address)
        self.assertEqual("alexey", config.infra.admin_user)
        self.assertEqual(2_000_000, config.performance_cpu_khz)

    def test_loads_split_application_topology(self) -> None:
        config = LAB.load_config(self.write_config(topology="split-application"))

        self.assertTrue(config.split)
        self.assertEqual("198.51.100.11", config.application.lab_address)
        self.assertEqual("app-node", config.application.k3s_name)

    def test_rejects_unknown_topology(self) -> None:
        with self.assertRaisesRegex(ValueError, "single-host or split-application"):
            LAB.load_config(self.write_config(topology="split-sut"))

    def test_writes_compatibility_environment_for_unprivileged_sync(self) -> None:
        config = LAB.load_config(self.write_config())

        environment = LAB.write_compatibility_environment(config).read_text(encoding="utf-8")

        self.assertIn("LAB_SSH_HOST=192.0.2.10", environment)
        self.assertIn("LAB_USER=ckc-lab", environment)
        self.assertIn("LAB_ROOT=/opt/ckc-lab", environment)

    def test_writes_split_worker_runtime_environment(self) -> None:
        config = LAB.load_config(self.write_config(topology="split-application"))

        environment = LAB.write_compatibility_environment(config).read_text(encoding="utf-8")

        self.assertIn("LAB_APPLICATION_TARGET=ckc-lab@198.51.100.11", environment)
        self.assertIn("LAB_APPLICATION_NODE_SELECTOR=ckc.dev/role=application", environment)
        self.assertIn("LAB_CONTROLLER_NODE_SELECTOR=ckc.dev/role=controller", environment)

    def test_split_bootstrap_uses_server_and_agent_roles(self) -> None:
        config = LAB.load_config(self.write_config(topology="split-application"))

        server = LAB.bootstrap_arguments(
            config, config.infra, self.public_key, node_role="server",
        )
        agent = LAB.bootstrap_arguments(
            config, config.application, self.public_key, node_role="agent", token_file=Path("/tmp/token"),
        )

        self.assertIn("server", server)
        self.assertIn("agent", agent)
        self.assertIn("192.0.2.10", agent)
        self.assertIn("app-node", agent)
        self.assertIn("/tmp/token", agent)

    def test_bootstrap_limits_passwordless_sudo_to_exact_helpers(self) -> None:
        script = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("NOPASSWD: /usr/local/libexec/ckc-lab/import-k3s-images", script)
        self.assertIn("/usr/local/libexec/ckc-lab/cpu-performance-acquire", script)
        self.assertIn("/usr/local/libexec/ckc-lab/cpu-performance-release", script)
        self.assertNotIn("NOPASSWD: ALL", script)
        self.assertNotIn("cpupower frequency-set", script)
        self.assertNotIn("intel_pstate/no_turbo", script)
        self.assertIn("cpu-policy.snapshot", script)
        self.assertIn('usermod -aG docker "${RUNTIME_USER}"', script)
        self.assertIn('K3S_URL="https://${SERVER_ADDRESS}:6443"', script)
        self.assertIn("ckc.dev/role=application", script)

    @mock.patch.object(LAB, "capture")
    @mock.patch.object(LAB, "run")
    def test_start_writes_request_and_uses_user_systemd(self, run: mock.Mock, capture: mock.Mock) -> None:
        config_path = self.write_config()
        capture.return_value = mock.Mock(returncode=3)
        args = mock.Mock(
            config=config_path,
            experiment=Path("smoke.yaml"),
            no_update=True,
            dry_run=True,
            env=["AUDIT_LOG_ENABLED=true"],
            skip_archives=False,
        )

        self.assertEqual(0, LAB.experiment_start_command(args))

        request = (self.root / "experiment-request.json").read_text(encoding="utf-8")
        self.assertIn('"experiment": "smoke.yaml"', request)
        self.assertIn('"AUDIT_LOG_ENABLED=true"', request)
        command = " ".join(str(value) for value in run.call_args_list[-1].args[0])
        self.assertIn("progress.json", command)
        self.assertIn("rm -f", command)
        self.assertIn("systemctl --user start ckc-experiment.service", command)

    @mock.patch.object(LAB, "managed_unit_properties")
    def test_status_shows_live_target_eta_and_drain_lag(self, properties: mock.Mock) -> None:
        config = LAB.load_config(self.write_config())
        properties.return_value = (
            {"ActiveState": "active", "SubState": "running", "Result": "success"},
            {"experiment": "smoke.yaml"},
            {
                "label": "draining consumer lag",
                "step": "draining",
                "started_at": "2026-09-25T10:00:00Z",
                "target": {"index": 1, "total": 1, "name": "ckc", "started_at": "2026-09-25T10:01:00Z"},
                "details": {"lag": 42},
            },
        )
        output = io.StringIO()

        with redirect_stdout(output):
            LAB.print_managed_status(config)

        rendered = output.getvalue()
        self.assertIn("step:         draining consumer lag", rendered)
        self.assertIn("target:       1/1 — ckc", rendered)
        self.assertIn("consumer lag: 42", rendered)


if __name__ == "__main__":
    unittest.main()
