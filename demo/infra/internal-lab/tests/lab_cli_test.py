from __future__ import annotations

import importlib.util
import io
import json
import shutil
import subprocess
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
network:
  application_link: {"direct" if topology == "split-application" else "local"}
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
        self.assertEqual("direct", config.application_link)

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
        self.assertIn("LAB_APPLICATION_LINK=direct", environment)

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
        self.assertEqual("198.51.100.11", server[server.index("--network-peer-address") + 1])
        self.assertEqual("192.0.2.10", agent[agent.index("--network-peer-address") + 1])

    def test_bootstrap_limits_passwordless_sudo_to_exact_helpers(self) -> None:
        script = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("NOPASSWD: /usr/local/libexec/ckc-lab/import-k3s-images", script)
        self.assertIn("/usr/local/libexec/ckc-lab/cpu-performance-acquire", script)
        self.assertIn("/usr/local/libexec/ckc-lab/cpu-performance-release", script)
        self.assertNotIn("NOPASSWD: ALL", script)

    def test_bootstrap_imports_images_into_the_cri_namespace(self) -> None:
        script = BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")

        self.assertIn('k3s ctr --namespace k8s.io images import "${archive}"', script)
        self.assertNotIn("cpupower frequency-set", script)
        self.assertNotIn("intel_pstate/no_turbo", script)
        self.assertIn("cpu-policy.snapshot", script)
        self.assertIn('usermod -aG docker "${RUNTIME_USER}"', script)
        self.assertIn('K3S_URL="https://${SERVER_ADDRESS}:6443"', script)
        self.assertIn("ckc.dev/role=application", script)
        self.assertIn("ethtool", script)
        self.assertIn("flannel-iface:", script)
        self.assertIn('ip -o route get "${NETWORK_PEER_ADDRESS}"', script)

    def test_bootstrap_route_parser_extracts_interface_with_ubuntu_awk(self) -> None:
        route = "10.10.20.3 dev eno1 src 10.10.20.2 uid 0 cache\n"
        parser = (
            "{for (field = 1; field <= NF; field++) "
            'if ($field == "dev") {print $(field + 1); exit}}'
        )

        result = subprocess.run(
            ["awk", parser], input=route, text=True, capture_output=True, check=True,
        )

        self.assertEqual("eno1", result.stdout.strip())

    @mock.patch.object(LAB, "print_managed_status", return_value=0)
    @mock.patch.object(LAB.subprocess, "run")
    def test_start_submits_one_ordered_request(self, run: mock.Mock, _status: mock.Mock) -> None:
        config_path = self.write_config()
        args = LAB.parser().parse_args([
            "--config", str(config_path), "experiment", "start",
            "smoke.yaml", "ckc-spring-boot-smoke.yaml", "smoke.yaml",
            "--no-update", "--env", "AUDIT_LOG_ENABLED=true", "--skip-archives",
        ])

        self.assertEqual(0, LAB.experiment_start_command(args))

        run.assert_called_once()
        self.assertIn("/libexec/experiment-service.py start", run.call_args.args[0][-1])
        request = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(
            ["smoke.yaml", "ckc-spring-boot-smoke.yaml", "smoke.yaml"], request["experiments"],
        )
        self.assertEqual(["AUDIT_LOG_ENABLED=true"], request["env"])
        self.assertTrue(request["skip_archives"])

    @mock.patch.object(LAB, "up_command")
    @mock.patch.object(LAB.subprocess, "run")
    def test_start_rejects_existing_invalid_yaml_before_updating_or_starting(
        self, run: mock.Mock, up: mock.Mock,
    ) -> None:
        config_path = self.write_config()
        catalog = self.root / "demo/infra/experiments"
        catalog.mkdir(parents=True)
        shutil.copyfile(LAB.REPO_ROOT / "demo/infra/experiments/smoke.yaml", catalog / "smoke.yaml")
        broken = LAB.yaml.safe_load((catalog / "smoke.yaml").read_text(encoding="utf-8"))
        broken["targets"][0]["annotation_label"] = ""
        (catalog / "broken.yaml").write_text(LAB.yaml.safe_dump(broken), encoding="utf-8")

        with mock.patch.object(LAB, "REPO_ROOT", self.root):
            with self.assertRaisesRegex(ValueError, r"broken\.yaml: .*annotation_label must not be blank"):
                args = LAB.parser().parse_args([
                    "--config", str(config_path), "experiment", "start", "smoke.yaml", "broken.yaml",
                ])
                LAB.experiment_start_command(args)

        run.assert_not_called()
        up.assert_not_called()

    def test_management_commands_do_not_invoke_legacy_runner(self) -> None:
        config = self.write_config()
        lab_root = self.root / "installed"
        config.write_text(
            config.read_text(encoding="utf-8").replace("/opt/ckc-lab", str(lab_root)), encoding="utf-8",
        )
        legacy = lab_root / "libexec/managed-experiment.py"
        legacy.parent.mkdir(parents=True)
        marker = legacy.parent / "legacy-ran"
        legacy.write_text('#!/bin/sh\ntouch "$(dirname "$0")/legacy-ran"\n', encoding="utf-8")
        legacy.chmod(0o755)
        real_run = subprocess.run

        def run_remote(command, **kwargs):
            self.assertEqual("ssh", command[0])
            kwargs.setdefault("text", True)
            return real_run(["sh", "-c", command[-1]], **kwargs, stderr=subprocess.PIPE)

        for options in (["status"], ["stop"], ["start", "smoke.yaml", "--no-update"]):
            with self.subTest(options=options), mock.patch.object(LAB.subprocess, "run", side_effect=run_remote):
                args = LAB.parser().parse_args(["--config", str(config), "experiment", *options])
                with self.assertRaises(subprocess.CalledProcessError) as error:
                    args.handler(args)
                self.assertIn("lab up", error.exception.stderr)
                self.assertFalse(marker.exists())

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

    @mock.patch.object(LAB, "managed_unit_properties")
    def test_status_exposes_queue_and_target_progress_in_both_formats(self, properties: mock.Mock) -> None:
        config = LAB.load_config(self.write_config())
        properties.return_value = (
            {"ActiveState": "active", "SubState": "running", "Result": "success"},
            {
                "experiment": "baseline.yaml",
                "queue": {
                    "status": "running", "current_index": 2, "exit_code": None,
                    "experiments": [
                        {"experiment": "baseline.yaml", "status": "succeeded", "exit_code": 0, "run_id": "baseline"},
                        {"experiment": "ckc.yaml", "status": "running", "exit_code": None, "run_id": "ckc"},
                        {"experiment": "repeat.yaml", "status": "not_started", "exit_code": None, "run_id": None},
                    ],
                },
            },
            {"target": {"index": 4, "total": 7, "name": "ckc"}},
        )

        with redirect_stdout(io.StringIO()) as output:
            LAB.print_managed_status(config)
        self.assertIn("1/3 succeeded", output.getvalue())
        self.assertIn("2/3 — ckc.yaml", output.getvalue())
        self.assertIn("4/7 — ckc", output.getvalue())

        with redirect_stdout(io.StringIO()) as output:
            LAB.print_managed_status(config, json_output=True)
        document = json.loads(output.getvalue())
        queue = document["queue"]
        self.assertEqual((1, 3, 2), (queue["succeeded"], queue["total"], queue["current_index"]))
        self.assertIsNone(queue["experiments"][2]["exit_code"])
        self.assertIsNone(queue["experiments"][2]["run_id"])
        self.assertEqual(4, document["progress"]["target"]["index"])

    @mock.patch.object(LAB, "managed_unit_properties")
    def test_service_cleanup_failure_overrides_queue_success(self, properties: mock.Mock) -> None:
        config = LAB.load_config(self.write_config())
        completed = {"experiment": "smoke.yaml", "status": "succeeded", "exit_code": 0, "run_id": "run"}
        properties.return_value = (
            {"ActiveState": "failed", "SubState": "failed", "Result": "exit-code", "ExecMainStatus": "0"},
            {"queue": {"status": "succeeded", "current_index": None, "exit_code": 0, "experiments": [completed]}},
            {},
        )
        with redirect_stdout(io.StringIO()) as output:
            LAB.print_managed_status(config, json_output=True)
        document = json.loads(output.getvalue())
        self.assertEqual("exit-code", document["result"])
        self.assertEqual("failed", document["active_state"])
        self.assertEqual([completed], document["queue"]["experiments"])

        with redirect_stdout(io.StringIO()) as output:
            LAB.print_managed_status(config)
        self.assertIn("result:       exit-code", output.getvalue())
        self.assertIn("smoke.yaml: succeeded (exit 0)", output.getvalue())

    @mock.patch.object(LAB, "managed_unit_properties")
    def test_status_reports_cancelled_instead_of_systemd_success(self, properties: mock.Mock) -> None:
        config = LAB.load_config(self.write_config())
        properties.return_value = (
            {"ActiveState": "inactive", "SubState": "dead", "Result": "success", "ExecMainStatus": "130"},
            {"experiment": "comparison.yaml"},
            {"status": "cancelled", "step": "cancelled", "label": "experiment cancelled"},
        )
        output = io.StringIO()

        with redirect_stdout(output):
            LAB.print_managed_status(config)

        rendered = output.getvalue()
        self.assertIn("result:       cancelled", rendered)
        self.assertIn("step:         experiment cancelled", rendered)


if __name__ == "__main__":
    unittest.main()
