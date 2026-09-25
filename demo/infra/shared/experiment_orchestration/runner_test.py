from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from .runner import AwsAdapter, InternalLabAdapter, RunRequest, run


REPO_ROOT = Path(__file__).resolve().parents[4]
EXAMPLE = REPO_ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"


class SharedExperimentRunnerTest(unittest.TestCase):
    def request(self, environment: str) -> RunRequest:
        return RunRequest(
            repo_root=REPO_ROOT,
            experiment=EXAMPLE,
            environment=environment,
            work_dir=Path(self.temp.name),
            lab_root=Path("/opt/ckc-lab"),
            env=("AUDIT_LOG_ENABLED=true",),
            build_images=False,
            internal_lab_host="optilab",
            internal_lab_user="ckc-lab",
            notify_hook="",
            telegram_env=Path("/home/test/.config/ckc-lab/telegram.env"),
        )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @mock.patch("demo.infra.shared.experiment_orchestration.runner.execute", return_value=0)
    def test_shared_runner_validates_then_dispatches_aws(self, execute: mock.Mock) -> None:
        self.assertEqual(0, run(self.request("aws")))
        command = execute.call_args.args[0]
        self.assertIn("demo/infra/aws/scripts/run-experiment.py", command[1])
        self.assertIn("eu-central-1", command)
        self.assertIn("--skip-build-images", command)
        self.assertIn("--telegram-env", command)
        self.assertIn("/home/test/.config/ckc-lab/telegram.env", command)

    def test_internal_lab_adapter_uses_bounded_noninteractive_runtime_ssh(self) -> None:
        request = self.request("internal-lab")
        experiment = mock.Mock(environment_definition={})
        command = InternalLabAdapter().command(request, experiment)

        self.assertEqual("ssh", command[0])
        self.assertIn("BatchMode=yes", command)
        self.assertEqual("ckc-lab@optilab", command[-2])
        self.assertIn("/opt/ckc-lab/experiments/portable-smoke.yaml", command[-1])
        self.assertIn("AUDIT_LOG_ENABLED=true", command[-1])

    def test_aws_adapter_keeps_operator_env_out_of_cloud_command(self) -> None:
        request = self.request("aws")
        experiment = mock.Mock(environment_definition={"region": "eu-central-1"})
        command = AwsAdapter().command(request, experiment)
        self.assertNotIn("AUDIT_LOG_ENABLED=true", command)

    def test_notification_hook_is_passed_only_to_the_selected_controller(self) -> None:
        request = self.request("aws")
        request = RunRequest(**{**request.__dict__, "notify_hook": "/tmp/operator-notify"})
        experiment = mock.Mock(environment_definition={"region": "eu-central-1"})
        aws_command = AwsAdapter().command(request, experiment)
        internal_command = InternalLabAdapter().command(request, experiment)

        self.assertEqual("/tmp/operator-notify", aws_command[aws_command.index("--notify-hook") + 1])
        self.assertIn("--notify-hook /tmp/operator-notify", internal_command[-1])


if __name__ == "__main__":
    unittest.main()
