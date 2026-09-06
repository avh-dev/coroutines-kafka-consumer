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

    @mock.patch("demo.infra.shared.experiment_orchestration.runner.os.geteuid", return_value=1000)
    def test_internal_lab_adapter_uses_bounded_noninteractive_root_ssh(self, _geteuid: mock.Mock) -> None:
        request = self.request("internal-lab")
        experiment = mock.Mock(environment_definition={})
        command = InternalLabAdapter().command(request, experiment)

        self.assertEqual("ssh", command[0])
        self.assertIn("BatchMode=yes", command)
        self.assertEqual("root@optilab", command[-2])
        self.assertIn(str(EXAMPLE), command[-1])
        self.assertIn("AUDIT_LOG_ENABLED=true", command[-1])

    def test_aws_adapter_keeps_operator_env_out_of_cloud_command(self) -> None:
        request = self.request("aws")
        experiment = mock.Mock(environment_definition={"region": "eu-central-1"})
        command = AwsAdapter().command(request, experiment)
        self.assertNotIn("AUDIT_LOG_ENABLED=true", command)


if __name__ == "__main__":
    unittest.main()
