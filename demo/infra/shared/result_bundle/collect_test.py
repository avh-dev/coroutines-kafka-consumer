from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from .collect import create_prometheus_blocks


class PrometheusBlockExportTest(unittest.TestCase):
    @mock.patch("demo.infra.shared.result_bundle.collect.os.getgid", return_value=2345)
    @mock.patch("demo.infra.shared.result_bundle.collect.os.getuid", return_value=1234)
    @mock.patch("demo.infra.shared.result_bundle.collect.subprocess.run")
    def test_promtool_runs_as_the_calling_runtime_user(
        self,
        run: mock.Mock,
        _getuid: mock.Mock,
        _getgid: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            openmetrics = root / "metrics.openmetrics"
            openmetrics.write_text("# EOF\n", encoding="utf-8")

            self.assertEqual(0, create_prometheus_blocks(openmetrics, root / "output"))

        command = run.call_args.args[0]
        self.assertEqual("1234:2345", command[command.index("-u") + 1])


if __name__ == "__main__":
    unittest.main()
