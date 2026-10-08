from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path


RESTORE_SOURCE = Path(__file__).resolve().parent / "restore"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class RestoreLifecycleTest(unittest.TestCase):
    def prepare_bundle(self, root: Path) -> tuple[Path, dict[str, str]]:
        bundle = root / "ckc-experiment-test"
        implementation = bundle / "restore/_implementation"
        implementation.mkdir(parents=True)
        (bundle / "restore/dashboard").mkdir()
        (bundle / "restore/prometheus").mkdir()
        (bundle / "restore/dashboard/ckc-experiment.json").write_text("{}\n", encoding="utf-8")
        for name in ("docker-compose.yml", "import-loki.py", "select_port.py"):
            shutil.copy2(RESTORE_SOURCE / name, implementation / name)
        for name in ("start-grafana.sh", "stop-grafana.sh"):
            shutil.copy2(RESTORE_SOURCE / name, bundle / name)

        fake_bin = root / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "${FAKE_DOCKER_LOG}"
case "$*" in
  *" ps --status running -q")
    test ! -f "${FAKE_DOCKER_RUNNING}" || echo restore-container
    ;;
  *" up -d loki")
    if [ "${FAKE_DOCKER_FAIL_LOKI:-0}" = 1 ]; then exit 1; fi
    touch "${FAKE_DOCKER_RUNNING}"
    ;;
  *" up -d "*) touch "${FAKE_DOCKER_RUNNING}" ;;
  *" down") rm -f "${FAKE_DOCKER_RUNNING}" ;;
esac
""",
            encoding="utf-8",
        )
        docker.chmod(0o755)
        curl = fake_bin / "curl"
        curl.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        curl.chmod(0o755)

        environment = os.environ.copy()
        environment.update({
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "FAKE_DOCKER_LOG": str(root / "docker.log"),
            "FAKE_DOCKER_RUNNING": str(root / "docker.running"),
            "CKC_RESTORE_GRAFANA_PORT": str(free_port()),
            "CKC_RESTORE_LOKI_PORT": str(free_port()),
        })
        return bundle, environment

    def test_start_is_detached_repeatable_and_stopped_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle, environment = self.prepare_bundle(root)

            started = subprocess.run(
                [str(bundle / "start-grafana.sh")],
                cwd=bundle,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(0, started.returncode, started.stderr)
            self.assertIn("[5/7] Importing the preserved Loki records.", started.stdout)
            self.assertIn("running in the background", started.stdout)
            self.assertTrue((root / "docker.running").is_file())

            repeated = subprocess.run(
                [str(bundle / "start-grafana.sh")],
                cwd=bundle,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(0, repeated.returncode, repeated.stderr)
            self.assertIn("already running", repeated.stdout)

            stopped = subprocess.run(
                [str(bundle / "stop-grafana.sh")],
                cwd=bundle,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(0, stopped.returncode, stopped.stderr)
            self.assertFalse((root / "docker.running").exists())
            self.assertFalse((bundle / "restore/_implementation/.runtime/restore.env").exists())

    def test_failed_start_removes_partial_stack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle, environment = self.prepare_bundle(root)
            environment["FAKE_DOCKER_FAIL_LOKI"] = "1"

            started = subprocess.run(
                [str(bundle / "start-grafana.sh")],
                cwd=bundle,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertNotEqual(0, started.returncode)
            self.assertIn("stopping the partial stack", started.stdout)
            self.assertFalse((root / "docker.running").exists())
            self.assertIn(" down\n", (root / "docker.log").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
