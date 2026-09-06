from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update-lab.sh"
RUN_TEST = Path(__file__).resolve().parents[1] / "assets/bin/run-test.sh"


class UpdateLabSyncTest(unittest.TestCase):
    def test_syncs_only_the_canonical_experiment_catalog(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'demo/infra/experiments" "${LAB_ROOT}/experiments"',
            script,
        )
        self.assertNotIn("shared/workloads", script)
        self.assertNotIn("internal-lab/workloads", script)
        self.assertIn("demo/infra/shared/result_bundle", script)

    def test_keeps_canonical_experiments_and_shared_helpers_after_sync(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertNotIn(
            'sync_path "${REPO_ROOT}/demo/infra/internal-lab/assets/helpers" "${LAB_ROOT}/helpers"',
            script,
        )
        cleanup = next(line for line in script.splitlines() if "${LAB_ROOT}/test-definitions" in line)
        self.assertNotIn("${LAB_ROOT}/experiments", cleanup)

    def test_canonical_plan_does_not_reuse_persisted_stub_replica_override(self) -> None:
        script = RUN_TEST.read_text(encoding="utf-8")

        self.assertIn('if [ -n "${DEPLOYMENT_PLAN_PATH}" ]; then', script)
        self.assertIn('STUB_REPLICA_COUNT=""', script)


if __name__ == "__main__":
    unittest.main()
