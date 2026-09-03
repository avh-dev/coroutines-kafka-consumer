from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update-lab.sh"


class UpdateLabSyncTest(unittest.TestCase):
    def test_syncs_shared_workloads_without_deleting_internal_lab_experiments(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'internal-lab/workloads/experiments" "${LAB_ROOT}/workloads/experiments"',
            script,
        )
        self.assertIn(
            'shared/workloads/test-definitions" "${LAB_ROOT}/workloads/test-definitions"',
            script,
        )
        self.assertIn(
            'shared/workloads/sla-profiles" "${LAB_ROOT}/workloads/sla-profiles"',
            script,
        )
        self.assertNotIn(
            'internal-lab/workloads" "${LAB_ROOT}/workloads"',
            script,
        )
        self.assertIn(
            '${LAB_ROOT}/workloads/sla-profiles/consumer-baseline.yaml',
            script,
        )


if __name__ == "__main__":
    unittest.main()
