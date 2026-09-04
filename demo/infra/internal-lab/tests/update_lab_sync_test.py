from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update-lab.sh"


class UpdateLabSyncTest(unittest.TestCase):
    def test_syncs_only_the_canonical_experiment_catalog(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'demo/infra/experiments" "${LAB_ROOT}/experiments"',
            script,
        )
        self.assertNotIn("shared/workloads", script)
        self.assertNotIn("internal-lab/workloads", script)


if __name__ == "__main__":
    unittest.main()
