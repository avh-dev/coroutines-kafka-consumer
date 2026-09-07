from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from .definition import resolve_experiment_definition


REPO_ROOT = Path(__file__).resolve().parents[4]
EXAMPLE = REPO_ROOT / "demo/infra/shared/experiment_orchestration/examples/portable-smoke.yaml"


class ExperimentDefinitionTest(unittest.TestCase):
    def test_resolves_only_self_contained_canonical_experiments(self) -> None:
        resolved = resolve_experiment_definition(EXAMPLE, environment="internal-lab")
        self.assertEqual(2, resolved.schema_version)
        self.assertEqual("internal-lab", resolved.environment)
        self.assertIsNotNone(resolved.snapshot)
        self.assertEqual("experiment", resolved.test.source_name)

    def test_rejects_external_test_and_sla_profile_indirection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "legacy.yaml"
            source.write_text(yaml.safe_dump({
                "name": "legacy",
                "test_definition": "smoke",
                "sla_profile": "delivery",
                "targets": [{"profile": "ckc"}],
            }), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Only self-contained"):
                resolve_experiment_definition(source, environment="internal-lab")


if __name__ == "__main__":
    unittest.main()
