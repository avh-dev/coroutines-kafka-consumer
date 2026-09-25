from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update-lab.sh"
RUN_TEST = Path(__file__).resolve().parents[1] / "assets/bin/run-test.sh"


class UpdateLabSyncTest(unittest.TestCase):
    def test_lab_entrypoints_install_thread_stats_starter_and_agent(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("-pl thread-stats-agent,thread-stats-spring-boot-starter", script)
        self.assertIn("-am install", script)

    def test_update_uses_runtime_user_and_no_privileged_package_install(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('LAB_TARGET="${LAB_USER}@${LAB_SSH_HOST}"', script)
        self.assertNotIn('ssh "root@', script)
        self.assertNotIn("apt-get", script)
        self.assertNotIn("chown", script)

    def test_update_refuses_to_mutate_an_active_managed_experiment(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('systemctl --user is-active --quiet ckc-experiment.service', script)
        self.assertIn("stop it before updating the lab", script)
        self.assertIn("install-user-service.sh", script)

    def test_update_checks_worker_and_distributes_images(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        rebuild = (SCRIPT.parents[1] / "assets/libexec/rebuild-images.sh").read_text(encoding="utf-8")

        self.assertIn("worker_image_is_current", script)
        self.assertIn("systemctl is-active --quiet k3s-agent", script)
        self.assertIn("LAB_APPLICATION_TARGET", rebuild)
        self.assertIn("import-k3s-images", rebuild)

    def test_syncs_only_the_canonical_experiment_catalog(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'demo/infra/experiments" "${LAB_ROOT}/experiments"',
            script,
        )
        self.assertNotIn("shared/workloads", script)
        self.assertNotIn("internal-lab/workloads", script)
        self.assertIn("demo/infra/shared/result_bundle", script)

    def test_materializes_the_shared_dashboard_for_internal_lab(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('result_bundle/dashboard.py"', script)
        self.assertIn("--environment internal-lab", script)
        self.assertIn("--kafka-mode kubernetes", script)
        self.assertNotIn(
            'sync_path "${REPO_ROOT}/demo/infra/shared/grafana/dashboards"',
            script,
        )

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
