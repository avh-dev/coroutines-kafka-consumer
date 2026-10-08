from __future__ import annotations

import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update-lab.sh"
TARGET_RUNNER = Path(__file__).resolve().parents[1] / "assets/libexec/run-target.sh"
PREPARE_TEST = Path(__file__).resolve().parents[1] / "assets/libexec/prepare-test.sh"
QUIESCE_APPLICATION = Path(__file__).resolve().parents[1] / "assets/libexec/quiesce-application.sh"


class UpdateLabSyncTest(unittest.TestCase):
    def test_per_topic_groups_are_comma_separated_and_cleanup_accepts_qualified_resources(self) -> None:
        prepare = PREPARE_TEST.read_text(encoding="utf-8")
        quiesce = QUIESCE_APPLICATION.read_text(encoding="utf-8")

        self.assertIn('print(",".join(', prepare)
        self.assertIn('name="${application#*/}"', quiesce)

    def test_target_runner_exports_topology_to_environment_evidence_collector(self) -> None:
        script = TARGET_RUNNER.read_text(encoding="utf-8")

        self.assertIn("export LAB_APPLICATION_LINK LAB_APPLICATION_HOST LAB_APPLICATION_TARGET", script)
        self.assertIn("export LAB_APPLICATION_NODE_SELECTOR LAB_CONTROLLER_NODE_SELECTOR", script)

    def test_target_runner_maps_per_target_workload_placement_to_lab_roles(self) -> None:
        script = TARGET_RUNNER.read_text(encoding="utf-8")
        prepare = PREPARE_TEST.read_text(encoding="utf-8")

        self.assertIn("node_selector_for_placement", script)
        self.assertIn('EXPERIMENT_STUBS_PLACEMENT:-controller', script)
        self.assertIn('EXPERIMENT_GENERATOR_PLACEMENT:-controller', script)
        self.assertIn("placement 'worker' requires a split internal lab", script)
        self.assertIn(
            'REQUESTED_APPLICATION_NODE_SELECTOR="${LAB_APPLICATION_NODE_SELECTOR:-}"', prepare
        )
        self.assertIn(
            'LAB_APPLICATION_NODE_SELECTOR="${REQUESTED_APPLICATION_NODE_SELECTOR:-${LAB_APPLICATION_NODE_SELECTOR:-}}"',
            prepare,
        )

    def test_target_runner_is_internal_and_public_run_test_is_removed(self) -> None:
        script = TARGET_RUNNER.read_text(encoding="utf-8")

        self.assertFalse((TARGET_RUNNER.parents[1] / "bin/run-test.sh").exists())
        self.assertIn('CKC_EXPERIMENT_INTERNAL:-', script)
        self.assertIn("lab experiment start", script)

    def test_thread_stats_agent_resolves_as_a_dependency_without_building_neighbor(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        root_build = (SCRIPT.parents[4] / "build.gradle.kts").read_text(encoding="utf-8")
        demo_build = (SCRIPT.parents[4] / "demo/ckc-demo/build.gradle.kts").read_text(encoding="utf-8")

        self.assertIn("resolve_thread_stats_agent", script)
        self.assertIn(".m2/repository", script)
        self.assertNotIn("mvnw", script)
        self.assertNotIn("../thread-stats", script)
        self.assertIn('tasks.register<Sync>("stageThreadStatsAgent")', root_build)
        self.assertIn('gradleProperty("threadStatsVersion")', root_build)
        self.assertIn('gradleProperty("threadStatsVersion")', demo_build)

    def test_unchanged_update_exits_before_remote_mutation_or_sync(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        fast_exit = script.index("Internal lab is already current")
        legacy_cleanup = script.index("rm -rf '${LEGACY_LAB_ROOT}'")
        first_sync = script.index("  sync_internal_lab_assets\n", fast_exit)
        self.assertLess(fast_exit, legacy_cleanup)
        self.assertLess(fast_exit, first_sync)
        self.assertIn('record_remote_fingerprint "update" "${UPDATE_FINGERPRINT}"', script)
        self.assertIn("no files transferred", script)

    def test_fingerprints_are_locale_independent_and_migrate_the_legacy_value(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('LC_ALL="${sort_locale}" sort -z', script)
        self.assertIn('fingerprint_paths_with_locale C "$@"', script)
        self.assertIn('REMOTE_PROBE}" == "inactive|${LEGACY_UPDATE_FINGERPRINT}|complete', script)
        self.assertIn("Migrated locale-dependent lab fingerprints", script)
        self.assertIn('record_remote_image_fingerprint demo "${DEMO_FINGERPRINT}"', script)

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

    def test_user_service_installer_enables_durable_telegram_retry_timer(self) -> None:
        installer = (SCRIPT.parents[1] / "assets/libexec/install-user-service.sh").read_text(encoding="utf-8")
        systemd = SCRIPT.parents[1] / "assets/systemd"

        self.assertIn("systemctl --user enable --now ckc-telegram-dispatch.timer", installer)
        self.assertIn("systemctl --user start --no-block ckc-telegram-dispatch.service", installer)
        self.assertTrue((systemd / "ckc-telegram-dispatch.service.in").is_file())
        self.assertTrue((systemd / "ckc-telegram-dispatch.timer.in").is_file())

    def test_update_checks_worker_and_distributes_images(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        rebuild = (SCRIPT.parents[1] / "assets/libexec/rebuild-images.sh").read_text(encoding="utf-8")

        self.assertIn("worker_image_is_current", script)
        self.assertIn("systemctl is-active --quiet k3s-agent", script)
        self.assertIn("LAB_APPLICATION_TARGET", rebuild)
        self.assertIn("import-k3s-images", rebuild)
        self.assertIn('load-test)', script)
        self.assertIn('demo|demo-stubs|load-test)', rebuild)
        self.assertIn('REBUILD_ARGS+=("load-test=${LOAD_TEST_FINGERPRINT}")', script)

    def test_syncs_only_the_canonical_experiment_catalog(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'demo/infra/experiments" "${LAB_ROOT}/experiments"',
            script,
        )
        self.assertNotIn("shared/workloads", script)
        self.assertNotIn("internal-lab/workloads", script)
        self.assertIn("demo/infra/shared/result_bundle", script)
        self.assertIn("demo/infra/shared/experiment_notifications", script)
        self.assertIn("demo/infra/shared/kafka_warmup", script)

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
        script = TARGET_RUNNER.read_text(encoding="utf-8")

        self.assertIn('if [ -n "${DEPLOYMENT_PLAN_PATH}" ]; then', script)
        self.assertIn('STUB_REPLICA_COUNT=""', script)

    def test_load_generator_packet_capture_is_scoped_and_enabled(self) -> None:
        script = TARGET_RUNNER.read_text(encoding="utf-8")

        self.assertIn('LOAD_RENDER_ARGS+=(--packet-capture)', script)
        self.assertIn(
            '--load-test-selector "app.kubernetes.io/name=ckc-load-test,ckc.dev/test-run-id=${RUN_ID}"',
            script,
        )


if __name__ == "__main__":
    unittest.main()
