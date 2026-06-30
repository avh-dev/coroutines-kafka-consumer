# Test Bundles

`demo/infra/shared/test-bundles` contains sequential internal-lab comparison
bundles. A bundle calls the existing single-test runner several times with a
shared default environment and per-test overrides.

Internal-lab bundles are synced to `/opt/ckc-lab/test-bundles`. Start the
interactive selector with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-bundle.sh
```

Or run a specific bundle with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-bundle.sh telemetry-fairness-profile-comparison
```

Run every synced bundle sequentially with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-bundle.sh all
```

After the initial selection and bundle-wide settings prompt, bundle tests run
unattended. Type `q` and press Enter to stop the current test gracefully,
finalize its raw audit log, and abort the remaining tests in the bundle run.
Bundle runs collect raw audit logs first and run audit analysis as a separate
final phase.

Use `smoke-repeat` for quick bundle smoke checks; it runs the short `smoke`
definition twice.

Bundle-wide overrides can be passed without prompts:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-bundle.sh telemetry-fairness-profile-comparison \
  --env PROCESSING_ENABLED=true \
  --env AUDIT_LOG_ENABLED=true \
  --env METRICS_IMPLEMENTATION=MICROMETER
```

If `/opt/ckc-lab/notify/notify.sh` or `/opt/ckc-lab/notify/notify.py` exists
and is executable, the bundle runner calls it as
`notify-hook event-name payload.json`.
See `/opt/ckc-lab/notify/README.md` and
`/opt/ckc-lab/notify/notify-telegram.py` for a Telegram hook example that keeps
bot tokens out of the repository.

Bundle schema:

```yaml
name: telemetry-fairness-profile-comparison
description: Compare telemetry fairness on the same fixed-fleet overload test.

defaults:
  env:
    PROCESSING_ENABLED: true
    AUDIT_LOG_ENABLED: true
    METRICS_IMPLEMENTATION: MICROMETER
    WORKER_DISPATCHER_THREADS: 2

tests:
  - name: ckc
    deployment: ckc
    test_definition: telemetry-freshness-fairness

  - name: spring-naive
    deployment: spring-kafka-coroutines-naive
    test_definition: telemetry-freshness-fairness
    env:
      WORKER_DISPATCHER_THREADS: 4
```
