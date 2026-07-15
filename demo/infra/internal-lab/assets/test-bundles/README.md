# Test Bundles

`demo/infra/internal-lab/assets/test-bundles` contains sequential internal-lab comparison
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
definition twice. Use `ckc-sync-dispatcher-comparison` to run suspend CKC plus the
blocking CKC sync IO and virtual-thread dispatcher modes on the baseline definition.
Use `queue-backlog-crash-comparison` to stress large in-memory order/batch
worker queues under downstream slowdown and app crashes, targeting listener
implementations that can acknowledge Kafka records before queued worker
processing reaches a terminal outcome.

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

tests:
  - name: ckc
    profile: ckc
    base_rate: 2000
    capacity_factor: 1.2
    replicas: 2
    order_processing_mode: AT_LEAST_ONCE_PARTITION_ORDERING
    batch_processing_mode: AT_LEAST_ONCE_PARTITION_ORDERING
    order_partitions: 8
    order_workers: 4
    order_pollers: 1
    test_definition: telemetry-freshness-fairness
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 2

  - name: spring-naive
    profile: spring-kafka-coroutines-naive
    test_definition: telemetry-freshness-fairness
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 4
```

Use `/opt/ckc-lab/bin/bundle-snippet.sh` after a tuned `run-test.sh` run to
print a ready-to-paste `tests:` item with the run's profile, base rate, capacity
factor, replicas, processing modes, generated partitions, workers, pollers, and
environment settings.
