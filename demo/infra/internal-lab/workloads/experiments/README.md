# Internal-lab Experiments

`demo/infra/internal-lab/workloads/experiments` contains experiment definitions.
An experiment runs one `test_definition` at one explicit `base_tps` against one
or more inline consumer targets.

Internal-lab experiments are synced to `/opt/ckc-lab/workloads/experiments`.
Start the interactive selector with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh
```

Run one or more named experiments with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison ckc-sync-dispatcher-comparison
```

Run every synced experiment sequentially with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh all
```

After the initial selection and experiment-wide settings prompt, targets run
unattended. Type `q` and press Enter to stop the current target gracefully,
finalize its raw audit log, and abort the remaining targets. Experiment runs
collect raw audit logs first and run audit analysis as a separate final phase.

Experiment-wide overrides can be passed without prompts:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison \
  --env PROCESSING_ENABLED=true \
  --env AUDIT_LOG_ENABLED=true \
  --env METRICS_IMPLEMENTATION=MICROMETER
```

Experiment schema:

```yaml
name: telemetry-fairness-profile-comparison
description: Compare telemetry fairness on the same fixed-fleet overload test.
test_definition: telemetry-freshness-fairness
base_tps: 2000

defaults:
  replicas: 2
  env:
    PROCESSING_ENABLED: true
    AUDIT_LOG_ENABLED: true
    METRICS_IMPLEMENTATION: MICROMETER

targets:
  - name: ckc.fixed.2
    profile: ckc
    planning_latency:
      order_ms: 2
      batch_ms: 5
      telemetry_ms: 35
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 2

  - name: spring-kafka-coroutines-naive.fixed.2
    profile: spring-kafka-coroutines-naive
    planning_latency:
      order_ms: 2
      batch_ms: 5
      telemetry_ms: 35
    telemetry_processing_mode: HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 2
```

`defaults.replicas` and target-level overrides are passed to the generated Helm
values. The shared `demo/infra/shared/workloads/consumer-profiles.yaml` catalog
only describes the reusable consumer-profile capabilities used by the planner;
environment and deployment choices should be visible in the experiment.

`planning_latency` is an experiment input for generated partitions, workers,
and pollers. It should be calibrated for this experiment's `base_tps`, target
consumer profile, and current lab infrastructure.

Use `/opt/ckc-lab/bin/target-draft.sh` after a tuned `run-test.sh` run to print
a ready-to-paste target draft with the run's profile, planning latencies,
replicas, processing modes, generated partitions, workers, pollers, and
environment settings.
