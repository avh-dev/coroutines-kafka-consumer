# Load Test

This module contains the Kotlin-based traffic generator for the demo domain.

Module: `ckc-demo-load-test`

Current scope:

- load scenario phases over time
- external shard identity for local runs and Kubernetes indexed jobs
- in-process generator workers with isolated simulation state
- event-generator driven order, batch, and cauldron publishers over one base TPS

It is intended to grow into the main load-test entry point for:

- local end-to-end checks against Kafka, Redis, and demo stubs
- cloud load tests with multiple external generator shards

## Load Profile

The current load profile format is a single string, intended to be passed as an env var or CLI setting.

Example:

```text
0 -> (200s, warmup) -> 100 -> (1600s, maximum) -> 100 -> (100s, cool-down) -> 0
```

Rules:

- the profile starts with an integer percentage of `BASE_TPS`
- then alternates between `(duration, optional label)` and the next integer target percentage
- labels are kept for logging and diagnostics
- durations use compact units: `s`, `m`, `h`
- `BASE_TPS` defines total generated messages per second at profile `100` for this JVM process
- `LOAD_TEST_WORKERS` controls in-process generator workers; it defaults to available CPU cores
- workers split `BASE_TPS` across themselves and keep separate simulation state
- active worker count is capped by `BASE_TPS` so each worker has at least one integer TPS permit
- generated entity ids include both identity dimensions, for example `order-1-5-00021212`
- `ORDER_EVENT_PERCENT`, `BATCH_EVENT_PERCENT`, and `CAULDRON_TELEMETRY_PERCENT` split that total event budget across topics
- event generators use state queues when a suitable simulated entity exists and delegate prerequisite event generation while the state is warming up
- `BREWING_STEP_BURST_EVERY`, `MIN_BREWING_STEP_BURST`, and `MAX_BREWING_STEP_BURST` emit same-key `BATCH_BREWING_STEP_COMPLETED` bursts, capped by remaining batch brewing steps, so ordered-by-key contention is observable without increasing total TPS
- `TELEMETRY_SOURCE_MODE=FIXED_FLEET` prepares one active synthetic batch per configured cauldron and emits telemetry round-robin across the fixed cauldron fleet
- the default `TELEMETRY_SOURCE_MODE=ACTIVE_BATCHES` keeps cauldron telemetry tied to batches that are active in the simulated business pipeline
- `PUBLISH_ENABLED=false` keeps generation and audit output enabled but skips Kafka sends for local debugging
- the load-test process flushes producers and exits when the profile schedule ends
