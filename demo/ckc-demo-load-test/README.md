# Load Test

This module contains the Kotlin-based traffic generator for the demo domain.

Module: `ckc-demo-load-test`

Current scope:

- load scenario phases over time
- shard identity for local runs and Kubernetes indexed jobs
- event-generator driven order, batch, and cauldron publishers over one base TPS

It is intended to grow into the main load-test entry point for:

- local end-to-end checks against Kafka, Redis, and demo stubs
- cloud load tests with multiple generator shards

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
- `BASE_TPS` defines total generated messages per second at profile `100`
- `ORDER_EVENT_PERCENT`, `BATCH_EVENT_PERCENT`, and `CAULDRON_TELEMETRY_PERCENT` split that total event budget across topics
- event generators use state queues when a suitable simulated entity exists and `fake-*` fallback entities while the state is warming up
- the load-test process flushes producers and exits when the profile schedule ends
