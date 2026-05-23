# Load Test

This module contains the Kotlin-based traffic generator for the demo domain.

Module: `ckc-demo-load-test`

Current scope:

- load scenario phases over time
- shard identity for local runs and Kubernetes indexed jobs
- rate-driven order, batch, and cauldron publishers that keep demo-domain event shapes coherent

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

- the profile starts with an integer percentage of each stream base rate
- then alternates between `(duration, optional label)` and the next integer target percentage
- labels are kept for logging and diagnostics
- durations use compact units: `s`, `m`, `h`
- `LIFECYCLE_BASE_RATE` defines order/batch lifecycle messages per second for `100`
- `TELEMETRY_BASE_RATE` defines telemetry messages per second for `100`
- the load-test process flushes producers and exits when the profile schedule ends
