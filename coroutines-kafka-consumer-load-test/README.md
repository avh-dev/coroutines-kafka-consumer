# Load Test

This module contains the Kotlin-based traffic generator for the demo domain.

Current scope:

- load scenario phases over time
- shard identity for local runs and Kubernetes indexed jobs
- a simple lifecycle state machine that generates coherent order events

It is intended to grow into the main load-test entry point for:

- local end-to-end checks against Kafka, Redis, and WireMock
- cloud load tests with multiple generator shards

## Load Profile

The current load profile format is a single string, intended to be passed as an env var or CLI setting.

Example:

```text
0 -> (200s, warmup) -> 100 -> (1600s, maximum) -> 100 -> (100s, cool-down) -> 0
```

Rules:

- the profile starts with an integer percentage of `BASE_RATE`
- then alternates between `(duration, optional label)` and the next integer target percentage
- labels are kept for logging and diagnostics
- durations use compact units: `s`, `m`, `h`
- `BASE_RATE` defines the effective rate for `100`
