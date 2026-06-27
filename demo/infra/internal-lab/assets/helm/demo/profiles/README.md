# Internal-Lab Demo Helm Profiles

Deployment profiles in this chart are offered by the interactive internal-lab runner.

Telemetry freshness fairness profiles:

- `ckc-telemetry-freshness-first`: CKC telemetry overload using queue-level `FRESHNESS_FIRST`.
- `ckc-telemetry-freshness-first-by-key`: CKC telemetry overload using key-coalescing `FRESHNESS_FIRST_BY_KEY`.
- `spring-kafka-coroutines-naive-telemetry-threshold`: naive Spring Kafka coroutine worker overload using `FRESHNESS_FIRST` stale-threshold discard.
