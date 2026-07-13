# Internal-Lab Demo Helm Profiles

Deployment profiles in this chart are offered by the interactive internal-lab runner.

Starter-backed CKC profiles:

- `ckc-spring-boot`: CKC with consumers wired by the CKC Spring Boot starter and runtime settings in application configuration.

Sync CKC profiles:

- `ckc-sync`: CKC with blocking demo services on `Dispatchers.IO`.
- `ckc-sync-loom`: legacy overlay that runs `ckc-sync` with `PROCESSING_DISPATCHER_TYPE=VIRTUAL`.

Telemetry freshness fairness profiles:

- `ckc-telemetry-freshness-first-drop-oldest`: CKC telemetry overload using queue-level `FRESHNESS_FIRST_DROP_OLDEST`.
- `ckc-telemetry-freshness-first-replace-pending-by-key`: CKC telemetry overload using key-coalescing `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`.
- `spring-kafka-coroutines-naive-telemetry-threshold`: naive Spring Kafka coroutine worker overload using `FRESHNESS_FIRST_DROP_OLDEST` stale-threshold discard.
