package avh.ckc.loadtest.config

import java.time.Duration

data class LoadTestConfig(
    val bootstrapServers: String,
    val orderLifecycleTopic: String,
    val cauldronTelemetryTopic: String,
    val baseRate: Int,
    val telemetryRateMultiplier: Double,
    val loadProfile: String,
    val cauldronCount: Int,
    val ordersPerBatch: Int,
    val maxBatchWait: Duration,
    val brewDuration: Duration,
    val tickInterval: Duration,
    val diagnosticsBlobSize: Int
) {
    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestConfig =
            LoadTestConfig(
                bootstrapServers = environment["BOOTSTRAP_SERVERS"] ?: "localhost:9092",
                orderLifecycleTopic = environment["ORDER_LIFECYCLE_TOPIC"] ?: "potion.orders.lifecycle.v1",
                cauldronTelemetryTopic = environment["CAULDRON_TELEMETRY_TOPIC"] ?: "potion.cauldrons.telemetry.v1",
                baseRate = environment["BASE_RATE"]?.toIntOrNull() ?: 1000,
                telemetryRateMultiplier = environment["TELEMETRY_RATE_MULTIPLIER"]?.toDoubleOrNull() ?: 10.0,
                loadProfile = environment["LOAD_PROFILE"]
                    ?: "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0",
                cauldronCount = environment["CAULDRON_COUNT"]?.toIntOrNull() ?: 8,
                ordersPerBatch = environment["ORDERS_PER_BATCH"]?.toIntOrNull() ?: 3,
                maxBatchWait = Duration.ofSeconds(environment["MAX_BATCH_WAIT_SECONDS"]?.toLongOrNull() ?: 30L),
                brewDuration = Duration.ofSeconds(environment["BREW_DURATION_SECONDS"]?.toLongOrNull() ?: 120L),
                tickInterval = Duration.ofMillis(environment["TICK_INTERVAL_MILLIS"]?.toLongOrNull() ?: 200L),
                diagnosticsBlobSize = environment["DIAGNOSTICS_BLOB_SIZE"]?.toIntOrNull() ?: 512
            )
    }
}
