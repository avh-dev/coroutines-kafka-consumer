package avh.ckc.loadtest.config

import java.time.Duration

data class LoadTestConfig(
    val bootstrapServers: String,
    val orderLifecycleTopic: String,
    val cauldronTelemetryTopic: String,
    val lifecycleBaseRate: Int,
    val telemetryBaseRate: Int,
    val loadProfile: String,
    val lifecycleOrdersPerBatch: Int,
    val telemetryInterval: Duration,
    val tickInterval: Duration,
    val diagnosticsBlobSize: Int,
    val auditLogEnabled: Boolean
) {
    init {
        require(lifecycleBaseRate > 0) { "lifecycleBaseRate must be positive" }
        require(telemetryBaseRate > 0) { "telemetryBaseRate must be positive" }
        require(lifecycleOrdersPerBatch > 0) { "lifecycleOrdersPerBatch must be positive" }
        require(!telemetryInterval.isNegative && !telemetryInterval.isZero) { "telemetryInterval must be positive" }
        require(!tickInterval.isNegative && !tickInterval.isZero) { "tickInterval must be positive" }
        require(diagnosticsBlobSize >= 0) { "diagnosticsBlobSize must be non-negative" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestConfig =
            LoadTestConfig(
                bootstrapServers = environment["BOOTSTRAP_SERVERS"] ?: "localhost:9092",
                orderLifecycleTopic = environment["ORDER_LIFECYCLE_TOPIC"] ?: "potion.orders.lifecycle.v1",
                cauldronTelemetryTopic = environment["CAULDRON_TELEMETRY_TOPIC"] ?: "potion.cauldrons.telemetry.v1",
                lifecycleBaseRate = environment["LIFECYCLE_BASE_RATE"]?.toIntOrNull() ?: 1000,
                telemetryBaseRate = environment["TELEMETRY_BASE_RATE"]?.toIntOrNull() ?: 10_000,
                loadProfile = environment["LOAD_PROFILE"]
                    ?: "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0",
                lifecycleOrdersPerBatch = environment["LIFECYCLE_ORDERS_PER_BATCH"]?.toIntOrNull() ?: 3,
                telemetryInterval = Duration.ofSeconds(environment["TELEMETRY_INTERVAL_SECONDS"]?.toLongOrNull() ?: 10L),
                tickInterval = Duration.ofMillis(environment["TICK_INTERVAL_MILLIS"]?.toLongOrNull() ?: 200L),
                diagnosticsBlobSize = environment["DIAGNOSTICS_BLOB_SIZE"]?.toIntOrNull() ?: 512,
                auditLogEnabled = environment["AUDIT_LOG_ENABLED"]?.toBooleanStrictOrNull() ?: true
            )
    }
}
