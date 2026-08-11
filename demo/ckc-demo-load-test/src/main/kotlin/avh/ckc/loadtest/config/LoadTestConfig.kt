package avh.ckc.loadtest.config

import java.time.Duration

data class LoadTestConfig(
    val bootstrapServers: String,
    val orderEventsTopic: String,
    val batchEventsTopic: String,
    val cauldronEventsTopic: String,
    val baseTps: Int,
    val orderEventPercent: Int,
    val batchEventPercent: Int,
    val cauldronTelemetryPercent: Int,
    val loadProfile: String,
    val cauldronCount: Int,
    val minOrdersPerBatch: Int,
    val maxOrdersPerBatch: Int,
    val minBrewingSteps: Int,
    val maxBrewingSteps: Int,
    val brewingStepBurstEvery: Int,
    val minBrewingStepBurst: Int,
    val maxBrewingStepBurst: Int,
    val maxBurst: Int,
    val statsLogInterval: Duration,
    val diagnosticsBlobSize: Int,
    val telemetrySourceMode: TelemetrySourceMode,
    val publishEnabled: Boolean,
    val auditLogEnabled: Boolean,
    val auditHost: String = "127.0.0.1",
    val auditPort: Int = 5170,
    val auditRunId: String = "local",
    val generatorWorkers: Int = defaultGeneratorWorkers(),
    val kafkaProducer: KafkaProducerSettings = KafkaProducerSettings(),
    val producerCapacity: TopicProducerCapacity = TopicProducerCapacity(),
    val metricsPort: Int = 9405
) {
    init {
        require(baseTps > 0) { "baseTps must be positive" }
        require(orderEventPercent >= 0) { "orderEventPercent must be non-negative" }
        require(batchEventPercent >= 0) { "batchEventPercent must be non-negative" }
        require(cauldronTelemetryPercent >= 0) { "cauldronTelemetryPercent must be non-negative" }
        require(orderEventPercent + batchEventPercent + cauldronTelemetryPercent > 0) {
            "at least one topic traffic percentage must be positive"
        }
        require(cauldronCount > 0) { "cauldronCount must be positive" }
        require(minOrdersPerBatch > 0) { "minOrdersPerBatch must be positive" }
        require(maxOrdersPerBatch >= minOrdersPerBatch) { "maxOrdersPerBatch must be >= minOrdersPerBatch" }
        require(minBrewingSteps > 0) { "minBrewingSteps must be positive" }
        require(maxBrewingSteps >= minBrewingSteps) { "maxBrewingSteps must be >= minBrewingSteps" }
        require(brewingStepBurstEvery >= 0) { "brewingStepBurstEvery must be non-negative" }
        require(minBrewingStepBurst > 0) { "minBrewingStepBurst must be positive" }
        require(maxBrewingStepBurst >= minBrewingStepBurst) { "maxBrewingStepBurst must be >= minBrewingStepBurst" }
        require(maxBurst > 0) { "maxBurst must be positive" }
        require(!statsLogInterval.isNegative && !statsLogInterval.isZero) { "statsLogInterval must be positive" }
        require(diagnosticsBlobSize >= 0) { "diagnosticsBlobSize must be non-negative" }
        require(auditHost.isNotBlank()) { "auditHost must not be blank" }
        require(auditPort > 0) { "auditPort must be positive" }
        require(auditRunId.isNotBlank()) { "auditRunId must not be blank" }
        require(generatorWorkers > 0) { "generatorWorkers must be positive" }
        require(kafkaProducer.lingerMs >= 0) { "kafkaProducer.lingerMs must be non-negative" }
        require(kafkaProducer.batchSize > 0) { "kafkaProducer.batchSize must be positive" }
        require(kafkaProducer.bufferMemory > 0) { "kafkaProducer.bufferMemory must be positive" }
        require(kafkaProducer.compressionType.isNotBlank()) { "kafkaProducer.compressionType must not be blank" }
        require(producerCapacity.orderTps > 0) { "producerCapacity.orderTps must be positive" }
        require(producerCapacity.batchTps > 0) { "producerCapacity.batchTps must be positive" }
        require(producerCapacity.cauldronTelemetryTps > 0) { "producerCapacity.cauldronTelemetryTps must be positive" }
        require(metricsPort in 1..65535) { "metricsPort must be a valid TCP port" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestConfig =
            LoadTestConfig(
                bootstrapServers = environment["BOOTSTRAP_SERVERS"] ?: "localhost:9092",
                orderEventsTopic = environment["ORDER_EVENTS_TOPIC"] ?: "order.events.v1",
                batchEventsTopic = environment["BATCH_EVENTS_TOPIC"] ?: "batch.events.v1",
                cauldronEventsTopic = environment["CAULDRON_EVENTS_TOPIC"] ?: "cauldron.events.v1",
                baseTps = environment["BASE_TPS"]?.toIntOrNull() ?: 10_000,
                orderEventPercent = environment["ORDER_EVENT_PERCENT"]?.toIntOrNull() ?: 40,
                batchEventPercent = environment["BATCH_EVENT_PERCENT"]?.toIntOrNull() ?: 20,
                cauldronTelemetryPercent = environment["CAULDRON_TELEMETRY_PERCENT"]?.toIntOrNull() ?: 40,
                loadProfile = environment["LOAD_PROFILE"]
                    ?: "0 -> (60s, warmup) -> 100 -> (120s, maximum) -> 100 -> (30s, cool-down) -> 0",
                cauldronCount = environment["CAULDRON_COUNT"]?.toIntOrNull() ?: 32,
                minOrdersPerBatch = environment["MIN_ORDERS_PER_BATCH"]?.toIntOrNull() ?: 3,
                maxOrdersPerBatch = environment["MAX_ORDERS_PER_BATCH"]?.toIntOrNull() ?: 8,
                minBrewingSteps = environment["MIN_BREWING_STEPS"]?.toIntOrNull() ?: 5,
                maxBrewingSteps = environment["MAX_BREWING_STEPS"]?.toIntOrNull() ?: 10,
                brewingStepBurstEvery = environment["BREWING_STEP_BURST_EVERY"]?.toIntOrNull() ?: 1,
                minBrewingStepBurst = environment["MIN_BREWING_STEP_BURST"]?.toIntOrNull() ?: 5,
                maxBrewingStepBurst = environment["MAX_BREWING_STEP_BURST"]?.toIntOrNull() ?: 10,
                maxBurst = environment["MAX_BURST"]?.toIntOrNull() ?: 1000,
                statsLogInterval = Duration.ofSeconds(environment["STATS_LOG_INTERVAL_SECONDS"]?.toLongOrNull() ?: 30L),
                diagnosticsBlobSize = environment["DIAGNOSTICS_BLOB_SIZE"]?.toIntOrNull() ?: 512,
                telemetrySourceMode = environment["TELEMETRY_SOURCE_MODE"]
                    ?.let(TelemetrySourceMode::valueOf)
                    ?: TelemetrySourceMode.ACTIVE_BATCHES,
                publishEnabled = environment["PUBLISH_ENABLED"]?.toBooleanStrictOrNull() ?: true,
                auditLogEnabled = environment["AUDIT_LOG_ENABLED"]?.toBooleanStrictOrNull() ?: true,
                auditHost = environment["AUDIT_TCP_HOST"] ?: "127.0.0.1",
                auditPort = environment["AUDIT_TCP_PORT"]?.toIntOrNull() ?: 5170,
                auditRunId = environment["AUDIT_RUN_ID"] ?: environment["TEST_RUN_ID"] ?: "local",
                generatorWorkers = environment["LOAD_TEST_WORKERS"]?.toIntOrNull() ?: defaultGeneratorWorkers(),
                kafkaProducer = KafkaProducerSettings.fromEnvironment(environment),
                producerCapacity = TopicProducerCapacity.fromEnvironment(environment),
                metricsPort = environment["LOAD_TEST_METRICS_PORT"]?.toIntOrNull() ?: 9405
            )
    }
}

data class TopicProducerCapacity(
    val orderTps: Int = 1_000,
    val batchTps: Int = 1_000,
    val cauldronTelemetryTps: Int = 1_000
) {
    companion object {
        fun fromEnvironment(environment: Map<String, String>): TopicProducerCapacity =
            TopicProducerCapacity(
                orderTps = environment["ORDER_TPS_PER_PRODUCER"]?.toIntOrNull() ?: 1_000,
                batchTps = environment["BATCH_TPS_PER_PRODUCER"]?.toIntOrNull() ?: 1_000,
                cauldronTelemetryTps = environment["CAULDRON_TELEMETRY_TPS_PER_PRODUCER"]?.toIntOrNull() ?: 1_000
            )
    }
}

data class KafkaProducerSettings(
    val lingerMs: Int = 20,
    val batchSize: Int = 64 * 1024,
    val compressionType: String = "lz4",
    val bufferMemory: Long = 32 * 1024 * 1024L
) {
    companion object {
        fun fromEnvironment(environment: Map<String, String>): KafkaProducerSettings =
            KafkaProducerSettings(
                lingerMs = environment["KAFKA_PRODUCER_LINGER_MS"]?.toIntOrNull() ?: 20,
                batchSize = environment["KAFKA_PRODUCER_BATCH_SIZE"]?.toIntOrNull() ?: 64 * 1024,
                compressionType = environment["KAFKA_PRODUCER_COMPRESSION_TYPE"]?.takeIf(String::isNotBlank) ?: "lz4",
                bufferMemory = environment["KAFKA_PRODUCER_BUFFER_MEMORY"]?.toLongOrNull() ?: 32 * 1024 * 1024L
            )
    }
}

enum class TelemetrySourceMode {
    ACTIVE_BATCHES,
    FIXED_FLEET
}

private fun defaultGeneratorWorkers(): Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
