package avh.ckc.loadtest.config

import java.time.Duration
import kotlinx.serialization.json.Json

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
    val topicKafkaProducers: TopicKafkaProducerSettings = TopicKafkaProducerSettings.shared(kafkaProducer),
    val producerConfigSteps: List<ProducerConfigStep> = emptyList(),
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
        topicKafkaProducers.all().forEach { (topic, producer) ->
            require(producer.lingerMs >= 0) { "$topic kafkaProducer.lingerMs must be non-negative" }
            require(producer.batchSize > 0) { "$topic kafkaProducer.batchSize must be positive" }
            require(producer.bufferMemory > 0) { "$topic kafkaProducer.bufferMemory must be positive" }
            require(producer.compressionType.isNotBlank()) { "$topic kafkaProducer.compressionType must not be blank" }
        }
        producerConfigSteps.forEachIndexed { index, step ->
            require(step.atSeconds >= 0) { "producerConfigSteps[$index].atSeconds must be non-negative" }
            if (index > 0) {
                require(step.atSeconds >= producerConfigSteps[index - 1].atSeconds) {
                    "producerConfigSteps must be ordered by atSeconds"
                }
            }
            step.validate("producerConfigSteps[$index]")
        }
        require(producerCapacity.orderTps > 0) { "producerCapacity.orderTps must be positive" }
        require(producerCapacity.batchTps > 0) { "producerCapacity.batchTps must be positive" }
        require(producerCapacity.cauldronTelemetryTps > 0) { "producerCapacity.cauldronTelemetryTps must be positive" }
        require(metricsPort in 1..65535) { "metricsPort must be a valid TCP port" }
    }

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): LoadTestConfig {
            val sharedKafkaProducer = KafkaProducerSettings.fromEnvironment(environment)
            return LoadTestConfig(
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
                kafkaProducer = sharedKafkaProducer,
                topicKafkaProducers = TopicKafkaProducerSettings.fromEnvironment(environment, sharedKafkaProducer),
                producerConfigSteps = parseProducerConfigSteps(environment["PRODUCER_CONFIG_STEPS_JSON"]),
                producerCapacity = TopicProducerCapacity.fromEnvironment(environment),
                metricsPort = environment["LOAD_TEST_METRICS_PORT"]?.toIntOrNull() ?: 9405
            )
        }

        private fun parseProducerConfigSteps(value: String?): List<ProducerConfigStep> =
            Json.decodeFromString(value?.takeIf(String::isNotBlank) ?: "[]")
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
    fun withOverrides(step: ProducerConfigStep): KafkaProducerSettings = copy(
        lingerMs = step.lingerMs ?: lingerMs,
        batchSize = step.batchSize ?: batchSize,
        compressionType = step.compressionType?.takeIf(String::isNotBlank) ?: compressionType,
        bufferMemory = step.bufferMemory ?: bufferMemory
    )

    companion object {
        fun fromEnvironment(
            environment: Map<String, String>,
            prefix: String = "",
            fallback: KafkaProducerSettings = KafkaProducerSettings()
        ): KafkaProducerSettings =
            KafkaProducerSettings(
                lingerMs = environment["${prefix}KAFKA_PRODUCER_LINGER_MS"]?.toIntOrNull() ?: fallback.lingerMs,
                batchSize = environment["${prefix}KAFKA_PRODUCER_BATCH_SIZE"]?.toIntOrNull() ?: fallback.batchSize,
                compressionType = environment["${prefix}KAFKA_PRODUCER_COMPRESSION_TYPE"]
                    ?.takeIf(String::isNotBlank)
                    ?: fallback.compressionType,
                bufferMemory = environment["${prefix}KAFKA_PRODUCER_BUFFER_MEMORY"]?.toLongOrNull()
                    ?: fallback.bufferMemory
            )
    }
}

data class TopicKafkaProducerSettings(
    val order: KafkaProducerSettings,
    val batch: KafkaProducerSettings,
    val telemetry: KafkaProducerSettings
) {
    fun all(): List<Pair<String, KafkaProducerSettings>> = listOf(
        "order" to order,
        "batch" to batch,
        "telemetry" to telemetry
    )

    companion object {
        fun shared(settings: KafkaProducerSettings): TopicKafkaProducerSettings =
            TopicKafkaProducerSettings(settings, settings, settings)

        fun fromEnvironment(
            environment: Map<String, String>,
            shared: KafkaProducerSettings
        ): TopicKafkaProducerSettings = TopicKafkaProducerSettings(
            order = KafkaProducerSettings.fromEnvironment(environment, "ORDER_", shared),
            batch = KafkaProducerSettings.fromEnvironment(environment, "BATCH_", shared),
            telemetry = KafkaProducerSettings.fromEnvironment(environment, "TELEMETRY_", shared)
        )
    }
}

enum class TelemetrySourceMode {
    ACTIVE_BATCHES,
    FIXED_FLEET
}

private fun defaultGeneratorWorkers(): Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
