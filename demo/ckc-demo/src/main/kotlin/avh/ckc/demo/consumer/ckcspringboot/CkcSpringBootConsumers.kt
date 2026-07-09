package avh.ckc.demo.consumer.ckcspringboot

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logDropped
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import avh.ckc.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.micrometer.RecordDrivenTagExtractors
import avh.ckc.micrometer.RecordMetricTagDefinition
import avh.ckc.micrometer.micrometerConsumerMetrics
import avh.ckc.micrometer.recordDrivenTagExtractors
import avh.ckc.spring.CkcConsumer
import avh.ckc.spring.CkcConsumerMetrics
import avh.ckc.spring.CkcConsumerProperties
import avh.ckc.spring.CkcKafkaConsumer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Profile("ckc-spring-boot")
@CkcKafkaConsumer(name = "order-events")
class OrderLifecycleCkcSpringBootConsumer(
    private val service: SuspendOrderLifecycleService,
    private val properties: DemoApplicationProperties
) : CkcConsumer<String, OrderLifecycleEvent> {
    override suspend fun process(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        val value = record.value() ?: return
        if (properties.consumers.processingEnabled) {
            service.apply(value)
        } else {
            latencyOnlyDelay()
        }
        logProcessed(record, properties.audit)
    }

    override suspend fun handleFailure(record: ConsumerRecord<String, OrderLifecycleEvent>, reason: Throwable) {
        logFailed(record, properties.audit)
    }
}

@Profile("ckc-spring-boot")
@CkcKafkaConsumer(name = "batch-events")
class BatchLifecycleCkcSpringBootConsumer(
    private val service: SuspendBatchLifecycleService,
    private val properties: DemoApplicationProperties
) : CkcConsumer<String, BatchLifecycleEvent> {
    override suspend fun process(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        val value = record.value() ?: return
        if (properties.consumers.processingEnabled) {
            service.apply(value)
        } else {
            latencyOnlyDelay()
        }
        logProcessed(record, properties.audit)
    }

    override suspend fun handleFailure(record: ConsumerRecord<String, BatchLifecycleEvent>, reason: Throwable) {
        logFailed(record, properties.audit)
    }
}

@Profile("ckc-spring-boot")
@CkcKafkaConsumer(name = "cauldron-events")
class CauldronTelemetryCkcSpringBootConsumer(
    private val service: SuspendCauldronTelemetryService,
    private val properties: DemoApplicationProperties
) : CkcConsumer<String, CauldronTelemetryEvent> {
    override suspend fun process(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        val value = record.value() ?: return
        if (properties.consumers.processingEnabled) {
            service.recalculate(value)
        } else {
            latencyOnlyDelay()
        }
        logProcessed(record, properties.audit)
    }

    override suspend fun handleFailure(record: ConsumerRecord<String, CauldronTelemetryEvent>, reason: Throwable) {
        logFailed(record, properties.audit)
    }
}

@Profile("ckc-spring-boot")
@Configuration(proxyBeanMethods = false)
class CkcSpringBootMetricsConfiguration {
    @Bean
    @CkcConsumerMetrics(consumer = "order-events")
    fun orderConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        micrometerConsumerMetrics<String, OrderLifecycleEvent>(demoMicrometerSchema(meterRegistry, ckcProperties)) {
            consumerId = "order-events"
            recordDrivenTagExtractors = orderEventRecordTags()
        }.withAudit(properties.audit)

    @Bean
    @CkcConsumerMetrics(consumer = "batch-events")
    fun batchConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        micrometerConsumerMetrics<String, BatchLifecycleEvent>(demoMicrometerSchema(meterRegistry, ckcProperties)) {
            consumerId = "batch-events"
            recordDrivenTagExtractors = batchEventRecordTags()
        }.withAudit(properties.audit)

    @Bean
    @CkcConsumerMetrics(consumer = "cauldron-events")
    fun cauldronConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        micrometerConsumerMetrics<String, CauldronTelemetryEvent>(demoMicrometerSchema(meterRegistry, ckcProperties)) {
            consumerId = "cauldron-events"
            recordDrivenTagExtractors = cauldronEventRecordTags()
        }.withAudit(properties.audit)

    fun orderEventRecordTags(): RecordDrivenTagExtractors<String, OrderLifecycleEvent> =
        recordDrivenTagExtractors {
            tag(EVENT_TYPE_TAG) { record -> record.value()?.eventType?.name }
        }

    fun batchEventRecordTags(): RecordDrivenTagExtractors<String, BatchLifecycleEvent> =
        recordDrivenTagExtractors {
            tag(EVENT_TYPE_TAG) { record -> record.value()?.eventType?.name }
        }

    fun cauldronEventRecordTags(): RecordDrivenTagExtractors<String, CauldronTelemetryEvent> =
        recordDrivenTagExtractors {
            tag(EVENT_TYPE_TAG) { "CAULDRON_TELEMETRY" }
        }

    private fun demoMicrometerSchema(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties
    ): MicrometerConsumerMetricsSchema {
        val configuredSchemas = ckcProperties.metrics.micrometer.schemas
        val schemaName = ckcProperties.metrics.micrometer.defaultSchema
            ?: "default".takeIf { it in configuredSchemas }
            ?: configuredSchemas.keys.singleOrNull()
            ?: error("Missing CKC Spring Boot demo Micrometer schema")
        val schema = configuredSchemas[schemaName]
            ?: error("Unknown CKC Spring Boot demo Micrometer schema '$schemaName'")
        return MicrometerConsumerMetricsSchema(
            meterRegistry = meterRegistry,
            metricPrefix = schema.metricPrefix,
            staticTags = schema.staticTags.map { Tag.of(it.name, it.value) },
            recordDrivenTags = schema.recordDrivenTags.map { RecordMetricTagDefinition(it.name, it.default) }
        )
    }

    private fun <K, V> ConsumerMetrics<K, V>.withAudit(
        audit: DemoApplicationProperties.Audit
    ): ConsumerMetrics<K, V> {
        val delegate = this
        return object : ConsumerMetrics<K, V> by delegate {
            override fun onRetry(key: K?, value: V?, record: ConsumerRecord<K, V>, attempt: Int, error: Throwable) {
                delegate.onRetry(key, value, record, attempt, error)
                logRetryAttempt(record, audit)
            }

            override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
                delegate.onRecordDropped(record, reason)
                logDropped(record, audit, reason.name.lowercase())
            }
        }
    }
}

private const val EVENT_TYPE_TAG = "event_type"

private suspend fun latencyOnlyDelay() {
    delay((5L..8L).random())
}
