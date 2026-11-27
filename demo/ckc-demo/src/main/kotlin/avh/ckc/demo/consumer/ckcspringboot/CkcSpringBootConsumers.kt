package avh.ckc.demo.consumer.ckcspringboot

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.metrics.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.core.metrics.micrometer.RecordDrivenTagExtractors
import avh.ckc.core.metrics.micrometer.RecordMetricTagDefinition
import avh.ckc.core.metrics.micrometer.micrometerConsumerMetrics
import avh.ckc.core.metrics.micrometer.recordDrivenTagExtractors
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logDropped
import avh.ckc.demo.logHandedOff
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.logRetryAttempt
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import avh.ckc.spring.CkcConsumer
import avh.ckc.spring.CkcConsumerMetrics
import avh.ckc.spring.CkcConsumerProperties
import avh.ckc.spring.CkcKafkaConsumer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment

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
class CkcSpringBootMetricsConfiguration(
    @Value("\${CKC_METRICS_ENABLED:true}") private val micrometerEnabled: Boolean
) {
    companion object {
        @Bean
        @JvmStatic
        fun retainCkcAuditBinding(environment: Environment): BeanPostProcessor = object : BeanPostProcessor {
            override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
                if (bean is CkcConsumerProperties && !bean.metrics.enabled &&
                    bean.metrics.implementation == CkcConsumerProperties.MetricsImplementation.CUSTOM &&
                    !environment.getProperty("CKC_METRICS_ENABLED", Boolean::class.java, true)
                ) {
                    // System environment binding maps CKC_METRICS_ENABLED to the starter switch.
                    bean.metrics.enabled = Binder.get(environment).bind("demo.audit.enabled", Boolean::class.java)
                        .orElse(DemoApplicationProperties.Audit().enabled)
                }
                return bean
            }
        }
    }

    @Bean
    @CkcConsumerMetrics(consumer = "order-events")
    fun orderConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, OrderLifecycleEvent> =
        consumerMetrics("order-events", meterRegistry, ckcProperties, properties.audit, ::orderEventRecordTags)

    @Bean
    @CkcConsumerMetrics(consumer = "batch-events")
    fun batchConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, BatchLifecycleEvent> =
        consumerMetrics("batch-events", meterRegistry, ckcProperties, properties.audit, ::batchEventRecordTags)

    @Bean
    @CkcConsumerMetrics(consumer = "cauldron-events")
    fun cauldronConsumerMetrics(
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        properties: DemoApplicationProperties
    ): ConsumerMetrics<String, CauldronTelemetryEvent> =
        consumerMetrics("cauldron-events", meterRegistry, ckcProperties, properties.audit, ::cauldronEventRecordTags)

    private fun <V> consumerMetrics(
        name: String,
        meterRegistry: MeterRegistry,
        ckcProperties: CkcConsumerProperties,
        audit: DemoApplicationProperties.Audit,
        recordTags: () -> RecordDrivenTagExtractors<String, V>
    ): ConsumerMetrics<String, V> {
        @Suppress("UNCHECKED_CAST")
        val delegate = if (micrometerEnabled) {
            micrometerConsumerMetrics<String, V>(demoMicrometerSchema(meterRegistry, ckcProperties)) {
                consumerId = name
                recordDrivenTagExtractors = recordTags()
            }
        } else {
            ConsumerMetrics.NOOP as ConsumerMetrics<String, V>
        }
        return if (audit.enabled) delegate.withAudit(audit) else delegate
    }

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
            recordDrivenTags = schema.recordDrivenTags.map { RecordMetricTagDefinition(it.name, it.default) },
            kafkaClientMetricsEnabled = ckcProperties.metrics.micrometer.kafkaClientMetricsEnabled
        )
    }

    private fun <K, V> ConsumerMetrics<K, V>.withAudit(
        audit: DemoApplicationProperties.Audit
    ): ConsumerMetrics<K, V> {
        val delegate = this
        return object : ConsumerMetrics<K, V> by delegate {
            override fun runtimeBinding(bindingId: String): ConsumerMetrics<K, V> =
                delegate.runtimeBinding(bindingId).withAudit(audit)

            override fun onRecordProcessed(
                key: K?,
                value: V?,
                record: ConsumerRecord<K, V>,
                endToEndLatencyMillis: Long,
                durationNanos: Long,
                processedRemotely: Boolean
            ) {
                delegate.onRecordProcessed(key, value, record, endToEndLatencyMillis, durationNanos, processedRemotely)
                // Local and recipient handlers already log success. A delegated source bypasses the handler.
                if (processedRemotely) logProcessed(record, audit)
            }

            override fun onRetry(key: K?, value: V?, record: ConsumerRecord<K, V>, attempt: Int, error: Throwable) {
                delegate.onRetry(key, value, record, attempt, error)
                logRetryAttempt(record, audit)
            }

            override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
                delegate.onRecordDropped(record, reason)
                logDropped(record, audit, reason.name.lowercase())
            }

            override fun onRecordHandedOff(record: ConsumerRecord<K, V>, reason: RecordHandoffReason) {
                delegate.onRecordHandedOff(record, reason)
                logHandedOff(record, audit, reason.name.lowercase())
            }
        }
    }
}

private const val EVENT_TYPE_TAG = "event_type"

private suspend fun latencyOnlyDelay() {
    delay((5L..8L).random())
}
