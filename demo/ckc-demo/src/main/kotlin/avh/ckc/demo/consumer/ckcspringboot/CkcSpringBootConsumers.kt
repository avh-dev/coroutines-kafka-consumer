package avh.ckc.demo.consumer.ckcspringboot

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logFailed
import avh.ckc.demo.logProcessed
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import avh.ckc.micrometer.recordDrivenTagExtractors
import avh.ckc.spring.CkcConsumer
import avh.ckc.spring.CkcKafkaConsumer
import avh.ckc.spring.CkcMetricsCustomizer
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
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

    override fun metricsCustomizer(): CkcMetricsCustomizer<String, OrderLifecycleEvent> =
        CkcMetricsCustomizer {
            recordDrivenTagExtractors {
                tag(EVENT_TYPE_TAG) { record -> record.value()?.eventType?.name ?: UNKNOWN_EVENT_TYPE }
            }
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

    override fun metricsCustomizer(): CkcMetricsCustomizer<String, BatchLifecycleEvent> =
        CkcMetricsCustomizer {
            recordDrivenTagExtractors {
                tag(EVENT_TYPE_TAG) { record -> record.value()?.eventType?.name ?: UNKNOWN_EVENT_TYPE }
            }
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

    override fun metricsCustomizer(): CkcMetricsCustomizer<String, CauldronTelemetryEvent> =
        CkcMetricsCustomizer {
            recordDrivenTagExtractors {
                tag(EVENT_TYPE_TAG) { "CAULDRON_TELEMETRY" }
            }
        }
}

private const val EVENT_TYPE_TAG = "event_type"
private const val UNKNOWN_EVENT_TYPE = "UNKNOWN"

private suspend fun latencyOnlyDelay() {
    delay((5L..8L).random())
}
