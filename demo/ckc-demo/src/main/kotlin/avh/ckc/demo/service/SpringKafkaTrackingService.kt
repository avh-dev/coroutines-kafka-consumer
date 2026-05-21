package avh.ckc.demo.service

import avh.ckc.demo.AuditLog
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka")
class SpringKafkaTrackingService(
    private val properties: DemoApplicationProperties,
    private val brewingLifecycleService: SyncBrewingLifecycleService,
    private val etaRecalculationService: SyncEtaRecalculationService,
    private val recordMetrics: DemoRecordMetrics,
    @Qualifier("springKafkaLifecycleConsumerMetrics")
    private val lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    @Qualifier("springKafkaConsumerMetrics")
    private val telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(
        context: DemoConsumerRecordContext,
        event: OrderLifecycleEvent
    ) {
        val startedAt = System.nanoTime()
        try {
            brewingLifecycleService.applyLifecycleEvent(event)
            recordMetrics.onProcessed(lifecycleConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
            logger.debug("Spring Kafka lifecycle event received for key={}, order={}", context.key, event.orderId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(lifecycleConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    fun processCauldronTelemetry(
        context: DemoConsumerRecordContext,
        event: CauldronTelemetryEvent
    ) {
        val startedAt = System.nanoTime()
        try {
            etaRecalculationService.recalculate(event)
            recordMetrics.onProcessed(telemetryConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
            logger.debug("Spring Kafka telemetry event received for key={}, cauldron={}", context.key, event.cauldronId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(telemetryConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    private fun auditProcessed(topic: String, partition: Int, offset: Long) {
        if (properties.audit.enabled) {
            AuditLog.processed(topic, partition, offset)
        }
    }
}
