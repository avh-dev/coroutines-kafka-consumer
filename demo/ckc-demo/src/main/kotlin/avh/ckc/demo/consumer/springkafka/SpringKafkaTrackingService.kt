package avh.ckc.demo.consumer.springkafka

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.AuditDropReasons
import avh.ckc.demo.consumer.FreshnessFirstRecordFilter
import avh.ckc.demo.logDropped
import avh.ckc.demo.logProcessed
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.DemoConsumerRecordContext
import avh.ckc.demo.service.DemoRecordMetrics
import avh.ckc.demo.service.batch.SyncBatchLifecycleService
import avh.ckc.demo.service.cauldron.SyncCauldronTelemetryService
import avh.ckc.demo.service.order.SyncOrderLifecycleService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka")
class SpringKafkaTrackingService(
    private val properties: DemoApplicationProperties,
    private val orderLifecycleService: SyncOrderLifecycleService,
    private val batchLifecycleService: SyncBatchLifecycleService,
    private val cauldronTelemetryService: SyncCauldronTelemetryService,
    private val recordMetrics: DemoRecordMetrics,
    private val freshnessFirstRecordFilter: FreshnessFirstRecordFilter,
    @Qualifier("springKafkaOrderConsumerMetrics")
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    @Qualifier("springKafkaBatchConsumerMetrics")
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
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
            if (shouldDiscard(properties.consumers.order, context)) {
                auditDropped(context)
                return
            }
            if (properties.consumers.processingEnabled) {
                orderLifecycleService.apply(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(orderConsumerMetrics, context, event, startedAt)
            auditProcessed(context)
            logger.debug("Spring Kafka order event received for key={}, order={}", context.key, event.orderId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(orderConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    fun processBatchLifecycle(
        context: DemoConsumerRecordContext,
        event: BatchLifecycleEvent
    ) {
        val startedAt = System.nanoTime()
        try {
            if (shouldDiscard(properties.consumers.batch, context)) {
                auditDropped(context)
                return
            }
            if (properties.consumers.processingEnabled) {
                batchLifecycleService.apply(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(batchConsumerMetrics, context, event, startedAt)
            auditProcessed(context)
            logger.debug("Spring Kafka batch event received for key={}, batch={}", context.key, event.batchId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(batchConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    fun processCauldronTelemetry(
        context: DemoConsumerRecordContext,
        event: CauldronTelemetryEvent
    ) {
        val startedAt = System.nanoTime()
        try {
            if (shouldDiscard(properties.consumers.telemetry, context)) {
                auditDropped(context)
                return
            }
            if (properties.consumers.processingEnabled) {
                cauldronTelemetryService.recalculate(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(telemetryConsumerMetrics, context, event, startedAt)
            auditProcessed(context)
            logger.debug("Spring Kafka telemetry event received for key={}, cauldron={}", context.key, event.cauldronId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(telemetryConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    private fun latencyOnlySleep() {
        Thread.sleep((5L..8L).random())
    }

    private fun auditProcessed(context: DemoConsumerRecordContext) {
        logProcessed(context.topic, context.key, context.partition, context.offset, context.timestamp, properties.audit)
    }

    private fun auditDropped(context: DemoConsumerRecordContext) {
        logDropped(
            context.topic,
            context.key,
            context.partition,
            context.offset,
            context.timestamp,
            properties.audit,
            AuditDropReasons.STALE_AGE
        )
    }

    private fun shouldDiscard(
        runtime: DemoApplicationProperties.ConsumerRuntime,
        context: DemoConsumerRecordContext
    ): Boolean =
        freshnessFirstRecordFilter.shouldDiscard(runtime, context.timestamp).also { discard ->
            if (discard) {
                logger.debug("Discarding stale Spring Kafka record for topic={}, offset={}", context.topic, context.offset)
            }
        }
}
