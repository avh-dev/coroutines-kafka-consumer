package avh.ckc.demo.consumer.springkafka

import avh.ckc.demo.AuditLog
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
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
            if (properties.consumers.processingEnabled) {
                orderLifecycleService.apply(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(orderConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
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
            if (properties.consumers.processingEnabled) {
                batchLifecycleService.apply(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(batchConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
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
            if (properties.consumers.processingEnabled) {
                cauldronTelemetryService.recalculate(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(telemetryConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
            logger.debug("Spring Kafka telemetry event received for key={}, cauldron={}", context.key, event.cauldronId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(telemetryConsumerMetrics, context, event, startedAt, error)
            throw error
        }
    }

    private fun latencyOnlySleep() {
        Thread.sleep((5L..8L).random())
    }

    private fun auditProcessed(topic: String, partition: Int, offset: Long) {
        if (properties.audit.enabled) {
            AuditLog.processed(topic, partition, offset)
        }
    }
}
