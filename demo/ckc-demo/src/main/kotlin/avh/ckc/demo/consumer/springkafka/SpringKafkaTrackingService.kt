package avh.ckc.demo.consumer.springkafka

import avh.ckc.demo.AuditLog
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.handler.batch.BatchEventHandler
import avh.ckc.demo.handler.cauldron.CauldronEventHandler
import avh.ckc.demo.handler.order.OrderEventHandler
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.DemoConsumerRecordContext
import avh.ckc.demo.service.DemoRecordMetrics
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka")
class SpringKafkaTrackingService(
    private val properties: DemoApplicationProperties,
    private val orderEventHandler: OrderEventHandler,
    private val batchEventHandler: BatchEventHandler,
    private val cauldronEventHandler: CauldronEventHandler,
    private val recordMetrics: DemoRecordMetrics,
    @Qualifier("springKafkaLifecycleConsumerMetrics")
    private val lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
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
                orderEventHandler.handle(event)
            } else {
                latencyOnlySleep()
            }
            recordMetrics.onProcessed(lifecycleConsumerMetrics, context, event, startedAt)
            auditProcessed(context.topic, context.partition, context.offset)
            logger.debug("Spring Kafka lifecycle event received for key={}, order={}", context.key, event.orderId)
        } catch (error: Throwable) {
            recordMetrics.onFailed(lifecycleConsumerMetrics, context, event, startedAt, error)
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
                batchEventHandler.handle(event)
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
                cauldronEventHandler.handle(event)
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
