package avh.ckc.demo.consumer.confluent

import avh.ckc.demo.AuditLog
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SyncBatchLifecycleService
import avh.ckc.demo.service.cauldron.SyncCauldronTelemetryService
import avh.ckc.demo.service.order.SyncOrderLifecycleService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("confluent-parallel")
class ConfluentParallelTrackingService(
    private val properties: DemoApplicationProperties,
    private val orderLifecycleService: SyncOrderLifecycleService,
    private val batchLifecycleService: SyncBatchLifecycleService,
    private val cauldronTelemetryService: SyncCauldronTelemetryService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        try {
            if (properties.consumers.processingEnabled) {
                orderLifecycleService.apply(record.value())
            } else {
                latencyOnlySleep()
            }
            auditProcessed(record)
            logger.debug("Confluent Parallel order event received for key={}, order={}", record.key(), record.value().orderId)
        } catch (error: Throwable) {
            throw error
        }
    }

    fun processBatchLifecycle(record: ConsumerRecord<String, BatchLifecycleEvent>) {
        try {
            if (properties.consumers.processingEnabled) {
                batchLifecycleService.apply(record.value())
            } else {
                latencyOnlySleep()
            }
            auditProcessed(record)
            logger.debug("Confluent Parallel batch event received for key={}, batch={}", record.key(), record.value().batchId)
        } catch (error: Throwable) {
            throw error
        }
    }

    fun processCauldronTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        try {
            if (properties.consumers.processingEnabled) {
                cauldronTelemetryService.recalculate(record.value())
            } else {
                latencyOnlySleep()
            }
            auditProcessed(record)
            logger.debug("Confluent Parallel cauldron event received for key={}, cauldron={}", record.key(), record.value().cauldronId)
        } catch (error: Throwable) {
            throw error
        }
    }

    private fun latencyOnlySleep() {
        Thread.sleep((5L..8L).random())
    }

    private fun auditProcessed(record: ConsumerRecord<*, *>) {
        if (properties.audit.enabled) {
            AuditLog.processed(record.topic(), record.partition(), record.offset())
        }
    }
}
