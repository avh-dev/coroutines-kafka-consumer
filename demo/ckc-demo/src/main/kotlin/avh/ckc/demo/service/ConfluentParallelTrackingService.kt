package avh.ckc.demo.service

import avh.ckc.demo.AuditLog
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("confluent-parallel")
class ConfluentParallelTrackingService(
    private val properties: DemoApplicationProperties,
    private val brewingLifecycleService: SyncBrewingLifecycleService,
    private val etaRecalculationService: SyncEtaRecalculationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(record: ConsumerRecord<String, OrderLifecycleEvent>) {
        try {
            if (properties.consumers.processingEnabled) {
                brewingLifecycleService.applyLifecycleEvent(record.value())
                auditProcessed(record)
            }
            logger.debug("Confluent Parallel lifecycle event received for key={}, order={}", record.key(), record.value().orderId)
        } catch (error: Throwable) {
            throw error
        }
    }

    fun processCauldronTelemetry(record: ConsumerRecord<String, CauldronTelemetryEvent>) {
        try {
            if (properties.consumers.processingEnabled) {
                etaRecalculationService.recalculate(record.value())
                auditProcessed(record)
            }
            logger.debug("Confluent Parallel telemetry event received for key={}, cauldron={}", record.key(), record.value().cauldronId)
        } catch (error: Throwable) {
            throw error
        }
    }

    private fun auditProcessed(record: ConsumerRecord<*, *>) {
        if (properties.audit.enabled) {
            AuditLog.processed(record.topic(), record.partition(), record.offset())
        }
    }
}
