package avh.ckc.demo.service

import avh.ckc.demo.AuditLog
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka")
class SpringKafkaTrackingService(
    private val properties: DemoApplicationProperties,
    private val brewingLifecycleService: SyncBrewingLifecycleService,
    private val etaRecalculationService: SyncEtaRecalculationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun processOrderLifecycle(
        key: String?,
        topic: String,
        partition: Int,
        offset: Long,
        event: OrderLifecycleEvent
    ) {
        brewingLifecycleService.applyLifecycleEvent(event)
        auditProcessed(topic, partition, offset)
        logger.debug("Spring Kafka lifecycle event received for key={}, order={}", key, event.orderId)
    }

    fun processCauldronTelemetry(
        key: String?,
        topic: String,
        partition: Int,
        offset: Long,
        event: CauldronTelemetryEvent
    ) {
        etaRecalculationService.recalculate(event)
        auditProcessed(topic, partition, offset)
        logger.debug("Spring Kafka telemetry event received for key={}, cauldron={}", key, event.cauldronId)
    }

    private fun auditProcessed(topic: String, partition: Int, offset: Long) {
        if (properties.audit.enabled) {
            AuditLog.processed(topic, partition, offset)
        }
    }
}
