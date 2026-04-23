package avh.ckc.demo.consumer

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.service.BrewingLifecycleService
import avh.ckc.demo.service.EtaRecalculationService
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
@Profile("spring-kafka")
@ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
class SpringKafkaTrackingListeners(
    private val brewingStateRepository: BrewingStateRepository,
    private val brewingLifecycleService: BrewingLifecycleService,
    private val etaRecalculationService: EtaRecalculationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        id = "spring-kafka-order-lifecycle",
        topics = ["\${demo.topics.order-lifecycle}"],
        containerFactory = "orderLifecycleListenerContainerFactory"
    )
    fun onOrderLifecycle(
        @Header(KafkaHeaders.RECEIVED_KEY, required = false) key: String?,
        event: OrderLifecycleEvent
    ) {
        brewingLifecycleService.applyLifecycleEvent(event).toCompletableFuture().join()
        logger.debug("Spring Kafka lifecycle event received for key={}, order={}", key, event.orderId)
    }

    @KafkaListener(
        id = "spring-kafka-cauldron-telemetry",
        topics = ["\${demo.topics.cauldron-telemetry}"],
        containerFactory = "cauldronTelemetryListenerContainerFactory"
    )
    fun onCauldronTelemetry(
        @Header(KafkaHeaders.RECEIVED_KEY, required = false) key: String?,
        event: CauldronTelemetryEvent
    ) {
        val batchId = event.batchId.ifBlank {
            brewingStateRepository.findActiveBatchId(event.cauldronId).toCompletableFuture().join() ?: ""
        }
        if (batchId.isBlank()) {
            return
        }
        val batchState = brewingStateRepository.findBatch(batchId).toCompletableFuture().join() ?: return
        val estimate = etaRecalculationService.recalculate(batchState, event).toCompletableFuture().join()
        logger.info(
            "Spring Kafka ETA recalculated for key={}, batch={}, cauldron={}, etaSeconds={}",
            key,
            estimate.batchId,
            estimate.cauldronId,
            estimate.etaSeconds
        )
    }
}
