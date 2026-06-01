package avh.ckc.demo.service.order

import avh.ckc.demo.ml.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("ckc", "confluent-parallel-reactor")
class SuspendOrderLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository,
    private val flavourModelClient: SuspendOrderFlavourModelClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrder(event, existingOrder))

            if (event.eventType == OrderLifecycleEventType.ORDER_CREATED) {
                val flavour = flavourModelClient.analyse(flavourRequest(event))
                brewingStateRepository.saveOrderFlavour(orderFlavour(event, flavour))
            }
        } catch (error: Throwable) {
            logger.error(
                "CKC order processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }
}
