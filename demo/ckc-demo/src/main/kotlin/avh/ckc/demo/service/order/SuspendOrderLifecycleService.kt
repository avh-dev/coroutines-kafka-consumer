package avh.ckc.demo.service.order

import avh.ckc.demo.modelclient.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SuspendOrderLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository,
    private val flavourModelClient: SuspendOrderFlavourModelClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrderState(event, existingOrder))

            if (event.eventType == OrderLifecycleEventType.ORDER_CREATED) {
                val flavour = flavourModelClient.analyse(flavourRequest(event))
                brewingStateRepository.saveOrderFlavour(flavourState(event, flavour))
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
