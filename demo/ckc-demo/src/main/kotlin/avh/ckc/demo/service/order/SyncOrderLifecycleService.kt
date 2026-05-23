package avh.ckc.demo.service.order

import avh.ckc.demo.ml.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncOrderLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository,
    private val flavourModelClient: SyncOrderFlavourModelClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun apply(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrder(event, existingOrder))

            if (event.eventType == OrderLifecycleEventType.ORDER_CREATED) {
                val flavour = flavourModelClient.analyse(flavourRequest(event))
                brewingStateRepository.saveOrderFlavour(orderFlavour(event, flavour))
            }
        } catch (error: Throwable) {
            logger.error(
                "Spring Kafka order processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }
}
