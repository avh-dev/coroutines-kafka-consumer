package avh.ckc.demo.api

import avh.ckc.demo.model.OrderState
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.springframework.stereotype.Service

@Service
class OrderQueryService(
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    fun findOrder(orderId: String): OrderTrackingResponse? {
        val orderState = brewingStateRepository.findOrder(orderId) ?: return null
        val batchState = loadBatch(orderState)
        return OrderTrackingResponse(
            order = orderState,
            batch = batchState,
            flavour = brewingStateRepository.findOrderFlavour(orderId)
        )
    }

    private fun loadBatch(orderState: OrderState) =
        orderState.batchId?.let(brewingStateRepository::findBatch)
}
