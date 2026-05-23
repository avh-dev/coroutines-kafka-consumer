package avh.ckc.demo.api

import avh.ckc.demo.model.Order
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.springframework.stereotype.Service

@Service
class OrderQueryService(
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    fun findOrder(orderId: String): OrderTrackingResponse? {
        val order = brewingStateRepository.findOrder(orderId) ?: return null
        val batch = loadBatch(order)
        return OrderTrackingResponse(
            order = order,
            batch = batch,
            flavour = brewingStateRepository.findOrderFlavour(orderId)
        )
    }

    private fun loadBatch(order: Order) =
        order.batchId?.let(brewingStateRepository::findBatch)
}
