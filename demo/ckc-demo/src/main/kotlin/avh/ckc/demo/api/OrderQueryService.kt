package avh.ckc.demo.api

import avh.ckc.demo.repository.OrderState
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
            order = orderState.toView(),
            batch = batchState?.let {
                BatchView(
                    batchId = it.batchId,
                    recipeId = it.recipeId,
                    potionId = it.potionId,
                    cauldronId = it.cauldronId,
                    status = it.status,
                    orderIds = it.orderIds,
                    updatedAt = it.updatedAt
                )
            }
        )
    }

    private fun loadBatch(orderState: OrderState) =
        orderState.batchId?.let(brewingStateRepository::findBatch)

    private fun OrderState.toView(): OrderView =
        OrderView(
            orderId = orderId,
            batchId = batchId,
            potionId = potionId,
            recipeId = recipeId,
            customerId = customerId,
            cauldronId = cauldronId,
            status = status,
            updatedAt = updatedAt
        )
}
