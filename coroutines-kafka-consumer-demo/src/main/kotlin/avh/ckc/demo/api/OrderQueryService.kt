package avh.ckc.demo.api

import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.repository.OrderState
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

@Service
class OrderQueryService(
    private val brewingStateRepository: BrewingStateRepository
) {
    fun findOrder(orderId: String): CompletionStage<OrderTrackingResponse?> =
        brewingStateRepository.findOrder(orderId).thenCompose { orderState ->
            if (orderState == null) {
                CompletableFuture.completedFuture(null)
            } else {
                loadBatch(orderState).thenApply { batchState ->
                    OrderTrackingResponse(
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
            }
        }

    private fun loadBatch(orderState: OrderState) =
        orderState.batchId?.let(brewingStateRepository::findBatch)
            ?: CompletableFuture.completedFuture(null)

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
