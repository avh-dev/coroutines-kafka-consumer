package avh.ckc.demo.repository

import avh.ckc.demo.proto.OrderLifecycleEvent
import java.util.concurrent.CompletionStage

interface BrewingStateRepository {
    fun applyLifecycleEvent(event: OrderLifecycleEvent): CompletionStage<Void>

    fun findOrder(orderId: String): CompletionStage<OrderState?>

    fun findBatch(batchId: String): CompletionStage<BatchState?>

    fun findActiveBatchId(cauldronId: String): CompletionStage<String?>

    fun findModelContext(batchId: String): CompletionStage<ModelContextState?>

    fun saveModelContext(context: ModelContextState): CompletionStage<Void>
}
