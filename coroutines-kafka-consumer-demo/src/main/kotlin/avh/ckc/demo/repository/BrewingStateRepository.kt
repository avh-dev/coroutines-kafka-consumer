package avh.ckc.demo.repository

import java.util.concurrent.CompletionStage

interface BrewingStateRepository {
    fun findOrder(orderId: String): CompletionStage<OrderState?>

    fun saveOrder(orderState: OrderState): CompletionStage<Void>

    fun findBatch(batchId: String): CompletionStage<BatchState?>

    fun saveBatch(batchState: BatchState): CompletionStage<Void>

    fun findActiveBatchId(cauldronId: String): CompletionStage<String?>

    fun saveActiveBatchId(cauldronId: String, batchId: String): CompletionStage<Void>

    fun deleteActiveBatchId(cauldronId: String): CompletionStage<Void>

    fun findModelContext(batchId: String): CompletionStage<ModelContextState?>

    fun saveModelContext(context: ModelContextState): CompletionStage<Void>
}
