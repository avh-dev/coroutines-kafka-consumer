package avh.ckc.demo.repository

import avh.ckc.demo.model.BatchState
import avh.ckc.demo.model.ModelContextState
import avh.ckc.demo.model.OrderFlavourState
import avh.ckc.demo.model.OrderState

interface SyncBrewingStateRepository {
    fun findOrder(orderId: String): OrderState?

    fun saveOrder(orderState: OrderState)

    fun findBatch(batchId: String): BatchState?

    fun saveBatch(batchState: BatchState)

    fun findActiveBatchId(cauldronId: String): String?

    fun saveActiveBatchId(cauldronId: String, batchId: String)

    fun deleteActiveBatchId(cauldronId: String)

    fun findModelContext(batchId: String): ModelContextState?

    fun saveModelContext(context: ModelContextState)

    fun findOrderFlavour(orderId: String): OrderFlavourState?

    fun saveOrderFlavour(state: OrderFlavourState)
}

interface SuspendBrewingStateRepository {
    suspend fun findOrder(orderId: String): OrderState?

    suspend fun saveOrder(orderState: OrderState)

    suspend fun findBatch(batchId: String): BatchState?

    suspend fun saveBatch(batchState: BatchState)

    suspend fun findActiveBatchId(cauldronId: String): String?

    suspend fun saveActiveBatchId(cauldronId: String, batchId: String)

    suspend fun deleteActiveBatchId(cauldronId: String)

    suspend fun findModelContext(batchId: String): ModelContextState?

    suspend fun saveModelContext(context: ModelContextState)

    suspend fun findOrderFlavour(orderId: String): OrderFlavourState?

    suspend fun saveOrderFlavour(state: OrderFlavourState)
}
