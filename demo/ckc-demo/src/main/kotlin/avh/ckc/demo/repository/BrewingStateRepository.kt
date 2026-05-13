package avh.ckc.demo.repository

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
}
