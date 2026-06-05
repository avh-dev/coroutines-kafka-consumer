package avh.ckc.demo.repository

import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.BrewingStepReceipt
import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.model.OrderFlavour
import avh.ckc.demo.model.Order

interface SyncBrewingStateRepository {
    fun findOrder(orderId: String): Order?

    fun saveOrder(order: Order)

    fun findBatch(batchId: String): Batch?

    fun saveBatch(batch: Batch)

    fun findActiveBatchId(cauldronId: String): String?

    fun saveActiveBatchId(cauldronId: String, batchId: String)

    fun findEtaContext(batchId: String): EtaContext?

    fun saveEtaContext(context: EtaContext)

    fun findOrderFlavour(orderId: String): OrderFlavour?

    fun saveOrderFlavour(state: OrderFlavour)

    fun saveBrewingStepReceipt(receipt: BrewingStepReceipt)
}

interface SuspendBrewingStateRepository {
    suspend fun findOrder(orderId: String): Order?

    suspend fun saveOrder(order: Order)

    suspend fun findBatch(batchId: String): Batch?

    suspend fun saveBatch(batch: Batch)

    suspend fun findActiveBatchId(cauldronId: String): String?

    suspend fun saveActiveBatchId(cauldronId: String, batchId: String)

    suspend fun findEtaContext(batchId: String): EtaContext?

    suspend fun saveEtaContext(context: EtaContext)

    suspend fun findOrderFlavour(orderId: String): OrderFlavour?

    suspend fun saveOrderFlavour(state: OrderFlavour)

    suspend fun saveBrewingStepReceipt(receipt: BrewingStepReceipt)
}
