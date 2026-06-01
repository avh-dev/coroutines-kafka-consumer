package avh.ckc.demo.repository

import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.model.OrderFlavour
import avh.ckc.demo.model.Order
import avh.ckc.demo.config.DemoRedisCommands
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class RedisSyncBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SyncBrewingStateRepository {
    override fun findOrder(orderId: String): Order? =
        store.findOrder(orderId)

    override fun saveOrder(order: Order) {
        store.saveOrder(order)
    }

    override fun findBatch(batchId: String): Batch? =
        store.findBatch(batchId)

    override fun saveBatch(batch: Batch) {
        store.saveBatch(batch)
    }

    override fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchId(cauldronId)

    override fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchId(cauldronId, batchId)
    }

    override fun findEtaContext(batchId: String): EtaContext? =
        store.findEtaContext(batchId)

    override fun saveEtaContext(context: EtaContext) {
        store.saveEtaContext(context)
    }

    override fun findOrderFlavour(orderId: String): OrderFlavour? =
        store.findOrderFlavour(orderId)

    override fun saveOrderFlavour(state: OrderFlavour) {
        store.saveOrderFlavour(state)
    }
}

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisSuspendBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SuspendBrewingStateRepository {
    override suspend fun findOrder(orderId: String): Order? =
        store.findOrderSuspending(orderId)

    override suspend fun saveOrder(order: Order) {
        store.saveOrderSuspending(order)
    }

    override suspend fun findBatch(batchId: String): Batch? =
        store.findBatchSuspending(batchId)

    override suspend fun saveBatch(batch: Batch) {
        store.saveBatchSuspending(batch)
    }

    override suspend fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchIdSuspending(cauldronId)

    override suspend fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchIdSuspending(cauldronId, batchId)
    }

    override suspend fun findEtaContext(batchId: String): EtaContext? =
        store.findEtaContextSuspending(batchId)

    override suspend fun saveEtaContext(context: EtaContext) {
        store.saveEtaContextSuspending(context)
    }

    override suspend fun findOrderFlavour(orderId: String): OrderFlavour? =
        store.findOrderFlavourSuspending(orderId)

    override suspend fun saveOrderFlavour(state: OrderFlavour) {
        store.saveOrderFlavourSuspending(state)
    }
}

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisBrewingStateStore(
    private val redisCommands: DemoRedisCommands,
    private val json: Json = Json { ignoreUnknownKeys = true }
) {
    fun findOrder(orderId: String): Order? =
        loadJson(orderKey(orderId), Order.serializer())

    fun saveOrder(order: Order) {
        saveJson(orderKey(order.orderId), Order.serializer(), order)
    }

    fun findBatch(batchId: String): Batch? =
        loadJson(batchKey(batchId), Batch.serializer())

    fun saveBatch(batch: Batch) {
        saveJson(batchKey(batch.batchId), Batch.serializer(), batch)
    }

    fun findActiveBatchId(cauldronId: String): String? =
        load(activeBatchKey(cauldronId))?.decodeToString()

    fun saveActiveBatchId(cauldronId: String, batchId: String) {
        save(activeBatchKey(cauldronId), batchId.encodeToByteArray())
    }

    fun findEtaContext(batchId: String): EtaContext? =
        loadJson(etaContextKey(batchId), EtaContext.serializer())

    fun saveEtaContext(context: EtaContext) {
        saveJson(etaContextKey(context.batchId), EtaContext.serializer(), context)
    }

    fun findOrderFlavour(orderId: String): OrderFlavour? =
        loadJson(orderFlavourKey(orderId), OrderFlavour.serializer())

    fun saveOrderFlavour(state: OrderFlavour) {
        saveJson(orderFlavourKey(state.orderId), OrderFlavour.serializer(), state)
    }

    suspend fun findOrderSuspending(orderId: String): Order? =
        loadJsonSuspending(orderKey(orderId), Order.serializer())

    suspend fun saveOrderSuspending(order: Order) {
        saveJsonSuspending(orderKey(order.orderId), Order.serializer(), order)
    }

    suspend fun findBatchSuspending(batchId: String): Batch? =
        loadJsonSuspending(batchKey(batchId), Batch.serializer())

    suspend fun saveBatchSuspending(batch: Batch) {
        saveJsonSuspending(batchKey(batch.batchId), Batch.serializer(), batch)
    }

    suspend fun findActiveBatchIdSuspending(cauldronId: String): String? =
        loadSuspending(activeBatchKey(cauldronId))?.decodeToString()

    suspend fun saveActiveBatchIdSuspending(cauldronId: String, batchId: String) {
        saveSuspending(activeBatchKey(cauldronId), batchId.encodeToByteArray())
    }

    suspend fun findEtaContextSuspending(batchId: String): EtaContext? =
        loadJsonSuspending(etaContextKey(batchId), EtaContext.serializer())

    suspend fun saveEtaContextSuspending(context: EtaContext) {
        saveJsonSuspending(etaContextKey(context.batchId), EtaContext.serializer(), context)
    }

    suspend fun findOrderFlavourSuspending(orderId: String): OrderFlavour? =
        loadJsonSuspending(orderFlavourKey(orderId), OrderFlavour.serializer())

    suspend fun saveOrderFlavourSuspending(state: OrderFlavour) {
        saveJsonSuspending(orderFlavourKey(state.orderId), OrderFlavour.serializer(), state)
    }

    private fun <T> loadJson(key: String, serializer: KSerializer<T>): T? =
        load(key)?.let { json.decodeFromString(serializer, it.decodeToString()) }

    private fun <T> saveJson(key: String, serializer: KSerializer<T>, value: T) {
        save(key, json.encodeToString(serializer, value).encodeToByteArray())
    }

    private fun save(key: String, value: ByteArray) {
        redisCommands.sync().set(key, value)
    }

    private fun load(key: String): ByteArray? =
        redisCommands.sync().get(key)

    private suspend fun <T> loadJsonSuspending(key: String, serializer: KSerializer<T>): T? =
        loadSuspending(key)?.let { json.decodeFromString(serializer, it.decodeToString()) }

    private suspend fun <T> saveJsonSuspending(key: String, serializer: KSerializer<T>, value: T) {
        saveSuspending(key, json.encodeToString(serializer, value).encodeToByteArray())
    }

    private suspend fun saveSuspending(key: String, value: ByteArray) {
        redisCommands.coroutines().set(key, value)
    }

    private suspend fun loadSuspending(key: String): ByteArray? =
        redisCommands.coroutines().get(key)

    private fun orderKey(orderId: String): String = "order-state:$orderId"

    private fun batchKey(batchId: String): String = "batch-state:$batchId"

    private fun activeBatchKey(cauldronId: String): String = "cauldron-active-batch:$cauldronId"

    private fun etaContextKey(batchId: String): String = "eta-context:$batchId"

    private fun orderFlavourKey(orderId: String): String = "order-flavour:$orderId"

}
