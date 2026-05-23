package avh.ckc.demo.repository

import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.model.OrderFlavour
import avh.ckc.demo.model.Order
import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono
import java.util.concurrent.CompletionStage

class RedisSyncBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SyncBrewingStateRepository {
    override fun findOrder(orderId: String): Order? =
        store.findOrder(orderId).toCompletableFuture().join()

    override fun saveOrder(order: Order) {
        store.saveOrder(order).toCompletableFuture().join()
    }

    override fun findBatch(batchId: String): Batch? =
        store.findBatch(batchId).toCompletableFuture().join()

    override fun saveBatch(batch: Batch) {
        store.saveBatch(batch).toCompletableFuture().join()
    }

    override fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchId(cauldronId).toCompletableFuture().join()

    override fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchId(cauldronId, batchId).toCompletableFuture().join()
    }

    override fun deleteActiveBatchId(cauldronId: String) {
        store.deleteActiveBatchId(cauldronId).toCompletableFuture().join()
    }

    override fun findEtaContext(batchId: String): EtaContext? =
        store.findEtaContext(batchId).toCompletableFuture().join()

    override fun saveEtaContext(context: EtaContext) {
        store.saveEtaContext(context).toCompletableFuture().join()
    }

    override fun findOrderFlavour(orderId: String): OrderFlavour? =
        store.findOrderFlavour(orderId).toCompletableFuture().join()

    override fun saveOrderFlavour(state: OrderFlavour) {
        store.saveOrderFlavour(state).toCompletableFuture().join()
    }
}

class RedisSuspendBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SuspendBrewingStateRepository {
    override suspend fun findOrder(orderId: String): Order? =
        store.findOrder(orderId).await()

    override suspend fun saveOrder(order: Order) {
        store.saveOrder(order).await()
    }

    override suspend fun findBatch(batchId: String): Batch? =
        store.findBatch(batchId).await()

    override suspend fun saveBatch(batch: Batch) {
        store.saveBatch(batch).await()
    }

    override suspend fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchId(cauldronId).await()

    override suspend fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchId(cauldronId, batchId).await()
    }

    override suspend fun deleteActiveBatchId(cauldronId: String) {
        store.deleteActiveBatchId(cauldronId).await()
    }

    override suspend fun findEtaContext(batchId: String): EtaContext? =
        store.findEtaContext(batchId).await()

    override suspend fun saveEtaContext(context: EtaContext) {
        store.saveEtaContext(context).await()
    }

    override suspend fun findOrderFlavour(orderId: String): OrderFlavour? =
        store.findOrderFlavour(orderId).await()

    override suspend fun saveOrderFlavour(state: OrderFlavour) {
        store.saveOrderFlavour(state).await()
    }
}

class RedisBrewingStateStore(
    private val orderRedisTemplate: ReactiveRedisTemplate<String, ByteArray>,
    private val json: Json = Json { ignoreUnknownKeys = true }
) {
    fun findOrder(orderId: String): CompletionStage<Order?> =
        loadJson(orderKey(orderId), Order.serializer())

    fun saveOrder(order: Order): CompletionStage<Void> =
        saveJson(orderKey(order.orderId), Order.serializer(), order)

    fun findBatch(batchId: String): CompletionStage<Batch?> =
        loadJson(batchKey(batchId), Batch.serializer())

    fun saveBatch(batch: Batch): CompletionStage<Void> =
        saveJson(batchKey(batch.batchId), Batch.serializer(), batch)

    fun findActiveBatchId(cauldronId: String): CompletionStage<String?> =
        load(activeBatchKey(cauldronId)).thenApply { bytes -> bytes?.decodeToString() }

    fun saveActiveBatchId(cauldronId: String, batchId: String): CompletionStage<Void> =
        save(activeBatchKey(cauldronId), batchId.encodeToByteArray())

    fun deleteActiveBatchId(cauldronId: String): CompletionStage<Void> =
        delete(activeBatchKey(cauldronId))

    fun findEtaContext(batchId: String): CompletionStage<EtaContext?> =
        loadJson(etaContextKey(batchId), EtaContext.serializer())

    fun saveEtaContext(context: EtaContext): CompletionStage<Void> =
        saveJson(etaContextKey(context.batchId), EtaContext.serializer(), context)

    fun findOrderFlavour(orderId: String): CompletionStage<OrderFlavour?> =
        loadJson(orderFlavourKey(orderId), OrderFlavour.serializer())

    fun saveOrderFlavour(state: OrderFlavour): CompletionStage<Void> =
        saveJson(orderFlavourKey(state.orderId), OrderFlavour.serializer(), state)

    private fun <T> loadJson(key: String, serializer: KSerializer<T>): CompletionStage<T?> =
        load(key).thenApply { bytes -> bytes?.let { json.decodeFromString(serializer, it.decodeToString()) } }

    private fun <T> saveJson(key: String, serializer: KSerializer<T>, value: T): CompletionStage<Void> =
        save(key, json.encodeToString(serializer, value).encodeToByteArray())

    private fun save(key: String, value: ByteArray): CompletionStage<Void> =
        orderRedisTemplate.opsForValue().set(key, value).toFuture().thenAccept { }

    private fun delete(key: String): CompletionStage<Void> =
        orderRedisTemplate.delete(key).toFuture().thenAccept { }

    private fun load(key: String): CompletionStage<ByteArray?> =
        orderRedisTemplate.opsForValue().get(key)
            .switchIfEmpty(Mono.empty())
            .toFuture()

    private fun orderKey(orderId: String): String = "order-state:$orderId"

    private fun batchKey(batchId: String): String = "batch-state:$batchId"

    private fun activeBatchKey(cauldronId: String): String = "cauldron-active-batch:$cauldronId"

    private fun etaContextKey(batchId: String): String = "eta-context:$batchId"

    private fun orderFlavourKey(orderId: String): String = "order-flavour:$orderId"
}
