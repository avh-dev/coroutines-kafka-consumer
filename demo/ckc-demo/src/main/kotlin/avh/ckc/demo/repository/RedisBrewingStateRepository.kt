package avh.ckc.demo.repository

import kotlinx.coroutines.future.await
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono
import java.util.concurrent.CompletionStage

class RedisSyncBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SyncBrewingStateRepository {
    override fun findOrder(orderId: String): OrderState? =
        store.findOrder(orderId).toCompletableFuture().join()

    override fun saveOrder(orderState: OrderState) {
        store.saveOrder(orderState).toCompletableFuture().join()
    }

    override fun findBatch(batchId: String): BatchState? =
        store.findBatch(batchId).toCompletableFuture().join()

    override fun saveBatch(batchState: BatchState) {
        store.saveBatch(batchState).toCompletableFuture().join()
    }

    override fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchId(cauldronId).toCompletableFuture().join()

    override fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchId(cauldronId, batchId).toCompletableFuture().join()
    }

    override fun deleteActiveBatchId(cauldronId: String) {
        store.deleteActiveBatchId(cauldronId).toCompletableFuture().join()
    }

    override fun findModelContext(batchId: String): ModelContextState? =
        store.findModelContext(batchId).toCompletableFuture().join()

    override fun saveModelContext(context: ModelContextState) {
        store.saveModelContext(context).toCompletableFuture().join()
    }

    override fun findOrderFlavour(orderId: String): OrderFlavourState? =
        store.findOrderFlavour(orderId).toCompletableFuture().join()

    override fun saveOrderFlavour(state: OrderFlavourState) {
        store.saveOrderFlavour(state).toCompletableFuture().join()
    }
}

class RedisSuspendBrewingStateRepository(
    private val store: RedisBrewingStateStore
) : SuspendBrewingStateRepository {
    override suspend fun findOrder(orderId: String): OrderState? =
        store.findOrder(orderId).await()

    override suspend fun saveOrder(orderState: OrderState) {
        store.saveOrder(orderState).await()
    }

    override suspend fun findBatch(batchId: String): BatchState? =
        store.findBatch(batchId).await()

    override suspend fun saveBatch(batchState: BatchState) {
        store.saveBatch(batchState).await()
    }

    override suspend fun findActiveBatchId(cauldronId: String): String? =
        store.findActiveBatchId(cauldronId).await()

    override suspend fun saveActiveBatchId(cauldronId: String, batchId: String) {
        store.saveActiveBatchId(cauldronId, batchId).await()
    }

    override suspend fun deleteActiveBatchId(cauldronId: String) {
        store.deleteActiveBatchId(cauldronId).await()
    }

    override suspend fun findModelContext(batchId: String): ModelContextState? =
        store.findModelContext(batchId).await()

    override suspend fun saveModelContext(context: ModelContextState) {
        store.saveModelContext(context).await()
    }

    override suspend fun findOrderFlavour(orderId: String): OrderFlavourState? =
        store.findOrderFlavour(orderId).await()

    override suspend fun saveOrderFlavour(state: OrderFlavourState) {
        store.saveOrderFlavour(state).await()
    }
}

class RedisBrewingStateStore(
    private val orderStateRedisTemplate: ReactiveRedisTemplate<String, ByteArray>,
    private val json: Json = Json { ignoreUnknownKeys = true }
) {
    fun findOrder(orderId: String): CompletionStage<OrderState?> =
        loadJson(orderKey(orderId), OrderState.serializer())

    fun saveOrder(orderState: OrderState): CompletionStage<Void> =
        saveJson(orderKey(orderState.orderId), OrderState.serializer(), orderState)

    fun findBatch(batchId: String): CompletionStage<BatchState?> =
        loadJson(batchKey(batchId), BatchState.serializer())

    fun saveBatch(batchState: BatchState): CompletionStage<Void> =
        saveJson(batchKey(batchState.batchId), BatchState.serializer(), batchState)

    fun findActiveBatchId(cauldronId: String): CompletionStage<String?> =
        load(activeBatchKey(cauldronId)).thenApply { bytes -> bytes?.decodeToString() }

    fun saveActiveBatchId(cauldronId: String, batchId: String): CompletionStage<Void> =
        save(activeBatchKey(cauldronId), batchId.encodeToByteArray())

    fun deleteActiveBatchId(cauldronId: String): CompletionStage<Void> =
        delete(activeBatchKey(cauldronId))

    fun findModelContext(batchId: String): CompletionStage<ModelContextState?> =
        loadJson(modelContextKey(batchId), ModelContextState.serializer())

    fun saveModelContext(context: ModelContextState): CompletionStage<Void> =
        saveJson(modelContextKey(context.batchId), ModelContextState.serializer(), context)

    fun findOrderFlavour(orderId: String): CompletionStage<OrderFlavourState?> =
        loadJson(orderFlavourKey(orderId), OrderFlavourState.serializer())

    fun saveOrderFlavour(state: OrderFlavourState): CompletionStage<Void> =
        saveJson(orderFlavourKey(state.orderId), OrderFlavourState.serializer(), state)

    private fun <T> loadJson(key: String, serializer: KSerializer<T>): CompletionStage<T?> =
        load(key).thenApply { bytes -> bytes?.let { json.decodeFromString(serializer, it.decodeToString()) } }

    private fun <T> saveJson(key: String, serializer: KSerializer<T>, value: T): CompletionStage<Void> =
        save(key, json.encodeToString(serializer, value).encodeToByteArray())

    private fun save(key: String, value: ByteArray): CompletionStage<Void> =
        orderStateRedisTemplate.opsForValue().set(key, value).toFuture().thenAccept { }

    private fun delete(key: String): CompletionStage<Void> =
        orderStateRedisTemplate.delete(key).toFuture().thenAccept { }

    private fun load(key: String): CompletionStage<ByteArray?> =
        orderStateRedisTemplate.opsForValue().get(key)
            .switchIfEmpty(Mono.empty())
            .toFuture()

    private fun orderKey(orderId: String): String = "order-state:$orderId"

    private fun batchKey(batchId: String): String = "batch-state:$batchId"

    private fun activeBatchKey(cauldronId: String): String = "cauldron-active-batch:$cauldronId"

    private fun modelContextKey(batchId: String): String = "model-context:$batchId"

    private fun orderFlavourKey(orderId: String): String = "order-flavour:$orderId"
}
