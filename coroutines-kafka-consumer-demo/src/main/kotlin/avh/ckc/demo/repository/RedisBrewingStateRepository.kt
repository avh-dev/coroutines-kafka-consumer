package avh.ckc.demo.repository

import kotlinx.serialization.json.Json
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono
import java.util.concurrent.CompletionStage

class RedisBrewingStateRepository(
    private val orderStateRedisTemplate: ReactiveRedisTemplate<String, ByteArray>,
    private val json: Json = Json { ignoreUnknownKeys = true }
) : BrewingStateRepository {
    override fun findOrder(orderId: String): CompletionStage<OrderState?> =
        load(orderKey(orderId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(OrderState.serializer(), it.decodeToString()) } }

    override fun saveOrder(orderState: OrderState): CompletionStage<Void> =
        save(orderKey(orderState.orderId), json.encodeToString(OrderState.serializer(), orderState).encodeToByteArray())

    override fun findBatch(batchId: String): CompletionStage<BatchState?> =
        load(batchKey(batchId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(BatchState.serializer(), it.decodeToString()) } }

    override fun saveBatch(batchState: BatchState): CompletionStage<Void> =
        save(batchKey(batchState.batchId), json.encodeToString(BatchState.serializer(), batchState).encodeToByteArray())

    override fun findActiveBatchId(cauldronId: String): CompletionStage<String?> =
        load(activeBatchKey(cauldronId))
            .thenApply { bytes -> bytes?.decodeToString() }

    override fun saveActiveBatchId(cauldronId: String, batchId: String): CompletionStage<Void> =
        save(activeBatchKey(cauldronId), batchId.encodeToByteArray())

    override fun deleteActiveBatchId(cauldronId: String): CompletionStage<Void> =
        delete(activeBatchKey(cauldronId))

    override fun findModelContext(batchId: String): CompletionStage<ModelContextState?> =
        load(modelContextKey(batchId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(ModelContextState.serializer(), it.decodeToString()) } }

    override fun saveModelContext(context: ModelContextState): CompletionStage<Void> =
        save(
            modelContextKey(context.batchId),
            json.encodeToString(ModelContextState.serializer(), context).encodeToByteArray()
        )

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
}
