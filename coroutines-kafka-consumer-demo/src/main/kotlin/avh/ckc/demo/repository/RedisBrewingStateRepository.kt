package avh.ckc.demo.repository

import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import kotlinx.serialization.json.Json
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class RedisBrewingStateRepository(
    private val orderStateRedisTemplate: ReactiveRedisTemplate<String, ByteArray>,
    private val json: Json = Json { ignoreUnknownKeys = true }
) : BrewingStateRepository {
    override fun applyLifecycleEvent(event: OrderLifecycleEvent): CompletionStage<Void> {
        val saveOrder = findOrder(event.orderId).thenCompose { existing ->
            val orderState = mergeOrderState(event, existing)
            save(orderKey(event.orderId), json.encodeToString(OrderState.serializer(), orderState).encodeToByteArray())
        }

        val saveBatch = event.batchId.takeIf(String::isNotBlank)?.let { batchId ->
            findBatch(batchId).thenCompose { existing ->
                val batchState = mergeBatchState(event, batchId, existing)
                save(batchKey(batchId), json.encodeToString(BatchState.serializer(), batchState).encodeToByteArray())
                    .thenCompose { updateActiveBatch(event, batchId) }
            }
        } ?: completedVoid()

        return CompletableFuture.allOf(saveOrder.toCompletableFuture(), saveBatch.toCompletableFuture())
    }

    override fun findOrder(orderId: String): CompletionStage<OrderState?> =
        load(orderKey(orderId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(OrderState.serializer(), it.decodeToString()) } }

    override fun findBatch(batchId: String): CompletionStage<BatchState?> =
        load(batchKey(batchId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(BatchState.serializer(), it.decodeToString()) } }

    override fun findActiveBatchId(cauldronId: String): CompletionStage<String?> =
        load(activeBatchKey(cauldronId))
            .thenApply { bytes -> bytes?.decodeToString() }

    override fun findModelContext(batchId: String): CompletionStage<ModelContextState?> =
        load(modelContextKey(batchId))
            .thenApply { bytes -> bytes?.let { json.decodeFromString(ModelContextState.serializer(), it.decodeToString()) } }

    override fun saveModelContext(context: ModelContextState): CompletionStage<Void> =
        save(
            modelContextKey(context.batchId),
            json.encodeToString(ModelContextState.serializer(), context).encodeToByteArray()
        )

    private fun mergeOrderState(event: OrderLifecycleEvent, existing: OrderState?): OrderState {
        return OrderState(
            orderId = event.orderId,
            batchId = event.batchId.ifBlank { existing?.batchId },
            potionId = event.potionId.ifBlank { existing?.potionId ?: "" },
            recipeId = event.recipeId.ifBlank { existing?.recipeId },
            customerId = event.customerId.ifBlank { existing?.customerId ?: "" },
            cauldronId = event.cauldronId.ifBlank { existing?.cauldronId },
            status = event.eventType.name,
            updatedAt = event.metadata.occurredAt
        )
    }

    private fun mergeBatchState(event: OrderLifecycleEvent, batchId: String, existing: BatchState?): BatchState {
        val orderIds = (existing?.orderIds.orEmpty() + event.orderId)
            .filter(String::isNotBlank)
            .distinct()

        return BatchState(
            batchId = batchId,
            recipeId = event.recipeId.ifBlank { existing?.recipeId },
            potionId = event.potionId.ifBlank { existing?.potionId },
            cauldronId = event.cauldronId.ifBlank { existing?.cauldronId },
            status = event.eventType.name,
            orderIds = orderIds,
            updatedAt = event.metadata.occurredAt
        )
    }

    private fun updateActiveBatch(event: OrderLifecycleEvent, batchId: String): CompletionStage<Void> {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return completedVoid()
        return when (event.eventType) {
            OrderLifecycleEventType.CAULDRON_ASSIGNED,
            OrderLifecycleEventType.BREWING_STARTED -> save(activeBatchKey(cauldronId), batchId.encodeToByteArray())

            OrderLifecycleEventType.BREWING_COMPLETED -> findActiveBatchId(cauldronId).thenCompose { activeBatchId ->
                if (activeBatchId == batchId) {
                    delete(activeBatchKey(cauldronId))
                } else {
                    completedVoid()
                }
            }

            else -> completedVoid()
        }
    }

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

    private fun completedVoid(): CompletionStage<Void> = CompletableFuture.completedFuture(null)
}
