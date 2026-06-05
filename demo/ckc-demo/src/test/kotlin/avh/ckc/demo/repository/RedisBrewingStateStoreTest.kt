package avh.ckc.demo.repository

import avh.ckc.demo.config.DemoRedisCommands
import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.BrewingStepReceipt
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommandsImpl
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.`when`
import reactor.core.publisher.Mono
import kotlin.test.Test

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisBrewingStateStoreTest {
    @Test
    fun `sync state writes do not expire`() {
        val redisCommands = mockRedisCommands()
        val syncCommands = mockSyncCommands()
        `when`(redisCommands.sync()).thenReturn(syncCommands)

        RedisBrewingStateStore(redisCommands).saveBatch(sampleBatch())

        verify(syncCommands).set(
            eq("batch-state:batch-1"),
            any(ByteArray::class.java)
        )
        verifyNoMoreInteractions(syncCommands)
    }

    @Test
    fun `suspend state writes do not expire`() = runBlocking {
        val redisCommands = mockRedisCommands()
        val reactiveCommands = mockReactiveCommands()
        `when`(redisCommands.coroutines()).thenReturn(RedisCoroutinesCommandsImpl(reactiveCommands))
        `when`(reactiveCommands.set(
            eq("batch-state:batch-1"),
            any(ByteArray::class.java)
        )).thenReturn(Mono.just("OK"))

        RedisBrewingStateStore(redisCommands).saveBatchSuspending(sampleBatch())

        verify(reactiveCommands).set(
            eq("batch-state:batch-1"),
            any(ByteArray::class.java)
        )
        verifyNoMoreInteractions(reactiveCommands)
    }

    @Test
    fun `sync brewing step receipt writes do not expire`() {
        val redisCommands = mockRedisCommands()
        val syncCommands = mockSyncCommands()
        `when`(redisCommands.sync()).thenReturn(syncCommands)

        RedisBrewingStateStore(redisCommands).saveBrewingStepReceipt(sampleReceipt())

        verify(syncCommands).set(
            eq("brewing-step-receipt:batch-1:3"),
            any(ByteArray::class.java)
        )
        verifyNoMoreInteractions(syncCommands)
    }

    private fun sampleBatch(): Batch =
        Batch(
            batchId = "batch-1",
            recipeId = "recipe-1",
            potionId = null,
            cauldronId = "cauldron-1",
            status = "BREWING",
            orderIds = listOf("order-1"),
            updatedAt = "2026-06-01T12:00:00Z"
        )

    private fun sampleReceipt(): BrewingStepReceipt =
        BrewingStepReceipt(
            batchId = "batch-1",
            cauldronId = "cauldron-1",
            stepNumber = 3,
            stepCode = "PHOENIX_ASH_FOLD",
            receiptId = "acr-batch-1-3",
            acceptedAt = "2026-06-01T12:00:01Z",
            registryShard = "acr-shard-01",
            regulatoryTraceId = "mrb-test",
            updatedAt = "2026-06-01T12:00:00Z"
        )

    private fun mockRedisCommands(): DemoRedisCommands =
        mock(DemoRedisCommands::class.java)

    @Suppress("UNCHECKED_CAST")
    private fun mockSyncCommands(): RedisCommands<String, ByteArray> =
        mock(RedisCommands::class.java) as RedisCommands<String, ByteArray>

    @Suppress("UNCHECKED_CAST")
    private fun mockReactiveCommands(): RedisReactiveCommands<String, ByteArray> =
        mock(RedisReactiveCommands::class.java) as RedisReactiveCommands<String, ByteArray>
}
