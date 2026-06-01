package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.DemoRedisCommands
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisCoroutinesCommandsImpl
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class AuditLogTest {
    @Test
    fun `encodes processed record as compact Redis audit payload`() {
        val encoded = encodeAuditRecord("C", DemoTopics.CAULDRON_EVENTS, 4, 77, 2_000, "cauldron\n1").decodeToString()

        assertTrue(encoded.matches(Regex("""C\t3\t4\t77\t2000\t\d+\tcauldron 1""")))
    }

    @Test
    fun `sync processing waits for Redis acknowledgement`() {
        val write = CompletableFuture<Long>()
        val audit = auditLog(write)

        val processed = CompletableFuture.runAsync {
            audit.processed(DemoTopics.ORDER_EVENTS, "order-1", 0, 1, 1_000)
        }

        assertFalse(processed.isDone)
        write.complete(1)
        processed.join()
    }

    @Test
    fun `suspend processing waits for Redis acknowledgement`() = runBlocking {
        val write = CompletableFuture<Long>()
        val audit = auditLog(write)
        val record = ConsumerRecord(DemoTopics.ORDER_EVENTS, 0, 1, "order-1", "value")

        val processed = async {
            audit.processedSuspending(record)
        }

        yield()
        assertFalse(processed.isCompleted)
        write.complete(1)
        processed.await()
    }

    @Test
    fun `disabled audit does not call Redis`() {
        val redisCommands = mockRedisCommands()
        val properties = DemoApplicationProperties(audit = DemoApplicationProperties.Audit(enabled = false))

        AuditLog(properties, redisCommands).processed(DemoTopics.ORDER_EVENTS, "order-1", 0, 1, 1_000)

        verifyNoInteractions(redisCommands)
    }

    private fun auditLog(write: CompletableFuture<Long>): AuditLog {
        val redisCommands = mockRedisCommands()
        val syncCommands = mockSyncCommands()
        val reactiveCommands = mockReactiveCommands()
        `when`(redisCommands.sync()).thenReturn(syncCommands)
        `when`(redisCommands.coroutines()).thenReturn(RedisCoroutinesCommandsImpl(reactiveCommands))
        `when`(syncCommands.rpush(anyString(), any(ByteArray::class.java))).thenAnswer { write.join() }
        `when`(reactiveCommands.rpush(anyString(), any(ByteArray::class.java))).thenReturn(Mono.fromFuture(write))
        return AuditLog(DemoApplicationProperties(), redisCommands)
    }

    private fun mockRedisCommands(): DemoRedisCommands =
        mock(DemoRedisCommands::class.java)

    @Suppress("UNCHECKED_CAST")
    private fun mockSyncCommands(): RedisCommands<String, ByteArray> =
        mock(RedisCommands::class.java) as RedisCommands<String, ByteArray>

    @Suppress("UNCHECKED_CAST")
    private fun mockReactiveCommands(): RedisReactiveCommands<String, ByteArray> =
        mock(RedisReactiveCommands::class.java) as RedisReactiveCommands<String, ByteArray>

}
