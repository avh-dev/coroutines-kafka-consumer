package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SwitchableWarmupRecordForwardingTest {
    @Test
    fun `finishing a session waits for readers that captured it before returning to local routing`() = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val delegate = object : WarmupRecordForwarding<String, String> {
            var closed = false
            override suspend fun forwardIfSelected(record: ConsumerRecord<String, String>): Boolean {
                entered.complete(Unit)
                release.await()
                return false
            }

            override suspend fun complete() = WarmupForwardingBoundary(
                mapOf(org.apache.kafka.common.TopicPartition("warmup", 0) to null)
            )

            override suspend fun close() { closed = true }
        }
        val session = WarmupSessionId("session")
        val gateway = SwitchableWarmupRecordForwarding<String, String> { _, _ -> delegate }
        gateway.activate(
            WarmupForwardingConfiguration(
                sessionId = session,
                consumerId = WarmupConsumerId("orders"),
                assignments = listOf(WarmupPartitionAssignment(
                    WarmupConsumerId("orders"), "orders", "warmup", 0
                )),
                profile = WarmupForwardingProfile(),
                producerProperties = mapOf(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
                )
            ),
            WarmupForwardingObserver.NOOP
        )

        val forwarding = async {
            gateway.forwardIfSelected(ConsumerRecord("orders", 0, 1L, "key", "value"))
        }
        entered.await()
        val finishing = async { gateway.finish(session) }
        assertFalse(finishing.isCompleted)
        assertFalse(delegate.closed)

        release.complete(Unit)
        assertFalse(forwarding.await())
        finishing.await()
        assertTrue(delegate.closed)
        assertFalse(gateway.forwardIfSelected(ConsumerRecord("orders", 0, 2L, "key", "value")))
    }
}
