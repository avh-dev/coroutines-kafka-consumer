package avh.ckc.core

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

class ManagedConsumerLifecycleTest {
    @Test
    fun `public lifecycle delegates to attached library lifecycle without starting runtime`() = runBlocking {
        val consumer = createTestConsumer<String, String>(
            records = emptyList(),
            consumerProperties = mapOf(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
            ),
            handler = KafkaRecordHandler { }
        )
        val calls = AtomicInteger()
        consumer.attachManagedLifecycle(object : ManagedConsumerLifecycle {
            override fun start() { calls.incrementAndGet() }
            override suspend fun awaitReady() { calls.incrementAndGet() }
            override suspend fun stop() { calls.incrementAndGet() }
            override suspend fun awaitTermination() { calls.incrementAndGet() }
        })

        consumer.start()
        consumer.awaitReady()
        consumer.stop()
        consumer.awaitTermination()

        assertEquals(4, calls.get())
        assertEquals(false, consumer.stateSnapshot().started)
    }
}
