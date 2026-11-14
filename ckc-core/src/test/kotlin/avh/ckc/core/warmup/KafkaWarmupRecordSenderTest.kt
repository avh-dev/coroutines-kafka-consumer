package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class KafkaWarmupRecordSenderTest {
    @Test
    fun `sender replaces foreign session headers with exactly one current session`() = runBlocking {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            ),
            blockingDispatcher = Dispatchers.Unconfined,
            producerFactory = { producer }
        )
        val source = ConsumerRecord("source", 2, 9, "key", "value")
        source.headers().add(WARMUP_SESSION_HEADER, "old-a".toByteArray())
        source.headers().add(WARMUP_SESSION_HEADER, "old-b".toByteArray())

        sender.send(source, WarmupDestination("warmup"), WarmupSessionId("current"))

        val forwarded = producer.history().single()
        assertEquals("warmup", forwarded.topic())
        assertEquals(0, forwarded.partition())
        assertEquals("key", forwarded.key())
        assertEquals("value", forwarded.value())
        val sessionHeaders = forwarded.headers().headers(WARMUP_SESSION_HEADER).toList()
        assertEquals(1, sessionHeaders.size)
        assertArrayEquals("current".toByteArray(), sessionHeaders.single().value())
    }
}
