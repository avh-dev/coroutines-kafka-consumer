package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.util.Optional

class KafkaWarmupRecordSenderTest {
    @Test
    fun `acknowledged send preserves key value headers and timestamp`() = runBlocking {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            ),
            sessionId = WarmupSessionId("session-7"),
            producerFactory = { producer }
        )
        val headers = RecordHeaders().add("trace-id", "trace-7".encodeToByteArray())
        val source = ConsumerRecord(
            "source-orders",
            2,
            7L,
            1_234L,
            TimestampType.CREATE_TIME,
            5,
            7,
            "key-7",
            "value-7",
            headers,
            Optional.empty()
        )

        val acknowledgement = sender.send(source, WarmupDestination("warmup-orders", 0))

        val sent = producer.history().single()
        assertEquals("warmup-orders", sent.topic())
        assertEquals(0, sent.partition())
        assertEquals(1_234L, sent.timestamp())
        assertEquals("key-7", sent.key())
        assertEquals("value-7", sent.value())
        assertEquals("trace-7", sent.headers().lastHeader("trace-id").value().decodeToString())
        assertEquals("session-7", sent.headers().lastHeader(WARMUP_SESSION_HEADER).value().decodeToString())
        assertEquals(0L, acknowledgement.offset)
        sender.close()
    }

    @Test
    fun `blocking producer send remains bounded by caller deadline`() {
        val producer = mock<Producer<String, String>>()
        whenever(producer.send(any<ProducerRecord<String, String>>())).thenAnswer {
            Thread.sleep(5_000)
            error("sleep should be interrupted")
        }
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            ),
            producerFactory = { producer }
        )
        val started = System.nanoTime()

        assertThrows(kotlinx.coroutines.TimeoutCancellationException::class.java) {
            runBlocking {
                withTimeout(50) {
                    sender.send(ConsumerRecord("source", 0, 0L, "key", "value"), WarmupDestination("warmup", 0))
                }
            }
        }

        val elapsedMillis = (System.nanoTime() - started) / 1_000_000
        org.junit.jupiter.api.Assertions.assertTrue(elapsedMillis < 1_000, "elapsed=$elapsedMillis ms")
    }
}
