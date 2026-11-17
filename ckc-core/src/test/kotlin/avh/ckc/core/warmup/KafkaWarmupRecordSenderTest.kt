package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.seconds

class KafkaWarmupRecordSenderTest {
    @Test
    fun `sender replaces foreign session headers with exactly one current session`() = runBlocking {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            ),
            submissionDispatcher = Dispatchers.Unconfined,
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

    @Test
    fun `pending acknowledgements do not occupy one submission worker per record`() = runBlocking {
        val producer = MockProducer<String, String>(false, StringSerializer(), StringSerializer())
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = producerProperties(),
            submissionDispatcher = dispatcher,
            producerFactory = { producer }
        )

        try {
            coroutineScope {
                val sends = List(64) { offset ->
                    async {
                        sender.send(
                            ConsumerRecord("source", 0, offset.toLong(), "key-$offset", "value"),
                            WarmupDestination("warmup"),
                            WarmupSessionId("session")
                        )
                    }
                }

                withTimeout(1.seconds) {
                    while (producer.history().size < sends.size) kotlinx.coroutines.yield()
                }
                assertTrue(sends.none { it.isCompleted })
                repeat(sends.size) { assertTrue(producer.completeNext()) }
                withTimeout(1.seconds) { sends.awaitAll() }
            }
        } finally {
            sender.close()
            dispatcher.close()
            executor.shutdownNow()
        }
        Unit
    }

    @Test
    fun `cancellation while awaiting acknowledgement does not retain a dispatcher worker`() = runBlocking {
        val producer = MockProducer<String, String>(false, StringSerializer(), StringSerializer())
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = producerProperties(),
            submissionDispatcher = dispatcher,
            producerFactory = { producer }
        )

        try {
            val cancelled = launch {
                sender.send(
                    ConsumerRecord("source", 0, 1, "cancelled", "value"),
                    WarmupDestination("warmup"),
                    WarmupSessionId("session")
                )
            }
            withTimeout(1.seconds) {
                while (producer.history().isEmpty()) kotlinx.coroutines.yield()
            }
            cancelled.cancelAndJoin()

            val next = async {
                sender.send(
                    ConsumerRecord("source", 0, 2, "next", "value"),
                    WarmupDestination("warmup"),
                    WarmupSessionId("session")
                )
            }
            withTimeout(1.seconds) {
                while (producer.history().size < 2) kotlinx.coroutines.yield()
            }
            assertTrue(producer.completeNext())
            assertTrue(producer.completeNext())
            withTimeout(1.seconds) { next.await() }
        } finally {
            sender.close()
            dispatcher.close()
            executor.shutdownNow()
        }
        Unit
    }

    @Test
    fun `synchronous producer failure is returned to the delegation`() = runBlocking {
        val failure = KafkaException("send rejected")
        val producer = mock<Producer<String, String>>()
        whenever(
            producer.send(
                any<ProducerRecord<String, String>>(),
                any<Callback>()
            )
        ).thenThrow(failure)
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = producerProperties(),
            submissionDispatcher = Dispatchers.Unconfined,
            producerFactory = { producer }
        )

        val actual = runCatching {
            sender.send(
                ConsumerRecord("source", 0, 1, "key", "value"),
                WarmupDestination("warmup"),
                WarmupSessionId("session")
            )
        }.exceptionOrNull()

        assertSame(failure, actual)
    }

    @Test
    fun `asynchronous producer failure is returned to the delegation`() = runBlocking {
        val producer = MockProducer<String, String>(false, StringSerializer(), StringSerializer())
        val sender = KafkaWarmupRecordSender<String, String>(
            producerProperties = producerProperties(),
            submissionDispatcher = Dispatchers.Unconfined,
            producerFactory = { producer }
        )
        val failure = KafkaException("acknowledgement failed")

        try {
            val send = async {
                runCatching {
                    sender.send(
                        ConsumerRecord("source", 0, 1, "key", "value"),
                        WarmupDestination("warmup"),
                        WarmupSessionId("session")
                    )
                }
            }
            withTimeout(1.seconds) {
                while (producer.history().isEmpty()) kotlinx.coroutines.yield()
            }
            assertTrue(producer.errorNext(failure))

            val actual = withTimeout(1.seconds) { send.await() }.exceptionOrNull()
            assertTrue(actual is KafkaException)
            assertEquals(failure.message, actual?.message)
        } finally {
            sender.close()
        }
    }

    private fun producerProperties(): Map<String, Any?> = mapOf(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
    )
}
