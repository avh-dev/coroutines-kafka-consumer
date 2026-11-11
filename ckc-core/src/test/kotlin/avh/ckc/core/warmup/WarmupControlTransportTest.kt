package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration as JavaDuration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.milliseconds

class WarmupControlTransportTest {
    private val local = WarmupInstanceId("local")
    private val mapping = WarmupTopicMapping(WarmupConsumerId("orders"), "orders", "warmup-orders")

    @Test
    fun `bounded history is dispatched before transport becomes ready`() = runBlocking {
        val messages = listOf(request("first"), request("second"))
        val consumer = replayConsumer(messages, beginning = 0L, end = 5L)
        val producer = mock<Producer<String, ByteArray>>()
        val handled = mutableListOf<String>()
        val firstEntered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val transport = transport(producer, consumer, historyLimit = 2) { record ->
            handled += record.message.sessionId.value
            if (handled.size == 1) {
                firstEntered.complete(Unit)
                release.await()
            }
        }

        try {
            transport.start()
            val ready = async { transport.awaitReady() }
            withTimeout(1_000) { firstEntered.await() }
            assertFalse(ready.isCompleted)
            release.complete(Unit)
            withTimeout(1_000) { ready.await() }

            assertEquals(listOf("first", "second"), handled)
            assertEquals(3L, transport.logPosition().replayStartOffset)
            verify(consumer).seek(TopicPartition("control", 0), 3L)
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `send deadline interrupts blocking producer and caps kafka max block`() = runBlocking {
        val sendEntered = CompletableDeferred<Unit>()
        val sendInterrupted = CompletableDeferred<Unit>()
        val properties = AtomicReference<Map<String, Any?>>()
        val producer = mock<Producer<String, ByteArray>>()
        doAnswer {
            sendEntered.complete(Unit)
            try {
                CountDownLatch(1).await()
            } catch (interrupted: InterruptedException) {
                sendInterrupted.complete(Unit)
                throw interrupted
            }
            mock<Future<RecordMetadata>>()
        }.whenever(producer).send(any<ProducerRecord<String, ByteArray>>(), any<Callback>())
        val transport = transport(
            producer,
            replayConsumer(emptyList(), beginning = 0L, end = 0L),
            sendTimeoutMillis = 50,
            producerPropertiesObserver = properties::set
        ) { }

        try {
            transport.start()
            withTimeout(1_000) { transport.awaitReady() }
            assertThrows(TimeoutCancellationException::class.java) {
                runBlocking { transport.send(accepted()) }
            }
            withTimeout(1_000) {
                sendEntered.await()
                sendInterrupted.await()
            }
            assertTrue(
                properties.get().getValue(ProducerConfig.MAX_BLOCK_MS_CONFIG).toString().toLong() <= 50L
            )
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `malformed replay record fails closed before readiness`() {
        val request = request("session")
        val transport = transport(
            mock(),
            replayConsumer(listOf(request), beginning = 0L, end = 1L) { "wrong-session-key" }
        ) { }

        transport.start()
        try {
            assertThrows(InvalidWarmupControlRecordException::class.java) {
                runBlocking { withTimeout(1_000) { transport.awaitReady() } }
            }
        } finally {
            runBlocking { transport.stop() }
        }
    }

    @Test
    fun `cancelled stop caller does not cancel shared bounded cleanup`() = runBlocking {
        val closeEntered = CompletableDeferred<Unit>()
        val releaseClose = CountDownLatch(1)
        val producer = mock<Producer<String, ByteArray>>()
        doAnswer {
            closeEntered.complete(Unit)
            releaseClose.await()
            Unit
        }.whenever(producer).close(any<JavaDuration>())
        val transport = transport(producer, replayConsumer(emptyList(), 0L, 0L)) { }
        transport.start()
        withTimeout(1_000) { transport.awaitReady() }

        val firstStop = async(Dispatchers.Default) { transport.stop() }
        withTimeout(1_000) { closeEntered.await() }
        firstStop.cancelAndJoin()
        val repeatedStop = async(Dispatchers.Default) { transport.stop() }
        assertFalse(repeatedStop.isCompleted)

        releaseClose.countDown()
        withTimeout(1_000) { repeatedStop.await() }
        verify(producer).close(any<JavaDuration>())
    }

    private fun transport(
        producer: Producer<String, ByteArray>,
        consumer: Consumer<String, ByteArray>,
        historyLimit: Int = 10,
        sendTimeoutMillis: Long = 1_000,
        producerPropertiesObserver: (Map<String, Any?>) -> Unit = { },
        handler: WarmupControlMessageHandler
    ) = WarmupControlTransport(
        configuration = WarmupControlTransportConfiguration(
            instanceId = local,
            topic = "control",
            producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            historyRecordLimit = historyLimit,
            pollTimeout = 1.milliseconds,
            sendTimeout = sendTimeoutMillis.milliseconds
        ),
        handler = handler,
        parentContext = Dispatchers.Default,
        producerFactory = {
            producerPropertiesObserver(it)
            producer
        },
        consumerFactory = { consumer }
    )

    private fun replayConsumer(
        messages: List<WarmupControlMessage>,
        beginning: Long,
        end: Long,
        recordKey: (WarmupControlMessage) -> String = { it.sessionId.value }
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val firstPoll = AtomicBoolean(true)
        val partition = TopicPartition("control", 0)
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.beginningOffsets(listOf(partition))).thenReturn(mapOf(partition to beginning))
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to end))
        whenever(consumer.position(partition)).thenReturn(end)
        doAnswer {
            if (firstPoll.compareAndSet(true, false)) {
                listener.get().onPartitionsAssigned(listOf(partition))
                val firstOffset = end - messages.size
                ConsumerRecords(
                    mapOf(
                        partition to messages.mapIndexed { index, message ->
                            org.apache.kafka.clients.consumer.ConsumerRecord(
                                "control",
                                0,
                                firstOffset + index,
                                recordKey(message),
                                WarmupControlCodec.encode(message)
                            )
                        }
                    )
                )
            } else {
                Thread.sleep(1)
                ConsumerRecords.empty<String, ByteArray>()
            }
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun request(id: String) = WarmupRequested(
        WarmupSessionId(id), local, listOf(mapping), 2_000L, "fingerprint", 10_000L
    )

    private fun accepted() = DonorAccepted(
        WarmupSessionId("session"), local, WarmupInstanceId("recipient"), "fingerprint", 10_000L
    )
}
