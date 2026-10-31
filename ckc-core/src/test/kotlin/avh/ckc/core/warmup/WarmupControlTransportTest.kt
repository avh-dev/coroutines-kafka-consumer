package avh.ckc.core.warmup

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class WarmupControlTransportTest {
    private val instanceId = WarmupInstanceId("instance-1")
    private val sessionId = WarmupSessionId("session-1")

    @Test
    fun `handler timeout is a terminal transport failure`() = runBlocking {
        val message = WarmupRequest(
            sessionId,
            instanceId,
            listOf(WarmupParticipantConfiguration(
                WarmupConsumerId("orders"),
                listOf(WarmupTopicMapping("orders", "warmup-orders"))
            ))
        )
        val consumer = readyConsumer(listOf(message))
        val handlerFailure = CompletableDeferred<TimeoutCancellationException>()
        val transport = transport(consumer = consumer) {
            try {
                withTimeout(10) { awaitCancellation() }
            } catch (error: TimeoutCancellationException) {
                handlerFailure.complete(error)
                throw error
            }
        }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(2_000) {
                val expected = handlerFailure.await()
                val actual = assertFailsWith<TimeoutCancellationException> {
                    transport.awaitTermination()
                }
                assertSame(expected, actual)
            }
        } finally {
            transport.stop()
        }
        Unit
    }

    @Test
    fun `cancelled first stop does not prevent repeated stop from completing cleanup`() = runBlocking {
        val closeStarted = CompletableDeferred<Unit>()
        val releaseClose = CountDownLatch(1)
        val producer = mock<KafkaProducer<String, ByteArray>>()
        doAnswer {
            closeStarted.complete(Unit)
            releaseClose.await()
            Unit
        }.whenever(producer).close(any<Duration>())
        val transport = transport(producer, readyConsumer()) { }

        transport.start()
        transport.awaitReady()
        val firstStop = async(Dispatchers.Default) { transport.stop() }
        withTimeout(2_000) { closeStarted.await() }
        firstStop.cancelAndJoin()

        val repeatedStop = async(Dispatchers.Default) { transport.stop() }
        assertTrue(!repeatedStop.isCompleted)
        releaseClose.countDown()
        withTimeout(2_000) { repeatedStop.await() }
        verify(producer).close(any<Duration>())
    }

    @Test
    fun `send timeout interrupts blocking producer send and bounds Kafka max block`() = runBlocking {
        val sendEntered = CompletableDeferred<Unit>()
        val sendInterrupted = CompletableDeferred<Unit>()
        val properties = AtomicReference<Map<String, Any?>>()
        val producer = mock<KafkaProducer<String, ByteArray>>()
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
            producer = producer,
            consumer = readyConsumer(),
            sendTimeoutMillis = 100,
            producerPropertiesObserver = properties::set
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            assertFailsWith<TimeoutCancellationException> {
                transport.send(SessionFailed(sessionId, instanceId, null, "test"))
            }
            withTimeout(2_000) {
                sendEntered.await()
                sendInterrupted.await()
            }
            assertTrue(
                properties.get().getValue(ProducerConfig.MAX_BLOCK_MS_CONFIG).toString().toLong() <= 100L
            )
        } finally {
            transport.stop()
        }
    }

    private fun transport(
        producer: KafkaProducer<String, ByteArray> = mock(),
        consumer: KafkaConsumer<String, ByteArray>,
        sendTimeoutMillis: Long = 1_000,
        producerPropertiesObserver: (Map<String, Any?>) -> Unit = { },
        handler: WarmupControlMessageHandler
    ): WarmupControlTransport = WarmupControlTransport(
        configuration = WarmupControlTransportConfiguration(
            instanceId = instanceId,
            topic = "control",
            producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            pollTimeout = 1.milliseconds,
            sendTimeout = sendTimeoutMillis.milliseconds
        ),
        handler = handler,
        producerFactory = {
            producerPropertiesObserver(it)
            producer
        },
        consumerFactory = { consumer }
    )

    private fun readyConsumer(messages: List<WarmupControlMessage> = emptyList()): KafkaConsumer<String, ByteArray> {
        val consumer = mock<KafkaConsumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val firstPoll = AtomicBoolean(true)
        val partition = TopicPartition("control", 0)
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.position(partition)).thenReturn(0L)
        whenever(consumer.beginningOffsets(listOf(partition))).thenReturn(mapOf(partition to 0L))
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to 0L))
        doAnswer {
            if (firstPoll.compareAndSet(true, false)) {
                listener.get().onPartitionsAssigned(listOf(partition))
                ConsumerRecords(mapOf(
                    partition to messages.mapIndexed { index, message ->
                        org.apache.kafka.clients.consumer.ConsumerRecord(
                            "control", 0, index.toLong(), message.sessionId.value, WarmupControlCodec.encode(message)
                        )
                    }
                ))
            } else {
                Thread.sleep(1)
                ConsumerRecords.empty<String, ByteArray>()
            }
        }.whenever(consumer).poll(any<Duration>())
        return consumer
    }
}
