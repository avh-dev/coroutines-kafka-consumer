package avh.ckc.core.warmup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration as JavaDuration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupControlTransportTest {
    private val local = WarmupInstanceId("local")
    private val namespace = WarmupControlNamespace("global5-shared-partition-v6-test")
    private val mapping = WarmupTopicMapping(
        WarmupParticipantId("orders"), "orders", "warmup-orders", "mapping-fingerprint"
    )

    @Test
    fun `transport starts at current tail and ignores another v6 namespace`() = runBlocking {
        val releaseRecords = AtomicBoolean(false)
        val matching = request(namespace, "matching")
        val consumer = tailConsumer(
            listOf(request(WarmupControlNamespace("other-v6-test"), "foreign"), matching),
            end = 19_110,
            releaseRecords = releaseRecords
        )
        val handled = mutableListOf<String>()
        val transport = transport(consumer) { handled += it.message.sessionId.value }

        try {
            transport.start()
            withTimeout(1_000) { transport.awaitReady() }
            assertTrue(handled.isEmpty())
            verify(consumer).seek(TopicPartition("control", 0), 19_110)

            releaseRecords.set(true)
            withTimeout(1_000) {
                while (handled.isEmpty()) kotlinx.coroutines.yield()
            }
            assertEquals(listOf("matching"), handled)
            assertEquals(19_109, transport.initialOffsetBoundary())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `own send deadline becomes a transport failure while external timeout remains cancellation`() = runBlocking {
        val consumer = tailConsumer(emptyList(), end = 0, releaseRecords = AtomicBoolean(false))
        val transport = transport(consumer, sendTimeout = 20.milliseconds) { }
        try {
            transport.start()
            transport.awaitReady()
            assertThrows(WarmupControlSendTimeoutException::class.java) {
                runBlocking { transport.send(request(namespace, "own-timeout")) }
            }
        } finally {
            transport.stop()
        }

        val second = transport(
            tailConsumer(emptyList(), end = 0, releaseRecords = AtomicBoolean(false)),
            sendTimeout = 5.seconds
        ) { }
        try {
            second.start()
            second.awaitReady()
            assertThrows(TimeoutCancellationException::class.java) {
                runBlocking { withTimeout(20) { second.send(request(namespace, "outer-timeout")) } }
            }
        } finally {
            second.stop()
        }
        Unit
    }

    @Test
    fun `send timeout from control dispatch terminates transport instead of silently cancelling dispatch`() = runBlocking {
        val releaseRecords = AtomicBoolean(false)
        val consumer = tailConsumer(listOf(request(namespace, "incoming")), 0, releaseRecords)
        lateinit var transport: WarmupControlTransport
        transport = transport(consumer, sendTimeout = 20.milliseconds) {
            transport.send(request(namespace, "response"))
        }
        try {
            transport.start()
            transport.awaitReady()
            releaseRecords.set(true)
            assertThrows(WarmupControlSendTimeoutException::class.java) {
                runBlocking { withTimeout(1_000) { transport.awaitTermination() } }
            }
        } finally {
            transport.stop()
        }
        Unit
    }

    private fun transport(
        consumer: Consumer<String, ByteArray>,
        sendTimeout: kotlin.time.Duration = 30.seconds,
        handler: WarmupControlMessageHandler
    ) = WarmupControlTransport(
        configuration = WarmupControlTransportConfiguration(
            instanceId = local,
            namespace = namespace,
            topic = "control",
            producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            pollTimeout = 1.milliseconds,
            sendTimeout = sendTimeout
        ),
        handler = handler,
        parentContext = Dispatchers.Default,
        producerFactory = { mock<Producer<String, ByteArray>>() },
        consumerFactory = { consumer }
    )

    private fun tailConsumer(
        messages: List<WarmupControlMessage>,
        end: Long,
        releaseRecords: AtomicBoolean
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assigned = AtomicBoolean(false)
        val delivered = AtomicBoolean(false)
        val partition = TopicPartition("control", 0)
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to end))
        whenever(consumer.position(partition)).thenReturn(end)
        doAnswer {
            when {
                assigned.compareAndSet(false, true) -> {
                    listener.get().onPartitionsAssigned(listOf(partition))
                    ConsumerRecords.empty<String, ByteArray>()
                }
                releaseRecords.get() && delivered.compareAndSet(false, true) -> ConsumerRecords(
                    mapOf(
                        partition to messages.mapIndexed { index, message ->
                            org.apache.kafka.clients.consumer.ConsumerRecord(
                                "control", 0, end + index, message.sessionId.value,
                                WarmupControlCodec.encode(message)
                            )
                        }
                    )
                )
                else -> {
                    Thread.sleep(1)
                    ConsumerRecords.empty<String, ByteArray>()
                }
            }
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun request(namespace: WarmupControlNamespace, session: String) = WarmupRequested(
        namespace, WarmupSessionId(session), local, listOf(mapping), 2_000, 10, 2, "fingerprint", 10_000
    )
}
