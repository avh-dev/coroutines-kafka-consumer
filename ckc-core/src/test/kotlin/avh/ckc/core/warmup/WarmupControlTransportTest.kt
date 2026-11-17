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
import org.apache.kafka.common.PartitionInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration as JavaDuration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.CountDownLatch
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

    @Test
    fun `control metadata rejects a topic that does not expose exactly partition zero`() = runBlocking {
        val consumer = tailConsumer(
            emptyList(),
            end = 0,
            releaseRecords = AtomicBoolean(false),
            metadataPartitions = listOf(0, 1)
        )
        val transport = transport(consumer) { }
        try {
            transport.start()
            val failure = assertThrows(IllegalStateException::class.java) {
                runBlocking { withTimeout(1_000) { transport.awaitReady() } }
            }
            assertTrue(failure.message.orEmpty().contains("exactly partition 0"))
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `brief identity lookup outage keeps the current control clients`() = runBlocking {
        val consumer = tailConsumer(emptyList(), end = 0, releaseRecords = AtomicBoolean(false))
        val consumerCreations = AtomicInteger()
        val producerCreations = AtomicInteger()
        val identityReads = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = {
                producerCreations.incrementAndGet()
                mock()
            },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String? =
                    when (identityReads.incrementAndGet()) {
                        1 -> "control-topic-id"
                        2, 3 -> null
                        else -> "control-topic-id"
                    }
            },
            recoveryFailureThreshold = 3
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (identityReads.get() < 4) kotlinx.coroutines.delay(1)
            }
            assertEquals(1, consumerCreations.get())
            assertEquals(1, producerCreations.get())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `delayed initial assignment does not recreate the consumer before its deadline`() = runBlocking {
        val clock = AtomicLong()
        val consumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 100.milliseconds.inWholeNanoseconds
        )
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = { mock() },
            topicIdentity = fixedTopicIdentity(),
            assignmentTimeout = 200.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            withTimeout(1.seconds) { transport.awaitReady() }

            assertEquals(1, consumerCreations.get())
            verify(consumer, times(1)).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `revoke waits for replacement assignment without running active recovery checks`() = runBlocking {
        val clock = AtomicLong()
        val partition = TopicPartition("control", 0)
        val consumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds,
            revokeAtNanos = 20.milliseconds.inWholeNanoseconds,
            replacementAssignmentAtNanos = 100.milliseconds.inWholeNanoseconds
        )
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = { mock() },
            topicIdentity = fixedTopicIdentity(),
            assignmentTimeout = 200.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (clock.get() < 110.milliseconds.inWholeNanoseconds) kotlinx.coroutines.yield()
            }

            assertEquals(1, consumerCreations.get())
            verify(consumer, times(2)).seek(partition, 12)
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `replacement consumer may wait for assignment without another recreation`() = runBlocking {
        val clock = AtomicLong()
        val first = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds
        )
        val second = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 100.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val identityReads = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { mock() },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String? =
                    when (identityReads.incrementAndGet()) {
                        1 -> "control-topic-id"
                        2, 3 -> null
                        else -> "control-topic-id"
                    }
            },
            recoveryFailureThreshold = 2,
            assignmentTimeout = 200.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (clock.get() < 110.milliseconds.inWholeNanoseconds) kotlinx.coroutines.yield()
            }

            assertEquals(2, consumerCreations.get())
            verify(second).seek(TopicPartition("control", 0), 12)
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `shutdown completes while the control consumer awaits assignment`() = runBlocking {
        val clock = AtomicLong()
        val consumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = Long.MAX_VALUE,
            pollAdvanceNanos = 0
        )
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = { mock() },
            topicIdentity = fixedTopicIdentity(),
            assignmentTimeout = 5.seconds,
            nanoTime = clock::get
        ) { }

        transport.start()
        withTimeout(1.seconds) {
            while (consumerCreations.get() == 0) kotlinx.coroutines.yield()
        }
        withTimeout(1.seconds) { transport.stop() }

        assertEquals(1, consumerCreations.get())
    }

    @Test
    fun `assignment deadline performs one delayed replacement attempt`() = runBlocking {
        val clock = AtomicLong()
        val first = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = Long.MAX_VALUE,
            pollAdvanceNanos = 10.milliseconds.inWholeNanoseconds
        )
        val second = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 60.milliseconds.inWholeNanoseconds,
            pollAdvanceNanos = 10.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { mock() },
            topicIdentity = fixedTopicIdentity(),
            assignmentTimeout = 50.milliseconds,
            recoveryRetryBackoff = 1.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            withTimeout(1.seconds) { transport.awaitReady() }
            assertEquals(2, consumerCreations.get())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `same topic recovery resumes after admitted history without redelivery`() = runBlocking {
        val admittedMessage = request(namespace, "admitted-before-outage")
        val admitted = CountDownLatch(1)
        val replacementPolled = CountDownLatch(1)
        val first = consumerWithNewRecord(admittedMessage, initialEnd = 12)
        val second = resumingConsumer(admittedMessage, end = 13, replacementPolled)
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val failedIdentityReads = AtomicInteger()
        val handled = mutableListOf<String>()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { mock() },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String? {
                    if (admitted.count > 0) return "same-topic-id"
                    return if (failedIdentityReads.incrementAndGet() <= 2) null else "same-topic-id"
                }
            },
            recoveryFailureThreshold = 2
        ) {
            handled += it.message.sessionId.value
            admitted.countDown()
        }

        try {
            transport.start()
            transport.awaitReady()
            assertTrue(admitted.await(1, java.util.concurrent.TimeUnit.SECONDS))
            assertTrue(replacementPolled.await(1, java.util.concurrent.TimeUnit.SECONDS))

            verify(second).seek(TopicPartition("control", 0), 13)
            assertEquals(listOf("admitted-before-outage"), handled)
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `recreated control topic replaces clients and consumes the fresh log from its beginning`() = runBlocking {
        val first = tailConsumer(emptyList(), end = 12, releaseRecords = AtomicBoolean(false))
        val second = tailConsumer(
            listOf(request(namespace, "after-recreation")),
            end = 0,
            releaseRecords = AtomicBoolean(true)
        )
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val producerCreations = AtomicInteger()
        val identityReads = AtomicInteger()
        val handled = mutableListOf<String>()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = {
                producerCreations.incrementAndGet()
                mock()
            },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String =
                    if (identityReads.incrementAndGet() == 1) "old-topic-id" else "new-topic-id"
            }
        ) { handled += it.message.sessionId.value }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (handled.isEmpty()) kotlinx.coroutines.delay(1)
            }
            assertEquals(listOf("after-recreation"), handled)
            assertEquals(2, consumerCreations.get())
            assertEquals(2, producerCreations.get())
            verify(first).seek(TopicPartition("control", 0), 12)
            verify(second, org.mockito.kotlin.never()).seek(
                org.mockito.kotlin.eq(TopicPartition("control", 0)),
                any<Long>()
            )
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `shutdown cancels control recovery backoff`() = runBlocking {
        val consumer = tailConsumer(emptyList(), end = 0, releaseRecords = AtomicBoolean(false))
        val consumerCreations = AtomicInteger()
        val producerCreations = AtomicInteger()
        val identityReads = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = {
                producerCreations.incrementAndGet()
                mock()
            },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String =
                    if (identityReads.incrementAndGet() == 1) "old-topic-id" else "new-topic-id"
            },
            recoveryRetryBackoff = 5.seconds
        ) { }

        transport.start()
        transport.awaitReady()
        withTimeout(1.seconds) {
            while (producerCreations.get() < 2) kotlinx.coroutines.delay(1)
        }
        withTimeout(1.seconds) { transport.stop() }
        assertEquals(1, consumerCreations.get())
    }

    @Test
    fun `shutdown interrupts an unfinished identity lookup`() = runBlocking {
        val lookupStarted = CountDownLatch(1)
        val releaseLookup = CountDownLatch(1)
        val closed = AtomicBoolean(false)
        val reads = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { tailConsumer(emptyList(), 0, AtomicBoolean(false)) },
            producerFactory = { mock() },
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String {
                    if (reads.incrementAndGet() == 1) return "control-topic-id"
                    lookupStarted.countDown()
                    releaseLookup.await()
                    return "control-topic-id"
                }

                override fun close() {
                    closed.set(true)
                }
            }
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            assertTrue(lookupStarted.await(1, java.util.concurrent.TimeUnit.SECONDS))
            withTimeout(1.seconds) { transport.stop() }
            assertTrue(closed.get())
        } finally {
            releaseLookup.countDown()
            transport.stop()
        }
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
        consumerFactory = { consumer },
        topicIdentity = object : WarmupControlTopicIdentity {
            override fun read(topic: String, timeout: kotlin.time.Duration): String = "control-topic-id"
        }
    )

    private fun recoveringTransport(
        consumerFactory: () -> Consumer<String, ByteArray>,
        producerFactory: () -> Producer<String, ByteArray>,
        topicIdentity: WarmupControlTopicIdentity,
        recoveryFailureThreshold: Int = 3,
        recoveryRetryBackoff: kotlin.time.Duration = 1.milliseconds,
        assignmentTimeout: kotlin.time.Duration = 15.seconds,
        nanoTime: () -> Long = System::nanoTime,
        handler: WarmupControlMessageHandler
    ) = WarmupControlTransport(
        configuration = WarmupControlTransportConfiguration(
            instanceId = local,
            namespace = namespace,
            topic = "control",
            producerProperties = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            consumerProperties = mapOf(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"),
            pollTimeout = 1.milliseconds,
            assignmentTimeout = assignmentTimeout,
            recoveryCheckInterval = 5.milliseconds,
            recoveryRetryBackoff = recoveryRetryBackoff,
            recoveryFailureThreshold = recoveryFailureThreshold
        ),
        handler = handler,
        parentContext = Dispatchers.Default,
        producerFactory = { producerFactory() },
        consumerFactory = { consumerFactory() },
        topicIdentity = topicIdentity,
        nanoTime = nanoTime
    )

    private fun fixedTopicIdentity() = object : WarmupControlTopicIdentity {
        override fun read(topic: String, timeout: kotlin.time.Duration): String = "control-topic-id"
    }

    private fun assignmentLifecycleConsumer(
        clock: AtomicLong,
        initialAssignmentAtNanos: Long,
        revokeAtNanos: Long? = null,
        replacementAssignmentAtNanos: Long? = null,
        pollAdvanceNanos: Long = 1.milliseconds.inWholeNanoseconds
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assignmentActive = AtomicBoolean(false)
        val phase = AtomicInteger()
        val position = AtomicLong(12)
        val partition = TopicPartition("control", 0)
        whenever(consumer.partitionsFor(any(), any<JavaDuration>())).thenReturn(
            listOf(PartitionInfo("control", 0, null, emptyArray(), emptyArray()))
        )
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to 12L))
        doAnswer {
            position.set(it.getArgument(1))
            Unit
        }.whenever(consumer).seek(org.mockito.kotlin.eq(partition), any<Long>())
        whenever(consumer.position(partition)).thenAnswer { position.get() }
        whenever(consumer.assignment()).thenAnswer {
            if (assignmentActive.get()) setOf(partition) else emptySet<TopicPartition>()
        }
        whenever(consumer.position(org.mockito.kotlin.eq(partition), any<JavaDuration>()))
            .thenAnswer { position.get() }
        whenever(consumer.endOffsets(org.mockito.kotlin.eq(setOf(partition)), any<JavaDuration>()))
            .thenReturn(mapOf(partition to 12L))
        doAnswer {
            val now = clock.addAndGet(pollAdvanceNanos)
            when {
                phase.get() == 0 && now >= initialAssignmentAtNanos -> {
                    assignmentActive.set(true)
                    listener.get().onPartitionsAssigned(listOf(partition))
                    phase.set(1)
                }
                phase.get() == 1 && revokeAtNanos != null && now >= revokeAtNanos -> {
                    assignmentActive.set(false)
                    listener.get().onPartitionsRevoked(listOf(partition))
                    phase.set(2)
                }
                phase.get() == 2 && replacementAssignmentAtNanos != null &&
                    now >= replacementAssignmentAtNanos -> {
                    assignmentActive.set(true)
                    listener.get().onPartitionsAssigned(listOf(partition))
                    phase.set(3)
                }
            }
            if (pollAdvanceNanos == 0L) Thread.sleep(1)
            ConsumerRecords.empty<String, ByteArray>()
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun tailConsumer(
        messages: List<WarmupControlMessage>,
        end: Long,
        releaseRecords: AtomicBoolean,
        metadataPartitions: List<Int> = listOf(0)
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assigned = AtomicBoolean(false)
        val delivered = AtomicBoolean(false)
        val partition = TopicPartition("control", 0)
        whenever(consumer.partitionsFor(any(), any<JavaDuration>())).thenReturn(
            metadataPartitions.map { PartitionInfo("control", it, null, emptyArray(), emptyArray()) }
        )
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to end))
        whenever(consumer.position(partition)).thenReturn(end)
        whenever(consumer.assignment()).thenReturn(setOf(partition))
        whenever(consumer.position(org.mockito.kotlin.eq(partition), any<JavaDuration>())).thenReturn(end)
        whenever(consumer.endOffsets(org.mockito.kotlin.eq(setOf(partition)), any<JavaDuration>()))
            .thenReturn(mapOf(partition to end))
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

    private fun resumingConsumer(
        oldMessage: WarmupControlMessage,
        end: Long,
        pollObserved: CountDownLatch = CountDownLatch(0)
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assigned = AtomicBoolean(false)
        val delivered = AtomicBoolean(false)
        val position = AtomicLong(0)
        val partition = TopicPartition("control", 0)
        whenever(consumer.partitionsFor(any(), any<JavaDuration>())).thenReturn(
            listOf(PartitionInfo("control", 0, null, emptyArray(), emptyArray()))
        )
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        doAnswer {
            position.set(it.getArgument(1))
            Unit
        }.whenever(consumer).seek(org.mockito.kotlin.eq(partition), any<Long>())
        whenever(consumer.position(partition)).thenAnswer { position.get() }
        whenever(consumer.assignment()).thenReturn(setOf(partition))
        whenever(consumer.position(org.mockito.kotlin.eq(partition), any<JavaDuration>()))
            .thenAnswer { position.get() }
        whenever(consumer.endOffsets(org.mockito.kotlin.eq(setOf(partition)), any<JavaDuration>()))
            .thenReturn(mapOf(partition to end))
        doAnswer {
            if (assigned.compareAndSet(false, true)) {
                listener.get().onPartitionsAssigned(listOf(partition))
                pollObserved.countDown()
                ConsumerRecords.empty<String, ByteArray>()
            } else if (position.get() == 0L && delivered.compareAndSet(false, true)) {
                ConsumerRecords(
                    mapOf(
                        partition to listOf(
                            org.apache.kafka.clients.consumer.ConsumerRecord(
                                "control", 0, 0, oldMessage.sessionId.value,
                                WarmupControlCodec.encode(oldMessage)
                            )
                        )
                    )
                )
            } else {
                Thread.sleep(1)
                ConsumerRecords.empty<String, ByteArray>()
            }
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun consumerWithNewRecord(
        message: WarmupControlMessage,
        initialEnd: Long
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assigned = AtomicBoolean(false)
        val delivered = AtomicBoolean(false)
        val position = AtomicLong(initialEnd)
        val partition = TopicPartition("control", 0)
        whenever(consumer.partitionsFor(any(), any<JavaDuration>())).thenReturn(
            listOf(PartitionInfo("control", 0, null, emptyArray(), emptyArray()))
        )
        doAnswer {
            listener.set(it.getArgument(1))
            Unit
        }.whenever(consumer).subscribe(any<Collection<String>>(), any<ConsumerRebalanceListener>())
        whenever(consumer.endOffsets(listOf(partition))).thenReturn(mapOf(partition to initialEnd))
        doAnswer {
            position.set(it.getArgument(1))
            Unit
        }.whenever(consumer).seek(org.mockito.kotlin.eq(partition), any<Long>())
        whenever(consumer.position(partition)).thenAnswer { position.get() }
        whenever(consumer.assignment()).thenReturn(setOf(partition))
        whenever(consumer.position(org.mockito.kotlin.eq(partition), any<JavaDuration>()))
            .thenAnswer { position.get() }
        whenever(consumer.endOffsets(org.mockito.kotlin.eq(setOf(partition)), any<JavaDuration>()))
            .thenAnswer { mapOf(partition to if (delivered.get()) initialEnd + 1 else initialEnd) }
        doAnswer {
            if (assigned.compareAndSet(false, true)) {
                listener.get().onPartitionsAssigned(listOf(partition))
                ConsumerRecords.empty<String, ByteArray>()
            } else if (delivered.compareAndSet(false, true)) {
                position.set(initialEnd + 1)
                ConsumerRecords(
                    mapOf(
                        partition to listOf(
                            org.apache.kafka.clients.consumer.ConsumerRecord(
                                "control", 0, initialEnd, message.sessionId.value,
                                WarmupControlCodec.encode(message)
                            )
                        )
                    )
                )
            } else {
                Thread.sleep(1)
                ConsumerRecords.empty<String, ByteArray>()
            }
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun request(namespace: WarmupControlNamespace, session: String) = WarmupRequested(
        namespace, WarmupSessionId(session), local, listOf(mapping), 2_000,
        normalizedAdmissionBudget(listOf(mapping)), "fingerprint", 10_000
    )
}
