package avh.ckc.warmup.kafka

import avh.ckc.warmup.protocol.WarmupControlCodec
import avh.ckc.warmup.protocol.WarmupControlMessage
import avh.ckc.warmup.protocol.WarmupControlMessageHandler
import avh.ckc.warmup.protocol.WarmupControlNamespace
import avh.ckc.warmup.protocol.WarmupInstanceId
import avh.ckc.warmup.protocol.WarmupParticipantId
import avh.ckc.warmup.protocol.WarmupRequested
import avh.ckc.warmup.protocol.WarmupSessionId
import avh.ckc.warmup.protocol.WarmupTopicMapping
import avh.ckc.warmup.protocol.normalizedAdmissionBudget
import java.time.Duration as JavaDuration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

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
        val reportedFailures = ConcurrentLinkedQueue<Throwable>()
        lateinit var transport: WarmupControlTransport
        transport = transport(
            consumer,
            sendTimeout = 20.milliseconds,
            parentContext = Dispatchers.Default + CoroutineExceptionHandler { _, error -> reportedFailures.add(error) }
        ) {
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
        assertTrue(reportedFailures.all { it is WarmupControlSendTimeoutException }, reportedFailures.toString())
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
        val reportedFailures = ConcurrentLinkedQueue<Throwable>()
        val transport = transport(
            consumer,
            parentContext = Dispatchers.Default + CoroutineExceptionHandler { _, error -> reportedFailures.add(error) }
        ) { }
        try {
            transport.start()
            val failure = assertThrows(IllegalStateException::class.java) {
                runBlocking { withTimeout(1_000) { transport.awaitReady() } }
            }
            assertTrue(failure.message.orEmpty().contains("exactly partition 0"))
        } finally {
            transport.stop()
        }
        assertTrue(
            reportedFailures.all { it is IllegalStateException && it.message.orEmpty().contains("exactly partition 0") },
            reportedFailures.toString()
        )
    }

    @Test
    fun `temporary poll timeouts keep one consumer and continue delivering control records`() = runBlocking {
        val handled = CountDownLatch(1)
        val consumerCreations = AtomicInteger()
        val consumer = tailConsumer(
            listOf(request(namespace, "after-poll-timeouts")),
            end = 0,
            releaseRecords = AtomicBoolean(true),
            pollFailuresAfterAssignment = 5
        )
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = { mock() }
        ) { handled.countDown() }

        try {
            transport.start()
            transport.awaitReady()
            assertTrue(handled.await(1, java.util.concurrent.TimeUnit.SECONDS))
            assertEquals(1, consumerCreations.get())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `identity check timeouts do not replace the polling consumer`() = runBlocking {
        val handled = CountDownLatch(1)
        val identityReads = AtomicInteger()
        val consumerCreations = AtomicInteger()
        val consumer = tailConsumer(
            listOf(request(namespace, "during-identity-timeouts")),
            end = 0,
            releaseRecords = AtomicBoolean(true)
        )
        val transport = recoveringTransport(
            consumerFactory = {
                consumerCreations.incrementAndGet()
                consumer
            },
            producerFactory = { mock() },
            topicIdentityCheckInterval = 5.milliseconds,
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String {
                    val attempt = identityReads.incrementAndGet()
                    if (attempt in 2..5) throw KafkaTimeoutException("identity unavailable")
                    return "control-topic-id"
                }
            }
        ) { handled.countDown() }

        try {
            transport.start()
            transport.awaitReady()
            assertTrue(handled.await(1, java.util.concurrent.TimeUnit.SECONDS))
            withTimeout(1.seconds) { while (identityReads.get() < 6) kotlinx.coroutines.delay(1) }
            assertEquals(1, consumerCreations.get())
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `completed identity change is handled while poll keeps failing temporarily`() = runBlocking {
        val identityReads = AtomicInteger()
        val firstConsumer = tailConsumer(
            emptyList(),
            end = 0,
            releaseRecords = AtomicBoolean(false),
            pollFailuresAfterAssignment = Int.MAX_VALUE
        )
        val secondConsumer = tailConsumer(emptyList(), 0, AtomicBoolean(false))
        val consumers = listOf(firstConsumer, secondConsumer)
        val consumerCreations = AtomicInteger()
        val producerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = {
                producerCreations.incrementAndGet()
                mock()
            },
            topicIdentityCheckInterval = 5.milliseconds,
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String =
                    if (identityReads.incrementAndGet() == 1) "old-topic-id" else "new-topic-id"
            }
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (consumerCreations.get() < 2) kotlinx.coroutines.yield()
            }
            assertEquals(2, producerCreations.get())
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
    fun `revoke waits for Kafka to initialize the replacement position`() = runBlocking {
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
    fun `assignment-detected topic recreation replaces both control clients`() = runBlocking {
        val clock = AtomicLong()
        val partition = TopicPartition("control", 0)
        val identityReads = AtomicInteger()
        val firstConsumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds,
            revokeAtNanos = 20.milliseconds.inWholeNanoseconds,
            replacementAssignmentAtNanos = 100.milliseconds.inWholeNanoseconds
        )
        val secondConsumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 110.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(firstConsumer, secondConsumer)
        val consumerCreations = AtomicInteger()
        val firstProducer = mock<Producer<String, ByteArray>>()
        val secondProducer = acknowledgingProducer()
        val producers = listOf(firstProducer, secondProducer)
        val producerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { producers[producerCreations.getAndIncrement()] },
            assignmentTimeout = 200.milliseconds,
            nanoTime = clock::get,
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String =
                    if (identityReads.incrementAndGet() == 1) "old-topic-id" else "new-topic-id"
            }
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (consumerCreations.get() < 2) kotlinx.coroutines.yield()
            }

            transport.send(request(namespace, "after-recreation"))
            assertEquals(2, producerCreations.get())
            verify(secondProducer).send(any(), any<Callback>())
            verify(firstConsumer, times(1)).seek(partition, 12)
        } finally {
            transport.stop()
        }
    }

    @Test
    fun `Kafka-wrapped temporary identity timeout during reassignment performs bounded recovery`() = runBlocking {
        val clock = AtomicLong()
        val identityReads = AtomicInteger()
        val firstConsumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds,
            revokeAtNanos = 20.milliseconds.inWholeNanoseconds,
            replacementAssignmentAtNanos = 30.milliseconds.inWholeNanoseconds,
            wrapRebalanceCallbackFailures = true
        )
        val secondConsumer = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 40.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(firstConsumer, secondConsumer)
        val consumerCreations = AtomicInteger()
        val producerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = {
                producerCreations.incrementAndGet()
                mock()
            },
            assignmentTimeout = 200.milliseconds,
            nanoTime = clock::get,
            topicIdentity = object : WarmupControlTopicIdentity {
                override fun read(topic: String, timeout: kotlin.time.Duration): String {
                    if (identityReads.incrementAndGet() == 2) {
                        throw java.util.concurrent.TimeoutException("Admin lookup timed out")
                    }
                    return "control-topic-id"
                }
            }
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (consumerCreations.get() < 2) kotlinx.coroutines.yield()
            }
            assertEquals(1, producerCreations.get())
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
    fun `initial end offset lookup is bounded by the assignment deadline`() = runBlocking {
        val clock = AtomicLong()
        val partition = TopicPartition("control", 0)
        val first = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds
        )
        whenever(first.endOffsets(org.mockito.kotlin.eq(setOf(partition)), any<JavaDuration>()))
            .thenAnswer {
                clock.addAndGet(100.milliseconds.inWholeNanoseconds)
                mapOf(partition to 12L)
            }
        val second = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 102.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { mock() },
            assignmentTimeout = 50.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            withTimeout(1.seconds) { transport.awaitReady() }
            assertEquals(2, consumerCreations.get())
            verify(first, org.mockito.kotlin.never()).endOffsets(listOf(partition))
        } finally {
            transport.stop()
        }
        Unit
    }

    @Test
    fun `replacement position initialization is bounded by the reassignment deadline`() = runBlocking {
        val clock = AtomicLong()
        val partition = TopicPartition("control", 0)
        val positionCalls = AtomicInteger()
        val first = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 1.milliseconds.inWholeNanoseconds,
            revokeAtNanos = 2.milliseconds.inWholeNanoseconds,
            replacementAssignmentAtNanos = 3.milliseconds.inWholeNanoseconds
        )
        whenever(first.position(org.mockito.kotlin.eq(partition), any<JavaDuration>()))
            .thenAnswer {
                if (positionCalls.incrementAndGet() == 2) {
                    clock.addAndGet(100.milliseconds.inWholeNanoseconds)
                    throw KafkaTimeoutException("replacement position initialization timed out")
                }
                12L
            }
        val second = assignmentLifecycleConsumer(
            clock = clock,
            initialAssignmentAtNanos = 104.milliseconds.inWholeNanoseconds
        )
        val consumers = listOf(first, second)
        val consumerCreations = AtomicInteger()
        val transport = recoveringTransport(
            consumerFactory = { consumers[consumerCreations.getAndIncrement()] },
            producerFactory = { mock() },
            assignmentTimeout = 50.milliseconds,
            nanoTime = clock::get
        ) { }

        try {
            transport.start()
            transport.awaitReady()
            withTimeout(1.seconds) {
                while (consumerCreations.get() < 2) kotlinx.coroutines.yield()
            }
            withTimeout(1.seconds) {
                while (clock.get() < 110.milliseconds.inWholeNanoseconds) kotlinx.coroutines.yield()
            }

            assertEquals(2, consumerCreations.get())
            verify(second).seek(partition, 12)
        } finally {
            transport.stop()
        }
    }

    private fun transport(
        consumer: Consumer<String, ByteArray>,
        sendTimeout: kotlin.time.Duration = 30.seconds,
        parentContext: CoroutineContext = Dispatchers.Default,
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
        parentContext = parentContext,
        producerFactory = { mock<Producer<String, ByteArray>>() },
        consumerFactory = { consumer },
        topicIdentity = fixedTopicIdentity()
    )

    private fun recoveringTransport(
        consumerFactory: () -> Consumer<String, ByteArray>,
        producerFactory: () -> Producer<String, ByteArray>,
        recoveryRetryBackoff: kotlin.time.Duration = 1.milliseconds,
        assignmentTimeout: kotlin.time.Duration = 15.seconds,
        topicIdentityCheckInterval: kotlin.time.Duration = 5.seconds,
        nanoTime: () -> Long = System::nanoTime,
        topicIdentity: WarmupControlTopicIdentity = fixedTopicIdentity(),
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
            topicIdentityCheckInterval = topicIdentityCheckInterval,
            recoveryRetryBackoff = recoveryRetryBackoff
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

    private fun acknowledgingProducer(): Producer<String, ByteArray> =
        mock<Producer<String, ByteArray>>().also { producer ->
            doAnswer { invocation ->
                invocation.getArgument<Callback>(1).onCompletion(mock<RecordMetadata>(), null)
                mock<java.util.concurrent.Future<RecordMetadata>>()
            }.whenever(producer).send(any(), any<Callback>())
        }

    private fun assignmentLifecycleConsumer(
        clock: AtomicLong,
        initialAssignmentAtNanos: Long,
        revokeAtNanos: Long? = null,
        replacementAssignmentAtNanos: Long? = null,
        pollAdvanceNanos: Long = 1.milliseconds.inWholeNanoseconds,
        wrapRebalanceCallbackFailures: Boolean = false
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
                    invokeRebalanceCallback(wrapRebalanceCallbackFailures) {
                        listener.get().onPartitionsAssigned(listOf(partition))
                    }
                    phase.set(1)
                }
                phase.get() == 1 && revokeAtNanos != null && now >= revokeAtNanos -> {
                    assignmentActive.set(false)
                    invokeRebalanceCallback(wrapRebalanceCallbackFailures) {
                        listener.get().onPartitionsRevoked(listOf(partition))
                    }
                    phase.set(2)
                }
                phase.get() == 2 && replacementAssignmentAtNanos != null &&
                    now >= replacementAssignmentAtNanos -> {
                    assignmentActive.set(true)
                    invokeRebalanceCallback(wrapRebalanceCallbackFailures) {
                        listener.get().onPartitionsAssigned(listOf(partition))
                    }
                    phase.set(3)
                }
            }
            if (pollAdvanceNanos == 0L) Thread.sleep(1)
            ConsumerRecords.empty<String, ByteArray>()
        }.whenever(consumer).poll(any<JavaDuration>())
        return consumer
    }

    private fun invokeRebalanceCallback(wrapFailure: Boolean, callback: () -> Unit) {
        try {
            callback()
        } catch (error: Throwable) {
            if (wrapFailure) throw KafkaException("User rebalance callback throws an error", error)
            throw error
        }
    }

    private fun tailConsumer(
        messages: List<WarmupControlMessage>,
        end: Long,
        releaseRecords: AtomicBoolean,
        metadataPartitions: List<Int> = listOf(0),
        pollFailuresAfterAssignment: Int = 0
    ): Consumer<String, ByteArray> {
        val consumer = mock<Consumer<String, ByteArray>>()
        val listener = AtomicReference<ConsumerRebalanceListener>()
        val assigned = AtomicBoolean(false)
        val delivered = AtomicBoolean(false)
        val remainingPollFailures = AtomicInteger(pollFailuresAfterAssignment)
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
                remainingPollFailures.getAndDecrement() > 0 -> {
                    Thread.sleep(1)
                    throw KafkaTimeoutException("temporary poll timeout")
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
        namespace, WarmupSessionId(session), local, listOf(mapping), 2_000,
        normalizedAdmissionBudget(listOf(mapping)), "fingerprint", 10_000
    )
}
