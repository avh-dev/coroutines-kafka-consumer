package avh.ckc.core

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class ConsumerPollLoopTest {

    @Nested
    inner class Lossy {

        @Test
        fun `when lossy loop polls records then they are sent to work channel`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.LOSSY,
                workChannelCapacity = 16,
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(topicPartition, record(offset = 123L))
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            val received = withTimeout(2_000) { fixture.workChannel.receive() }

            assertEquals("topic-a", received.topic())
            assertEquals(0, received.partition())
            assertEquals(123L, received.offset())
            assertEquals("key", received.key()!!.decodeToString())
            assertEquals("value", received.value()!!.decodeToString())

            job.cancel()
            job.join()

            verify(fixture.consumer)
                .subscribe(eq(listOf("topic-a")), any<ConsumerRebalanceListener>())
            verify(fixture.consumer).close()
        }

        @Test
        fun `when prepare for shutdown called in lossy mode then ready signal completes`() = runBlocking {
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.LOSSY,
                workChannelCapacity = 16,
                pollAnswer = {
                    Thread.sleep(50)
                    emptyRecords()
                }
            )

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000))
                .subscribe(eq(listOf("topic-a")), any<ConsumerRebalanceListener>())

            val readyForShutdown = fixture.loop.prepareForShutdown()

            withTimeout(2_000) { readyForShutdown.await() }
            verify(fixture.consumer, timeout(2_000)).wakeup()

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when lossy channel overflows then oldest buffered records are dropped`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.LOSSY,
                workChannelCapacity = 1,
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(
                            topicPartition,
                            record(offset = 101L),
                            record(offset = 102L),
                            record(offset = 103L)
                        )
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()
            delay(100)

            val received = withTimeout(2_000) { fixture.workChannel.receive() }

            assertEquals(103L, received.offset())
            verify(fixture.consumer, never()).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())
            verify(fixture.consumer, never()).pause(any())
            verify(fixture.consumer, never()).resume(any())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }
    }

    @Nested
    inner class Backpressure {

        @Test
        fun `when work channel is full in backpressure mode then consumer is paused`() = runBlocking {
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 2,
                assignmentPosition = 100L,
                initialChannelRecords = listOf(record(topic = "prefill", offset = 0L)),
                pollAnswer = {
                    recordsOf(
                        topicPartition,
                        record(offset = 123L),
                        record(offset = 124L, key = "key-2", value = "value-2")
                    )
                }
            )

            val job = fixture.start()

            fixture.awaitPause()

            val prefilled = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("prefill", prefilled.topic())

            val delivered = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("topic-a", delivered.topic())
            assertEquals(123L, delivered.offset())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when stashed records are drained then consumer is resumed and poll batch tail is delivered in order`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 1,
                assignmentPosition = 100L,
                initialChannelRecords = listOf(record(topic = "prefill", offset = 0L)),
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(
                            topicPartition,
                            record(offset = 123L),
                            record(offset = 124L, key = "key-2", value = "value-2")
                        )
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            fixture.awaitPause()

            val prefilled = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("prefill", prefilled.topic())

            val drained = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("topic-a", drained.topic())
            assertEquals(0, drained.partition())
            assertEquals(123L, drained.offset())

            val tail = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("topic-a", tail.topic())
            assertEquals(0, tail.partition())
            assertEquals(124L, tail.offset())

            fixture.awaitResume()

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when partitions assigned in backpressure mode then registry is updated and position is queried`() = runBlocking {
            val telemetry = RecordingTelemetry()
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                telemetry = telemetry,
                workChannelCapacity = 4,
                assignmentPosition = 42L,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000)).position(fixture.topicPartition)

            val state = fixture.awaitAssignedState()

            assertEquals(fixture.topicPartition, state.topicPartition)
            assertEquals(41L, state.trackerRefForTest().lastCommitedOffset)
            assertFalse(telemetry.polls.isEmpty())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when prepare for shutdown called then wakeup is invoked`() = runBlocking {
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 16,
                pollAnswer = {
                    Thread.sleep(50)
                    emptyRecords()
                }
            )

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000))
                .subscribe(eq(listOf("topic-a")), any<ConsumerRebalanceListener>())

            fixture.loop.prepareForShutdown()

            verify(fixture.consumer, timeout(2_000)).wakeup()

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when partition is revoked in backpressure mode then ready offsets are committed`() = runBlocking {
            val listenerRef = AtomicReference<ConsumerRebalanceListener?>()
            val revokeRequested = AtomicBoolean(false)
            val revokedOnce = AtomicBoolean(false)
            val telemetry = RecordingTelemetry()
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                telemetry = telemetry,
                workChannelCapacity = 4,
                assignmentPosition = 101L,
                listenerRef = listenerRef,
                pollAnswer = {
                    if (revokeRequested.get() && revokedOnce.compareAndSet(false, true)) {
                        listenerRef.get()!!.onPartitionsRevoked(listOf(topicPartition))
                    }
                    emptyRecords()
                }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState()
            state.trackerRefForTest().markProcessed(101L)
            revokeRequested.set(true)

            fixture.awaitCommit(101L)
            assertEquals(true, telemetry.commits.single().success)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commit interval elapses in backpressure mode then ready offsets are committed`() = runBlocking {
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 4,
                assignmentPosition = 201L,
                commitIntervalMs = 25L,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState()
            state.trackerRefForTest().markProcessed(201L)

            fixture.awaitCommit(201L)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commit fails then next commit still advances to newer processed offset`() = runBlocking {
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 4,
                assignmentPosition = 300L,
                commitIntervalMs = 25L,
                pollAnswer = { emptyRecords() }
            )

            whenever(fixture.consumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()))
                .thenThrow(IllegalStateException("commit failed"))
                .then { }

            val job = fixture.start()
            val state = fixture.awaitAssignedState()

            state.trackerRefForTest().markProcessed(300L)
            fixture.awaitCommitAttempts(1)

            state.trackerRefForTest().markProcessed(301L)
            fixture.awaitCommitAttempts(2)

            verify(fixture.consumer, timeout(2_000).times(2)).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.offset() in listOf(300L, 301L)
                }
            )

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when shutdown drains stashed records then ready signal completes without resume`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 1,
                assignmentPosition = 100L,
                initialChannelRecords = listOf(record(topic = "prefill", offset = 0L)),
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(topicPartition, record(offset = 123L))
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            fixture.awaitPause()

            val readyForShutdown = fixture.loop.prepareForShutdown()
            delay(100)
            assertFalse(readyForShutdown.isCompleted)

            val prefilled = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("prefill", prefilled.topic())

            val drained = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals("topic-a", drained.topic())
            assertEquals(123L, drained.offset())

            withTimeout(2_000) { readyForShutdown.await() }
            verify(fixture.consumer, timeout(2_000)).wakeup()

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }
    }
}

private class PollLoopFixture(
    deliveryStrategy: DeliveryStrategy,
    telemetry: ConsumerTelemetry = ConsumerTelemetry.NOOP,
    workChannelCapacity: Int,
    assignmentPosition: Long = 0L,
    commitIntervalMs: Long = 60_000L,
    initialChannelRecords: List<ConsumerRecord<ByteArray, ByteArray>> = emptyList(),
    private val listenerRef: AtomicReference<ConsumerRebalanceListener?> = AtomicReference(),
    private val pollAnswer: PollLoopFixture.() -> ConsumerRecords<ByteArray, ByteArray>
) {
    val consumer: KafkaConsumer<ByteArray, ByteArray> = mock()
    val workChannel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
        capacity = workChannelCapacity,
        onBufferOverflow = when (deliveryStrategy) {
            DeliveryStrategy.BACKPRESSURE -> BufferOverflow.SUSPEND
            DeliveryStrategy.LOSSY -> BufferOverflow.DROP_OLDEST
        }
    )
    val registry = PartitionRegistry()
    val topicPartition = TopicPartition("topic-a", 0)
    private val consumerProperties = testConsumerProperties()
    val loop = ConsumerPollLoop(
        id = 1,
        parentContext = Dispatchers.Default,
        deliveryStrategy = deliveryStrategy,
        commitIntervalMs = commitIntervalMs,
        telemetry = telemetry,
        consumerProperties = consumerProperties,
        consumerConfigAdapter = ConsumerConfigAdapter(consumerProperties),
        topics = listOf(topicPartition.topic()),
        topicsPattern = null,
        workChannel = workChannel,
        partitionStateRegistry = registry,
        kafkaConsumerFactory = { consumer }
    )

    private val assignedOnce = AtomicBoolean(false)

    init {
        initialChannelRecords.forEach { workChannel.trySendBlocking(it) }

        whenever(consumer.subscribe(any<List<String>>(), any<ConsumerRebalanceListener>()))
            .thenAnswer { invocation ->
                listenerRef.set(invocation.getArgument(1))
            }

        whenever(consumer.position(topicPartition)).thenReturn(assignmentPosition)
        whenever(consumer.assignment()).thenReturn(setOf(topicPartition))
        whenever(consumer.close()).then { }

        whenever(consumer.poll(any<Duration>()))
            .thenAnswer {
                if (deliveryStrategy == DeliveryStrategy.BACKPRESSURE &&
                    assignedOnce.compareAndSet(false, true)
                ) {
                    listenerRef.get()?.onPartitionsAssigned(listOf(topicPartition))
                }
                pollAnswer()
            }
    }

    fun start(): Job = loop.start()

    suspend fun awaitAssignedState(): PartitionState =
        awaitFor(2_000L, 10L) { registry.partitionStateFor(topicPartition) }

    fun awaitPause() {
        verify(consumer, timeout(2_000)).pause(eq(setOf(topicPartition)))
    }

    fun awaitResume() {
        verify(consumer, timeout(2_000)).resume(eq(setOf(topicPartition)))
    }

    fun awaitCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                get(topicPartition)?.offset() == offset
            }
        )
    }

    fun awaitCommitAttempts(attempts: Int) {
        verify(consumer, timeout(2_000).times(attempts)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())
    }
}

private fun record(
    topic: String = "topic-a",
    partition: Int = 0,
    offset: Long,
    key: String = "key",
    value: String = "value"
) = ConsumerRecord(
    topic,
    partition,
    offset,
    key.toByteArray(),
    value.toByteArray()
)
