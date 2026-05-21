package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.offset.OffsetTracker
import avh.ckc.core.offset.OffsetTrackerMetadata
import avh.ckc.core.partition.PartitionRegistry
import avh.ckc.core.partition.PartitionState
import avh.ckc.core.polling.ConsumerPollLoop
import avh.ckc.core.processing.PolledRecordSink
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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doThrow
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
            val metrics = RecordingMetrics<Any?, Any?>()
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                metrics = metrics,
                workChannelCapacity = 4,
                assignmentPosition = 42L,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000)).position(fixture.topicPartition)

            val state = fixture.awaitAssignedState(lastCommittedOffset = 41L)

            assertEquals(fixture.topicPartition, state.topicPartition)
            assertEquals(41L, state.trackerRefForTest().lastCommitedOffset)
            assertEquals(listOf(state), metrics.boundPartitionStats.toList())
            assertFalse(metrics.polls.isEmpty())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when partitions assigned with offset metadata then tracker is restored from committed metadata`() = runBlocking {
            val restoredTracker = OffsetTracker(lastCommitedOffset = 41L)
            restoredTracker.markProcessed(43L)
            val metadata = OffsetTrackerMetadata.encode(restoredTracker.snapshot())!!
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 4,
                committedOffsets = mapOf(TopicPartition("topic-a", 0) to OffsetAndMetadata(42L, metadata)),
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState(lastCommittedOffset = 41L)

            assertEquals(41L, state.trackerRefForTest().lastCommitedOffset)
            assertFalse(state.isProcessed(42L))
            assertTrue(state.isProcessed(43L))
            verify(fixture.consumer, never()).position(fixture.topicPartition)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when polled record is already processed from metadata then it is not sent to work channel`() = runBlocking {
            val restoredTracker = OffsetTracker(lastCommitedOffset = 41L)
            restoredTracker.markProcessed(43L)
            val metadata = OffsetTrackerMetadata.encode(restoredTracker.snapshot())!!
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                workChannelCapacity = 4,
                committedOffsets = mapOf(TopicPartition("topic-a", 0) to OffsetAndMetadata(42L, metadata)),
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(
                            topicPartition,
                            record(offset = 42L),
                            record(offset = 43L)
                        )
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            val delivered = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals(42L, delivered.offset())
            delay(100)
            assertTrue(fixture.workChannel.tryReceive().isFailure)

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
            val metrics = RecordingMetrics<Any?, Any?>()
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                metrics = metrics,
                workChannelCapacity = 4,
                assignmentPosition = 101L,
                listenerRef = listenerRef,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState(lastCommittedOffset = 100L)
            state.trackerRefForTest().markProcessed(101L)
            listenerRef.get()!!.onPartitionsRevoked(listOf(fixture.topicPartition))

            fixture.awaitCommit(101L)
            awaitFor(2_000L, 10L) { metrics.commits.firstOrNull() }
            assertEquals(true, metrics.commits.single().success)
            assertEquals(1L, metrics.commits.single().offsetsCount)
            awaitFor(2_000L, 10L) { metrics.unboundPartitionMetrics.firstOrNull() }
            assertEquals(listOf(PartitionMetricsKey("topic-a", 0)), metrics.unboundPartitionMetrics.toList())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commit interval elapses in backpressure mode then ready offsets are committed`() = runBlocking {
            val metrics = RecordingMetrics<Any?, Any?>()
            val fixture = PollLoopFixture(
                deliveryStrategy = DeliveryStrategy.BACKPRESSURE,
                metrics = metrics,
                workChannelCapacity = 4,
                assignmentPosition = 201L,
                commitIntervalMs = 25L,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState(lastCommittedOffset = 200L)
            state.trackerRefForTest().markProcessed(201L)
            state.trackerRefForTest().markProcessed(202L)
            state.trackerRefForTest().markProcessed(203L)

            fixture.awaitCommit(203L)
            awaitFor(2_000L, 10L) { metrics.commits.firstOrNull() }
            assertEquals(3L, metrics.commits.single().offsetsCount)

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

            doThrow(IllegalStateException("commit failed"))
                .doNothing()
                .whenever(fixture.consumer)
                .commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            val job = fixture.start()
            val state = fixture.awaitAssignedState(lastCommittedOffset = 299L)

            state.trackerRefForTest().markProcessed(300L)
            fixture.awaitCommitAttempts(1)

            state.trackerRefForTest().markProcessed(301L)
            fixture.awaitCommitAttempts(2)

            verify(fixture.consumer, timeout(2_000).times(2)).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.offset() in listOf(301L, 302L)
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
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<Any?, Any?> = ConsumerMetrics.NOOP as ConsumerMetrics<Any?, Any?>,
    workChannelCapacity: Int,
    assignmentPosition: Long = 0L,
    private val committedOffsets: Map<TopicPartition, OffsetAndMetadata> = emptyMap(),
    commitIntervalMs: Long = 5_000L,
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
    private val recordSink = object : PolledRecordSink {
        override fun tryEmit(record: ConsumerRecord<ByteArray, ByteArray>): Boolean {
            if (deliveryStrategy == DeliveryStrategy.BACKPRESSURE &&
                registry.partitionStateFor(record)?.isProcessed(record.offset()) == true
            ) {
                return true
            }
            return workChannel.trySend(record).isSuccess
        }
    }
    val registry = PartitionRegistry()
    val topicPartition = TopicPartition("topic-a", 0)
    private val consumerProperties = testConsumerProperties()
    val loop = ConsumerPollLoop<Any?, Any?>(
        id = 1,
        parentContext = Dispatchers.Default,
        deliveryStrategy = deliveryStrategy,
        commitIntervalMs = commitIntervalMs,
        metrics = metrics,
        consumerProperties = consumerProperties,
        consumerConfigAdapter = ConsumerConfigAdapter(consumerProperties),
        topics = listOf(topicPartition.topic()),
        topicsPattern = null,
        recordSink = recordSink,
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
        whenever(consumer.committed(any<Set<TopicPartition>>())).thenReturn(committedOffsets)
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

    suspend fun awaitAssignedState(lastCommittedOffset: Long? = null): PartitionState =
        awaitFor(2_000L, 10L) {
            val state = registry.partitionStateFor(topicPartition) ?: return@awaitFor null
            if (lastCommittedOffset == null || state.trackerRefForTest().lastCommitedOffset == lastCommittedOffset) {
                state
            } else {
                null
            }
        }

    fun awaitPause() {
        verify(consumer, timeout(2_000)).pause(eq(setOf(topicPartition)))
    }

    fun awaitResume() {
        verify(consumer, timeout(2_000)).resume(eq(setOf(topicPartition)))
    }

    fun awaitCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && !committed.metadata().isNullOrEmpty()
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
