package avh.ckc.core

import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadata
import avh.ckc.core.polling.partition.offset.OffsetTrackerMetadataContext
import avh.ckc.core.polling.partition.PartitionRegistry
import avh.ckc.core.polling.partition.PartitionState
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
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito.never
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class ConsumerPollLoopTest {

    @Test
    fun `when poll loops are repeatedly created and released then dispatcher threads terminate`() = runBlocking {
        val releasedThreads = mutableListOf<Thread>()

        repeat(3) {
            val pollThread = AtomicReference<Thread?>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 1,
                pollAnswer = {
                    pollThread.compareAndSet(null, Thread.currentThread())
                    emptyRecords()
                }
            )
            val job = fixture.start()
            val thread = awaitFor(2_000L, 10L) { pollThread.get() }

            job.cancel()
            job.join()
            fixture.loop.releaseResources()
            fixture.loop.releaseResources()
            awaitFor(2_000L, 10L) { true.takeIf { !thread.isAlive } }
            releasedThreads += thread
        }

        assertTrue(releasedThreads.none(Thread::isAlive))
    }

    @Nested
    inner class FRESHNESS_FIRST_DROP_OLDEST {

        @Test
        fun `when FRESHNESS_FIRST_DROP_OLDEST loop polls records then they are sent to work channel`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
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
        fun `when freshness commit interval elapses then latest accepted position is committed before processing`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 16,
                commitIntervalMs = 25L,
                commitRecordsThreshold = 1_000,
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(topicPartition, record(offset = 123L))
                    } else {
                        emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            fixture.awaitFreshnessCommit(123L)
            assertEquals(123L, fixture.workChannel.tryReceive().getOrNull()?.offset())

            job.cancel()
            job.join()
        }

        @Test
        fun `when freshness record threshold is reached then intentionally dropped records are committed`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 1,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 3,
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

            fixture.awaitFreshnessCommit(103L)
            assertEquals(103L, withTimeout(2_000) { fixture.workChannel.receive() }.offset())

            job.cancel()
            job.join()
        }

        @Test
        fun `when freshness commit fails then accepted position is retried`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 4,
                commitIntervalMs = 25L,
                commitRecordsThreshold = 1,
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(topicPartition, record(offset = 77L))
                    } else {
                        emptyRecords()
                    }
                }
            )
            doThrow(IllegalStateException("commit failed"))
                .doNothing()
                .whenever(fixture.consumer)
                .commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000).times(2)).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.offset() == 78L
                }
            )
            awaitFor(2_000L, 10L) {
                fixture.registry.partitionStateFor(fixture.topicPartition)
                    ?.lastCommittedOffset
                    ?.takeIf { it == 77L }
            }

            job.cancel()
            job.join()
        }

        @Test
        fun `when freshness commits keep failing then retry requires another full record threshold`() = runBlocking {
            val batches = ConcurrentLinkedQueue<ConsumerRecords<ByteArray, ByteArray>>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 16,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 3,
                pollAnswer = { batches.poll() ?: emptyRecords() }
            )
            batches += recordsOf(
                fixture.topicPartition,
                record(offset = 0L),
                record(offset = 1L),
                record(offset = 2L)
            )
            doThrow(IllegalStateException("first commit failed"))
                .doThrow(IllegalStateException("second commit failed"))
                .whenever(fixture.consumer)
                .commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            val job = fixture.start()
            fixture.awaitCommitAttempts(1)

            batches += recordsOf(
                fixture.topicPartition,
                record(offset = 3L),
                record(offset = 4L)
            )
            awaitFor(2_000L, 10L) {
                fixture.registry.partitionStateFor(fixture.topicPartition)
                    ?.acceptedRecordsPendingCommit()
                    ?.takeIf { it == 5L }
            }
            verify(fixture.consumer, times(1)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            batches += recordsOf(fixture.topicPartition, record(offset = 5L))
            fixture.awaitCommitAttempts(2)

            batches += recordsOf(fixture.topicPartition, record(offset = 6L))
            awaitFor(2_000L, 10L) {
                fixture.registry.partitionStateFor(fixture.topicPartition)
                    ?.acceptedRecordsPendingCommit()
                    ?.takeIf { it == 7L }
            }
            verify(fixture.consumer, times(2)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            job.cancel()
            job.join()
        }

        @Test
        fun `when freshness loop shuts down then accepted position receives a final commit`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                workChannelCapacity = 4,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 1_000,
                pollAnswer = {
                    if (firstPoll.compareAndSet(true, false)) {
                        recordsOf(topicPartition, record(offset = 88L))
                    } else {
                        emptyRecords()
                    }
                }
            )
            val job = fixture.start()
            withTimeout(2_000) { fixture.workChannel.receive() }

            withTimeout(2_000) { fixture.loop.prepareForShutdown().await() }
            fixture.loop.finalizeShutdown()
            withTimeout(2_000) { job.join() }

            fixture.awaitDeadlineBoundedFreshnessCommit(88L)
        }

        @Test
        fun `when prepare for shutdown called in FRESHNESS_FIRST_DROP_OLDEST mode then ready signal completes`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
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
        fun `when FRESHNESS_FIRST_DROP_OLDEST channel overflows then oldest buffered records are dropped`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
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
    inner class AT_LEAST_ONCE_NO_ORDERING {

        @Test
        fun `when poll fails then terminal completion releases shutdown readiness`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 16,
                pollAnswer = { throw IllegalStateException("poll failed") }
            )

            val job = fixture.start()
            withTimeout(2_000) { job.join() }

            withTimeout(2_000) { fixture.loop.prepareForShutdown().await() }

            assertTrue(job.isCancelled)
            verify(fixture.consumer).close()
        }

        @Test
        fun `when work channel is full in AT_LEAST_ONCE_NO_ORDERING mode then consumer is paused`() = runBlocking {
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                metrics = metrics,
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
            assertEquals(
                BackpressurePauseResumeCall(BackpressureAction.PAUSE, 1),
                metrics.backpressurePauseResume.first()
            )

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when stashed records are drained then consumer is resumed and poll batch tail is delivered in order`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                metrics = metrics,
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
            assertEquals(
                listOf(
                    BackpressurePauseResumeCall(BackpressureAction.PAUSE, 1),
                    BackpressurePauseResumeCall(BackpressureAction.RESUME, 1)
                ),
                metrics.backpressurePauseResume
            )

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when partitions assigned in AT_LEAST_ONCE_NO_ORDERING mode then registry is updated and position is queried`() = runBlocking {
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                metrics = metrics,
                workChannelCapacity = 4,
                assignmentPosition = 42L,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            verify(fixture.consumer, timeout(2_000)).position(fixture.topicPartition)

            val state = fixture.awaitAssignedState(lastCommittedOffset = 41L)

            assertEquals(fixture.topicPartition, state.topicPartition)
            assertEquals(41L, state.trackerRefForTest().lastProcessedOffset)
            assertEquals(41L, state.lastCommittedOffset)
            assertEquals(listOf(state), metrics.boundPartitionStats.toList())
            assertFalse(metrics.polls.isEmpty())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when partitions assigned with offset metadata then tracker is restored from committed metadata`() = runBlocking {
            val restoredTracker = OffsetTracker(initialProcessedOffset = 41L)
            restoredTracker.markProcessed(43L)
            val metadata = OffsetTrackerMetadata.encode(
                restoredTracker.snapshot(),
                OffsetTrackerMetadataContext("test-group", TopicPartition("topic-a", 0), 42L)
            ).metadata!!
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                committedOffsets = mapOf(TopicPartition("topic-a", 0) to OffsetAndMetadata(42L, metadata)),
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()

            val state = fixture.awaitAssignedState(lastCommittedOffset = 41L)

            assertEquals(41L, state.trackerRefForTest().lastProcessedOffset)
            assertEquals(41L, state.lastCommittedOffset)
            assertFalse(state.isProcessed(42L))
            assertTrue(state.isProcessed(43L))
            verify(fixture.consumer, never()).position(fixture.topicPartition)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when kafka omits offsets then only the missing range is marked processed`() = runBlocking {
            val pollCount = AtomicInteger()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                committedOffsets = mapOf(TopicPartition("topic-a", 0) to OffsetAndMetadata(42L)),
                pollAnswer = {
                    when (pollCount.getAndIncrement()) {
                        0 -> recordsOf(topicPartition, record(offset = 42L))
                        1 -> recordsOf(topicPartition, record(offset = 45L))
                        else -> emptyRecords()
                    }
                }
            )

            val job = fixture.start()

            val first = withTimeout(2_000) { fixture.workChannel.receive() }
            val second = withTimeout(2_000) { fixture.workChannel.receive() }
            assertEquals(42L, first.offset())
            assertEquals(45L, second.offset())

            val state = fixture.awaitAssignedState(lastCommittedOffset = 41L)
            state.advanceAndGetPendingOffsetsCount()
            assertEquals(41L, state.trackerRefForTest().lastProcessedOffset)
            assertFalse(state.isProcessed(42L))
            assertTrue(state.isProcessed(43L))
            assertTrue(state.isProcessed(44L))
            assertFalse(state.isProcessed(45L))

            state.markProcessed(42L)
            state.advanceAndGetPendingOffsetsCount()
            assertEquals(44L, state.trackerRefForTest().lastProcessedOffset)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when polled record is already processed from metadata then it is not sent to work channel`() = runBlocking {
            val restoredTracker = OffsetTracker(initialProcessedOffset = 41L)
            restoredTracker.markProcessed(43L)
            val metadata = OffsetTrackerMetadata.encode(
                restoredTracker.snapshot(),
                OffsetTrackerMetadataContext("test-group", TopicPartition("topic-a", 0), 42L)
            ).metadata!!
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
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
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
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
        fun `when partition is revoked in AT_LEAST_ONCE_NO_ORDERING mode then ready offsets are committed`() = runBlocking {
            val listenerRef = AtomicReference<ConsumerRebalanceListener?>()
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
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

            fixture.awaitDeadlineBoundedCommit(101L)
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
        fun `when partition is revoked with an inflight offset then later completed offsets are not committed`() = runBlocking {
            val listenerRef = AtomicReference<ConsumerRebalanceListener?>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 101L,
                commitIntervalMs = 25L,
                listenerRef = listenerRef,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()
            val state = fixture.awaitAssignedState(lastCommittedOffset = 100L)
            state.markProcessed(102L)
            state.markProcessed(103L)

            listenerRef.get()!!.onPartitionsRevoked(listOf(fixture.topicPartition))
            state.markProcessed(101L)
            delay(100)

            verify(fixture.consumer, never())
                .commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commit interval elapses in AT_LEAST_ONCE_NO_ORDERING mode then ready offsets are committed`() = runBlocking {
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
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
            val metadataStats = metrics.offsetCommitMetadata.single()
            assertEquals("topic-a", metadataStats.topic)
            assertEquals(true, metadataStats.includedInCommit)
            assertTrue(metadataStats.candidateSizeBytes > 0)
            assertEquals(1.0, metadataStats.compressionRatio)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `oversized metadata remains observable while commit contains only offset`() = runBlocking {
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                metrics = metrics,
                workChannelCapacity = 4,
                commitIntervalMs = 25L,
                pollAnswer = { emptyRecords() }
            )
            val job = fixture.start()
            try {
                val state = fixture.awaitAssignedState(lastCommittedOffset = -1L)
                // Leave offset 1 pending and create a large, poorly compressible completion bitmap.
                val random = kotlin.random.Random(62)
                for (offset in 2L until 65_536L) {
                    if (random.nextBoolean()) state.markProcessed(offset)
                }
                state.markProcessed(0L)

                awaitFor(2_000L, 10L) { metrics.commits.firstOrNull() }
                val stats = metrics.offsetCommitMetadata.single()
                assertFalse(stats.includedInCommit)
                assertTrue(stats.limitUtilization > 1.0)
                verify(fixture.consumer).commitSync(
                    argThat<Map<TopicPartition, OffsetAndMetadata>> {
                        get(fixture.topicPartition)?.let { it.offset() == 1L && it.metadata().isEmpty() } == true
                    }
                )
            } finally {
                job.cancel()
                job.join()
            }
        }

        @Test
        fun `when record threshold is reached then offsets are committed before interval`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 201L,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 3,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()
            val state = fixture.awaitAssignedState(lastCommittedOffset = 200L)

            state.markProcessed(201L)
            state.markProcessed(202L)
            state.markProcessed(203L)

            fixture.awaitCommit(203L)

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `record threshold sums pending offsets across assigned partitions`() = runBlocking {
            val secondPartition = TopicPartition("topic-a", 1)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 201L,
                additionalAssignmentPositions = mapOf(secondPartition to 501L),
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 3,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()
            val firstState = fixture.awaitAssignedState(lastCommittedOffset = 200L)
            val secondState = awaitFor(2_000L, 10L) {
                fixture.registry.partitionStateFor(secondPartition)
            }

            firstState.markProcessed(201L)
            secondState.markProcessed(501L)
            secondState.markProcessed(502L)

            verify(fixture.consumer, timeout(2_000)).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.offset() == 202L &&
                            get(secondPartition)?.offset() == 503L
                }
            )

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commit fails then unchanged processed frontier and threshold are retried`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 300L,
                commitIntervalMs = 250L,
                commitRecordsThreshold = 1,
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
            delay(100L)
            verify(fixture.consumer, times(1)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            fixture.awaitCommitAttempts(2)

            verify(fixture.consumer, timeout(2_000).times(2)).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.offset() == 301L
                }
            )
            awaitFor(2_000L, 10L) {
                state.lastCommittedOffset.takeIf { it == 300L }
            }

            job.cancel()
            job.join()

            verify(fixture.consumer).close()
        }

        @Test
        fun `when commits keep failing then retry requires another full record threshold`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 300L,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 3,
                pollAnswer = { emptyRecords() }
            )
            doThrow(IllegalStateException("first commit failed"))
                .doThrow(IllegalStateException("second commit failed"))
                .whenever(fixture.consumer)
                .commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            val job = fixture.start()
            val state = fixture.awaitAssignedState(lastCommittedOffset = 299L)

            state.markProcessed(300L)
            state.markProcessed(301L)
            state.markProcessed(302L)
            fixture.awaitCommitAttempts(1)

            state.markProcessed(303L)
            state.markProcessed(304L)
            delay(100L)
            verify(fixture.consumer, times(1)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            state.markProcessed(305L)
            fixture.awaitCommitAttempts(2)

            state.markProcessed(306L)
            delay(100L)
            verify(fixture.consumer, times(2)).commitSync(any<Map<TopicPartition, OffsetAndMetadata>>())

            job.cancel()
            job.join()
        }

        @Test
        fun `when shutdown drains stashed records then ready signal completes without resume`() = runBlocking {
            val firstPoll = AtomicBoolean(true)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
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

        @Test
        fun `when shutdown performs final commit then remaining drain deadline is used`() = runBlocking {
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 10L,
                pollAnswer = { emptyRecords() }
            )
            fixture.loop.configureShutdownDrainTimeout(kotlin.time.Duration.parse("2s"))
            val job = fixture.start()
            fixture.awaitAssignedState(lastCommittedOffset = 9L)

            withTimeout(2_000) { fixture.loop.prepareForShutdown().await() }
            withTimeout(2_000) { fixture.loop.finalizeShutdown().await() }
            job.join()

            verify(fixture.consumer).commitSync(
                any<Map<TopicPartition, OffsetAndMetadata>>(),
                any<Duration>()
            )
        }

        @Test
        fun `when finalization starts after poll then pending wakeup does not interrupt final commit`() = runBlocking {
            val pendingWakeup = AtomicBoolean(false)
            val pollCalls = AtomicInteger(0)
            val initialPollEntered = CountDownLatch(1)
            val releaseInitialPoll = CountDownLatch(1)
            val finalizationWindowEntered = CountDownLatch(1)
            val releaseFinalizationWindow = CountDownLatch(1)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 10L,
                commitIntervalMs = 60_000L,
                commitRecordsThreshold = 1_000,
                pollAnswer = {
                    when (pollCalls.incrementAndGet()) {
                        1 -> {
                            initialPollEntered.countDown()
                            check(releaseInitialPoll.await(2, TimeUnit.SECONDS)) {
                                "Timed out waiting to release the initial poll"
                            }
                        }
                        3 -> {
                            finalizationWindowEntered.countDown()
                            check(releaseFinalizationWindow.await(2, TimeUnit.SECONDS)) {
                                "Timed out waiting to release the finalization window"
                            }
                        }
                    }
                    if (pendingWakeup.compareAndSet(true, false)) {
                        throw org.apache.kafka.common.errors.WakeupException()
                    }
                    emptyRecords()
                }
            )
            doAnswer {
                pendingWakeup.set(true)
                null
            }.whenever(fixture.consumer).wakeup()
            doAnswer {
                if (pendingWakeup.compareAndSet(true, false)) {
                    throw org.apache.kafka.common.errors.WakeupException()
                }
                null
            }.whenever(fixture.consumer).commitSync(
                any<Map<TopicPartition, OffsetAndMetadata>>(),
                any<Duration>()
            )

            val job = fixture.start()
            val state = fixture.awaitAssignedState(lastCommittedOffset = 9L)
            assertTrue(initialPollEntered.await(2, TimeUnit.SECONDS))
            state.markProcessed(10L)

            try {
                val readyForShutdown = fixture.loop.prepareForShutdown()
                releaseInitialPoll.countDown()
                withTimeout(2_000) { readyForShutdown.await() }
                assertTrue(finalizationWindowEntered.await(2, TimeUnit.SECONDS))

                val finalized = fixture.loop.finalizeShutdown()
                releaseFinalizationWindow.countDown()
                withTimeout(2_000) { finalized.await() }
                withTimeout(2_000) { job.join() }
            } finally {
                releaseInitialPoll.countDown()
                releaseFinalizationWindow.countDown()
            }

            assertFalse(job.isCancelled)
            verify(fixture.consumer, times(1)).wakeup()
            verify(fixture.consumer).commitSync(
                argThat<Map<TopicPartition, OffsetAndMetadata>> {
                    get(fixture.topicPartition)?.let { committed ->
                        committed.offset() == 11L && committed.metadata().isNotEmpty()
                    } == true
                },
                any<Duration>()
            )
            verify(fixture.consumer).close()
        }

        @Test
        fun `when recovery initializes assignment then deadline and reason are preserved`() = runBlocking {
            val listener = AtomicReference<ConsumerRebalanceListener?>()
            val polls = AtomicInteger(0)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 10L,
                listenerRef = listener,
                pollAnswer = {
                    when (polls.incrementAndGet()) {
                        2 -> listener.get()!!.onPartitionsLost(setOf(topicPartition))
                        3 -> listener.get()!!.onPartitionsAssigned(setOf(topicPartition))
                    }
                    emptyRecords()
                }
            )
            val job = fixture.start()

            awaitFor(2_000L, 10L) {
                fixture.loop.stateSnapshot().takeIf {
                    !it.recovering && it.lastRecoveryReason?.startsWith("partitions lost") == true
                }
            }

            verify(fixture.consumer, timeout(2_000)).committed(
                any<Set<TopicPartition>>(),
                any<Duration>()
            )
            verify(fixture.consumer, timeout(2_000)).position(eq(fixture.topicPartition), any<Duration>())

            job.cancel()
            job.join()
        }

        @Test
        fun `when recovery backoff exceeds deadline then backoff and client close are bounded`() = runBlocking {
            val listener = AtomicReference<ConsumerRebalanceListener?>()
            val triggerLoss = AtomicBoolean(false)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 10L,
                listenerRef = listener,
                pollAnswer = {
                    if (triggerLoss.compareAndSet(true, false)) {
                        listener.get()!!.onPartitionsLost(setOf(topicPartition))
                    }
                    emptyRecords()
                }
            )
            fixture.loop.configureRecovery(
                timeout = kotlin.time.Duration.parse("25ms"),
                retryBackoff = kotlin.time.Duration.parse("30s")
            )
            val job = fixture.start()
            fixture.awaitAssignedState(lastCommittedOffset = 9L)

            triggerLoss.set(true)
            withTimeout(2_000) { job.join() }

            assertTrue(job.isCancelled)
            verify(fixture.consumer).close(any<Duration>())
        }

        @Test
        fun `when lost callback timeout is wrapped then failure is terminal and consumer is not recreated`() = runBlocking {
            val listener = AtomicReference<ConsumerRebalanceListener?>()
            val triggerLoss = AtomicBoolean(false)
            val lossTriggered = AtomicBoolean(false)
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
                workChannelCapacity = 4,
                assignmentPosition = 10L,
                listenerRef = listener,
                pollAnswer = {
                    if (triggerLoss.get() && lossTriggered.compareAndSet(false, true)) {
                        try {
                            listener.get()!!.onPartitionsLost(setOf(topicPartition))
                        } catch (error: Throwable) {
                            throw KafkaException("rebalance callback failed", error)
                        }
                    }
                    emptyRecords()
                }
            )
            fixture.loop.configureProcessingCancellationTimeout(kotlin.time.Duration.parse("1ms"))
            val job = fixture.start()
            fixture.awaitAssignedState(lastCommittedOffset = 9L)
            val record = record(offset = 10L)
            fixture.registry.observe(record)
            assertTrue(fixture.registry.tryStart(record))

            triggerLoss.set(true)
            withTimeout(2_000) { job.join() }

            assertTrue(job.isCancelled)
            verify(fixture.consumer, times(1)).subscribe(eq(listOf("topic-a")), any<ConsumerRebalanceListener>())
        }

        @Test
        fun `kafka client metrics follow poll loop lifecycle`() = runBlocking {
            val metrics = RecordingMetrics<ByteArray, ByteArray>()
            val fixture = PollLoopFixture(
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                metrics = metrics,
                workChannelCapacity = 1,
                pollAnswer = { emptyRecords() }
            )

            val job = fixture.start()
            awaitFor(2_000L, 10L) {
                metrics.boundKafkaConsumers.singleOrNull()
            }

            job.cancel()
            job.join()

            assertEquals(listOf(1 to fixture.consumer), metrics.boundKafkaConsumers)
            assertEquals(listOf(1), metrics.unboundKafkaConsumers)
            verify(fixture.consumer).close()
        }
    }
}

private class PollLoopFixture(
    processingMode: ProcessingMode,
    @Suppress("UNCHECKED_CAST")
    metrics: ConsumerMetrics<ByteArray, ByteArray> = ConsumerMetrics.NOOP as ConsumerMetrics<ByteArray, ByteArray>,
    workChannelCapacity: Int,
    assignmentPosition: Long = 0L,
    private val committedOffsets: Map<TopicPartition, OffsetAndMetadata> = emptyMap(),
    commitIntervalMs: Long = 5_000L,
    commitRecordsThreshold: Int = 1_000,
    private val additionalAssignmentPositions: Map<TopicPartition, Long> = emptyMap(),
    initialChannelRecords: List<ConsumerRecord<ByteArray, ByteArray>> = emptyList(),
    private val listenerRef: AtomicReference<ConsumerRebalanceListener?> = AtomicReference(),
    private val pollAnswer: PollLoopFixture.() -> ConsumerRecords<ByteArray, ByteArray>
) {
    val consumer: KafkaConsumer<ByteArray, ByteArray> = mock()
    val workChannel = Channel<ConsumerRecord<ByteArray, ByteArray>>(
        capacity = workChannelCapacity,
        onBufferOverflow = when (processingMode) {
            ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
            ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> BufferOverflow.SUSPEND
            ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
            ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> BufferOverflow.DROP_OLDEST
        }
    )
    private val recordSink = object : PolledRecordSink<ByteArray, ByteArray> {
        override fun tryEmit(record: ConsumerRecord<ByteArray, ByteArray>): Boolean {
            if (processingMode.tracksProcessedOffsets() &&
                registry.partitionStateFor(record)?.isProcessed(record.offset()) == true
            ) {
                return true
            }
            return workChannel.trySend(record).isSuccess
        }
    }
    val registry = PartitionRegistry()
    val topicPartition = TopicPartition("topic-a", 0)
    private val assignmentPositions = mapOf(topicPartition to assignmentPosition) + additionalAssignmentPositions
    private val consumerProperties = testConsumerProperties()
    val loop = ConsumerPollLoop<ByteArray, ByteArray>(
        id = 1,
        parentContext = Dispatchers.Default,
        processingMode = processingMode,
        commitIntervalMs = commitIntervalMs,
        commitRecordsThreshold = commitRecordsThreshold,
        metrics = metrics,
        consumerProperties = consumerProperties,
        consumerConfigAdapter = KafkaConsumerConfigAdapter(consumerProperties),
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

        whenever(consumer.position(any<TopicPartition>())).thenAnswer { invocation ->
            assignmentPositions.getValue(invocation.getArgument(0))
        }
        whenever(consumer.position(any<TopicPartition>(), any<Duration>())).thenAnswer { invocation ->
            assignmentPositions.getValue(invocation.getArgument(0))
        }
        whenever(consumer.committed(any<Set<TopicPartition>>())).thenReturn(committedOffsets)
        whenever(consumer.committed(any<Set<TopicPartition>>(), any<Duration>())).thenReturn(committedOffsets)
        whenever(consumer.assignment()).thenReturn(assignmentPositions.keys)
        whenever(consumer.close()).then { }

        whenever(consumer.poll(any<Duration>()))
            .thenAnswer {
                if (assignedOnce.compareAndSet(false, true)) {
                    listenerRef.get()?.onPartitionsAssigned(assignmentPositions.keys)
                }
                pollAnswer()
            }
    }

    fun start(): Job = loop.start()

    suspend fun awaitAssignedState(lastCommittedOffset: Long? = null): PartitionState =
        awaitFor(2_000L, 10L) {
            val state = registry.partitionStateFor(topicPartition) ?: return@awaitFor null
            if (lastCommittedOffset == null || state.lastCommittedOffset == lastCommittedOffset) {
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

    fun awaitFreshnessCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && committed.metadata().isEmpty()
            }
        )
    }

    fun awaitDeadlineBoundedCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && !committed.metadata().isNullOrEmpty()
            },
            any<Duration>()
        )
    }

    fun awaitDeadlineBoundedFreshnessCommit(offset: Long) {
        verify(consumer, timeout(2_000)).commitSync(
            argThat<Map<TopicPartition, OffsetAndMetadata>> {
                val committed = get(topicPartition)
                committed?.offset() == offset + 1 && committed.metadata().isEmpty()
            },
            any<Duration>()
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
