package avh.ckc.core.warmup

import avh.ckc.core.RecordingMetrics
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import avh.ckc.core.metrics.WarmupResultDeliveryFailureReason
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds

class WarmupRecipientOutcomeMetricsTest {
    @Test
    fun `recipient runtime isolates kafka binding without bypassing processing callbacks`() {
        var processed = 0
        var kafkaBindings = 0
        val callbacks = object : ConsumerMetrics<String, String> {
            override fun runtimeBinding(bindingId: String): ConsumerMetrics<String, String> =
                object : ConsumerMetrics<String, String> {
                    override fun bindKafkaClientMetrics(
                        pollLoopId: Int,
                        consumer: Consumer<String, String>
                    ) {
                        kafkaBindings += 1
                    }
                }

            override fun onRecordProcessed(
                key: String?,
                value: String?,
                record: ConsumerRecord<String, String>,
                endToEndLatencyMillis: Long,
                durationNanos: Long
            ) {
                processed += 1
            }
        }
        val metrics = warmupRecipientRuntimeMetrics(callbacks, "mapping-one")
        val record = ConsumerRecord("warmup-orders", 0, 11L, "key", "value")

        metrics.bindKafkaClientMetrics(0, mock())
        metrics.onRecordProcessed("key", "value", record, 1, 2)

        assertEquals(1, kafkaBindings)
        assertEquals(1, processed)
    }

    @Test
    fun `recipient publishes terminal runtime outcomes with its session coordinate`() {
        val published = mutableListOf<WarmupRecordCompleted>()
        val delegate = RecordingMetrics<String, String>()
        val metrics = WarmupRecipientOutcomeMetrics(
            WarmupSessionId("session"),
            WarmupCompletionPublisher(published::add),
            delegate
        )
        val record = ConsumerRecord("warmup-orders", 0, 11L, "key", "value")

        metrics.onRecordProcessed("key", "value", record, 1, 2, processedRemotely = false)
        metrics.onRecordFailed("key", "value", record, IllegalStateException("failed"), 3)
        metrics.onRecordDropped(record, RecordDropReason.STALE_AGE)

        assertEquals(3, published.size)
        assertEquals(
            List(3) { WarmupRecordId(WarmupSessionId("session"), TopicPartition("warmup-orders", 0), 11) },
            published.map { it.recordId }
        )
        assertEquals(
            listOf(
                WarmupRemoteOutcome.Succeeded,
                WarmupRemoteOutcome.TerminalFailure,
                WarmupRemoteOutcome.Discarded(RecordDropReason.STALE_AGE)
            ),
            published.map { it.outcome }
        )
        assertEquals(1, delegate.processed.size)
        assertEquals(1, delegate.failed.size)
        assertEquals(1, delegate.dropped.size)
    }

    @Test
    fun `completion outbox flush waits for transport acknowledgement`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val releaseFirst = CompletableDeferred<Unit>()
        val sent = mutableListOf<WarmupRecordCompleted>()
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 4, senderConcurrency = 1) { completed ->
            if (sent.isEmpty()) releaseFirst.await()
            sent += completed
        }
        val lane = outbox.lane("orders")
        val first = completed(offset = 1)
        val second = completed(offset = 2).copy(outcome = WarmupRemoteOutcome.TerminalFailure)
        val discarded = completed(offset = 3).copy(
            outcome = WarmupRemoteOutcome.Discarded(RecordDropReason.LIFECYCLE_DISCARD)
        )
        admitAndPublish(lane, first)
        admitAndPublish(lane, second)
        admitAndPublish(lane, discarded)
        assertEquals(2L, outbox.processedRecords())

        val flush = async { outbox.flush(2.seconds) }
        delay(50)
        assertEquals(false, flush.isCompleted)
        releaseFirst.complete(Unit)
        flush.await()

        assertEquals(listOf(first, second, discarded), sent)
        outbox.stop(2.seconds)
        scope.cancel()
    }

    @Test
    fun `scope cancellation closes outbox and graceful stop reports lost delivery`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val sendStarted = CompletableDeferred<Unit>()
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 2) {
            sendStarted.complete(Unit)
            awaitCancellation()
        }
        val lane = outbox.lane("orders")
        try {
            admitAndPublish(lane, completed(offset = 1))
            sendStarted.await()

            scope.cancel()
            val stopFailure = withTimeout(2.seconds) {
                runCatching { outbox.stop(2.seconds) }.exceptionOrNull()
            }
            assertInstanceOf(WarmupCompletionOutboxCancelledException::class.java, stopFailure)

            assertFalse(lane.tryAcquire(record(offset = 2)))
            assertInstanceOf(
                WarmupCompletionOutboxCancelledException::class.java,
                runCatching { outbox.flush(2.seconds) }.exceptionOrNull()
            )
        } finally {
            outbox.close()
            scope.cancel()
        }
        Unit
    }

    @Test
    fun `abort abandons queued results and cancels sends awaiting acknowledgement`() = runBlocking {
        val sendStarted = CompletableDeferred<Unit>()
        val outbox = WarmupCompletionOutbox(this, laneCapacity = 2, senderConcurrency = 1) {
            sendStarted.complete(Unit)
            awaitCancellation()
        }
        val lane = outbox.lane("orders")
        try {
            admitAndPublish(lane, completed(offset = 1))
            admitAndPublish(lane, completed(offset = 2))
            withTimeout(2.seconds) { sendStarted.await() }
            assertEquals(1, outbox.snapshot().awaitingAcknowledgement)
            assertEquals(1, outbox.snapshot().queued)

            outbox.abort(2.seconds)

            assertEquals(0, outbox.snapshot().outstanding)
            assertFalse(lane.tryAcquire(record(offset = 3)))
            outbox.stop(2.seconds)
        } finally {
            outbox.close()
        }
    }

    @Test
    fun `send cancellation closes outbox instead of accepting undeliverable results`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 2) {
            throw kotlinx.coroutines.CancellationException("transport cancelled")
        }
        val lane = outbox.lane("orders")
        try {
            admitAndPublish(lane, completed(offset = 1))
            val stopFailure = withTimeout(2.seconds) {
                runCatching { outbox.stop(2.seconds) }.exceptionOrNull()
            }
            assertInstanceOf(WarmupCompletionOutboxCancelledException::class.java, stopFailure)

            assertFalse(lane.tryAcquire(record(offset = 2)))
        } finally {
            outbox.close()
            scope.cancel()
        }
        Unit
    }

    @Test
    fun `three mappings can retain a burst larger than the former shared capacity`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val releaseAcks = CompletableDeferred<Unit>()
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 512, senderConcurrency = 8) {
            releaseAcks.await()
        }
        try {
            val lanes = List(3) { outbox.lane("mapping-$it") }
            lanes.forEachIndexed { laneIndex, lane ->
                repeat(400) { offset ->
                    val completed = completed("warmup-$laneIndex", offset.toLong())
                    admitAndPublish(lane, completed)
                }
            }

            assertEquals(1_200, outbox.snapshot().outstanding)
            assertTrue(outbox.snapshot().awaitingAcknowledgement <= 8)
            releaseAcks.complete(Unit)
            outbox.stop(5.seconds)
            assertEquals(0, outbox.snapshot().outstanding)
        } finally {
            releaseAcks.complete(Unit)
            outbox.close()
            scope.cancel()
        }
    }

    @Test
    fun `independent acknowledgements are awaited concurrently and may complete out of order`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = Channel<Long>(Channel.UNLIMITED)
        val acknowledgements = List(3) { CompletableDeferred<Unit>() }
        val completedAcks = Channel<Long>(Channel.UNLIMITED)
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 4, senderConcurrency = 3) { result ->
            val offset = result.recordId.offset
            started.send(offset)
            acknowledgements[offset.toInt()].await()
            completedAcks.send(offset)
        }
        try {
            val lane = outbox.lane("orders")
            repeat(3) { offset -> admitAndPublish(lane, completed(offset = offset.toLong())) }
            val startedOffsets = List(3) { withTimeout(2.seconds) { started.receive() } }.toSet()
            assertEquals(setOf(0L, 1L, 2L), startedOffsets)
            assertEquals(3, outbox.snapshot().awaitingAcknowledgement)

            acknowledgements[2].complete(Unit)
            assertEquals(2L, withTimeout(2.seconds) { completedAcks.receive() })
            acknowledgements[0].complete(Unit)
            assertEquals(0L, withTimeout(2.seconds) { completedAcks.receive() })
            acknowledgements[1].complete(Unit)
            assertEquals(1L, withTimeout(2.seconds) { completedAcks.receive() })
            outbox.flush(2.seconds)
        } finally {
            acknowledgements.forEach { it.complete(Unit) }
            runCatching { outbox.stop(2.seconds) }
            scope.cancel()
        }
    }

    @Test
    fun `slow transport bounds each mapping before handler admission without starving a neighbor`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val releaseFirst = CompletableDeferred<Unit>()
        val sent = Channel<String>(Channel.UNLIMITED)
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 2, senderConcurrency = 1) { result ->
            sent.send(result.recordId.topicPartition.topic())
            if (result.recordId.offset == 0L) releaseFirst.await()
        }
        try {
            val hot = outbox.lane("hot")
            val neighbor = outbox.lane("neighbor")
            admitAndPublish(hot, completed("hot-topic", 0))
            admitAndPublish(hot, completed("hot-topic", 1))
            assertFalse(hot.tryAcquire(record("hot-topic", 2)))
            assertTrue(neighbor.tryAcquire(record("neighbor-topic", 0)))
            neighbor.publish(completed("neighbor-topic", 0))

            assertEquals("hot-topic", withTimeout(2.seconds) { sent.receive() })
            releaseFirst.complete(Unit)
            assertEquals("neighbor-topic", withTimeout(2.seconds) { sent.receive() })
            outbox.stop(2.seconds)
        } finally {
            releaseFirst.complete(Unit)
            outbox.close()
            scope.cancel()
        }
    }

    @Test
    fun `flush barrier excludes work admitted after the barrier`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val acknowledgements = mapOf(
            1L to CompletableDeferred<Unit>(),
            2L to CompletableDeferred<Unit>()
        )
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 4, senderConcurrency = 2) { result ->
            requireNotNull(acknowledgements[result.recordId.offset]).await()
        }
        try {
            val lane = outbox.lane("orders")
            admitAndPublish(lane, completed(offset = 1))
            val barrier = async(start = CoroutineStart.UNDISPATCHED) { outbox.flush(2.seconds) }
            admitAndPublish(lane, completed(offset = 2))

            requireNotNull(acknowledgements[1L]).complete(Unit)
            withTimeout(2.seconds) { barrier.await() }
            assertEquals(1, outbox.snapshot().outstanding)

            requireNotNull(acknowledgements[2L]).complete(Unit)
            outbox.stop(2.seconds)
        } finally {
            acknowledgements.values.forEach { it.complete(Unit) }
            outbox.close()
            scope.cancel()
        }
    }

    @Test
    fun `terminal publication never throws when delivery state is already failed`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 1) { }
        val lane = outbox.lane("orders")
        try {
            lane.publish(completed(offset = 1))
            assertInstanceOf(
                WarmupResultDeliveryException::class.java,
                withTimeout(2.seconds) { outbox.awaitFailure() }
            )
            lane.publish(completed(offset = 2))
            assertFalse(lane.tryAcquire(record(offset = 3)))
        } finally {
            runCatching { outbox.stop(2.seconds) }
            scope.cancel()
        }
        Unit
    }

    @Test
    fun `transport timeout is diagnosed without escaping the terminal publisher`() = runBlocking {
        verifyTransportTimeout(WarmupControlSendTimeoutException(10.milliseconds))
        verifyTransportTimeout(KafkaTimeoutException("producer did not acknowledge the result"))
    }

    private suspend fun verifyTransportTimeout(error: Exception) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val failures = Channel<WarmupResultDeliveryFailureReason>(Channel.UNLIMITED)
        val observer = object : WarmupResultDeliveryObserver {
            override fun onFailure(reason: WarmupResultDeliveryFailureReason) {
                failures.trySend(reason)
            }
        }
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 1) {
            throw error
        }
        val lane = outbox.lane("orders", observer)
        try {
            admitAndPublish(lane, completed(offset = 1))
            assertEquals(
                WarmupResultDeliveryFailureReason.TIMEOUT,
                withTimeout(2.seconds) { failures.receive() }
            )
            assertInstanceOf(
                WarmupResultDeliveryException::class.java,
                withTimeout(2.seconds) { outbox.awaitFailure() }
            )
        } finally {
            runCatching { outbox.stop(2.seconds) }
            scope.cancel()
        }
    }

    @Test
    fun `handoff releases reservation without publishing a remote outcome`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val sent = mutableListOf<WarmupRecordCompleted>()
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 1) { sent += it }
        val lane = outbox.lane("orders")
        val delegate = RecordingMetrics<String, String>()
        val metrics = WarmupRecipientOutcomeMetrics(WarmupSessionId("session"), lane, delegate)
        try {
            RecordHandoffReason.entries.forEachIndexed { index, reason ->
                val record = record(offset = index.toLong())
                assertTrue(lane.tryAcquire(record))
                metrics.onRecordHandedOff(record, reason)
                assertEquals(0, outbox.snapshot().outstanding)
            }

            assertTrue(sent.isEmpty())
            assertEquals(RecordHandoffReason.entries, delegate.handedOff.map { it.reason })
            outbox.stop(2.seconds)
        } finally {
            outbox.close()
            scope.cancel()
        }
    }

    @Test
    fun `delivery metrics discard a delayed stale lane snapshot`() {
        val published = mutableListOf<Int>()
        val metrics = object : ConsumerMetrics<String, String> {
            override fun onWarmupResultDeliveryChanged(
                outstanding: Int,
                queued: Int,
                awaitingAcknowledgement: Int,
                admissionLimitedMappings: Int
            ) {
                published += outstanding
            }
        }
        val lane = WarmupResultDeliveryMetricsObserver(metrics).lane("orders")

        lane.onStateChanged(2, outstanding = 0, queued = 0, awaitingAcknowledgement = 0, limited = false)
        lane.onStateChanged(1, outstanding = 1, queued = 0, awaitingAcknowledgement = 1, limited = false)

        assertEquals(listOf(0), published)
    }

    private fun completed(offset: Long) = completed("warmup-orders", offset)

    private fun completed(topic: String, offset: Long) = WarmupRecordCompleted(
        WarmupRecordId(
            WarmupSessionId("session"),
            TopicPartition(topic, 0),
            offset
        ),
        WarmupRemoteOutcome.Succeeded
    )

    private fun record(offset: Long) = record("warmup-orders", offset)

    private fun record(topic: String, offset: Long) = ConsumerRecord(topic, 0, offset, "key", "value")

    private fun admitAndPublish(lane: WarmupResultLane, completed: WarmupRecordCompleted) {
        assertTrue(
            lane.tryAcquire(
                record(completed.recordId.topicPartition.topic(), completed.recordId.offset)
            )
        )
        lane.publish(completed)
    }
}
