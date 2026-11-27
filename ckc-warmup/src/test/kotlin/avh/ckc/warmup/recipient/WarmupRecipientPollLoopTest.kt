package avh.ckc.warmup.recipient

import avh.ckc.core.PollLoopFixture
import avh.ckc.core.ProcessingMode
import avh.ckc.core.RecordingMetrics
import avh.ckc.core.awaitFor
import avh.ckc.core.emptyRecords
import avh.ckc.core.recordsOf
import avh.ckc.core.testRecord
import avh.ckc.warmup.dataplane.WarmupCompletionOutbox
import avh.ckc.warmup.protocol.WARMUP_SESSION_HEADER
import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WarmupRecipientPollLoopTest {
    @Test
    fun `when result budget is full freshness modes retain records until acknowledgement frees admission`() =
        runBlocking {
            listOf(
                ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
                ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
            ).forEach { mode ->
                val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
                val releaseInitialAck = CompletableDeferred<Unit>()
                val initialSendStarted = CompletableDeferred<Unit>()
                val outbox = WarmupCompletionOutbox(scope, laneCapacity = 1) { completed ->
                    if (completed.recordId.offset == 9L) {
                        initialSendStarted.complete(Unit)
                        releaseInitialAck.await()
                    }
                }
                val lane = outbox.lane("mapping-$mode")
                val occupying = testRecord(offset = 9L)
                assertTrue(lane.tryAcquire(occupying))
                lane.publish(completed(occupying))
                withTimeout(2_000) { initialSendStarted.await() }

                val firstPoll = AtomicBoolean(true)
                val fixture = PollLoopFixture(
                    processingMode = mode,
                    workChannelCapacity = 4,
                    admission = WarmupRecipientAdmission(WarmupSessionId("recipient"), lane),
                    pollAnswer = {
                        if (firstPoll.compareAndSet(true, false)) {
                            recordsOf(
                                topicPartition,
                                testRecord(offset = 10L).withWarmupSession("recipient"),
                                testRecord(offset = 11L).withWarmupSession("recipient")
                            )
                        } else {
                            emptyRecords()
                        }
                    }
                )
                val job = fixture.start()
                try {
                    fixture.awaitPause()
                    assertTrue(fixture.workChannel.tryReceive().isFailure, "mode $mode")

                    releaseInitialAck.complete(Unit)
                    val first = withTimeout(2_000) { fixture.workChannel.receive() }
                    assertEquals(10L, first.offset(), "mode $mode")
                    lane.publish(completed(first))

                    val second = withTimeout(2_000) { fixture.workChannel.receive() }
                    assertEquals(11L, second.offset(), "mode $mode")
                    lane.publish(completed(second))
                    fixture.awaitResume()
                    outbox.stop(kotlin.time.Duration.parse("2s"))
                } finally {
                    releaseInitialAck.complete(Unit)
                    job.cancel()
                    job.join()
                    outbox.close()
                    scope.cancel()
                }
            }
        }

    @Test
    fun `when shutdown tail polls an own unadmitted record result delivery remains healthy`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val sent = mutableListOf<WarmupRecordCompleted>()
        val outbox = WarmupCompletionOutbox(scope, laneCapacity = 1) { sent += it }
        val lane = outbox.lane("orders")
        val delegateMetrics = RecordingMetrics<ByteArray, ByteArray>()
        val metrics = WarmupRecipientOutcomeMetrics(
            WarmupSessionId("recipient"),
            lane,
            delegateMetrics
        )
        val activePollEntered = CountDownLatch(1)
        val releaseActivePoll = CountDownLatch(1)
        val shutdownRecordReturned = AtomicBoolean(false)
        val own = testRecord(offset = 42L).withWarmupSession("recipient")
        val fixture = PollLoopFixture(
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
            metrics = metrics,
            workChannelCapacity = 1,
            admission = WarmupRecipientAdmission(WarmupSessionId("recipient"), lane),
            filterPausedPollRecords = false,
            pollAnswer = {
                if (activePollEntered.count == 1L) {
                    activePollEntered.countDown()
                    check(releaseActivePoll.await(2, TimeUnit.SECONDS))
                    emptyRecords()
                } else if (shutdownRecordReturned.compareAndSet(false, true)) {
                    recordsOf(topicPartition, own)
                } else {
                    emptyRecords()
                }
            }
        )
        val job = fixture.start()
        try {
            assertTrue(activePollEntered.await(2, TimeUnit.SECONDS))
            val ready = fixture.prepareForShutdown()
            releaseActivePoll.countDown()
            withTimeout(2_000) { ready.await() }
            awaitFor(2_000L, 10L) { delegateMetrics.dropped.singleOrNull() }

            assertTrue(sent.isEmpty())
            assertEquals(0, outbox.snapshot().outstanding)
            outbox.stop(kotlin.time.Duration.parse("2s"))
            fixture.finalizeShutdown()
            withTimeout(2_000) { job.join() }
        } finally {
            releaseActivePoll.countDown()
            job.cancel()
            job.join()
            outbox.close()
            scope.cancel()
        }
    }
}

private fun ConsumerRecord<ByteArray, ByteArray>.withWarmupSession(
    sessionId: String
): ConsumerRecord<ByteArray, ByteArray> = apply {
    headers().add(WARMUP_SESSION_HEADER, sessionId.toByteArray(Charsets.UTF_8))
}

private fun completed(record: ConsumerRecord<ByteArray, ByteArray>) = WarmupRecordCompleted(
    WarmupRecordId(
        WarmupSessionId("recipient"),
        TopicPartition(record.topic(), record.partition()),
        record.offset()
    ),
    WarmupRemoteOutcome.Succeeded
)
