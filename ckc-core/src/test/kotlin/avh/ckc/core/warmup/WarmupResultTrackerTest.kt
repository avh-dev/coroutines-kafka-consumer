package avh.ckc.core.warmup

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WarmupResultTrackerTest {
    private val session = WarmupSessionId("session-1")

    @Test
    fun `result after registration completes waiter and duplicate is idempotent`() = runBlocking {
        val tracker = tracker()
        val completed = completed(offset = 7L, WarmupRemoteOutcome.Succeeded)
        val waiting = async { tracker.await(completed.recordId, 1.seconds) }
        withTimeout(1_000) { while (tracker.pendingCount() != 1) kotlinx.coroutines.yield() }

        assertEquals(WarmupResultTracker.Completion.DELIVERED, tracker.complete(completed))
        assertEquals(WarmupRemoteOutcome.Succeeded, waiting.await())
        assertEquals(WarmupResultTracker.Completion.DUPLICATE, tracker.complete(completed))
        assertEquals(0, tracker.pendingCount())
    }

    @Test
    fun `result before registration is retained and consumed`() = runBlocking {
        val tracker = tracker()
        val completed = completed(offset = 8L, WarmupRemoteOutcome.TerminalFailure)

        assertEquals(WarmupResultTracker.Completion.STORED_EARLY, tracker.complete(completed))
        assertEquals(WarmupRemoteOutcome.TerminalFailure, tracker.await(completed.recordId, 1.seconds))
        assertEquals(WarmupResultTracker.Completion.DUPLICATE, tracker.complete(completed))
    }

    @Test
    fun `timeout wins once and late result cannot revive source`() = runBlocking {
        val tracker = tracker()
        val completed = completed(offset = 9L, WarmupRemoteOutcome.Succeeded)

        assertThrows(WarmupResultTimeoutException::class.java) {
            runBlocking { tracker.await(completed.recordId, 20.milliseconds) }
        }
        assertEquals(WarmupResultTracker.Completion.DUPLICATE, tracker.complete(completed))
        assertEquals(0, tracker.pendingCount())
    }

    @Test
    fun `early results are bounded and expire`() {
        val now = AtomicLong(0L)
        val tracker = tracker(maxEarly = 1, now = now)

        assertEquals(WarmupResultTracker.Completion.STORED_EARLY, tracker.complete(completed(1L)))
        assertEquals(WarmupResultTracker.Completion.OVERFLOW, tracker.complete(completed(2L)))
        now.set(2.seconds.inWholeNanoseconds)
        assertEquals(WarmupResultTracker.Completion.STORED_EARLY, tracker.complete(completed(2L)))
    }

    @Test
    fun `completed result remains an idempotent duplicate after early result TTL`() = runBlocking {
        val now = AtomicLong(0L)
        val tracker = tracker(now = now)
        val completed = completed(3L)
        val waiting = async { tracker.await(completed.recordId, 1.seconds) }
        withTimeout(1_000) { while (tracker.pendingCount() != 1) kotlinx.coroutines.yield() }
        assertEquals(WarmupResultTracker.Completion.DELIVERED, tracker.complete(completed))
        assertEquals(WarmupRemoteOutcome.Succeeded, waiting.await())

        now.set(2.seconds.inWholeNanoseconds)

        assertEquals(WarmupResultTracker.Completion.DUPLICATE, tracker.complete(completed))
    }

    @Test
    fun `foreign session is ignored`() {
        val tracker = tracker()
        val foreign = WarmupRecordCompleted(
            WarmupRecordId(WarmupSessionId("other"), TopicPartition("warmup", 0), 1L),
            WarmupRemoteOutcome.Succeeded
        )

        assertEquals(WarmupResultTracker.Completion.IGNORED, tracker.complete(foreign))
    }

    private fun tracker(
        maxEarly: Int = 4,
        now: AtomicLong = AtomicLong(0L)
    ) = WarmupResultTracker(session, maxEarly, 1.seconds, now::get)

    private fun completed(
        offset: Long,
        outcome: WarmupRemoteOutcome = WarmupRemoteOutcome.Succeeded
    ) = WarmupRecordCompleted(
        WarmupRecordId(session, TopicPartition("warmup", 0), offset),
        outcome
    )
}
