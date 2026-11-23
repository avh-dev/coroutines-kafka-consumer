package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupRecordCompleted
import avh.ckc.warmup.protocol.WarmupRecordId
import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class WarmupResultTrackerTest {
    private val sessionId = WarmupSessionId("session-a")
    private val recordId = WarmupRecordId(sessionId, TopicPartition("warmup", 0), 7)

    @Test
    fun `early result is delivered to the matching session and coordinate`() = runBlocking {
        val tracker = WarmupResultTracker(sessionId, 4, 5.seconds)
        val completed = WarmupRecordCompleted(recordId, WarmupRemoteOutcome.Succeeded)

        assertEquals(WarmupResultTracker.Completion.STORED_EARLY, tracker.complete(completed))
        assertEquals(WarmupRemoteOutcome.Succeeded, tracker.await(recordId, 1.seconds))
        assertEquals(WarmupResultTracker.Completion.DUPLICATE, tracker.complete(completed))
    }

    @Test
    fun `session failure resolves only its own pending result`() = runBlocking {
        val tracker = WarmupResultTracker(sessionId, 4, 5.seconds)
        val waiting = async { runCatching { tracker.await(recordId, 5.seconds) }.exceptionOrNull() }
        while (tracker.pendingCount() == 0) kotlinx.coroutines.yield()

        val expected = IllegalStateException("session failed")
        tracker.fail(expected)

        val actual = waiting.await()
        assertInstanceOf(IllegalStateException::class.java, actual)
        assertEquals(expected.message, actual?.message)
    }
}
