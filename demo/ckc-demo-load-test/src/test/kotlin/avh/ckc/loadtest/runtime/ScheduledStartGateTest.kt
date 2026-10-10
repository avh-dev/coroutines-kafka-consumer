package avh.ckc.loadtest.runtime

import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ScheduledStartGateTest {
    @Test
    fun `future start waits without advancing the generator clock`() = runBlocking {
        val scheduledAt = Instant.parse("2026-10-10T12:00:00Z")
        var now = scheduledAt.minusSeconds(300)
        val pauses = mutableListOf<Duration>()
        val gate = ScheduledStartGate(
            clock = { now },
            pause = { duration ->
                pauses += duration
                now = minOf(scheduledAt, now.plus(duration))
            }
        )

        assertEquals(scheduledAt, gate.await(scheduledAt))
        assertTrue(pauses.isNotEmpty())
        assertTrue(pauses.all { it <= Duration.ofMillis(100) })
    }

    @Test
    fun `past start activates immediately so callers select the current phase`() = runBlocking {
        val now = Instant.parse("2026-10-10T12:05:00Z")
        var pauses = 0
        val gate = ScheduledStartGate(
            clock = { now },
            pause = { pauses++ }
        )

        assertEquals(now, gate.await(now.minusSeconds(60)))
        assertEquals(0, pauses)
    }
}
