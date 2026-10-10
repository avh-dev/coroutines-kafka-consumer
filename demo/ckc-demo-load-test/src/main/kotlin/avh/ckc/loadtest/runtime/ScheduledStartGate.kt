package avh.ckc.loadtest.runtime

import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant

/** Waits for the absolute start instant without carrying elapsed wait time into a generator. */
class ScheduledStartGate(
    private val clock: () -> Instant = Instant::now,
    private val pause: suspend (Duration) -> Unit = { duration -> delay(duration.toMillis().coerceAtLeast(1L)) }
) {
    suspend fun await(scheduledAt: Instant): Instant {
        while (true) {
            val now = clock()
            if (!now.isBefore(scheduledAt)) return now
            pause(Duration.between(now, scheduledAt).coerceAtMost(RECHECK_INTERVAL))
        }
    }

    private companion object {
        val RECHECK_INTERVAL: Duration = Duration.ofMillis(100)
    }
}
