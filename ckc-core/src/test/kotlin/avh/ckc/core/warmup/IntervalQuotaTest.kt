package avh.ckc.core.warmup

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class IntervalQuotaTest {
    @Test
    fun `quota ramps quadratically from the initial limit without carrying unused capacity`() {
        var now = 0L
        val quota = IntervalQuota(
            limit = 10,
            interval = 1.seconds,
            initialLimit = 2,
            rampDuration = 10.seconds,
            nanoTime = { now }
        )

        assertTrue(quota.tryAcquire())
        assertTrue(quota.tryAcquire())
        assertFalse(quota.tryAcquire())

        now = 5.seconds.inWholeNanoseconds
        repeat(4) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())

        now = 7_500_000_000L
        repeat(6) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())

        now = 10.seconds.inWholeNanoseconds
        repeat(10) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())
    }

    @Test
    fun `linear curve retains a constant increase`() {
        var now = 0L
        val quota = IntervalQuota(
            limit = 10,
            interval = 1.seconds,
            initialLimit = 2,
            rampDuration = 10.seconds,
            rampCurve = QuotaRampCurve.LINEAR,
            nanoTime = { now }
        )

        assertTrue(quota.tryAcquire())
        assertTrue(quota.tryAcquire())
        assertFalse(quota.tryAcquire())

        now = 5.seconds.inWholeNanoseconds
        repeat(6) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())
    }
}
