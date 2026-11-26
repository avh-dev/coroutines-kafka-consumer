package avh.ckc.warmup.donor

import avh.ckc.warmup.protocol.WarmupQuotaProfileParser
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IntervalQuotaTest {
    @Test
    fun `profile starts lazily and unused budget does not carry between windows`() {
        var now = 100.seconds.inWholeNanoseconds
        val quota = IntervalQuota(
            profile = WarmupQuotaProfileParser.parse("2 -> (10s, ramp) -> 10"),
            interval = 1.seconds,
            nanoTime = { now }
        )

        repeat(2) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())

        now += 5.seconds.inWholeNanoseconds
        repeat(6) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())

        now += 5.seconds.inWholeNanoseconds
        repeat(10) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())
    }

    @Test
    fun `profile transition does not reset the current quota window`() {
        var now = 0L
        val quota = IntervalQuota(
            profile = WarmupQuotaProfileParser.parse("2 -> (5s) -> 6"),
            interval = 10.seconds,
            nanoTime = { now }
        )

        repeat(2) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())

        now = 5.seconds.inWholeNanoseconds
        repeat(4) { assertTrue(quota.tryAcquire()) }
        assertFalse(quota.tryAcquire())
    }

    @Test
    fun `mapping and session quota instances keep independent lazy profile clocks`() {
        var firstNow = 0L
        var secondNow = 0L
        val profile = WarmupQuotaProfileParser.parse("0 -> (10s) -> 10")
        val first = IntervalQuota(profile, 1.seconds) { firstNow }
        val second = IntervalQuota(profile, 1.seconds) { secondNow }

        assertFalse(first.tryAcquire())
        assertFalse(second.tryAcquire())

        firstNow = 5.seconds.inWholeNanoseconds
        repeat(5) { assertTrue(first.tryAcquire()) }
        assertFalse(first.tryAcquire())
        assertFalse(second.tryAcquire())
    }
}
