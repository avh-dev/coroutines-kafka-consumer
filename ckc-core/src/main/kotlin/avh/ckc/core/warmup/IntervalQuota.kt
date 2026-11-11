package avh.ckc.core.warmup

import kotlin.time.Duration

/** A lazy, non-carrying quota whose rollover and acquisition are one atomic operation. */
internal class IntervalQuota(
    private val limit: Int,
    private val interval: Duration,
    private val nanoTime: () -> Long = System::nanoTime
) {
    private val intervalNanos = interval.inWholeNanoseconds
    private var windowStartedAt = Long.MIN_VALUE
    private var used = 0

    init {
        require(limit > 0) { "Warm-up interval quota must be > 0" }
        require(interval.isPositive()) { "Warm-up quota interval must be > 0" }
    }

    @Synchronized
    fun tryAcquire(): Boolean {
        val now = nanoTime()
        if (windowStartedAt == Long.MIN_VALUE || now < windowStartedAt || now - windowStartedAt >= intervalNanos) {
            windowStartedAt = now
            used = 0
        }
        if (used >= limit) return false
        used++
        return true
    }
}
