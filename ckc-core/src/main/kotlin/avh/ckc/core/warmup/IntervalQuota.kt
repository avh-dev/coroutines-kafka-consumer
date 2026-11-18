package avh.ckc.core.warmup

import kotlin.time.Duration

/**
 * Atomic interval quota with no carry-over of unused capacity.
 *
 * The first acquisition attempt starts both the first quota window and [profile]. Each later window
 * starts lazily on the first attempt after [interval] elapsed. Profile transitions never reset the
 * current window: its already used budget is compared with the profile's current absolute quota.
 * Unused capacity is discarded when a new window begins.
 */
internal class IntervalQuota(
    private val profile: WarmupQuotaProfile,
    private val interval: Duration,
    private val nanoTime: () -> Long = System::nanoTime
) {
    private val intervalNanos = interval.inWholeNanoseconds
    private var profileStartedAt = Long.MIN_VALUE
    private var windowStartedAt = Long.MIN_VALUE
    private var used = 0

    init {
        require(interval.isPositive()) { "Warm-up quota interval must be > 0" }
    }

    @Synchronized
    fun tryAcquire(): Boolean {
        val now = nanoTime()
        if (profileStartedAt == Long.MIN_VALUE || now < profileStartedAt) profileStartedAt = now
        if (windowStartedAt == Long.MIN_VALUE || now < windowStartedAt || now - windowStartedAt >= intervalNanos) {
            windowStartedAt = now
            used = 0
        }
        if (used >= profile.quotaAt(now - profileStartedAt)) return false
        used++
        return true
    }
}
