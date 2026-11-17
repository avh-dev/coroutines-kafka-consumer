package avh.ckc.core.warmup

import kotlin.math.floor
import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO

/**
 * Atomic interval quota with no carry-over of unused capacity.
 *
 * The first acquisition attempt starts both the first quota window and the optional ramp. Each
 * later window starts lazily on the first attempt after [interval] elapsed. Ramp progress is thus
 * measured from the first [tryAcquire], not from object construction or donor-session activation.
 * The selected curve has no user-defined coefficients. Quadratic growth keeps early cold-start
 * work small and concentrates capacity near the end of the ramp; linear growth remains available
 * for workloads that need a constant increase.
 */
internal class IntervalQuota(
    private val limit: Int,
    private val interval: Duration,
    private val initialLimit: Int = limit,
    private val rampDuration: Duration = ZERO,
    private val rampCurve: QuotaRampCurve = QuotaRampCurve.QUADRATIC,
    private val nanoTime: () -> Long = System::nanoTime
) {
    private val intervalNanos = interval.inWholeNanoseconds
    private val rampNanos = rampDuration.inWholeNanoseconds
    private var rampStartedAt = Long.MIN_VALUE
    private var windowStartedAt = Long.MIN_VALUE
    private var used = 0

    init {
        require(limit > 0) { "Warm-up interval quota must be > 0" }
        require(initialLimit in 0..limit) { "Warm-up initial interval quota must be between 0 and $limit" }
        require(interval.isPositive()) { "Warm-up quota interval must be > 0" }
        require(!rampDuration.isNegative()) { "Warm-up quota ramp duration must not be negative" }
    }

    @Synchronized
    fun tryAcquire(): Boolean {
        val now = nanoTime()
        if (rampStartedAt == Long.MIN_VALUE || now < rampStartedAt) rampStartedAt = now
        if (windowStartedAt == Long.MIN_VALUE || now < windowStartedAt || now - windowStartedAt >= intervalNanos) {
            windowStartedAt = now
            used = 0
        }
        if (used >= currentLimit(now)) return false
        used++
        return true
    }

    private fun currentLimit(now: Long): Int {
        if (initialLimit == limit || rampNanos == 0L) return limit
        val elapsed = now - rampStartedAt
        if (elapsed >= rampNanos) return limit
        val progress = elapsed.toDouble() / rampNanos.toDouble()
        val curvedProgress = when (rampCurve) {
            QuotaRampCurve.LINEAR -> progress
            QuotaRampCurve.QUADRATIC -> progress * progress
        }
        return initialLimit + floor((limit - initialLimit) * curvedProgress).toInt()
    }
}
