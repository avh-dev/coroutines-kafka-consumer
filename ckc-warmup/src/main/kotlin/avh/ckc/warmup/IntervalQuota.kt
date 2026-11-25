package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupQuotaProfile
import kotlin.time.Duration

/**
 * Atomic mapping quota tied to fixed window boundaries. The ramp starts on the first attempt in an
 * open window and its current absolute quota is compared with that window's already used budget.
 */
internal class IntervalQuota(
    private val profile: WarmupQuotaProfile,
    interval: Duration,
    private val nanoTime: () -> Long = System::nanoTime
) {
    private val intervalNanos = interval.inWholeNanoseconds
    private var windowEpoch: Long? = null
    private var profileStartedAt: Long? = null
    private var currentWindow = -1L
    private var used = 0

    init {
        require(interval.isPositive()) { "Warm-up quota interval must be > 0" }
    }

    @Synchronized
    fun startAt(firstWindowStart: Long) {
        check(windowEpoch == null) { "Warm-up quota windows already started" }
        windowEpoch = firstWindowStart
    }

    /** Consumes quota only after the caller also reserves capacity for an eligible record. */
    @Synchronized
    fun tryAcquire(tryReserve: () -> Boolean = { true }): Boolean {
        val now = nanoTime()
        val firstWindow = windowEpoch ?: now.also { windowEpoch = it }
        val elapsed = now - firstWindow
        if (elapsed < 0) return false
        val window = elapsed / intervalNanos
        if (currentWindow != window) {
            currentWindow = window
            used = 0
        }
        val profileStart = profileStartedAt ?: now.also { profileStartedAt = it }
        val profileElapsed = now - profileStart
        if (used >= profile.quotaAt(profileElapsed)) return false
        if (!tryReserve()) return false
        used++
        return true
    }
}
