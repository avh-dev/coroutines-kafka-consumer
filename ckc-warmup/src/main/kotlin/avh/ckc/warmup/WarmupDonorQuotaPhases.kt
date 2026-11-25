package avh.ckc.warmup

import kotlin.time.Duration

/** Coordinates the quota phases of all active logical sessions on one donor. */
internal class WarmupDonorQuotaPhases(private val nanoTime: () -> Long = System::nanoTime) {
    private class Period(val epoch: Long) {
        val phases = sortedSetOf<Long>()
    }

    private data class Reservation(val period: Period, val phase: Long, val firstWindowStart: Long)

    private val periods = mutableMapOf<Duration, Period>()

    inner class Session(private val intervals: Set<Duration>) {
        private var reservations: Map<Duration, Reservation>? = null
        private var released = false

        fun activate(): Boolean = synchronized(this@WarmupDonorQuotaPhases) {
            if (released || reservations != null) return@synchronized false
            val now = nanoTime()
            val selected = intervals.associateWith { interval ->
                val period = periods[interval] ?: Period(now)
                val periodNanos = interval.inWholeNanoseconds
                val phase = choosePhase(period, periodNanos) ?: return@synchronized false
                val currentPhase = (now - period.epoch).mod(periodNanos)
                val delay = (phase - currentPhase).mod(periodNanos)
                Reservation(period, phase, now + delay)
            }
            selected.forEach { (interval, reservation) ->
                periods[interval] = reservation.period
                reservation.period.phases.add(reservation.phase)
            }
            reservations = selected
            true
        }

        fun firstWindowStart(interval: Duration): Long = synchronized(this@WarmupDonorQuotaPhases) {
            checkNotNull(reservations)[interval]?.firstWindowStart
                ?: error("No warm-up quota phase for interval $interval")
        }

        fun release() = synchronized(this@WarmupDonorQuotaPhases) {
            released = true
            reservations?.forEach { (interval, reservation) ->
                reservation.period.phases.remove(reservation.phase)
                if (reservation.period.phases.isEmpty()) periods.remove(interval)
            }
            reservations = null
        }
    }

    private fun choosePhase(period: Period, periodNanos: Long): Long? {
        val phases = period.phases
        if (phases.isEmpty()) return 0L
        // Visit the wrap-around gap first, followed by the gaps in sorted order.
        var left = phases.last()
        var largestGap = 1L
        var chosen: Long? = null
        for (right in phases) {
            val toPeriodEnd = periodNanos - left
            val gap = if (right > left) right - left else toPeriodEnd + right
            val half = gap / 2
            val midpoint = if (half < toPeriodEnd) left + half else half - toPeriodEnd
            if (gap > largestGap || (gap == largestGap && chosen != null && midpoint < chosen)) {
                largestGap = gap
                chosen = midpoint
            }
            left = right
        }
        return chosen
    }
}
