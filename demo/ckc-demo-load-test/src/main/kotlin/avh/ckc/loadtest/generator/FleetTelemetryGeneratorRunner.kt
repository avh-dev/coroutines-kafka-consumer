package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.scenario.LoadScenario
import avh.ckc.loadtest.scenario.ScenarioEvaluationContext
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import java.util.PriorityQueue
import kotlin.math.roundToInt

class FleetTelemetryGeneratorRunner(
    private val generator: FleetTelemetryEventGenerator,
    private val config: LoadTestConfig,
    private val scenario: LoadScenario,
    private val startedAt: Instant,
    private val stats: TrafficStats,
    private val clock: () -> Instant = Instant::now
) {
    private data class ScheduledKey(val index: Int, val dueAt: Instant)

    suspend fun run() {
        val interval = config.telemetryPublishInterval
        val fleetSize = telemetryFleetSize(
            config.baseTps,
            config.cauldronTelemetryPercent,
            scenario.peakRatePercent(),
            interval
        )
        if (fleetSize == 0) return
        val schedule = PriorityQueue<ScheduledKey>(compareBy(ScheduledKey::dueAt))
        repeat(fleetSize) { index ->
            val phaseNanos = ((goldenFraction(index) * interval.toNanos()).toLong())
            schedule += ScheduledKey(index, nextDue(startedAt.plusNanos(phaseNanos), interval, clock()))
        }

        while (true) {
            val now = clock()
            val phase = scenario.phaseAt(now, startedAt, ScenarioEvaluationContext(config.baseTps)) ?: return
            val head = schedule.peek()
            if (head.dueAt.isAfter(now)) {
                delay(Duration.between(now, head.dueAt).toMillis().coerceIn(1L, 100L))
                continue
            }
            var handled = 0
            val active = activeTelemetryKeys(telemetryRate(phase.currentRate()), interval, fleetSize)
            while (schedule.isNotEmpty() && !schedule.peek().dueAt.isAfter(now) && handled < config.maxBurst) {
                val due = schedule.remove()
                val emitted = due.index < active
                var completedAt: Instant? = null
                if (emitted) {
                    stats.record(generator.name, generator.emitFleet(due.index, clock()))
                    completedAt = clock()
                }
                val rescheduledAt = clock()
                val nextCandidate = completedAt?.plus(interval) ?: due.dueAt.plus(interval)
                schedule += ScheduledKey(due.index, nextDue(nextCandidate, interval, rescheduledAt))
                handled++
            }
        }
    }

    private fun telemetryRate(baseRate: Double): Double = baseRate * config.cauldronTelemetryPercent / 100.0

    private fun nextDue(candidate: Instant, interval: Duration, now: Instant): Instant {
        if (candidate.isAfter(now)) return candidate
        val elapsed = Duration.between(candidate, now).toNanos()
        return candidate.plusNanos((elapsed / interval.toNanos() + 1) * interval.toNanos())
    }

    private fun goldenFraction(index: Int): Double = ((index + 1) * 0.6180339887498949) % 1.0
}

internal fun telemetryFleetSize(
    baseTps: Int,
    telemetryPercent: Int,
    peakPercent: Int,
    interval: Duration
): Int {
    val numerator = baseTps.toLong() * telemetryPercent * peakPercent * interval.toNanos()
    val denominator = 10_000L * 1_000_000_000L
    return ((numerator + denominator - 1) / denominator).toInt().coerceAtLeast(0)
}

internal fun activeTelemetryKeys(rate: Double, interval: Duration, fleetSize: Int): Int =
    (rate * interval.toNanos() / 1_000_000_000.0).roundToInt().coerceIn(0, fleetSize)
