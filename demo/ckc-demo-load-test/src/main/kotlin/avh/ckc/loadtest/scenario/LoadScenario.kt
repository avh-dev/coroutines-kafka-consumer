package avh.ckc.loadtest.scenario

import java.time.Duration
import java.time.Instant

data class LoadPhase(
    val fromRatePercent: Int,
    val toRatePercent: Int,
    val duration: Duration,
    val label: String?
)

data class ActiveLoadPhase(
    val name: String,
    val fromRatePercent: Int,
    val toRatePercent: Int,
    val duration: Duration,
    val elapsed: Duration,
    val baseRate: Int
) {
    fun currentRatePercent(): Double {
        if (duration.isZero) {
            return toRatePercent.toDouble()
        }

        val progress = elapsed.toMillis().toDouble() / duration.toMillis().toDouble()
        val clamped = progress.coerceIn(0.0, 1.0)
        return fromRatePercent + ((toRatePercent - fromRatePercent) * clamped)
    }

    fun currentRate(): Double = baseRate * (currentRatePercent() / 100.0)

    fun targetRate(): Int = ((baseRate.toDouble() * toRatePercent.toDouble()) / 100.0).toInt()

    fun startRate(): Int = ((baseRate.toDouble() * fromRatePercent.toDouble()) / 100.0).toInt()
}

data class ScenarioEvaluationContext(
    val baseRate: Int
) {
    init {
        require(baseRate > 0) { "baseRate must be positive" }
    }
}

data class LoadScenario(
    val phases: List<LoadPhase>
) {
    init {
        require(phases.isNotEmpty()) { "Scenario must contain at least one phase" }
        require(phases.all { !it.duration.isNegative && !it.duration.isZero }) { "Phase duration must be positive" }
    }

    fun phaseAt(now: Instant, startedAt: Instant, context: ScenarioEvaluationContext): ActiveLoadPhase? {
        val elapsed = Duration.between(startedAt, now)
        if (elapsed.isNegative) {
            val first = phases.first()
            return ActiveLoadPhase(
                name = first.label ?: "phase-0",
                fromRatePercent = first.fromRatePercent,
                toRatePercent = first.toRatePercent,
                duration = first.duration,
                elapsed = Duration.ZERO,
                baseRate = context.baseRate
            )
        }

        var cursor = Duration.ZERO
        phases.forEachIndexed { index, phase ->
            val phaseEnd = cursor.plus(phase.duration)
            if (elapsed < phaseEnd) {
                return ActiveLoadPhase(
                    name = phase.label ?: "phase-$index",
                    fromRatePercent = phase.fromRatePercent,
                    toRatePercent = phase.toRatePercent,
                    duration = phase.duration,
                    elapsed = elapsed.minus(cursor),
                    baseRate = context.baseRate
                )
            }
            cursor = phaseEnd
        }

        return null
    }

    fun peakRatePercent(): Int = phases.maxOf { maxOf(it.fromRatePercent, it.toRatePercent) }

    companion object {
        fun parse(profile: String): LoadScenario = LoadScenario(LoadProfileParser.parse(profile))
    }
}
