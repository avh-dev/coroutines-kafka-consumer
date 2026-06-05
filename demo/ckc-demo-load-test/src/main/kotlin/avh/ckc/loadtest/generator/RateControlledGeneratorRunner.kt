package avh.ckc.loadtest.generator

import avh.ckc.loadtest.config.LoadTestConfig
import avh.ckc.loadtest.scenario.LoadScenario
import avh.ckc.loadtest.scenario.ScenarioEvaluationContext
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant
import kotlin.math.ceil
import kotlin.math.floor

class RateControlledGeneratorRunner(
    private val generator: EventGenerator,
    private val config: LoadTestConfig,
    private val scenario: LoadScenario,
    private val startedAt: Instant,
    private val topicWeightTotal: Double,
    private val stats: TrafficStats,
    private val clock: () -> Instant = Instant::now
) {
    suspend fun run() {
        var lastLoopAt = clock()
        var lastRate = 0.0
        var permits = 0.0

        while (true) {
            val now = clock()
            val phase = scenario.phaseAt(now, startedAt, ScenarioEvaluationContext(config.baseTps)) ?: return
            val rate = generatorRate(phase.currentRate())
            val elapsedSeconds = elapsedSeconds(lastLoopAt, now)
            permits += ((lastRate + rate) / 2.0) * elapsedSeconds
            lastLoopAt = now
            lastRate = rate

            val emitCount = floor(permits).toInt().coerceAtMost(config.maxBurst)
            if (emitCount > 0) {
                repeat(emitCount) {
                    val result = generator.emit(clock())
                    stats.record(generator.name, result)
                }
                permits -= emitCount
                if (permits >= 1.0) {
                    continue
                }
            }

            delay(nextDelayMillis(rate, permits))
        }
    }

    private fun generatorRate(currentBaseTps: Double): Double {
        val topicRate = when (generator.topic) {
            TrafficTopic.ORDER -> currentBaseTps * config.orderEventPercent / 100.0
            TrafficTopic.BATCH -> currentBaseTps * config.batchEventPercent / 100.0
            TrafficTopic.CAULDRON -> currentBaseTps * config.cauldronTelemetryPercent / 100.0
        }
        return topicRate * generator.weight / topicWeightTotal
    }

    private fun nextDelayMillis(rate: Double, permits: Double): Long {
        if (rate <= 0.0) {
            return 100L
        }
        val missing = (1.0 - permits).coerceAtLeast(0.0)
        return ceil(missing / rate * 1000.0).toLong()
            .coerceAtLeast(1L)
            .coerceAtMost(MAX_RATE_RECHECK_DELAY_MILLIS)
    }

    private fun elapsedSeconds(previous: Instant, current: Instant): Double =
        Duration.between(previous, current).toNanos().coerceAtLeast(0L).toDouble() / 1_000_000_000.0

    private companion object {
        private const val MAX_RATE_RECHECK_DELAY_MILLIS = 100L
    }
}
