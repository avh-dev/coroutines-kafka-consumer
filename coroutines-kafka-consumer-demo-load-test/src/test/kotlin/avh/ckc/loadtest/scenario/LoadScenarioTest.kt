package avh.ckc.loadtest.scenario

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import java.time.Duration
import java.time.Instant

class LoadScenarioTest {
    @Test
    fun `returns current phase for elapsed time`() {
        val startedAt = Instant.parse("2026-03-26T10:00:00Z")
        val scenario = LoadScenario(
            phases = listOf(
                LoadPhase(0, 100, Duration.ofMinutes(2), "warmup"),
                LoadPhase(100, 100, Duration.ofMinutes(5), "steady")
            )
        )
        val context = ScenarioEvaluationContext(baseRate = 1000)

        assertEquals("warmup", scenario.phaseAt(startedAt.plusSeconds(30), startedAt, context)?.name)
        assertEquals("steady", scenario.phaseAt(startedAt.plusSeconds(180), startedAt, context)?.name)
        assertNull(scenario.phaseAt(startedAt.plusSeconds(500), startedAt, context))
    }

    @Test
    fun `interpolates current rate inside active phase using base rate`() {
        val active = LoadScenario(
            phases = listOf(
                LoadPhase(0, 1000, Duration.ofSeconds(200), "warmup")
            )
        ).phaseAt(
            now = Instant.parse("2026-03-26T10:01:40Z"),
            startedAt = Instant.parse("2026-03-26T10:00:00Z"),
            context = ScenarioEvaluationContext(baseRate = 2000)
        )

        assertEquals(500.0, active?.currentRatePercent())
        assertEquals(10000.0, active?.currentRate())
    }
}
