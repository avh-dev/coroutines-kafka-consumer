package avh.ckc.loadtest.generator

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class FleetTelemetryGeneratorRunnerTest {
    @Test
    fun `derives peak fleet from telemetry throughput and per-key interval`() {
        assertEquals(10_000, telemetryFleetSize(5_000, 40, 100, Duration.ofSeconds(5)))
        assertEquals(102, telemetryFleetSize(51, 40, 100, Duration.ofSeconds(5)))
    }

    @Test
    fun `scales throughput through active keys and preserves fleet bound`() {
        val interval = Duration.ofSeconds(5)
        assertEquals(1_000, activeTelemetryKeys(200.0, interval, 10_000))
        assertEquals(5_000, activeTelemetryKeys(1_000.0, interval, 10_000))
        assertEquals(10_000, activeTelemetryKeys(3_000.0, interval, 10_000))
    }
}
