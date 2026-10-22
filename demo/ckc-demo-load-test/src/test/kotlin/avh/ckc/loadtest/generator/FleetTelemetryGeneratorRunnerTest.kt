package avh.ckc.loadtest.generator

import java.time.Duration
import java.time.Instant
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

    @Test
    fun `keeps repeated publications on the original interval grid`() {
        val interval = Duration.ofSeconds(1)
        var due = Instant.EPOCH

        repeat(600) {
            val completedAt = due.plusMillis(200)
            due = nextFleetTelemetryDue(due.plus(interval), interval, completedAt)
        }

        assertEquals(Instant.EPOCH.plusSeconds(600), due)
    }

    @Test
    fun `skips missed intervals without a catch-up deadline`() {
        val interval = Duration.ofSeconds(1)

        assertEquals(
            Instant.EPOCH.plusSeconds(4),
            nextFleetTelemetryDue(
                Instant.EPOCH.plusSeconds(1),
                interval,
                Instant.EPOCH.plusMillis(3_200)
            )
        )
    }
}
