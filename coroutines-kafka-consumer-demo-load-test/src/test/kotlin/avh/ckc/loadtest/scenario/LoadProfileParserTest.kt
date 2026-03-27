package avh.ckc.loadtest.scenario

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import java.time.Duration

class LoadProfileParserTest {
    @Test
    fun `parses compact string profile with labels`() {
        val phases = LoadProfileParser.parse(
            "0 -> (200s, warmup) -> 100 -> (1600s, maximum) -> 100 -> (100s, cool down) -> 0"
        )

        assertEquals(
            listOf(
                LoadPhase(0, 100, Duration.ofSeconds(200), "warmup"),
                LoadPhase(100, 100, Duration.ofSeconds(1600), "maximum"),
                LoadPhase(100, 0, Duration.ofSeconds(100), "cool down")
            ),
            phases
        )
    }

    @Test
    fun `supports compact minute and hour duration units`() {
        val phases = LoadProfileParser.parse("0 -> (2m, ramp) -> 10 -> (1h, soak) -> 10")

        assertEquals(Duration.ofMinutes(2), phases[0].duration)
        assertEquals(Duration.ofHours(1), phases[1].duration)
    }

    @Test
    fun `fails on invalid alternating structure`() {
        assertFailsWith<IllegalArgumentException> {
            LoadProfileParser.parse("0 -> 100 -> (10s, broken)")
        }
    }

    @Test
    fun `fails on invalid segment descriptor`() {
        assertFailsWith<IllegalArgumentException> {
            LoadProfileParser.parse("0 -> (warmup only) -> 1000")
        }
    }
}
