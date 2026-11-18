package avh.ckc.core.warmup

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class WarmupQuotaProfileTest {
    @Test
    fun `parses holds ramps jumps and labels into a normalized numeric profile`() {
        val profile = WarmupQuotaProfileParser.parse(
            "5 -> (5s, initial) -> 5 -> (0s, first step) -> 50 -> " +
                "(30s, middle) -> 50 -> (0s, final step) -> 300"
        )

        assertEquals(5, profile.initialQuota)
        assertEquals(
            listOf(
                WarmupQuotaPhase(5_000, 5),
                WarmupQuotaPhase(0, 50),
                WarmupQuotaPhase(30_000, 50),
                WarmupQuotaPhase(0, 300)
            ),
            profile.phases
        )
        assertEquals(300, profile.maximumQuota)
    }

    @Test
    fun `formatting and labels do not change the normalized profile`() {
        val first = WarmupQuotaProfileParser.parse("5 -> (10s, one label) -> 20")
        val second = WarmupQuotaProfileParser.parse("  5->( 10S , another label )->20  ")

        assertEquals(first, second)
    }

    @Test
    fun `evaluates increasing and decreasing linear phases and holds the final value`() {
        val profile = WarmupQuotaProfileParser.parse("0 -> (10s, up) -> 100 -> (10s, down) -> 20")

        assertEquals(0, profile.quotaAt(0))
        assertEquals(50, profile.quotaAt(5.seconds.inWholeNanoseconds))
        assertEquals(100, profile.quotaAt(10.seconds.inWholeNanoseconds))
        assertEquals(60, profile.quotaAt(15.seconds.inWholeNanoseconds))
        assertEquals(20, profile.quotaAt(20.seconds.inWholeNanoseconds))
        assertEquals(20, profile.quotaAt(200.seconds.inWholeNanoseconds))
    }

    @Test
    fun `crosses multiple instantaneous phases at the same timestamp`() {
        val profile = WarmupQuotaProfileParser.parse("1 -> (0s) -> 5 -> (0s) -> 0 -> (0s) -> 9")

        assertEquals(9, profile.quotaAt(0))
    }

    @Test
    fun `rejects malformed or behaviorally invalid profiles`() {
        listOf(
            "",
            "0",
            "1 -> 2",
            "1 -> (10s)",
            "-1",
            "1 -> (-1s) -> 2",
            "1 -> (10d) -> 2",
            "2147483648"
        ).forEach { profile ->
            assertThrows(IllegalArgumentException::class.java) {
                WarmupQuotaProfileParser.parse(profile)
            }
        }
    }
}
