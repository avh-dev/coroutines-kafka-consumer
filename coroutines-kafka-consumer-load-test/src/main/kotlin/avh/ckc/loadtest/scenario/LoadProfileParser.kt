package avh.ckc.loadtest.scenario

import java.time.Duration

object LoadProfileParser {
    private val phasePattern = Regex(
        """^\(\s*(\d+)([smh])\s*(?:,\s*(.+))?\s*\)$""",
        RegexOption.IGNORE_CASE
    )
    private val ratePattern = Regex("""^\d+$""")

    fun parse(profile: String): List<LoadPhase> {
        val tokens = profile.split("->")
            .map(String::trim)
            .filter(String::isNotEmpty)

        require(tokens.size >= 3) {
            "Load profile must contain at least one transition: '$profile'"
        }
        require(tokens.size % 2 == 1) {
            "Load profile must alternate rate -> (duration,label) -> rate: '$profile'"
        }
        require(ratePattern.matches(tokens.first())) {
            "Load profile must start with an integer percentage: '$profile'"
        }

        var currentRate = tokens.first().toInt()
        val phases = mutableListOf<LoadPhase>()

        var index = 1
        while (index < tokens.size) {
            val descriptor = parseDescriptor(tokens[index], profile)
            val nextRateToken = tokens.getOrNull(index + 1)
                ?: error("Missing target rate after '${tokens[index]}' in '$profile'")
            require(ratePattern.matches(nextRateToken)) {
                "Expected integer target percentage after '${tokens[index]}', got '$nextRateToken' in '$profile'"
            }

            val nextRate = nextRateToken.toInt()
            phases += LoadPhase(
                fromRatePercent = currentRate,
                toRatePercent = nextRate,
                duration = descriptor.duration,
                label = descriptor.label
            )
            currentRate = nextRate
            index += 2
        }

        return phases
    }

    private fun parseDescriptor(token: String, profile: String): Descriptor {
        val match = phasePattern.matchEntire(token)
            ?: throw IllegalArgumentException(
                "Invalid phase descriptor '$token' in '$profile'. Expected '(200s, optional label)'"
            )

        val amount = match.groupValues[1].toLong()
        val unit = match.groupValues[2]
        val label = match.groupValues[3].takeIf(String::isNotBlank)?.trim()

        return Descriptor(
            duration = when (unit.lowercase()) {
                "s" -> Duration.ofSeconds(amount)
                "m" -> Duration.ofMinutes(amount)
                "h" -> Duration.ofHours(amount)
                else -> error("Unsupported duration unit '$unit'")
            },
            label = label
        )
    }

    private data class Descriptor(
        val duration: Duration,
        val label: String?
    )
}
