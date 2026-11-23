package avh.ckc.warmup.protocol

/** One normalized transition in a mapping quota profile. Labels are intentionally discarded. */
data class WarmupQuotaPhase(
    val durationMillis: Long,
    val targetQuota: Int
) {
    init {
        require(durationMillis >= 0) { "Warm-up quota phase duration must not be negative" }
        require(durationMillis <= Long.MAX_VALUE / NANOS_PER_MILLISECOND) {
            "Warm-up quota phase duration is too large"
        }
        require(targetQuota >= 0) { "Warm-up quota must not be negative" }
    }
}

/** Immutable numeric form used by quota evaluation, fingerprints, and the control protocol. */
data class WarmupQuotaProfile(
    val initialQuota: Int,
    val phases: List<WarmupQuotaPhase>
) {
    val maximumQuota: Int

    init {
        require(initialQuota >= 0) { "Warm-up initial quota must not be negative" }
        maximumQuota = sequenceOf(initialQuota)
            .plus(phases.asSequence().map(WarmupQuotaPhase::targetQuota))
            .max()
        require(maximumQuota > 0) { "Warm-up quota profile must contain a positive quota" }
    }

    fun quotaAt(elapsedNanos: Long): Int {
        var remaining = elapsedNanos.coerceAtLeast(0)
        var current = initialQuota
        phases.forEach { phase ->
            val durationNanos = phase.durationMillis * NANOS_PER_MILLISECOND
            if (durationNanos == 0L) {
                current = phase.targetQuota
            } else if (remaining < durationNanos) {
                val progress = remaining.toDouble() / durationNanos.toDouble()
                // Truncation keeps an integer transition on the starting side of its exact value.
                return current + ((phase.targetQuota - current) * progress).toInt()
            } else {
                remaining -= durationNanos
                current = phase.targetQuota
            }
        }
        return current
    }

    companion object {
        fun constant(quota: Int): WarmupQuotaProfile = WarmupQuotaProfile(quota, emptyList())
    }
}

/** Parser for generator-style `quota -> (duration, label) -> quota` profiles. */
internal object WarmupQuotaProfileParser {
    private val quotaPattern = Regex("""^\d+$""")
    private val phasePattern = Regex(
        """^\(\s*(\d+)\s*(ms|s|m|h)\s*(?:,\s*([^()]*))?\s*\)$""",
        RegexOption.IGNORE_CASE
    )

    fun parse(profile: String): WarmupQuotaProfile {
        val tokens = profile.split("->").map(String::trim)
        require(tokens.isNotEmpty() && tokens.none(String::isEmpty)) {
            "Warm-up quota profile contains an empty element: '$profile'"
        }
        require(tokens.size % 2 == 1) {
            "Warm-up quota profile must alternate quota -> (duration, label) -> quota: '$profile'"
        }

        val initialQuota = parseQuota(tokens.first(), profile)
        val phases = buildList {
            var index = 1
            while (index < tokens.size) {
                val durationMillis = parseDurationMillis(tokens[index], profile)
                add(WarmupQuotaPhase(durationMillis, parseQuota(tokens[index + 1], profile)))
                index += 2
            }
        }
        return WarmupQuotaProfile(initialQuota, phases)
    }

    private fun parseQuota(token: String, profile: String): Int {
        require(quotaPattern.matches(token)) {
            "Expected a non-negative integer quota, got '$token' in '$profile'"
        }
        return requireNotNull(token.toIntOrNull()) {
            "Warm-up quota exceeds the integer range in '$profile'"
        }
    }

    private fun parseDurationMillis(token: String, profile: String): Long {
        val match = phasePattern.matchEntire(token)
            ?: throw IllegalArgumentException(
                "Invalid warm-up quota phase '$token' in '$profile'. Expected '(30s, optional label)'"
            )
        val amount = requireNotNull(match.groupValues[1].toLongOrNull()) {
            "Warm-up quota phase duration exceeds the supported range in '$profile'"
        }
        val multiplier = when (match.groupValues[2].lowercase()) {
            "ms" -> 1L
            "s" -> 1_000L
            "m" -> 60_000L
            "h" -> 3_600_000L
            else -> error("Unsupported warm-up quota duration unit")
        }
        return try {
            Math.multiplyExact(amount, multiplier)
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException(
                "Warm-up quota phase duration exceeds the supported range in '$profile'"
            )
        }
    }
}

private const val NANOS_PER_MILLISECOND = 1_000_000L
