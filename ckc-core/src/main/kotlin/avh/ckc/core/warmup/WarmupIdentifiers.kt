package avh.ckc.core.warmup

import java.util.UUID

private const val MAX_IDENTIFIER_LENGTH = 255

@JvmInline
value class WarmupInstanceId(val value: String) {
    init {
        validateWarmupIdentifier("instanceId", value)
    }

    override fun toString(): String = value

    companion object {
        fun random(): WarmupInstanceId = WarmupInstanceId(UUID.randomUUID().toString())
    }
}

@JvmInline
value class WarmupConsumerId(val value: String) {
    init {
        validateWarmupIdentifier("consumerId", value)
    }

    override fun toString(): String = value
}

@JvmInline
value class WarmupSessionId(val value: String) {
    init {
        validateWarmupIdentifier("sessionId", value)
    }

    override fun toString(): String = value

    companion object {
        fun random(): WarmupSessionId = WarmupSessionId(UUID.randomUUID().toString())
    }
}

private fun validateWarmupIdentifier(name: String, value: String) {
    require(value.isNotBlank()) { "$name must not be blank" }
    require(value.length <= MAX_IDENTIFIER_LENGTH) {
        "$name must be at most $MAX_IDENTIFIER_LENGTH characters"
    }
    require(value.none(Char::isISOControl)) { "$name must not contain control characters" }
}
