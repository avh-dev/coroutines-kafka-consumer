package avh.ckc.warmup.protocol

import java.util.UUID
import org.apache.kafka.common.TopicPartition

const val WARMUP_SESSION_HEADER = "ckc-warmup-session"

private const val MAX_WARMUP_IDENTIFIER_LENGTH = 255

@JvmInline
value class WarmupInstanceId(val value: String) {
    init {
        validateWarmupIdentifier("instanceId", value)
    }

    companion object {
        fun random(): WarmupInstanceId = WarmupInstanceId(UUID.randomUUID().toString())
    }
}

@JvmInline
value class WarmupParticipantId(val value: String) {
    init {
        validateWarmupIdentifier("participantId", value)
    }
}

@JvmInline
value class WarmupSessionId(val value: String) {
    init {
        validateWarmupIdentifier("sessionId", value)
    }

    companion object {
        fun random(): WarmupSessionId = WarmupSessionId(UUID.randomUUID().toString())
    }
}

data class WarmupRecordId(
    val sessionId: WarmupSessionId,
    val topicPartition: TopicPartition,
    val offset: Long
) {
    init {
        require(offset >= 0) { "Warm-up offset must not be negative" }
    }
}

sealed interface WarmupRemoteOutcome {
    data object Succeeded : WarmupRemoteOutcome
    data object TerminalFailure : WarmupRemoteOutcome
    data class Discarded(val reason: WarmupDiscardReason) : WarmupRemoteOutcome
}

data class WarmupRecordCompleted(
    val recordId: WarmupRecordId,
    val outcome: WarmupRemoteOutcome
)

enum class WarmupDiscardReason {
    ALREADY_PROCESSED,
    QUEUE_OVERFLOW,
    REPLACED_BY_NEWER_KEY_RECORD,
    NEW_KEY_QUEUE_FULL,
    STALE_AGE,
    LIFECYCLE_DISCARD
}

private fun validateWarmupIdentifier(name: String, value: String) {
    require(value.isNotBlank()) { "$name must not be blank" }
    require(value.length <= MAX_WARMUP_IDENTIFIER_LENGTH) {
        "$name must be at most $MAX_WARMUP_IDENTIFIER_LENGTH characters"
    }
    require(value.none(Char::isISOControl)) { "$name must not contain control characters" }
}
