package avh.ckc.warmup.protocol

import java.nio.ByteBuffer
import java.util.UUID
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers

const val WARMUP_SESSION_HEADER = "ckc-warmup-session"
private const val WARMUP_DELEGATION_HEADER = "ckc.warmup.delegation"
private const val DELEGATION_ID_BYTES = 2 * Long.SIZE_BYTES

internal fun Headers.setWarmupDelegationId(id: UUID?) {
    remove(WARMUP_DELEGATION_HEADER)
    if (id != null) {
        val encoded = ByteBuffer.allocate(DELEGATION_ID_BYTES)
            .putLong(id.mostSignificantBits).putLong(id.leastSignificantBits).array()
        add(WARMUP_DELEGATION_HEADER, encoded)
    }
}

internal fun Headers.warmupDelegationId(): UUID? =
    lastHeader(WARMUP_DELEGATION_HEADER)?.value()?.takeIf { it.size == DELEGATION_ID_BYTES }?.let { bytes ->
        ByteBuffer.wrap(bytes).let { UUID(it.long, it.long) }
    }

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
    val outcome: WarmupRemoteOutcome,
    val delegationId: UUID? = null
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
