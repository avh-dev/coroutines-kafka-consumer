package avh.ckc.warmup.protocol

import java.nio.ByteBuffer
import java.util.UUID
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers

internal const val WARMUP_SESSION_HEADER = "ckc-warmup-session"
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

/** Protocol identity of one context instance. */
@JvmInline
internal value class WarmupInstanceId(val value: String) {
    init {
        validateWarmupIdentifier("instanceId", value)
    }

    companion object {
        fun random(): WarmupInstanceId = WarmupInstanceId(UUID.randomUUID().toString())
    }
}

/** Protocol identity of a participant within a context. */
@JvmInline
internal value class WarmupParticipantId(val value: String) {
    init {
        validateWarmupIdentifier("participantId", value)
    }
}

/** Correlation ID shared by every lane of one recipient attempt. */
@JvmInline
internal value class WarmupSessionId(val value: String) {
    init {
        validateWarmupIdentifier("sessionId", value)
    }

    companion object {
        fun random(): WarmupSessionId = WarmupSessionId(UUID.randomUUID().toString())
    }
}

/** Identifies a delegated record by session and coordinates on the destination. */
internal data class WarmupRecordId(
    val sessionId: WarmupSessionId,
    val topicPartition: TopicPartition,
    val offset: Long
) {
    init {
        require(offset >= 0) { "Warm-up offset must not be negative" }
    }
}

/** Outcome expressed by the wire protocol without carrying handler exceptions. */
internal sealed interface WarmupRemoteOutcome {
    data object Succeeded : WarmupRemoteOutcome
    data object TerminalFailure : WarmupRemoteOutcome
    data class Discarded(val reason: WarmupDiscardReason) : WarmupRemoteOutcome
}

/** Correlates a terminal outcome with a record and any early-result token. */
internal data class WarmupRecordCompleted(
    val recordId: WarmupRecordId,
    val outcome: WarmupRemoteOutcome,
    val delegationId: UUID? = null
)

/** Wire representation of a discard reported by recipient processing policy. */
internal enum class WarmupDiscardReason {
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
