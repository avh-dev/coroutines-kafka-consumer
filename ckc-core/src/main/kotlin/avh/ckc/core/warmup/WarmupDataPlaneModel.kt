package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.WarmupFallbackReason
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.UUID

private const val MAX_WARMUP_IDENTIFIER_LENGTH = 255

@JvmInline
internal value class WarmupInstanceId(val value: String) {
    init {
        validateWarmupIdentifier("instanceId", value)
    }

    companion object {
        fun random(): WarmupInstanceId = WarmupInstanceId(UUID.randomUUID().toString())
    }
}

@JvmInline
internal value class WarmupParticipantId(val value: String) {
    init {
        validateWarmupIdentifier("participantId", value)
    }
}

@JvmInline
internal value class WarmupSessionId(val value: String) {
    init {
        validateWarmupIdentifier("sessionId", value)
    }

    companion object {
        fun random(): WarmupSessionId = WarmupSessionId(UUID.randomUUID().toString())
    }
}

internal data class WarmupDestination(val topic: String, val partition: Int = 0) {
    init {
        require(topic.isNotBlank()) { "Warm-up destination topic must not be blank" }
        require(partition >= 0) { "Warm-up destination partition must not be negative" }
    }
}

internal data class WarmupRecordId(
    val sessionId: WarmupSessionId,
    val topicPartition: TopicPartition,
    val offset: Long
) {
    init {
        require(offset >= 0) { "Warm-up offset must not be negative" }
    }
}

internal sealed interface WarmupRemoteOutcome {
    data object Succeeded : WarmupRemoteOutcome
    data object TerminalFailure : WarmupRemoteOutcome
    data class Discarded(val reason: RecordDropReason) : WarmupRemoteOutcome
}

internal data class WarmupRecordCompleted(
    val recordId: WarmupRecordId,
    val outcome: WarmupRemoteOutcome
)

internal data class WarmupForwardedEvent(
    val sessionId: WarmupSessionId,
    val sourceTopicPartition: TopicPartition,
    val sourceOffset: Long,
    val destinationTopicPartition: TopicPartition,
    val destinationOffset: Long
)

/** Session-aware observation seam for donor forwarding, results, fallback, and pending accounting. */
internal interface WarmupDataPlaneObserver<K, V> {
    fun onForwarded(record: ConsumerRecord<K, V>, event: WarmupForwardedEvent, durationNanos: Long) = Unit
    fun onResult(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome, durationNanos: Long) = Unit
    fun onFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) = Unit
    fun onPendingChanged(sessionId: WarmupSessionId, pending: Int) = Unit
}

internal fun <K, V> noOpWarmupDataPlaneObserver(): WarmupDataPlaneObserver<K, V> =
    object : WarmupDataPlaneObserver<K, V> {}

private fun validateWarmupIdentifier(name: String, value: String) {
    require(value.isNotBlank()) { "$name must not be blank" }
    require(value.length <= MAX_WARMUP_IDENTIFIER_LENGTH) {
        "$name must be at most $MAX_WARMUP_IDENTIFIER_LENGTH characters"
    }
    require(value.none(Char::isISOControl)) { "$name must not contain control characters" }
}
