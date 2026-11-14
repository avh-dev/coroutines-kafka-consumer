package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.UUID

private const val MAX_WARMUP_IDENTIFIER_LENGTH = 255

@JvmInline
internal value class WarmupSessionId(val value: String) {
    init {
        require(value.isNotBlank()) { "sessionId must not be blank" }
        require(value.length <= MAX_WARMUP_IDENTIFIER_LENGTH) {
            "sessionId must be at most $MAX_WARMUP_IDENTIFIER_LENGTH characters"
        }
        require(value.none(Char::isISOControl)) { "sessionId must not contain control characters" }
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

internal enum class WarmupFallbackReason {
    SEND_FAILURE,
    RESULT_TIMEOUT,
    SESSION_FAILURE,
    REMOTE_TERMINAL_FAILURE
}

/** Internal observation seam; CORE-78 connects it to the public metrics callback. */
internal interface WarmupDataPlaneObserver<K, V> {
    fun onForwarded(event: WarmupForwardedEvent, durationNanos: Long) = Unit
    fun onResult(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome, durationNanos: Long) = Unit
    fun onFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) = Unit
    fun onPendingChanged(sessionId: WarmupSessionId, pending: Int) = Unit
}

internal fun <K, V> noOpWarmupDataPlaneObserver(): WarmupDataPlaneObserver<K, V> =
    object : WarmupDataPlaneObserver<K, V> {}
