package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition
import java.util.UUID

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
value class WarmupConsumerId(val value: String) {
    init {
        validateWarmupIdentifier("consumerId", value)
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

data class WarmupDestination(val topic: String, val partition: Int) {
    init {
        require(topic.isNotBlank()) { "Warm-up destination topic must not be blank" }
        require(partition >= 0) { "Warm-up destination partition must not be negative" }
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
    data class Discarded(val reason: RecordDropReason) : WarmupRemoteOutcome
}

data class WarmupRecordCompleted(
    val recordId: WarmupRecordId,
    val outcome: WarmupRemoteOutcome
)

data class WarmupForwardedEvent(
    val sessionId: WarmupSessionId,
    val sourceTopicPartition: TopicPartition,
    val sourceOffset: Long,
    val destinationTopicPartition: TopicPartition,
    val destinationOffset: Long
)

fun interface WarmupForwardingObserver {
    fun onForwarded(event: WarmupForwardedEvent)
}

fun interface WarmupCompletionPublisher {
    /** Must enqueue without blocking; transport I/O belongs to the control loop. */
    fun publish(completed: WarmupRecordCompleted)
}

private fun validateWarmupIdentifier(name: String, value: String) {
    require(value.isNotBlank()) { "$name must not be blank" }
    require(value.length <= MAX_WARMUP_IDENTIFIER_LENGTH) {
        "$name must be at most $MAX_WARMUP_IDENTIFIER_LENGTH characters"
    }
    require(value.none(Char::isISOControl)) { "$name must not contain control characters" }
}
