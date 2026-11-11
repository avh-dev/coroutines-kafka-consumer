package avh.ckc.core.warmup

import avh.ckc.core.metrics.RecordDropReason
import org.apache.kafka.common.TopicPartition

@JvmInline
value class WarmupSessionId(val value: String) {
    init {
        require(value.isNotBlank()) { "Warm-up session id must not be blank" }
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
