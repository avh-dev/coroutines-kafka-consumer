package avh.ckc.warmup

import avh.ckc.warmup.protocol.WarmupRemoteOutcome
import avh.ckc.warmup.protocol.WarmupSessionId
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/** Kafka destination selected for one delegated source record. */
internal data class WarmupDestination(val topic: String, val partition: Int = 0) {
    init {
        require(topic.isNotBlank()) { "Warm-up destination topic must not be blank" }
        require(partition >= 0) { "Warm-up destination partition must not be negative" }
    }
}

/** Source and destination coordinates used by internal forwarding observers. */
internal data class WarmupForwardedEvent(
    val sessionId: WarmupSessionId,
    val sourceTopicPartition: TopicPartition,
    val sourceOffset: Long,
    val destinationTopicPartition: TopicPartition,
    val destinationOffset: Long
)

/** Distinguishes mapping-local trackers that belong to one logical donor session. */
internal data class WarmupPendingKey(val sessionId: WarmupSessionId, val laneId: String)

/** Session-aware observation seam for donor forwarding, results, fallback, and pending accounting. */
internal interface WarmupDataPlaneObserver<K, V> {
    fun onForwarded(record: ConsumerRecord<K, V>, event: WarmupForwardedEvent, durationNanos: Long) = Unit
    fun onResult(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome, durationNanos: Long) = Unit
    fun onFallback(record: ConsumerRecord<K, V>, reason: WarmupFallbackReason) = Unit
    fun onPendingChanged(key: WarmupPendingKey, pending: Int) = Unit
}

internal fun <K, V> noOpWarmupDataPlaneObserver(): WarmupDataPlaneObserver<K, V> =
    object : WarmupDataPlaneObserver<K, V> {}
