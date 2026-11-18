package avh.ckc.core.warmup

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import avh.ckc.core.metrics.RecordDropReason
import avh.ckc.core.metrics.RecordHandoffReason
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

internal fun interface WarmupCompletionPublisher {
    /** Enqueues a terminal recipient result without blocking the processing worker. */
    fun publish(completed: WarmupRecordCompleted)

    /** A shutdown-tail discard may not have passed through recipient admission. */
    fun publishLifecycleDiscard(completed: WarmupRecordCompleted) = publish(completed)

    /** Releases admitted work handed back to Kafka without manufacturing a remote outcome. */
    fun releaseHandedOff(record: ConsumerRecord<*, *>) = Unit
}

/** Keeps user processing callbacks while isolating runtime-owned gauge and Kafka-client bindings. */
internal fun <K, V> warmupRecipientRuntimeMetrics(
    callbacks: ConsumerMetrics<K, V>,
    bindingId: String
): ConsumerMetrics<K, V> {
    val binding = callbacks.runtimeBinding(bindingId)
    if (binding === callbacks) return callbacks
    return object : ConsumerMetrics<K, V> by callbacks {
        override fun bindKafkaClientMetrics(pollLoopId: Int, consumer: Consumer<K, V>) =
            binding.bindKafkaClientMetrics(pollLoopId, consumer)

        override fun unbindKafkaClientMetrics(pollLoopId: Int) =
            binding.unbindKafkaClientMetrics(pollLoopId)

        override fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) =
            binding.bindRuntimeMetrics(stats)

        override fun unbindRuntimeMetrics() = binding.unbindRuntimeMetrics()

        override fun bindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) =
            binding.bindPartitionMetrics(pollLoopId, stats)

        override fun unbindPartitionMetrics(pollLoopId: Int, stats: ConsumerPartitionStats) =
            binding.unbindPartitionMetrics(pollLoopId, stats)
    }
}

/** Publishes recipient results only after the ordinary runtime records a terminal outcome. */
internal class WarmupRecipientOutcomeMetrics<K, V>(
    private val sessionId: WarmupSessionId,
    private val publisher: WarmupCompletionPublisher,
    delegate: ConsumerMetrics<K, V>
) : ConsumerMetrics<K, V> by delegate {
    private val delegate = delegate

    override fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        endToEndLatencyMillis: Long,
        durationNanos: Long,
        processedRemotely: Boolean
    ) {
        delegate.onRecordProcessed(
            key, value, record, endToEndLatencyMillis, durationNanos, processedRemotely
        )
        publish(record, WarmupRemoteOutcome.Succeeded)
    }

    override fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        error: Throwable,
        durationNanos: Long
    ) {
        delegate.onRecordFailed(key, value, record, error, durationNanos)
        publish(record, WarmupRemoteOutcome.TerminalFailure)
    }

    override fun onRecordDropped(record: ConsumerRecord<K, V>, reason: RecordDropReason) {
        delegate.onRecordDropped(record, reason)
        val completed = completed(record, WarmupRemoteOutcome.Discarded(reason))
        if (reason == RecordDropReason.LIFECYCLE_DISCARD) {
            publisher.publishLifecycleDiscard(completed)
        } else {
            publisher.publish(completed)
        }
    }

    override fun onRecordHandedOff(record: ConsumerRecord<K, V>, reason: RecordHandoffReason) {
        delegate.onRecordHandedOff(record, reason)
        publisher.releaseHandedOff(record)
    }

    private fun publish(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome) {
        publisher.publish(completed(record, outcome))
    }

    private fun completed(record: ConsumerRecord<K, V>, outcome: WarmupRemoteOutcome) =
        WarmupRecordCompleted(
            WarmupRecordId(
                sessionId,
                TopicPartition(record.topic(), record.partition()),
                record.offset()
            ),
            outcome
        )
}
