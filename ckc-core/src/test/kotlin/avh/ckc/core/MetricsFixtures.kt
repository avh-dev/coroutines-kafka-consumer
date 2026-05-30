package avh.ckc.core

import avh.ckc.core.metrics.BackpressureAction
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.metrics.ConsumerRuntimeStats
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.CopyOnWriteArrayList

data class RecordProcessedCall<K, V>(
    val key: K?,
    val value: V?,
    val record: ConsumerRecord<K, V>,
    val recordAgeMillis: Long,
    val durationNanos: Long
)

data class RecordFailedCall<K, V>(
    val key: K?,
    val value: V?,
    val record: ConsumerRecord<K, V>,
    val recordAgeMillis: Long,
    val error: Throwable,
    val durationNanos: Long
)

data class RecordDroppedCall<K, V>(
    val record: ConsumerRecord<K, V>
)

data class RetryCall<K, V>(
    val key: K?,
    val value: V?,
    val record: ConsumerRecord<K, V>,
    val attempt: Int,
    val error: Throwable
)

data class CommitCall(
    val partitionsCount: Int,
    val offsetsCount: Long,
    val durationNanos: Long,
    val success: Boolean
)

data class BackpressurePauseResumeCall(
    val action: BackpressureAction,
    val partitionsCount: Int
)

data class PartitionMetricsKey(
    val topic: String,
    val partition: Int
)

internal class RecordingMetrics<K, V> : ConsumerMetrics<K, V> {
    val polls = CopyOnWriteArrayList<Pair<Int, Long>>()
    val processed = CopyOnWriteArrayList<RecordProcessedCall<K, V>>()
    val failed = CopyOnWriteArrayList<RecordFailedCall<K, V>>()
    val dropped = CopyOnWriteArrayList<RecordDroppedCall<K, V>>()
    val retries = CopyOnWriteArrayList<RetryCall<K, V>>()
    val commits = CopyOnWriteArrayList<CommitCall>()
    val backpressurePauseResume = CopyOnWriteArrayList<BackpressurePauseResumeCall>()
    val consumerFailures = CopyOnWriteArrayList<Throwable>()
    val boundRuntimeStats = CopyOnWriteArrayList<ConsumerRuntimeStats>()
    val unbindRuntimeMetricsCalls = CopyOnWriteArrayList<Unit>()
    val boundPartitionStats = CopyOnWriteArrayList<ConsumerPartitionStats>()
    val unboundPartitionMetrics = CopyOnWriteArrayList<PartitionMetricsKey>()

    override fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) {
        boundRuntimeStats += stats
    }

    override fun unbindRuntimeMetrics() {
        unbindRuntimeMetricsCalls += Unit
    }

    override fun bindPartitionMetrics(stats: ConsumerPartitionStats) {
        boundPartitionStats += stats
    }

    override fun unbindPartitionMetrics(topic: String, partition: Int) {
        unboundPartitionMetrics += PartitionMetricsKey(topic, partition)
    }

    override fun onPoll(recordsCount: Int, durationNanos: Long) {
        polls += recordsCount to durationNanos
    }

    override fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        recordAgeMillis: Long,
        durationNanos: Long
    ) {
        processed += RecordProcessedCall(key, value, record, recordAgeMillis, durationNanos)
    }

    override fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<K, V>,
        recordAgeMillis: Long,
        error: Throwable,
        durationNanos: Long
    ) {
        failed += RecordFailedCall(key, value, record, recordAgeMillis, error, durationNanos)
    }

    override fun onRecordDropped(record: ConsumerRecord<K, V>) {
        dropped += RecordDroppedCall(record)
    }

    override fun onRetry(key: K?, value: V?, record: ConsumerRecord<K, V>, attempt: Int, error: Throwable) {
        retries += RetryCall(key, value, record, attempt, error)
    }

    override fun onCommit(partitionsCount: Int, offsetsCount: Long, durationNanos: Long, success: Boolean) {
        commits += CommitCall(partitionsCount, offsetsCount, durationNanos, success)
    }

    override fun onBackpressurePauseResume(action: BackpressureAction, partitionsCount: Int) {
        backpressurePauseResume += BackpressurePauseResumeCall(action, partitionsCount)
    }

    override fun onConsumerFailure(error: Throwable) {
        consumerFailures += error
    }
}
