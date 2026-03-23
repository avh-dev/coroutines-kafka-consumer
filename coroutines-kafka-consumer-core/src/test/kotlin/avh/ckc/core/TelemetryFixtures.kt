package avh.ckc.core

import java.util.concurrent.CopyOnWriteArrayList

data class RecordProcessedCall(
    val topic: String,
    val partition: Int,
    val recordAgeMillis: Long,
    val durationNanos: Long
)

data class RecordFailedCall(
    val topic: String,
    val partition: Int,
    val recordAgeMillis: Long,
    val error: Throwable,
    val durationNanos: Long
)

data class RetryCall(
    val topic: String,
    val partition: Int,
    val attempt: Int,
    val error: Throwable
)

data class CommitCall(
    val partitionsCount: Int,
    val durationNanos: Long,
    val success: Boolean
)

internal class RecordingTelemetry : ConsumerTelemetry {
    val polls = CopyOnWriteArrayList<Pair<Int, Long>>()
    val processed = CopyOnWriteArrayList<RecordProcessedCall>()
    val failed = CopyOnWriteArrayList<RecordFailedCall>()
    val retries = CopyOnWriteArrayList<RetryCall>()
    val commits = CopyOnWriteArrayList<CommitCall>()
    val consumerFailures = CopyOnWriteArrayList<Throwable>()

    override fun onPoll(recordsCount: Int, durationNanos: Long) {
        polls += recordsCount to durationNanos
    }

    override fun onRecordProcessed(topic: String, partition: Int, recordAgeMillis: Long, durationNanos: Long) {
        processed += RecordProcessedCall(topic, partition, recordAgeMillis, durationNanos)
    }

    override fun onRecordFailed(topic: String, partition: Int, recordAgeMillis: Long, error: Throwable, durationNanos: Long) {
        failed += RecordFailedCall(topic, partition, recordAgeMillis, error, durationNanos)
    }

    override fun onRetry(topic: String, partition: Int, attempt: Int, error: Throwable) {
        retries += RetryCall(topic, partition, attempt, error)
    }

    override fun onCommit(partitionsCount: Int, durationNanos: Long, success: Boolean) {
        commits += CommitCall(partitionsCount, durationNanos, success)
    }

    override fun onConsumerFailure(error: Throwable) {
        consumerFailures += error
    }
}
