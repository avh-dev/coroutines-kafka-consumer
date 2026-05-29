package avh.ckc.core.metrics

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Metrics callback surface for observing a [avh.ckc.core.CoroutinesKafkaConsumer].
 *
 * Implementations are expected to be non-blocking and safe to call from multiple coroutine contexts:
 * poll loops, worker coroutines, and shutdown paths can invoke callbacks concurrently. All methods have no-op
 * defaults so consumers can implement only the signals they export.
 */
interface ConsumerMetrics<K, V> {
    /**
     * Binds runtime-level gauges for a consumer instance.
     *
     * The supplied [stats] object is a live read-only view. Gauge implementations should keep a reference to it
     * and sample values when their metrics backend scrapes or publishes measurements.
     */
    fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) = Unit

    /**
     * Unbinds runtime-level gauges previously registered for this consumer instance.
     *
     * Called during consumer shutdown so metrics backends can remove gauges that hold runtime object references.
     */
    fun unbindRuntimeMetrics() = Unit

    /**
     * Binds partition-level gauges after a partition is assigned to the consumer.
     *
     * The supplied [stats] object is a live read-only view for the assigned partition.
     */
    fun bindPartitionMetrics(stats: ConsumerPartitionStats) = Unit

    /**
     * Unbinds partition-level gauges when a partition is revoked from the consumer.
     */
    fun unbindPartitionMetrics(topic: String, partition: Int) = Unit

    /**
     * Records a Kafka poll attempt.
     *
     * [durationNanos] measures the poll call duration. [recordsCount] is the number of records returned by Kafka.
     */
    fun onPoll(recordsCount: Int, durationNanos: Long) = Unit

    /**
     * Records a successfully processed record after user handler execution completes.
     *
     * [recordAgeMillis] is measured from Kafka record timestamp to processing start. [durationNanos] covers
     * deserialization, configured handler retries, and successful handler execution.
     */
    fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        recordAgeMillis: Long,
        durationNanos: Long
    ) = Unit

    /**
     * Records a failed record after processing cannot continue.
     *
     * For deserialization failures [key] and [value] can be null because typed values may not exist.
     * [durationNanos] covers the processing attempt until failure is reported.
     */
    fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        recordAgeMillis: Long,
        error: Throwable,
        durationNanos: Long
    ) = Unit

    /**
     * Records a polled record intentionally discarded by the selected processing mode.
     *
     * This callback is not used for shutdown or cancellation cleanup.
     */
    fun onRecordDropped(record: ConsumerRecord<ByteArray, ByteArray>) = Unit

    /**
     * Records a retry of the user handler.
     *
     * [attempt] is one-based: the first retry after the initial failed attempt is reported as `1`.
     */
    fun onRetry(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        attempt: Int,
        error: Throwable
    ) = Unit

    /**
     * Records a manual commit attempt for backpressure consumers.
     *
     * [partitionsCount] is the number of partitions included in the attempt. [offsetsCount] is the total number of
     * offset positions advanced by the commit. [success] indicates whether Kafka accepted the commit.
     */
    fun onCommit(partitionsCount: Int, offsetsCount: Long, durationNanos: Long, success: Boolean) = Unit

    /**
     * Records a Kafka consumer pause or resume event caused by downstream backpressure.
     *
     * [partitionsCount] is the number of assigned partitions passed to KafkaConsumer.pause/resume.
     */
    fun onBackpressurePauseResume(action: BackpressureAction, partitionsCount: Int) = Unit

    /**
     * Records an unrecoverable consumer-level failure.
     */
    fun onConsumerFailure(error: Throwable) = Unit

    companion object {
        /** No-op metrics implementation used when metrics are not configured. */
        val NOOP: ConsumerMetrics<Any?, Any?> = object : ConsumerMetrics<Any?, Any?> {}
    }
}

enum class BackpressureAction {
    PAUSE,
    RESUME
}
