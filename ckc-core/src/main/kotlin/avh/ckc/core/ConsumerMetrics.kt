package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerMetrics<K, V> {
    fun bindRuntimeMetrics(stats: ConsumerRuntimeStats) = Unit

    fun unbindRuntimeMetrics() = Unit

    fun bindPartitionMetrics(stats: ConsumerPartitionStats) = Unit

    fun unbindPartitionMetrics(topic: String, partition: Int) = Unit

    fun onPoll(recordsCount: Int, durationNanos: Long) = Unit

    fun onRecordProcessed(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        recordAgeMillis: Long,
        durationNanos: Long
    ) = Unit

    fun onRecordFailed(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        recordAgeMillis: Long,
        error: Throwable,
        durationNanos: Long
    ) = Unit

    fun onRetry(
        key: K?,
        value: V?,
        record: ConsumerRecord<ByteArray, ByteArray>,
        attempt: Int,
        error: Throwable
    ) = Unit

    fun onCommit(partitionsCount: Int, offsetsCount: Long, durationNanos: Long, success: Boolean) = Unit

    fun onConsumerFailure(error: Throwable) = Unit

    companion object {
        val NOOP: ConsumerMetrics<Any?, Any?> = object : ConsumerMetrics<Any?, Any?> {}
    }
}
