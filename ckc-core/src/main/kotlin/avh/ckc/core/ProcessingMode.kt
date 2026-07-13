package avh.ckc.core

/**
 * Processing semantics for the Kafka consumer.
 */
enum class ProcessingMode {
    /**
     * At-least-once processing without record ordering guarantees.
     *
     * The poll loop applies backpressure when workers cannot keep up, slowing down polling
     * from Kafka instead of dropping records.
     */
    AT_LEAST_ONCE_NO_ORDERING,

    /**
     * At-least-once processing with ordering preserved for records that have equal deserialized Kafka keys.
     *
     * Records with different keys may be processed concurrently. Records with a null Kafka key share a
     * single ordering lane and are therefore processed sequentially relative to each other.
     */
    AT_LEAST_ONCE_KEY_ORDERING,

    /**
     * At-least-once processing with ordering preserved within each Kafka topic partition.
     *
     * Records from different partitions may be processed concurrently.
     */
    AT_LEAST_ONCE_PARTITION_ORDERING,

    /**
     * Freshness-first processing backed by a bounded queue that drops the oldest buffered records.
     *
     * Dropped records are not processed by this consumer instance. They may be redelivered only
     * if partition ownership changes before Kafka commits offsets past them.
     *
     * This mode intentionally trades reliability for throughput.
     */
    FRESHNESS_FIRST_DROP_OLDEST,

    /**
     * Freshness-first processing that keeps at most one queued record per deserialized Kafka key.
     *
     * If a newer record arrives for a key that is already waiting in the local queue, it replaces the older
     * queued record. If the bounded queue already contains the maximum number of distinct waiting keys, records
     * for new keys are dropped instead of applying backpressure.
     *
     * [CoroutinesKafkaConsumerBuilder.workChannelCapacity] limits queued distinct keys in this mode.
     *
     * Records with a null Kafka key share a single freshness lane.
     *
     * This mode intentionally trades reliability for freshness and throughput.
     */
    FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
}

internal fun ProcessingMode.tracksProcessedOffsets(): Boolean =
    when (this) {
        ProcessingMode.AT_LEAST_ONCE_NO_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING,
        ProcessingMode.AT_LEAST_ONCE_PARTITION_ORDERING -> true
        ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST,
        ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY -> false
    }
