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
    AT_LEAST_ONCE_UNORDERED,

    /**
     * At-least-once processing with ordering preserved for records that have equal deserialized Kafka keys.
     *
     * Records with different keys may be processed concurrently. Records with a null Kafka key share a
     * single ordering lane and are therefore processed sequentially relative to each other.
     */
    AT_LEAST_ONCE_ORDERED_BY_KEY,

    /**
     * At-least-once processing with ordering preserved within each Kafka topic partition.
     *
     * Records from different partitions may be processed concurrently.
     */
    AT_LEAST_ONCE_ORDERED_BY_PARTITION,

    /**
     * Freshness-first processing backed by a bounded queue that drops the oldest buffered records.
     *
     * Dropped records are not processed by this consumer instance. They may be redelivered only
     * if partition ownership changes before Kafka commits offsets past them.
     *
     * This mode intentionally trades reliability for throughput.
     */
    FRESHNESS_FIRST
}

internal fun ProcessingMode.tracksProcessedOffsets(): Boolean =
    when (this) {
        ProcessingMode.AT_LEAST_ONCE_UNORDERED,
        ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY,
        ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_PARTITION -> true
        ProcessingMode.FRESHNESS_FIRST -> false
    }
