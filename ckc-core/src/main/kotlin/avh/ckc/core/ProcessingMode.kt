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
     * Freshness-first processing backed by a bounded queue that drops the oldest buffered records.
     *
     * Dropped records are not processed by this consumer instance. They may be redelivered only
     * if partition ownership changes before Kafka commits offsets past them.
     *
     * This mode intentionally trades reliability for throughput.
     */
    FRESHNESS_FIRST
}
