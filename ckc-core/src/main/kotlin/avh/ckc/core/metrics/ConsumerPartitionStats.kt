package avh.ckc.core.metrics

/**
 * Read-only view of metrics state for an assigned Kafka partition.
 *
 * Implementations are owned by the consumer runtime. Metrics adapters receive this interface when partition
 * gauges are bound and should read values from it without mutating partition state.
 */
interface ConsumerPartitionStats {
    /** Kafka topic name for the assigned partition. */
    val topic: String

    /** Kafka partition number within [topic]. */
    val partition: Int

    /**
     * Current bit capacity of the partition offset tracker.
     *
     * The value can grow under out-of-order processing and is useful for detecting large commit gaps.
     */
    val offsetTrackerBitCapacity: Int
}
