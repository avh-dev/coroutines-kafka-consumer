package avh.ckc.core

/**
 * Delivery strategy for the Kafka consumer.
 */
enum class DeliveryStrategy {
    /**
     * Backpressure: suspend the Kafka consumer (poll loop) until there is room
     * in the work channel. This slows down polling from Kafka but does not drop messages.
     */
    BACKPRESSURE,

    /**
     * Lossy: dispatch records into the work channel configured with DROP_OLDEST
     * and rely on Kafka auto-commit for offset progression.
     *
     * Oldest buffered records may be dropped when the channel is full.
     * Dropped records are NOT considered processed and may be skipped permanently
     * once Kafka advances committed offsets via auto-commit.
     *
     * This mode intentionally trades reliability for throughput.
     */
    LOSSY
}
