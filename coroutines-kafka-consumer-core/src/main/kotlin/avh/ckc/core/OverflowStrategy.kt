package avh.ckc.core

/**
 * Overflow strategy for the Kafka consumer.
 */
enum class OverflowStrategy {
    /**
     * Backpressure: suspend the Kafka consumer (poll loop) until there is room
     * in the work channel. This slows down polling from Kafka but does not drop messages.
     */
    BACKPRESSURE,

    /**
     * Throttling: commit offsets immediately after each poll and push all records
     * into the work channel configured with DROP_OLDEST.
     *
     * Oldest buffered records may be dropped when the channel is full.
     * Dropped records are NOT considered processed and will never be seen again
     * (because offsets were already committed).
     *
     * This mode intentionally trades reliability for throughput.
     */
    THROTTLING
}