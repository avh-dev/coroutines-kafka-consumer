package avh.ckc.core

import org.apache.kafka.common.TopicPartition

/**
 * Per-partition state used by the backpressure pipeline.
 *
 * Wraps [OffsetTracker] for a single [TopicPartition] and allows resetting
 * offset tracking when the consumer position changes (rebalance / seek).
 *
 * Intentionally minimal: serves as an extension point for future per-partition
 * state without poll-loop/worker call sites changes.
 */
internal class PartitionState(
    val topicPartition: TopicPartition
) {
    private var offsetTracker: OffsetTracker = OffsetTracker(-1)

    /**
     * Initializes (or resets) the tracker if [initialPosition] is not the next offset
     * after the last committed one.
     */
    fun init(initialPosition: Long) {
        if ((initialPosition - offsetTracker.lastCommitedOffset) != 1L) {
            offsetTracker = OffsetTracker(initialPosition - 1)
        }
    }

    /**
     * Advances the committable offset if processed offsets allow it.
     */
    fun advanceCommitOffset() = offsetTracker.advanceCommitOffset()

    /**
     * Marks a record offset as fully processed by a worker.
     */
    fun markProcessed(offset: Long) {
        offsetTracker.markProcessed(offset)
    }

    @VisibleForTesting
    internal fun trackerRefForTest() = offsetTracker
}
