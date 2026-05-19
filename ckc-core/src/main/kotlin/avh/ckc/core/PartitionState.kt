package avh.ckc.core

import avh.ckc.core.offset.OffsetTracker
import avh.ckc.core.offset.OffsetTrackerSnapshot
import org.apache.kafka.common.TopicPartition

internal data class CommitOffsetProgress(
    val offset: Long,
    val offsetsCount: Long,
    val snapshot: OffsetTrackerSnapshot
)

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
) : ConsumerPartitionStats {
    private var offsetTracker: OffsetTracker = OffsetTracker(-1)

    override val topic: String
        get() = topicPartition.topic()

    override val partition: Int
        get() = topicPartition.partition()

    override val offsetTrackerBitCapacity: Int
        get() = offsetTracker.bitCapacity

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
     * Restores the tracker from committed Kafka offset plus CKC offset metadata.
     *
     * Kafka stores the next offset to consume, while [OffsetTracker] stores the last committed offset.
     */
    fun init(committedOffset: Long, snapshot: OffsetTrackerSnapshot) {
        offsetTracker = OffsetTracker(lastCommitedOffset = committedOffset - 1, snapshot = snapshot)
    }

    /**
     * Advances the committable offset if processed offsets allow it.
     */
    fun advanceCommitOffset() = offsetTracker.advanceCommitOffset()

    fun advanceCommitOffsetProgress(): CommitOffsetProgress? {
        val previousOffset = offsetTracker.lastCommitedOffset
        val offset = offsetTracker.advanceCommitOffset() ?: return null
        return CommitOffsetProgress(
            offset = offset,
            offsetsCount = offset - previousOffset,
            snapshot = offsetTracker.snapshot()
        )
    }

    /**
     * Marks a record offset as fully processed by a worker.
     */
    fun markProcessed(offset: Long) {
        offsetTracker.markProcessed(offset)
    }

    fun isProcessed(offset: Long): Boolean =
        offsetTracker.isProcessed(offset)

    @VisibleForTesting
    internal fun trackerRefForTest() = offsetTracker
}
