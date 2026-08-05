package avh.ckc.core.polling.partition

import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.polling.partition.offset.OffsetTrackerSnapshot
import avh.ckc.core.VisibleForTesting
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
) : ConsumerPartitionStats {
    private var offsetTracker: OffsetTracker = OffsetTracker(-1)
    private var initialized = false

    /** Last offset whose Kafka commit completed successfully. */
    internal var lastCommittedOffset: Long = -1
        private set

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
        val committedOffset = initialPosition - 1
        if (!initialized || committedOffset != lastCommittedOffset) {
            offsetTracker = OffsetTracker(committedOffset)
        }
        lastCommittedOffset = committedOffset
        initialized = true
    }

    /**
     * Restores the tracker from committed Kafka offset plus CKC offset metadata.
     *
     * Kafka stores the next offset to consume, while [OffsetTracker] stores the last committed offset.
     * If the in-memory tracker already matches Kafka's committed offset, keep it: workers may have marked
     * additional offsets processed while the rebalance was in progress.
     */
    fun init(committedOffset: Long, snapshot: OffsetTrackerSnapshot) {
        val lastCommittedOffset = committedOffset - 1
        if (!initialized || lastCommittedOffset != this.lastCommittedOffset) {
            offsetTracker = OffsetTracker(initialProcessedOffset = lastCommittedOffset, snapshot = snapshot)
        }
        this.lastCommittedOffset = lastCommittedOffset
        initialized = true
    }

    /**
     * Advances the committable offset if processed offsets allow it.
     */
    fun advanceProcessedOffset() = offsetTracker.advanceProcessedOffset()

    fun advanceAndGetCommitData(): OffsetCommitData? {
        offsetTracker.advanceProcessedOffset()
        val offset = offsetTracker.lastProcessedOffset
        if (offset <= lastCommittedOffset) return null
        return OffsetCommitData(
            offset = offset,
            advancedOffsetsCount = offset - lastCommittedOffset,
            offsetTrackerSnapshot = offsetTracker.snapshot()
        )
    }

    /** Confirms that Kafka successfully committed [offset]. */
    fun markCommitted(offset: Long) {
        require(offset <= offsetTracker.lastProcessedOffset) {
            "Committed offset $offset is ahead of processed offset ${offsetTracker.lastProcessedOffset}"
        }
        if (offset > lastCommittedOffset) {
            lastCommittedOffset = offset
        }
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
