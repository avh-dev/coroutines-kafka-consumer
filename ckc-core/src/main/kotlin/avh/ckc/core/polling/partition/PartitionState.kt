package avh.ckc.core.polling.partition

import avh.ckc.core.metrics.ConsumerPartitionStats
import avh.ckc.core.polling.partition.offset.OffsetTracker
import avh.ckc.core.polling.partition.offset.OffsetTrackerSnapshot
import avh.ckc.core.VisibleForTesting
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.common.TopicPartition
import kotlin.time.Duration

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
    val topicPartition: TopicPartition,
    private val maxTrackedOffsets: Int = 1_048_576
) : ConsumerPartitionStats {
    internal val maxTrackedOffsetsPerPartition: Int
        get() = maxTrackedOffsets

    private var offsetTracker: OffsetTracker = OffsetTracker(-1, maxCapacityBits = maxTrackedOffsets)
    private var initialized = false
    private var nextObservedOffset = 0L
    private var latestAcceptedOffset = -1L
    private var acceptedRecordsSinceCommit = 0L
    private var generation = 0L
    private var accepting = false
    private var activePipelines = 0
    private var revokeSignal = CompletableDeferred<Unit>().apply { complete(Unit) }

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
    fun init(initialPosition: Long, forceReset: Boolean = false) {
        val committedOffset = initialPosition - 1
        if (forceReset || !initialized || committedOffset != lastCommittedOffset) {
            offsetTracker = OffsetTracker(committedOffset, maxCapacityBits = maxTrackedOffsets)
            nextObservedOffset = initialPosition
            latestAcceptedOffset = committedOffset
            acceptedRecordsSinceCommit = 0L
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
    fun init(committedOffset: Long, snapshot: OffsetTrackerSnapshot, forceReset: Boolean = false) {
        val lastCommittedOffset = committedOffset - 1
        if (forceReset || !initialized || lastCommittedOffset != this.lastCommittedOffset) {
            offsetTracker = OffsetTracker(
                initialProcessedOffset = lastCommittedOffset,
                snapshot = snapshot,
                maxCapacityBits = maxTrackedOffsets
            )
            nextObservedOffset = committedOffset
            latestAcceptedOffset = lastCommittedOffset
            acceptedRecordsSinceCommit = 0L
        }
        this.lastCommittedOffset = lastCommittedOffset
        initialized = true
    }

    /** Advances the processed frontier and returns the offset backlog not yet confirmed by Kafka. */
    fun advanceAndGetPendingOffsetsCount(): Long {
        offsetTracker.advanceProcessedFrontier()
        return offsetTracker.lastProcessedOffset - lastCommittedOffset
    }

    /** Returns records accepted since the last successful freshness-first commit. */
    fun acceptedRecordsPendingCommit(): Long = acceptedRecordsSinceCommit

    fun advanceAndGetCommitData(forceMetadata: Boolean = false): OffsetCommitData? {
        offsetTracker.advanceProcessedFrontier()
        val offset = offsetTracker.lastProcessedOffset
        if (offset <= lastCommittedOffset && !forceMetadata) return null
        if (offsetTracker.bitCapacity > COMPACTION_CHECK_THRESHOLD_BITS) {
            offsetTracker.compact()
        }
        return OffsetCommitData(
            offset = offset,
            advancedOffsetsCount = (offset - lastCommittedOffset).coerceAtLeast(0),
            offsetTrackerSnapshot = offsetTracker.snapshot()
        )
    }

    /** Returns the latest accepted position without depending on processing completion. */
    fun getAcceptedCommitData(): OffsetCommitData? {
        if (latestAcceptedOffset <= lastCommittedOffset) return null
        return OffsetCommitData(
            offset = latestAcceptedOffset,
            advancedOffsetsCount = acceptedRecordsSinceCommit,
            offsetTrackerSnapshot = null
        )
    }

    /** Confirms that Kafka successfully committed [offset]. */
    fun markCommitted(offset: Long) {
        val maximumCommittableOffset = maxOf(offsetTracker.lastProcessedOffset, latestAcceptedOffset)
        require(offset <= maximumCommittableOffset) {
            "Committed offset $offset is ahead of ready offset $maximumCommittableOffset"
        }
        if (offset > lastCommittedOffset) {
            lastCommittedOffset = offset
        }
        if (offset >= latestAcceptedOffset) {
            acceptedRecordsSinceCommit = 0L
        }
    }

    /**
     * Marks a record offset as fully processed by a worker.
     */
    fun markProcessed(offset: Long) {
        offsetTracker.markProcessed(offset)
    }

    /**
     * Observes a record delivered by Kafka and closes any offset gap before it.
     *
     * Kafka preserves offset order within a partition but may omit offsets removed by compaction, retention,
     * or filtering. The delivered record itself remains pending until a worker calls [markProcessed].
     */
    fun observe(offset: Long): Boolean {
        if (offset < nextObservedOffset) return true
        if (offset > nextObservedOffset) {
            if (offsetTracker.lastProcessedOffset >= nextObservedOffset - 1) {
                offsetTracker.skipContiguousProcessedRange(nextObservedOffset, offset)
            } else {
                if (!offsetTracker.canTrack(offset)) return false
                offsetTracker.markProcessedRange(nextObservedOffset, offset)
            }
        }
        if (!offsetTracker.canTrack(offset)) return false
        nextObservedOffset = offset + 1
        return true
    }

    /** Records a position accepted from Kafka by a freshness-first runtime. */
    fun accept(offset: Long) {
        if (offset <= latestAcceptedOffset) return
        latestAcceptedOffset = offset
        acceptedRecordsSinceCommit++
    }

    fun isProcessed(offset: Long): Boolean =
        offsetTracker.isProcessed(offset)

    /** Opens a new assignment generation. A record from an older generation can never start work. */
    @Synchronized
    fun activate(): PartitionLease {
        generation++
        accepting = true
        revokeSignal = CompletableDeferred()
        return PartitionLease(topicPartition, generation)
    }

    /** Closes admission for this generation before revoke/loss cleanup begins. */
    @Synchronized
    fun revoke(): PartitionLease? {
        if (!accepting) return null
        accepting = false
        revokeSignal.complete(Unit)
        return PartitionLease(topicPartition, generation)
    }

    @Synchronized
    fun currentLease(): PartitionLease? =
        if (accepting) PartitionLease(topicPartition, generation) else null

    /** Atomically validates ownership and registers the whole processing pipeline as in-flight. */
    @Synchronized
    fun tryStart(lease: PartitionLease): Boolean {
        if (!accepting || lease.generation != generation || lease.topicPartition != topicPartition) return false
        activePipelines++
        return true
    }

    /**
     * Waits for retry backoff while allowing revoke to interrupt it. The final synchronized check is
     * the admission point for the next attempt, just as [tryStart] is for the whole pipeline.
     */
    suspend fun awaitRetryPermission(lease: PartitionLease, retryDelay: Duration): Boolean {
        val signal = synchronized(this) {
            if (!accepting || lease.generation != generation || lease.topicPartition != topicPartition) {
                null
            } else {
                revokeSignal
            }
        } ?: return false

        if (retryDelay.isPositive()) {
            val revoked = withTimeoutOrNull(retryDelay) {
                signal.await()
                true
            } == true
            if (revoked) return false
        }

        return synchronized(this) {
            accepting && lease.generation == generation && lease.topicPartition == topicPartition
        }
    }

    @Synchronized
    fun finish(lease: PartitionLease) {
        if (lease.generation == generation && activePipelines > 0) {
            activePipelines--
            if (activePipelines == 0) (this as java.lang.Object).notifyAll()
        }
    }

    @Synchronized
    fun markProcessed(lease: PartitionLease, offset: Long) {
        // Revoke closes only new admission. Completion from a pipeline registered before
        // the close belongs to this generation and must reach the final handoff commit.
        if (lease.generation == generation && lease.topicPartition == topicPartition) {
            offsetTracker.markProcessed(offset)
        }
    }

    /** Poll-thread revoke wait. Workers never need the poll thread to make progress. */
    @Synchronized
    fun awaitPipelines(deadlineNanos: Long): Boolean {
        while (activePipelines > 0) {
            val remaining = deadlineNanos - System.nanoTime()
            if (remaining <= 0L) return false
            val millis = remaining / 1_000_000L
            val nanos = (remaining % 1_000_000L).toInt()
            (this as java.lang.Object).wait(millis, nanos)
        }
        return true
    }

    @VisibleForTesting
    internal fun trackerRefForTest() = offsetTracker

    private companion object {
        const val COMPACTION_CHECK_THRESHOLD_BITS = 8 * 1_024
    }
}

/** Immutable ownership identity captured when a record is admitted from Kafka. */
internal data class PartitionLease(
    val topicPartition: TopicPartition,
    val generation: Long
)
