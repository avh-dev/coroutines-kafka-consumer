package avh.ckc.core.partition

import avh.ckc.core.offset.OffsetTrackerSnapshot

/**
 * Immutable data needed to build a Kafka offset commit for a single partition.
 *
 * [PartitionState] creates this object only after it advances its internal contiguous commit offset. The Kafka
 * commit offset itself is one greater than [offset], because Kafka stores the next offset to consume.
 */
internal data class OffsetCommitData(
    /** Last contiguous processed offset that can be committed for the partition. */
    val offset: Long,

    /** Number of offset positions advanced since the previous committed offset. */
    val advancedOffsetsCount: Long,

    /** Snapshot of processed-but-not-committed offsets to store in Kafka commit metadata. */
    val offsetTrackerSnapshot: OffsetTrackerSnapshot
)
