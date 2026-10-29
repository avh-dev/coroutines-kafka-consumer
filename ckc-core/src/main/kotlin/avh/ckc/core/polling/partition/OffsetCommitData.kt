package avh.ckc.core.polling.partition

import avh.ckc.core.polling.partition.offset.OffsetTrackerSnapshot

/**
 * Immutable data needed to build a Kafka offset commit for a single partition.
 *
 * [PartitionState] creates this object from its contiguous processed frontier and confirms it separately only
 * after Kafka accepts the commit. The Kafka commit offset itself is one greater than [offset], because Kafka stores
 * the next offset to consume.
 */
internal data class OffsetCommitData(
    /** Last contiguous processed offset that can be committed for the partition. */
    val offset: Long,

    /** Number of offset positions advanced since the last successfully committed offset. */
    val advancedOffsetsCount: Long,

    /** Snapshot for at-least-once metadata, or null for freshness-first commits. */
    val offsetTrackerSnapshot: OffsetTrackerSnapshot?
)
