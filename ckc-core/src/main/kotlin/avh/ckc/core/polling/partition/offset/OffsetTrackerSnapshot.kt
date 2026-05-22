package avh.ckc.core.polling.partition.offset

/**
 * Lightweight snapshot of [OffsetTracker] state for commit metadata.
 *
 * The snapshot stores only state Kafka does not already provide:
 * - [headWordOffset], the offset represented by bit 0 of the head word.
 * - [headWordIndex], the current head position inside the ring buffer.
 * - [words], the ring bitset itself.
 *
 * It deliberately does not store the committed offset; restore code receives that value from Kafka.
 *
 * This is an internal immutable-by-convention carrier. [words] is intentionally not defensively copied here:
 * [OffsetTracker.snapshot] already copies the array under the tracker lock, and serializer/restore code treats
 * the snapshot as read-only.
 */
internal class OffsetTrackerSnapshot(
    val headWordOffset: Long,
    val headWordIndex: Int,
    val words: LongArray
)
