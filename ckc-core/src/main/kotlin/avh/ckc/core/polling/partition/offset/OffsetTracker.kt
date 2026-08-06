package avh.ckc.core.polling.partition.offset

import java.lang.System.arraycopy
import kotlin.math.max

/**
 * Tracks processed offsets and determines the highest offset that can be safely committed.
 *
 * An offset is eligible for commit only if all preceding offsets have been marked as processed.
 * Offsets are stored in a ring buffer of 64-bit words.
 */
internal class OffsetTracker(
    initialProcessedOffset: Long,
    initialCapacity: Int = 128
) {

    /** Smallest capacity compaction may retain, independent from a restored snapshot's current size. */
    private val minimumCapacityBits = ceilPow2(max(128, initialCapacity))

    /** Highest contiguous offset known to be fully processed. */
    var lastProcessedOffset: Long = max(-1, initialProcessedOffset)
        private set

    /**
     * Ring bitset capacity in bits.
     * A minimum of two 64-bit words is required to handle out-of-order offsets.
     */
    private var capacityBits = minimumCapacityBits

    /** Bitset buffer (each Long = 64 bits). */
    private var words: LongArray = LongArray(capacityBits ushr 6)

    /** Mask to find an index in a ring buffer */
    private var wordMask = words.size - 1

    /** Offset corresponding to bitIndex 0. */
    private var headWordOffset: Long = lastProcessedOffset + 1

    /** Initial word in a ring buffer */
    private var headWordIndex = 0

    /**
     * Restores a tracker from a previously serialized snapshot.
     *
     * [initialProcessedOffset] is intentionally supplied by the caller because Kafka stores the committed offset
     * separately from commit metadata. A snapshot is created for the same processed frontier that is committed to
     * Kafka, so the committed offset restores that frontier while the snapshot carries the remaining ring internals.
     * The restored tracker initially keeps the snapshot capacity, but [compact] may later shrink it back toward
     * [initialCapacity] when the restored live range no longer requires the larger ring.
     */
    constructor(
        initialProcessedOffset: Long,
        snapshot: OffsetTrackerSnapshot,
        initialCapacity: Int = 128
    ) : this(initialProcessedOffset, initialCapacity) {
        val snapshotWords = snapshot.words
        require(snapshotWords.size >= 2) { "Offset tracker snapshot must contain at least two words" }
        require(snapshotWords.size.isPowerOfTwo()) { "Offset tracker snapshot word count must be a power of two" }
        require(snapshot.headWordIndex in snapshotWords.indices) { "Offset tracker snapshot head index is out of bounds" }
        require(snapshot.headWordOffset <= this.lastProcessedOffset + 1) {
            "Offset tracker snapshot head is ahead of the first uncommitted offset"
        }
        words = snapshotWords
        capacityBits = words.size shl 6
        wordMask = words.size - 1
        headWordIndex = snapshot.headWordIndex
        headWordOffset = snapshot.headWordOffset
    }

    /**
     * Marks an offset as processed.
     * Commit advancement is handled separately.
     * Thread-safe and designed for a hot path with minimal overhead.
     */
    fun markProcessed(offset: Long) {
        synchronized(this) {
            if (offset <= lastProcessedOffset) return

            val bitIndex = (offset - headWordOffset).toInt()
            if (bitIndex >= capacityBits) {
                extendCapacity(bitIndex)
            }

            val wordIndex = ((bitIndex ushr 6) + headWordIndex) and wordMask

            /*
             * Note: bitIndex may exceed 63, but this is safe.
             * On the JVM, shift operations on Long use only the lower 6 bits of the shift distance,
             * so (1L shl bitIndex) is equivalent to (1L shl (bitIndex and 63)).
             * Adding an explicit mask would be redundant here.
             */
            words[wordIndex] = words[wordIndex] or (1L shl bitIndex)
        }
    }

    /**
     * Returns whether [offset] is already known as processed.
     *
     * Offsets at or below [lastProcessedOffset] are considered processed by definition. Offsets beyond the
     * current ring capacity are not tracked yet and therefore are not considered processed.
     */
    fun isProcessed(offset: Long): Boolean =
        synchronized(this) {
            if (offset <= lastProcessedOffset) return true

            val bitIndex = (offset - headWordOffset).toInt()
            if (bitIndex < 0 || bitIndex >= capacityBits) return false

            val wordIndex = ((bitIndex ushr 6) + headWordIndex) and wordMask
            (words[wordIndex] and (1L shl bitIndex)) != 0L
        }

    /** Advances the processed frontier to the first gap. */
    fun advanceProcessedFrontier() {
        synchronized(this) {
            val mask = wordMask
            var head = headWordIndex
            var offset = headWordOffset

            /*
             * Fast-forward through fully completed words.
             *
             * A word with value -1L means all 64 bits are set (i.e. all offsets in this block are processed).
             * Such words can be safely skipped and reset so that they can be reused when the ring wraps around.
             */
            while (words[head] == -1L) {
                words[head] = 0L
                head = (head + 1) and mask
                offset += 64
            }
            // Persist updated head position
            headWordIndex = head
            headWordOffset = offset

            /*
             * Determine the highest contiguous offset that can be committed.
             *
             * We invert the current word and count trailing zero bits:
             *  - inversion turns the first 0-bit (gap) into the first 1-bit
             *  - countTrailingZeroBits() gives the index of that gap
             *
             * The resulting offset is:
             *   (base offset of this word) + (index of first gap) - 1
             */
            val offsetToCommit = offset - 1L + words[head].inv().countTrailingZeroBits()

            // Commit only if progress has been made.
            if (offsetToCommit > lastProcessedOffset) {
                lastProcessedOffset = offsetToCommit
            }
        }
    }

    val bitCapacity: Int
        get() = synchronized(this) { capacityBits }

    /**
     * Shrinks an oversized ring when its live words fit into at most one quarter of the current storage.
     *
     * Compaction requires at least a fourfold reduction to avoid frequent reallocations.
     * It preserves ring order and never shrinks below the configured initial capacity.
     */
    fun compact() {
        synchronized(this) {
            val targetWordCount = compactedWordCount() ?: return
            words = copyLogicalWords(targetWordCount)
            capacityBits = targetWordCount shl 6
            wordMask = targetWordCount - 1
            headWordIndex = 0
        }
    }

    private fun compactedWordCount(): Int? {
        val minimumWordCount = minimumCapacityBits ushr 6
        if (minimumWordCount > words.size / COMPACTION_RATIO) return null

        var requiredWordCount = minimumWordCount
        for (logicalWordIndex in words.lastIndex downTo 0) {
            val physicalWordIndex = (headWordIndex + logicalWordIndex) and wordMask
            if (words[physicalWordIndex] != 0L) {
                requiredWordCount = max(minimumWordCount, logicalWordIndex + 1)
                break
            }
        }

        val targetWordCount = ceilPow2(requiredWordCount)
        return targetWordCount.takeIf { it <= words.size / COMPACTION_RATIO }
    }

    private fun copyLogicalWords(wordCount: Int): LongArray {
        val copiedWords = LongArray(wordCount)
        val tailWordCount = minOf(wordCount, words.size - headWordIndex)
        arraycopy(words, headWordIndex, copiedWords, 0, tailWordCount)
        val wrappedWordCount = wordCount - tailWordCount
        if (wrappedWordCount > 0) {
            arraycopy(words, 0, copiedWords, tailWordCount, wrappedWordCount)
        }
        return copiedWords
    }

    /**
     * Captures current ring state.
     *
     * The words array is copied while holding the tracker lock; [OffsetTrackerSnapshot] itself does not add
     * defensive copies and is treated as immutable by convention inside the offset package.
     */
    fun snapshot(): OffsetTrackerSnapshot =
        synchronized(this) {
            OffsetTrackerSnapshot(
                headWordOffset = headWordOffset,
                headWordIndex = headWordIndex,
                words = words.copyOf()
            )
        }

    /**
     * Returns the smallest power of two greater than or equal to [v].
     *
     *  Assumes v > 1 (initial words size >= 2 and monotonically increasing),
     *  therefore no validation of (v - 1) is required.
     */
    private fun ceilPow2(v: Int) = 1 shl (32 - (v - 1).countLeadingZeroBits())

    private fun Int.isPowerOfTwo() = this > 0 && (this and (this - 1)) == 0

    /**
     * Extends ring buffer capacity to be able to store bitIndex
     */
    private fun extendCapacity(bitIndex: Int) {
        val newSize = ceilPow2((bitIndex ushr 6) + 1)
        val newWords = LongArray(newSize)

        // The current storage is a ring. To grow it, we "unwrap" the ring into a linear layout
        // so that the logical head word becomes index 0 in the new array.
        arraycopy(words, headWordIndex, newWords, 0, words.size - headWordIndex)
        arraycopy(words, 0, newWords, words.size - headWordIndex, headWordIndex)

        words = newWords
        capacityBits = newSize shl 6
        wordMask = newSize - 1
        headWordIndex = 0
    }

    private companion object {
        const val COMPACTION_RATIO = 4
    }

}
