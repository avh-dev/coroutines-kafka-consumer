package avh.ckc.core

import java.lang.System.arraycopy
import kotlin.math.max

/**
 * Tracks processed offsets and determines the highest offset that can be safely committed.
 *
 * An offset is eligible for commit only if all preceding offsets have been marked as processed.
 * Offsets are stored in a ring buffer of 64-bit words.
 */
internal class OffsetTracker(
    private var lastCommitedOffset: Long,
    initialCapacity: Int = 128
) {

    /**
     * Ring bitset capacity in bits.
     * A minimum of two 64-bit words is required to handle out-of-order offsets.
     */
    private var bitCapacity = ceilPow2( max(128, initialCapacity))

    /** Bitset buffer (each Long = 64 bits). */
    private var words: LongArray = LongArray(bitCapacity ushr 6)

    /** Mask to find an index in a ring buffer */
    private var wordMask = words.size - 1

    /** Offset corresponding to bitIndex 0. */
    private var headWordOffset: Long = lastCommitedOffset + 1

    /** Initial word in a ring buffer */
    private var headWordIndex = 0

    init {
        // the first offset in a new partition is 0, so lastCommitOffset can't be less than -1
        lastCommitedOffset = max(-1, lastCommitedOffset)
    }

    /**
     * Marks an offset as processed.
     * Commit advancement is handled separately.
     * Thread-safe and designed for a hot path with minimal overhead.
     */
    fun markProcessed(offset: Long) {
        synchronized(this) {
            if (offset <= lastCommitedOffset) return

            val bitIndex = (offset - headWordOffset).toInt()
            if (bitIndex >= bitCapacity) {
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
     * Advances and returns the highest contiguous offset eligible for commit,
     * or null if no progress can be made.
     * Thread-safe and designed for a hot path with minimal overhead.
     */
    fun advanceCommitOffset(): Long? {
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
            if (offsetToCommit > lastCommitedOffset) {
                lastCommitedOffset = offsetToCommit
                return offsetToCommit
            } else {
                return null
            }
        }
    }

    /**
     * Returns the smallest power of two greater than or equal to [v].
     *
     *  Assumes v > 1 (initial words size >= 2 and monotonically increasing),
     *  therefore no validation of (v - 1) is required.
     */
    private fun ceilPow2(v: Int) = 1 shl (32 - (v - 1).countLeadingZeroBits())

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
        bitCapacity = newSize shl 6
        wordMask = newSize - 1
        headWordIndex = 0
    }
}
