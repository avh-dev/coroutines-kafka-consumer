package avh.ckc.core.experiments.offset.tracker

import kotlin.math.max

/**
 * High-performance and zero GC pressure offset tracker implementation based on ring LongArray bitset.
 */
class RingBitsetOffsetTracker(
    private var lastCommitedOffset: Long,
    initialCapacity: Int = 128
) {

    /** Offset corresponding to bitIndex 0. */
    private var headWordOffset: Long = lastCommitedOffset + 1

    /** Bitset buffer (each Long = 64 bits). */
    private var words: LongArray = LongArray(max(ceilPow2((initialCapacity + 63) ushr 6), 2))

    /** Number of bits in ring bitset */
    private var bitCapacity = words.size shl 6

    /** Initial word in a ring buffer */
    private var headWordIndex = 0

    /** Mask to find an index in a ring buffer */
    private var wordMask = words.size - 1

    init {
        lastCommitedOffset = max(-1, lastCommitedOffset)
    }

    fun markProcessed(offset: Long) {
        if (offset <= lastCommitedOffset) return

        val bitIndex = (offset - headWordOffset).toInt()
        if (bitIndex >= bitCapacity) {
            extendCapacity(bitIndex)
        }

        val wordIndex = ((bitIndex ushr 6) + headWordIndex) and wordMask

        // Note: bitIndex may exceed 63, but this is safe.
        // On the JVM, shift operations on Long use only the lower 6 bits of the shift distance,
        // so (1L shl bitIndex) is equivalent to (1L shl (bitIndex and 63)).
        // Adding an explicit mask would be redundant here.
        words[wordIndex] = words[wordIndex] or (1L shl bitIndex)
    }

    fun advanceCommitOffset(): Long? {
        val mask = wordMask
        var head = headWordIndex
        var offset = headWordOffset

        // looking for
        while (words[head] == -1L) {
            words[head] = 0L
            head++
            head = head and mask
            offset += 64
        }
        headWordIndex = head
        headWordOffset = offset

        val offsetToCommit = offset - 1L + words[head].inv().countTrailingZeroBits()

        if (offsetToCommit > lastCommitedOffset) {
            lastCommitedOffset = offsetToCommit
            return offsetToCommit
        } else {
            return null
        }
    }

    private fun ceilPow2(v: Int) = 1 shl (32 - (v - 1).countLeadingZeroBits())

    private fun extendCapacity(bitIndex: Int) {
        val newSize = ceilPow2((bitIndex ushr 6) + 1)
        val newWords = LongArray(newSize)
        System.arraycopy(words, headWordIndex, newWords, 0, words.size - headWordIndex)
        System.arraycopy(words, 0, newWords, words.size - headWordIndex, headWordIndex)
        words = newWords
        bitCapacity = newSize shl 6
        wordMask = newSize - 1
        headWordIndex = 0
    }
}