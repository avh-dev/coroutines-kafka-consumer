package avh.ckc.core.experiments.offset.tracker

import java.util.Arrays

/**
 * High-performance offset tracker implementation based on a sliding LongArray bitset.
 *
 * Mapping:
 *   lastCommitOffset = last fully committed offset
 *   originOffset = lastCommitOffset + 1 = offset mapped to bitIndex = 0
 *
 * Bit storage:
 *   bitIndex = offset - originOffset
 *   bits[..] is a 64-bit word of bits
 */
class SlidingBitsetOffsetTracker(
    private var lastCommit: Long,
    initialCapacity: Int = 64
) {

    /** Offset corresponding to bitIndex 0. */
    private var originOffset: Long = lastCommit + 1

    /** Backing bitset (each Long = 64 bits). */
    private var bits: LongArray = LongArray((initialCapacity + 63) ushr 6)

    /** Index of the first *not-yet-committed* bit. */
    private var lastCommittedBit: Int = 0

    /** Highest bit index ever set (or -1 if none processed). */
    private var maxBitIndex: Int = -1

    fun markProcessed(offset: Long) {
        if (offset <= lastCommit) return

        val bitIndex = (offset - originOffset).toInt()

        ensureCapacity(bitIndex)

        val wordIndex = bitIndex ushr 6
        bits[wordIndex] = bits[wordIndex] or (1L shl (bitIndex and 63))

        if (bitIndex > maxBitIndex) {
            maxBitIndex = bitIndex
        }
    }

    fun advanceCommitOffset(): Long? {
        if (maxBitIndex < 0) return null

        var bit = lastCommittedBit
        val maxWord = maxBitIndex ushr 6

        var wordIndex = bit ushr 6
        var bitInWord = bit and 63

        while (wordIndex <= maxWord) {
            val word = bits[wordIndex]

            if (word != -1L) {

                // Treat all bits below [bitInWord] as already "filled" (1),
                // so we search for a zero bit only in [bitInWord..63].
                val maskBelow = if (bitInWord == 0) 0L else (1L shl bitInWord) - 1
                val maskedWord = word or maskBelow
                val zeroBits = maskedWord.inv()
                val zeroMask = zeroBits and -zeroBits

                if (zeroMask != 0L) {
                    val firstZeroInWord = java.lang.Long.numberOfTrailingZeros(zeroMask)
                    bit = (wordIndex shl 6) + firstZeroInWord
                    break
                }
            }

            // No zero bits in this word in [bitInWord..63] – move to next word
            wordIndex++
            bitInWord = 0
        }

        // If we scanned all words without finding a zero,
        // commit up to maxBitIndex + 1 (everything we know is 1).
        if (wordIndex > maxWord) {
            bit = maxBitIndex + 1
        }

        if (bit == lastCommittedBit) {
            return null
        }

        lastCommittedBit = bit
        lastCommit = originOffset + lastCommittedBit - 1

        slideWindowIfNeeded()

        return lastCommit
    }

    // ----------------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------------

    private fun ensureCapacity(bitIndex: Int) {
        val requiredWord = bitIndex ushr 6
        if (requiredWord < bits.size) return

        var newSize = bits.size
        while (requiredWord >= newSize) newSize = newSize shl 1

        bits = bits.copyOf(newSize)
    }

    /**
     * Slides the window left by full 64-bit words that are now committed.
     */
    private fun slideWindowIfNeeded() {
        val fullWords = lastCommittedBit ushr 6
        if (fullWords <= 0) return

        if (fullWords >= bits.size) {
            // Everything committed → simply clear entire array
            bits.fill(0L)
            originOffset += fullWords * 64L
            lastCommittedBit -= fullWords shl 6
            maxBitIndex -= fullWords shl 6
            if (maxBitIndex < -1) maxBitIndex = -1
            return
        }

        val remaining = bits.size - fullWords

        // Shift array left
        System.arraycopy(bits, fullWords, bits, 0, remaining)

        // Clear tail
        Arrays.fill(bits, remaining, bits.size, 0L)

        val shiftBits = fullWords shl 6
        originOffset += shiftBits.toLong()
        lastCommittedBit -= shiftBits
        maxBitIndex -= shiftBits
        if (maxBitIndex < -1) maxBitIndex = -1
    }
}