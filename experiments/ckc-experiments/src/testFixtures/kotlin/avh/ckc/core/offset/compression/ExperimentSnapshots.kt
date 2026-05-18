package avh.ckc.core.offset.compression

import kotlin.random.Random

fun commitMetadataExperimentSnapshot(seed: Int = 42): OffsetBitsetSnapshot {
    val bitCount = 16 * 1024 * 8
    val words = LongArray(wordsForBits(bitCount))
    val random = Random(seed)
    for (bitIndex in 0 until bitCount) {
        val byteIndex = bitIndex ushr 3
        val processed = when {
            byteIndex >= 16 * 1024 - 200 -> random.nextBoolean()
            byteIndex >= 15 * 1024 -> random.nextInt(100) >= 80
            else -> random.nextInt(100) < 98
        }
        if (processed) {
            words[bitIndex ushr 6] = words[bitIndex ushr 6] or (1L shl bitIndex)
        }
    }
    return OffsetBitsetSnapshot(baseOffset = 1_000_000L, bitCount = bitCount, words = words)
}
