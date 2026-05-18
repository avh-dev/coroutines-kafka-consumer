package avh.ckc.core.offset.compression

fun main() {
    val seeds = listOf(1, 2, 3, 42, 99)
    val cases = listOf(
        "commitMetadata" to 16 * 1024 * 8,
        "clustered" to 256,
        "clustered" to 1024,
        "clustered" to 8192,
        "clustered" to 65536,
        "sparse" to 256,
        "sparse" to 1024,
        "sparse" to 8192,
        "sparse" to 65536,
        "dense" to 256,
        "dense" to 1024,
        "dense" to 8192,
        "dense" to 65536,
        "random" to 256,
        "random" to 1024,
        "random" to 8192,
        "random" to 65536
    )
    val codecs = offsetMetadataCodecs()

    println("pattern,bitCount,seed,codec,encodedBytes")
    for ((pattern, bitCount) in cases) {
        val caseSeeds = if (pattern == "commitMetadata") seeds else listOf(42)
        for (seed in caseSeeds) {
            val snapshot = buildReportSnapshot(pattern, bitCount, seed)
            for (codec in codecs) {
                println("$pattern,$bitCount,$seed,${codec.name},${codec.encode(snapshot).size}")
            }
        }
    }
}

private fun buildReportSnapshot(pattern: String, bitCount: Int, seed: Int): OffsetBitsetSnapshot {
    if (pattern == "commitMetadata") {
        require(bitCount == 16 * 1024 * 8) { "commitMetadata uses exactly 16 KiB of bitset bytes" }
        return commitMetadataExperimentSnapshot(seed)
    }

    val words = LongArray(wordsForBits(bitCount))
    val random = kotlin.random.Random(seed)
    for (bitIndex in 0 until bitCount) {
        val processed = when (pattern) {
            "clustered" -> bitIndex in 0 until bitCount / 3 || bitIndex in (bitCount * 2 / 3) until (bitCount * 2 / 3 + bitCount / 16)
            "sparse" -> bitIndex % 97 == 0
            "dense" -> bitIndex % 97 != 0
            "random" -> random.nextInt(100) < 50
            else -> error("Unknown pattern: $pattern")
        }
        if (processed) {
            words[bitIndex ushr 6] = words[bitIndex ushr 6] or (1L shl bitIndex)
        }
    }
    return OffsetBitsetSnapshot(baseOffset = 1_000_000L, bitCount = bitCount, words = words)
}
