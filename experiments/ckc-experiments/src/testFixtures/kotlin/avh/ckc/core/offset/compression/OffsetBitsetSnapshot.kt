package avh.ckc.core.offset.compression

data class OffsetBitsetSnapshot(
    val baseOffset: Long,
    val bitCount: Int,
    val words: LongArray
) {
    init {
        require(bitCount >= 0) { "bitCount must be non-negative" }
        require(words.size >= wordsForBits(bitCount)) { "words does not cover bitCount" }
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            other is OffsetBitsetSnapshot &&
            baseOffset == other.baseOffset &&
            bitCount == other.bitCount &&
            normalizedWords().contentEquals(other.normalizedWords())

    override fun hashCode(): Int {
        var result = baseOffset.hashCode()
        result = 31 * result + bitCount
        result = 31 * result + normalizedWords().contentHashCode()
        return result
    }

    fun normalizedWords(): LongArray {
        val usedWords = wordsForBits(bitCount)
        val result = words.copyOf(usedWords)
        val tailBits = bitCount and 63
        if (tailBits != 0 && result.isNotEmpty()) {
            result[result.lastIndex] = result.last() and ((1L shl tailBits) - 1L)
        }
        return result
    }
}

interface OffsetMetadataCodec {
    val name: String
    fun encode(snapshot: OffsetBitsetSnapshot): ByteArray
    fun decode(payload: ByteArray): OffsetBitsetSnapshot
}

fun offsetMetadataCodecs(): List<OffsetMetadataCodec> = listOf(
    RawOffsetMetadataCodec,
    WordRunLengthOffsetMetadataCodec,
    BitRunLengthOffsetMetadataCodec,
    ByteRunLengthOffsetMetadataCodec(),
    ByteRunWithoutLiteralLengthOffsetMetadataCodec,
    ZstdOffsetMetadataCodec,
    Lz4OffsetMetadataCodec
)

fun wordsForBits(bitCount: Int): Int = (bitCount + 63) ushr 6

internal fun byteCountForBits(bitCount: Int): Int = (bitCount + 7) ushr 3
