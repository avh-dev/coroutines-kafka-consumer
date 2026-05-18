package avh.ckc.core.offset.compression

internal fun LongArray.bitAt(bitIndex: Int): Boolean =
    (this[bitIndex ushr 6] and (1L shl bitIndex)) != 0L

internal fun LongArray.setBits(fromInclusive: Int, toExclusive: Int) {
    for (bitIndex in fromInclusive until toExclusive) {
        this[bitIndex ushr 6] = this[bitIndex ushr 6] or (1L shl bitIndex)
    }
}

internal fun LongArray.toLittleEndianBytes(byteCount: Int): ByteArray {
    val bytes = ByteArray(byteCount)
    for (byteIndex in bytes.indices) {
        bytes[byteIndex] = (this[byteIndex ushr 3] ushr ((byteIndex and 7) * 8)).toByte()
    }
    return bytes
}

internal fun ByteArray.toLittleEndianWords(): LongArray {
    val words = LongArray(wordsForBits(size * 8))
    for (byteIndex in indices) {
        words[byteIndex ushr 3] = words[byteIndex ushr 3] or ((this[byteIndex].toLong() and 0xffL) shl ((byteIndex and 7) * 8))
    }
    return words
}
