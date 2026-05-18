package avh.ckc.core.offset.compression

import java.nio.ByteBuffer
import java.nio.ByteOrder

object RawOffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "raw"

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val words = snapshot.normalizedWords()
        val buffer = ByteBuffer
            .allocate(HEADER_BYTES + words.size * Long.SIZE_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
        writeHeader(buffer, snapshot.baseOffset, snapshot.bitCount)
        words.forEach(buffer::putLong)
        return buffer.array()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val buffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN)
        val header = readHeader(buffer)
        val words = LongArray(wordsForBits(header.bitCount))
        for (index in words.indices) {
            words[index] = buffer.long
        }
        return OffsetBitsetSnapshot(header.baseOffset, header.bitCount, words)
    }
}
