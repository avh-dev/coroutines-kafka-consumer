package avh.ckc.core.offset.compression

import java.io.ByteArrayOutputStream

object WordRunLengthOffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "wordRle"

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val words = snapshot.normalizedWords()
        val out = ByteArrayOutputStream(HEADER_BYTES + words.size)
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)

        var index = 0
        while (index < words.size) {
            val value = words[index]
            var runLength = 1
            while (index + runLength < words.size && words[index + runLength] == value) {
                runLength++
            }
            out.writeVarInt(runLength)
            out.writeLongLe(value)
            index += runLength
        }
        return out.toByteArray()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val reader = PayloadReader(payload)
        val header = reader.readHeader()
        val words = LongArray(wordsForBits(header.bitCount))

        var index = 0
        while (index < words.size) {
            val runLength = reader.readVarInt()
            val value = reader.readLongLe()
            repeat(runLength) {
                require(index < words.size) { "word RLE payload overflows snapshot" }
                words[index++] = value
            }
        }
        return OffsetBitsetSnapshot(header.baseOffset, header.bitCount, words)
    }
}
