package avh.ckc.core.offset.compression

import java.io.ByteArrayOutputStream

object BitRunLengthOffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "bitRle"

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val words = snapshot.normalizedWords()
        val out = ByteArrayOutputStream(HEADER_BYTES + (snapshot.bitCount / 8))
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)
        if (snapshot.bitCount == 0) return out.toByteArray()

        var current = words.bitAt(0)
        out.write(if (current) 1 else 0)
        var runLength = 1
        for (bitIndex in 1 until snapshot.bitCount) {
            val bit = words.bitAt(bitIndex)
            if (bit == current) {
                runLength++
            } else {
                out.writeVarInt(runLength)
                current = bit
                runLength = 1
            }
        }
        out.writeVarInt(runLength)
        return out.toByteArray()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val reader = PayloadReader(payload)
        val header = reader.readHeader()
        val words = LongArray(wordsForBits(header.bitCount))
        if (header.bitCount == 0) return OffsetBitsetSnapshot(header.baseOffset, 0, words)

        var bit = reader.readByte() != 0
        var bitIndex = 0
        while (bitIndex < header.bitCount) {
            val runLength = reader.readVarInt()
            val endExclusive = bitIndex + runLength
            require(endExclusive <= header.bitCount) { "bit RLE payload overflows snapshot" }
            if (bit) {
                words.setBits(bitIndex, endExclusive)
            }
            bit = !bit
            bitIndex = endExclusive
        }
        return OffsetBitsetSnapshot(header.baseOffset, header.bitCount, words)
    }
}
