package avh.ckc.core.offset.compression

import java.io.ByteArrayOutputStream
import java.util.Arrays

object ByteRunWithoutLiteralLengthOffsetMetadataCodec : OffsetMetadataCodec {
    override val name: String = "byteRleNoLiteralLength"

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val bytes = snapshot.normalizedWords().toLittleEndianBytes(byteCountForBits(snapshot.bitCount))
        val out = ByteArrayOutputStream(HEADER_BYTES + bytes.size)
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)

        var index = 0
        while (index < bytes.size) {
            val value = bytes[index].toInt() and 0xff
            if (value == 0x00 || value == 0xff) {
                val runLength = specialRunLength(bytes, index, value)
                out.write(value)
                out.writeVarInt(runLength)
                index += runLength
            } else {
                out.write(value)
                index++
            }
        }
        return out.toByteArray()
    }

    override fun decode(payload: ByteArray): OffsetBitsetSnapshot {
        val reader = PayloadReader(payload)
        val header = reader.readHeader()
        val bytes = ByteArray(byteCountForBits(header.bitCount))
        var index = 0

        while (!reader.isExhausted()) {
            when (val value = reader.readByte()) {
                0x00, 0xff -> {
                    val runLength = reader.readVarInt()
                    Arrays.fill(bytes, index, index + runLength, value.toByte())
                    index += runLength
                }
                else -> {
                    bytes[index++] = value.toByte()
                }
            }
        }
        require(index == bytes.size) { "byte RLE payload decodes to $index bytes, expected ${bytes.size}" }
        return OffsetBitsetSnapshot(
            baseOffset = header.baseOffset,
            bitCount = header.bitCount,
            words = bytes.toLittleEndianWords()
        )
    }

    private fun specialRunLength(bytes: ByteArray, start: Int, value: Int): Int {
        var index = start + 1
        while (index < bytes.size && (bytes[index].toInt() and 0xff) == value) {
            index++
        }
        return index - start
    }
}
