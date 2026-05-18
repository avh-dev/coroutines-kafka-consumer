package avh.ckc.core.offset.compression

import java.io.ByteArrayOutputStream
import java.util.Arrays

class ByteRunLengthOffsetMetadataCodec(
    private val minRunLength: Int = 3
) : OffsetMetadataCodec {
    override val name: String = "byteRle"

    init {
        require(minRunLength >= 2) { "minRunLength must be at least 2" }
    }

    override fun encode(snapshot: OffsetBitsetSnapshot): ByteArray {
        val bytes = snapshot.normalizedWords().toLittleEndianBytes(byteCountForBits(snapshot.bitCount))
        val out = ByteArrayOutputStream(HEADER_BYTES + bytes.size)
        out.writeHeader(snapshot.baseOffset, snapshot.bitCount)

        var index = 0
        while (index < bytes.size) {
            val literalStart = index
            while (index < bytes.size && specialRunLength(bytes, index) < minRunLength) {
                index++
            }
            out.writeVarInt(index - literalStart)
            out.write(bytes, literalStart, index - literalStart)

            if (index < bytes.size) {
                val value = bytes[index].toInt() and 0xff
                val runLength = specialRunLength(bytes, index)
                out.writeSpecialRun(value, runLength)
                index += runLength
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
            val literalLength = reader.readVarInt()
            reader.readBytesTo(bytes, index, literalLength)
            index += literalLength

            if (!reader.isExhausted()) {
                val run = reader.readSpecialRun()
                Arrays.fill(bytes, index, index + run.length, run.value.toByte())
                index += run.length
            }
        }
        require(index == bytes.size) { "byte RLE payload decodes to $index bytes, expected ${bytes.size}" }
        return OffsetBitsetSnapshot(
            baseOffset = header.baseOffset,
            bitCount = header.bitCount,
            words = bytes.toLittleEndianWords()
        )
    }

    private fun specialRunLength(bytes: ByteArray, start: Int): Int {
        val value = bytes[start].toInt() and 0xff
        if (value != 0x00 && value != 0xff) return 0

        var index = start + 1
        while (index < bytes.size && (bytes[index].toInt() and 0xff) == value) {
            index++
        }
        return index - start
    }
}
