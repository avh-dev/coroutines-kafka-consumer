package avh.ckc.core.offset.compression

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

internal const val HEADER_BYTES = Long.SIZE_BYTES + Int.SIZE_BYTES

internal data class Header(val baseOffset: Long, val bitCount: Int)

internal data class SpecialRun(val value: Int, val length: Int)

internal fun writeHeader(buffer: ByteBuffer, baseOffset: Long, bitCount: Int) {
    buffer.putLong(baseOffset)
    buffer.putInt(bitCount)
}

internal fun readHeader(buffer: ByteBuffer): Header = Header(
    baseOffset = buffer.long,
    bitCount = buffer.int
)

internal fun ByteArrayOutputStream.writeHeader(baseOffset: Long, bitCount: Int) {
    writeLongLe(baseOffset)
    writeIntLe(bitCount)
}

internal fun ByteArrayOutputStream.writeLongLe(value: Long) {
    var current = value
    repeat(Long.SIZE_BYTES) {
        write((current and 0xff).toInt())
        current = current ushr 8
    }
}

private fun ByteArrayOutputStream.writeIntLe(value: Int) {
    var current = value
    repeat(Int.SIZE_BYTES) {
        write(current and 0xff)
        current = current ushr 8
    }
}

internal fun ByteArrayOutputStream.writeVarInt(value: Int) {
    require(value >= 0) { "varint value must be non-negative" }
    var current = value
    while (current >= 0x80) {
        write((current and 0x7f) or 0x80)
        current = current ushr 7
    }
    write(current)
}

internal fun ByteArrayOutputStream.writeSpecialRun(value: Int, length: Int) {
    require(value == 0x00 || value == 0xff) { "special run value must be 0x00 or 0xff" }
    require(length >= 0) { "special run length must be non-negative" }

    var current = length
    val firstContinuation = current >= 0x40
    write((if (value == 0xff) 0x80 else 0x00) or (if (firstContinuation) 0x40 else 0x00) or (current and 0x3f))
    current = current ushr 6
    if (firstContinuation) {
        val secondContinuation = current >= 0x80
        write((if (secondContinuation) 0x80 else 0x00) or (current and 0x7f))
        current = current ushr 7
        if (secondContinuation) {
            write(current and 0xff)
        }
    }
}

internal class PayloadReader(private val bytes: ByteArray) {
    private var index = 0

    fun isExhausted(): Boolean = index >= bytes.size

    fun readHeader(): Header = Header(
        baseOffset = readLongLe(),
        bitCount = readIntLe()
    )

    fun readByte(): Int {
        require(index < bytes.size) { "unexpected end of payload" }
        return bytes[index++].toInt() and 0xff
    }

    fun readVarInt(): Int {
        var shift = 0
        var result = 0
        while (shift < 32) {
            val byte = readByte()
            result = result or ((byte and 0x7f) shl shift)
            if ((byte and 0x80) == 0) return result
            shift += 7
        }
        error("varint is too long")
    }

    fun readSpecialRun(): SpecialRun {
        val first = readByte()
        val value = if ((first and 0x80) == 0) 0x00 else 0xff
        var length = first and 0x3f
        if ((first and 0x40) != 0) {
            val second = readByte()
            length = length or ((second and 0x7f) shl 6)
            if ((second and 0x80) != 0) {
                length = length or (readByte() shl 13)
            }
        }
        return SpecialRun(value, length)
    }

    fun readRemainingBytes(): ByteArray {
        val remaining = bytes.copyOfRange(index, bytes.size)
        index = bytes.size
        return remaining
    }

    fun readBytesTo(target: ByteArray, targetOffset: Int, length: Int) {
        require(length >= 0) { "length must be non-negative" }
        require(index + length <= bytes.size) { "unexpected end of payload" }
        bytes.copyInto(target, destinationOffset = targetOffset, startIndex = index, endIndex = index + length)
        index += length
    }

    fun readLongLe(): Long {
        var result = 0L
        repeat(Long.SIZE_BYTES) { byteIndex ->
            result = result or (readByte().toLong() shl (byteIndex * 8))
        }
        return result
    }

    private fun readIntLe(): Int {
        var result = 0
        repeat(Int.SIZE_BYTES) { byteIndex ->
            result = result or (readByte() shl (byteIndex * 8))
        }
        return result
    }
}
