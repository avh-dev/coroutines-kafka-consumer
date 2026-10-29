package avh.ckc.core.polling.partition.offset

import com.github.luben.zstd.Zstd
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Serializes [OffsetTrackerSnapshot] values for Kafka commit metadata.
 *
 * The format stores a small fixed header followed by the raw ring words. Payloads at or below
 * [COMPRESSION_THRESHOLD_BYTES] are written raw; larger payloads are compressed with zstd only when that
 * actually reduces the byte count.
 *
 * Serialization is intentionally separate from [OffsetTracker] so the tracker only holds its lock long enough
 * to copy the ring words. Compression can run after the snapshot has been captured.
 */
internal object OffsetTrackerSerializer {
    private const val RAW: Byte = 0
    private const val ZSTD: Byte = 1
    private const val COMPRESSION_THRESHOLD_BYTES = 1024
    private const val HEADER_BYTES = Byte.SIZE_BYTES + Long.SIZE_BYTES + Int.SIZE_BYTES + Int.SIZE_BYTES

    fun serialize(snapshot: OffsetTrackerSnapshot): OffsetTrackerSerialization {
        val words = snapshot.words
        val rawWords = words.toLittleEndianBytes()
        val useCompression = rawWords.size > COMPRESSION_THRESHOLD_BYTES
        if (useCompression) {
            val compressedWords = Zstd.compress(rawWords)
            if (compressedWords.size < rawWords.size) {
                return OffsetTrackerSerialization(
                    bytes = pack(ZSTD, snapshot, words.size, compressedWords),
                    payload = OffsetTrackerPayloadEncoding(
                        compression = OffsetTrackerCompression.ZSTD,
                        rawPayloadSizeBytes = rawWords.size,
                        encodedPayloadSizeBytes = compressedWords.size
                    )
                )
            }
        }
        return OffsetTrackerSerialization(
            bytes = pack(RAW, snapshot, words.size, rawWords),
            payload = OffsetTrackerPayloadEncoding(
                compression = OffsetTrackerCompression.NONE,
                rawPayloadSizeBytes = rawWords.size,
                encodedPayloadSizeBytes = rawWords.size
            )
        )
    }

    fun deserialize(
        payload: ByteArray,
        maxCapacityBits: Int = 1_048_576
    ): OffsetTrackerSnapshot {
        require(payload.size >= HEADER_BYTES) { "Offset tracker snapshot payload is too short" }
        require(maxCapacityBits >= 128 && maxCapacityBits.isPowerOfTwo()) {
            "Offset tracker maximum capacity must be a power of two and >= 128"
        }

        val header = ByteBuffer.wrap(payload, 0, HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        val codec = header.get()
        val headWordOffset = header.long
        val headWordIndex = header.int
        val wordCount = header.int
        require(headWordOffset >= 0L) { "Offset tracker snapshot head offset must be non-negative" }
        require(wordCount >= 2) { "Offset tracker snapshot must contain at least two words" }
        require(wordCount.isPowerOfTwo()) {
            "Offset tracker snapshot word count must be a power of two"
        }
        require(wordCount <= maxCapacityBits / Long.SIZE_BITS) {
            "Offset tracker snapshot exceeds configured maximum capacity"
        }
        require(headWordIndex in 0 until wordCount) {
            "Offset tracker snapshot head index is out of bounds"
        }

        val rawSizeBytes = Math.multiplyExact(wordCount, Long.SIZE_BYTES)
        val encodedSizeBytes = payload.size - HEADER_BYTES

        val rawBytes = when (codec) {
            RAW -> {
                require(encodedSizeBytes == rawSizeBytes) {
                    "Offset tracker snapshot contains $encodedSizeBytes raw bytes, expected $rawSizeBytes"
                }
                payload.copyOfRange(HEADER_BYTES, payload.size)
            }
            ZSTD -> {
                require(encodedSizeBytes > 0) { "Compressed offset tracker snapshot payload is empty" }
                Zstd.decompress(payload.copyOfRange(HEADER_BYTES, payload.size), rawSizeBytes)
            }
            else -> error("Unsupported offset tracker snapshot codec: $codec")
        }
        require(rawBytes.size == rawSizeBytes) {
            "Offset tracker snapshot decoded to ${rawBytes.size} bytes, expected $rawSizeBytes"
        }
        return OffsetTrackerSnapshot(
            headWordOffset = headWordOffset,
            headWordIndex = headWordIndex,
            words = rawBytes.toLittleEndianWords()
        )
    }

    private fun pack(
        codec: Byte,
        snapshot: OffsetTrackerSnapshot,
        wordCount: Int,
        payload: ByteArray
    ): ByteArray {
        val buffer = ByteBuffer
            .allocate(Math.addExact(HEADER_BYTES, payload.size))
            .order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(codec)
        buffer.putLong(snapshot.headWordOffset)
        buffer.putInt(snapshot.headWordIndex)
        buffer.putInt(wordCount)
        buffer.put(payload)
        return buffer.array()
    }

    private fun LongArray.toLittleEndianBytes(): ByteArray {
        val bytes = ByteArray(Math.multiplyExact(size, Long.SIZE_BYTES))
        for (wordIndex in indices) {
            var word = this[wordIndex]
            repeat(Long.SIZE_BYTES) { byteIndex ->
                bytes[wordIndex * Long.SIZE_BYTES + byteIndex] = (word and 0xffL).toByte()
                word = word ushr 8
            }
        }
        return bytes
    }

    private fun ByteArray.toLittleEndianWords(): LongArray {
        val words = LongArray(size / Long.SIZE_BYTES)
        for (wordIndex in words.indices) {
            var word = 0L
            repeat(Long.SIZE_BYTES) { byteIndex ->
                word = word or ((this[wordIndex * Long.SIZE_BYTES + byteIndex].toLong() and 0xffL) shl (byteIndex * 8))
            }
            words[wordIndex] = word
        }
        return words
    }

    private fun Int.isPowerOfTwo() = this > 0 && (this and (this - 1)) == 0
}

internal data class OffsetTrackerSerialization(
    val bytes: ByteArray,
    val payload: OffsetTrackerPayloadEncoding
)

internal data class OffsetTrackerPayloadEncoding(
    val compression: OffsetTrackerCompression,
    val rawPayloadSizeBytes: Int,
    val encodedPayloadSizeBytes: Int
)

internal enum class OffsetTrackerCompression {
    NONE,
    ZSTD
}
