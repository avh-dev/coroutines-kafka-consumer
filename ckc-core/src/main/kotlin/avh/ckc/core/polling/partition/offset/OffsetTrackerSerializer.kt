package avh.ckc.core.polling.partition.offset

import com.github.luben.zstd.Zstd
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Serializes [OffsetTrackerSnapshot] values for Kafka commit metadata.
 *
 * The format stores a small fixed header followed by the raw ring words. Payloads below
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

    fun serialize(snapshot: OffsetTrackerSnapshot): ByteArray {
        val words = snapshot.words
        val rawWords = words.toLittleEndianBytes()
        val useCompression = rawWords.size >= COMPRESSION_THRESHOLD_BYTES
        if (useCompression) {
            val compressedWords = Zstd.compress(rawWords)
            if (compressedWords.size < rawWords.size) {
                return pack(ZSTD, snapshot, words.size, compressedWords)
            }
        }
        return pack(RAW, snapshot, words.size, rawWords)
    }

    fun deserialize(payload: ByteArray): OffsetTrackerSnapshot {
        require(payload.size >= HEADER_BYTES) { "Offset tracker snapshot payload is too short" }

        val header = ByteBuffer.wrap(payload, 0, HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        val codec = header.get()
        val headWordOffset = header.long
        val headWordIndex = header.int
        val wordCount = header.int
        require(wordCount >= 0) { "Offset tracker snapshot word count must be non-negative" }

        val body = payload.copyOfRange(HEADER_BYTES, payload.size)
        val rawBytes = when (codec) {
            RAW -> body
            ZSTD -> Zstd.decompress(body, wordCount * Long.SIZE_BYTES)
            else -> error("Unsupported offset tracker snapshot codec: $codec")
        }
        require(rawBytes.size == wordCount * Long.SIZE_BYTES) {
            "Offset tracker snapshot decoded to ${rawBytes.size} bytes, expected ${wordCount * Long.SIZE_BYTES}"
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
            .allocate(HEADER_BYTES + payload.size)
            .order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(codec)
        buffer.putLong(snapshot.headWordOffset)
        buffer.putInt(snapshot.headWordIndex)
        buffer.putInt(wordCount)
        buffer.put(payload)
        return buffer.array()
    }

    private fun LongArray.toLittleEndianBytes(): ByteArray {
        val bytes = ByteArray(size * Long.SIZE_BYTES)
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
}
