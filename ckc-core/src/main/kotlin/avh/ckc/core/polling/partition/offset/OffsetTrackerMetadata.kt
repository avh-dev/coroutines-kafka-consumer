package avh.ckc.core.polling.partition.offset

import org.apache.kafka.common.TopicPartition
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.Base64
import java.util.zip.CRC32C

/** Context that a persisted offset-tracker snapshot is bound to. */
internal data class OffsetTrackerMetadataContext(
    val groupId: String,
    val topicPartition: TopicPartition,
    val committedOffset: Long
)

/**
 * Converts versioned [OffsetTrackerSnapshot] envelopes to Kafka offset metadata strings.
 *
 * The `ckc:v2:` prefix identifies CKC-owned metadata and its format version. The binary envelope binds the
 * snapshot to the consumer group, topic partition, and Kafka committed offset. CRC32C detects accidental
 * corruption before the tracker snapshot is restored.
 *
 * Unprefixed metadata belongs to the legacy format or another client and is deliberately rejected. Callers fall
 * back to Kafka's authoritative committed offset, which can cause safe duplicate processing but cannot skip a
 * requested replay range.
 */
internal object OffsetTrackerMetadata {
    const val DEFAULT_MAX_METADATA_BYTES = 4096
    const val PREFIX = "ckc:v2:"

    private const val CONTEXT_FINGERPRINT_BYTES = 16
    private const val ENVELOPE_HEADER_BYTES = Long.SIZE_BYTES + CONTEXT_FINGERPRINT_BYTES + Int.SIZE_BYTES
    private const val CHECKSUM_BYTES = Int.SIZE_BYTES

    private val encoder = Base64.getUrlEncoder().withoutPadding()
    private val decoder = Base64.getUrlDecoder()

    fun encode(
        snapshot: OffsetTrackerSnapshot,
        context: OffsetTrackerMetadataContext,
        maxMetadataBytes: Int = DEFAULT_MAX_METADATA_BYTES
    ): EncodedOffsetTrackerMetadata {
        require(maxMetadataBytes > 0) { "Maximum metadata size must be positive" }
        val serialization = OffsetTrackerSerializer.serialize(snapshot)
        val serializedSnapshot = serialization.bytes
        val envelope = ByteBuffer
            .allocate(ENVELOPE_HEADER_BYTES + serializedSnapshot.size + CHECKSUM_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(context.committedOffset)
            .put(contextFingerprint(context))
            .putInt(serializedSnapshot.size)
            .put(serializedSnapshot)
            .array()

        val checksumOffset = envelope.size - CHECKSUM_BYTES
        ByteBuffer.wrap(envelope).order(ByteOrder.LITTLE_ENDIAN)
            .putInt(checksumOffset, crc32c(envelope, checksumOffset).toInt())

        val metadata = PREFIX + encoder.encodeToString(envelope)
        return EncodedOffsetTrackerMetadata(
            metadata = metadata.takeIf { it.length <= maxMetadataBytes },
            candidateSizeBytes = metadata.length,
            sizeLimitBytes = maxMetadataBytes,
            payload = serialization.payload
        )
    }

    fun decode(metadata: String, context: OffsetTrackerMetadataContext): OffsetTrackerSnapshot {
        require(metadata.startsWith(PREFIX)) { "Offset metadata is not in the supported CKC v2 format" }
        val envelope = decoder.decode(metadata.substring(PREFIX.length))
        require(envelope.size >= ENVELOPE_HEADER_BYTES + CHECKSUM_BYTES) {
            "CKC offset metadata envelope is too short"
        }

        val checksumOffset = envelope.size - CHECKSUM_BYTES
        val buffer = ByteBuffer.wrap(envelope).order(ByteOrder.LITTLE_ENDIAN)
        val committedOffset = buffer.long
        val fingerprint = ByteArray(CONTEXT_FINGERPRINT_BYTES).also(buffer::get)
        val snapshotLength = buffer.int
        require(snapshotLength >= 0 && snapshotLength == checksumOffset - ENVELOPE_HEADER_BYTES) {
            "CKC offset metadata snapshot length is invalid"
        }

        val expectedChecksum = buffer.getInt(checksumOffset).toLong() and 0xffff_ffffL
        require(expectedChecksum == crc32c(envelope, checksumOffset)) {
            "CKC offset metadata checksum does not match"
        }
        require(fingerprint.contentEquals(contextFingerprint(context))) {
            "CKC offset metadata belongs to another consumer group or topic partition"
        }
        require(committedOffset == context.committedOffset) {
            "CKC offset metadata was created for committed offset $committedOffset, " +
                    "but Kafka reports ${context.committedOffset}"
        }

        val serializedSnapshot = envelope.copyOfRange(ENVELOPE_HEADER_BYTES, checksumOffset)
        return OffsetTrackerSerializer.deserialize(serializedSnapshot)
    }

    private fun contextFingerprint(context: OffsetTrackerMetadataContext): ByteArray {
        val groupId = context.groupId.toByteArray(Charsets.UTF_8)
        val topic = context.topicPartition.topic().toByteArray(Charsets.UTF_8)
        val identity = ByteBuffer
            .allocate(Int.SIZE_BYTES + groupId.size + Int.SIZE_BYTES + topic.size + Int.SIZE_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(groupId.size)
            .put(groupId)
            .putInt(topic.size)
            .put(topic)
            .putInt(context.topicPartition.partition())
            .array()
        return MessageDigest.getInstance("SHA-256").digest(identity).copyOf(CONTEXT_FINGERPRINT_BYTES)
    }

    private fun crc32c(bytes: ByteArray, length: Int): Long =
        CRC32C().apply { update(bytes, 0, length) }.value
}

internal data class EncodedOffsetTrackerMetadata(
    val metadata: String?,
    val candidateSizeBytes: Int,
    val sizeLimitBytes: Int,
    val payload: OffsetTrackerPayloadEncoding
)
