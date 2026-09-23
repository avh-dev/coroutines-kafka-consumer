package avh.ckc.core.metrics

/** Compression applied to an offset-tracker bitset in Kafka commit metadata. */
enum class OffsetCommitMetadataCompression {
    NONE,
    ZSTD
}

/**
 * Measurements captured while preparing one partition's offset-tracker metadata for a Kafka commit.
 *
 * [candidateSizeBytes] is the complete ASCII metadata string size, including the CKC prefix, envelope, checksum,
 * and Base64 expansion. [rawPayloadSizeBytes] and [encodedPayloadSizeBytes] cover only the variable bitset body so
 * their ratio describes compression without fixed-format overhead. A value not [includedInCommit] exceeded
 * [sizeLimitBytes] and the commit falls back to an offset without CKC metadata.
 */
data class OffsetCommitMetadataStats(
    val topic: String,
    val compression: OffsetCommitMetadataCompression,
    val candidateSizeBytes: Int,
    val sizeLimitBytes: Int,
    val rawPayloadSizeBytes: Int,
    val encodedPayloadSizeBytes: Int
) {
    init {
        require(topic.isNotBlank()) { "Topic must not be blank" }
        require(candidateSizeBytes >= 0) { "Metadata candidate size must not be negative" }
        require(sizeLimitBytes > 0) { "Metadata size limit must be positive" }
        require(rawPayloadSizeBytes >= 0) { "Raw payload size must not be negative" }
        require(encodedPayloadSizeBytes >= 0) { "Encoded payload size must not be negative" }
        require(compression != OffsetCommitMetadataCompression.NONE || encodedPayloadSizeBytes == rawPayloadSizeBytes) {
            "Uncompressed metadata payload sizes must match"
        }
    }

    /** Whether the candidate fits CKC's configured metadata size limit and is included in the commit. */
    val includedInCommit: Boolean
        get() = candidateSizeBytes <= sizeLimitBytes

    /** Fraction of CKC's configured metadata size limit occupied by the encoded candidate. */
    val limitUtilization: Double
        get() = candidateSizeBytes.toDouble() / sizeLimitBytes

    /** Encoded bitset payload size divided by its raw size; lower values mean better compression. */
    val compressionRatio: Double
        get() = if (rawPayloadSizeBytes == 0) 1.0 else encodedPayloadSizeBytes.toDouble() / rawPayloadSizeBytes
}
