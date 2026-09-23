package avh.ckc.core.polling

import avh.ckc.core.metrics.OffsetCommitMetadataCompression
import avh.ckc.core.metrics.OffsetCommitMetadataStats
import avh.ckc.core.polling.partition.offset.EncodedOffsetTrackerMetadata
import avh.ckc.core.polling.partition.offset.OffsetTrackerCompression

/** Maps internal encoding measurements to the public metrics contract without retaining payload bytes. */
internal fun EncodedOffsetTrackerMetadata.toCommitMetadataStats(topic: String): OffsetCommitMetadataStats =
    OffsetCommitMetadataStats(
        topic = topic,
        compression = when (payload.compression) {
            OffsetTrackerCompression.NONE -> OffsetCommitMetadataCompression.NONE
            OffsetTrackerCompression.ZSTD -> OffsetCommitMetadataCompression.ZSTD
        },
        candidateSizeBytes = candidateSizeBytes,
        sizeLimitBytes = sizeLimitBytes,
        rawPayloadSizeBytes = payload.rawPayloadSizeBytes,
        encodedPayloadSizeBytes = payload.encodedPayloadSizeBytes
    )
