package avh.ckc.core.offset

import java.util.Base64

/**
 * Converts binary [OffsetTrackerSnapshot] payloads to Kafka offset metadata strings.
 *
 * Kafka exposes committed offset metadata as a String, while [OffsetTrackerSerializer] works with binary
 * payloads. This adapter uses URL-safe Base64 without padding and checks the final String size against
 * Kafka's default `offset.metadata.max.bytes` limit.
 */
internal object OffsetTrackerMetadata {
    const val DEFAULT_MAX_METADATA_BYTES = 4096

    private val encoder = Base64.getUrlEncoder().withoutPadding()
    private val decoder = Base64.getUrlDecoder()

    fun encode(
        snapshot: OffsetTrackerSnapshot,
        maxMetadataBytes: Int = DEFAULT_MAX_METADATA_BYTES
    ): String? {
        val metadata = encoder.encodeToString(OffsetTrackerSerializer.serialize(snapshot))
        return if (metadata.length <= maxMetadataBytes) metadata else null
    }

    fun decode(metadata: String): OffsetTrackerSnapshot =
        OffsetTrackerSerializer.deserialize(decoder.decode(metadata))
}
