package avh.ckc.core.processing.deserialization

internal data class DeserializedRecord<K, V>(
    val key: K?,
    val value: V?
)
