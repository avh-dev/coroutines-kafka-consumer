package avh.ckc.core.deserialization

internal data class DeserializedRecord<K, V>(
    val key: K?,
    val value: V?
)
