package avh.ckc.core.processing.runtime

/** Map identity for a deserialized Kafka record key, with immutable content semantics for [ByteArray]. */
internal sealed interface RecordKeyIdentity {
    data object NullKey : RecordKeyIdentity

    data class Value(val key: Any) : RecordKeyIdentity

    class ByteArrayValue(key: ByteArray) : RecordKeyIdentity {
        private val content = key.copyOf()

        override fun equals(other: Any?): Boolean =
            other is ByteArrayValue && content.contentEquals(other.content)

        override fun hashCode(): Int = content.contentHashCode()
    }

    companion object {
        fun from(key: Any?): RecordKeyIdentity =
            when (key) {
                null -> NullKey
                is ByteArray -> ByteArrayValue(key)
                else -> Value(key)
            }
    }
}
