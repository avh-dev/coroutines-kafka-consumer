package avh.ckc.core.deserialization

import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface RecordDeserializer<K, V> : AutoCloseable {
    suspend fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>): DeserializedRecord<K, V>

    override fun close()
}
