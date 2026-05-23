package avh.ckc.demo.serialization

import avh.ckc.demo.proto.BatchLifecycleEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class BatchLifecycleEventSerializer : Serializer<BatchLifecycleEvent> {
    override fun serialize(topic: String?, data: BatchLifecycleEvent?): ByteArray? = data?.toByteArray()
}

class BatchLifecycleEventDeserializer : Deserializer<BatchLifecycleEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): BatchLifecycleEvent? =
        data?.let(BatchLifecycleEvent::parseFrom)
}
