package avh.ckc.demo.serialization

import avh.ckc.demo.proto.OrderLifecycleEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class OrderLifecycleEventSerializer : Serializer<OrderLifecycleEvent> {
    override fun serialize(topic: String?, data: OrderLifecycleEvent?): ByteArray? = data?.toByteArray()
}

class OrderLifecycleEventDeserializer : Deserializer<OrderLifecycleEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): OrderLifecycleEvent? =
        data?.let(OrderLifecycleEvent::parseFrom)
}
