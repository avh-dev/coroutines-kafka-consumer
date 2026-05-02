package avh.ckc.demo.serialization

import avh.ckc.demo.proto.CauldronTelemetryEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class CauldronTelemetryEventSerializer : Serializer<CauldronTelemetryEvent> {
    override fun serialize(topic: String?, data: CauldronTelemetryEvent?): ByteArray? = data?.toByteArray()
}

class CauldronTelemetryEventDeserializer : Deserializer<CauldronTelemetryEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): CauldronTelemetryEvent? =
        data?.let(CauldronTelemetryEvent::parseFrom)
}
