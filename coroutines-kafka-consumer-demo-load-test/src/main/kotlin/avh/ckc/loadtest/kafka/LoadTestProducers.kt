package avh.ckc.loadtest.kafka

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.CauldronTelemetryEventSerializer
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import avh.ckc.loadtest.config.LoadTestConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

class LoadTestProducers(
    private val config: LoadTestConfig
) : AutoCloseable {
    private val lifecycleProducer = KafkaProducer<String, OrderLifecycleEvent>(
        producerProperties(OrderLifecycleEventSerializer::class.java)
    )
    private val telemetryProducer = KafkaProducer<String, CauldronTelemetryEvent>(
        producerProperties(CauldronTelemetryEventSerializer::class.java)
    )

    fun sendLifecycle(key: String, event: OrderLifecycleEvent) {
        lifecycleProducer.send(ProducerRecord(config.orderLifecycleTopic, key, event))
    }

    fun sendTelemetry(key: String, event: CauldronTelemetryEvent) {
        telemetryProducer.send(ProducerRecord(config.cauldronTelemetryTopic, key, event))
    }

    fun flush() {
        lifecycleProducer.flush()
        telemetryProducer.flush()
    }

    override fun close() {
        lifecycleProducer.close()
        telemetryProducer.close()
    }

    private fun producerProperties(valueSerializerClass: Class<*>): Map<String, Any> = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        ProducerConfig.LINGER_MS_CONFIG to 20,
        ProducerConfig.BATCH_SIZE_CONFIG to 64 * 1024,
        ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to valueSerializerClass
    )
}
