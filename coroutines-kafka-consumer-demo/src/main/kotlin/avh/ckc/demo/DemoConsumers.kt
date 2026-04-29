package avh.ckc.demo

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ConsumerMetrics
import avh.ckc.core.DeliveryStrategy
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object DemoConsumers {
    fun lifecycleConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        handler: suspend (String?, OrderLifecycleEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, OrderLifecycleEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.ORDER_LIFECYCLE)
            deliveryStrategy = DeliveryStrategy.BACKPRESSURE
            workerConcurrency = 2
            this.metrics = metrics
            handle { key, value, _ ->
                if (value != null) {
                    handler(key, value)
                }
            }
        }
    }

    fun telemetryConsumer(
        baseProperties: Map<String, Any>,
        metrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        handler: suspend (String?, CauldronTelemetryEvent) -> Unit
    ): CoroutinesKafkaConsumer<String, CauldronTelemetryEvent> {
        val properties = baseProperties + mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true
        )

        return coroutinesKafkaConsumer(properties) {
            topics(DemoTopics.CAULDRON_TELEMETRY)
            deliveryStrategy = DeliveryStrategy.LOSSY
            workerConcurrency = 4
            workChannelCapacity = 256
            this.metrics = metrics
            handle { key, value, _ ->
                if (value != null) {
                    handler(key, value)
                }
            }
        }
    }
}
