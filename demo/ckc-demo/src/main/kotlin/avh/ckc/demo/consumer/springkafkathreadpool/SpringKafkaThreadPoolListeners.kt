package avh.ckc.demo.consumer.springkafkathreadpool

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
@Profile("spring-kafka-thread-pool", "spring-kafka-virtual-thread-pool")
@ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
class SpringKafkaThreadPoolListeners(
    private val runtime: SpringKafkaThreadPoolRuntime
) {
    @KafkaListener(
        id = "spring-kafka-consumer-order-lifecycle",
        idIsGroup = false,
        topics = ["\${demo.topics.order-events}"],
        containerFactory = "springKafkaThreadPoolOrderListenerContainerFactory"
    )
    fun onOrderLifecycle(records: List<ConsumerRecord<String, OrderLifecycleEvent>>) {
        records.forEach(runtime::enqueueOrder)
    }

    @KafkaListener(
        id = "spring-kafka-consumer-batch-lifecycle",
        idIsGroup = false,
        topics = ["\${demo.topics.batch-events}"],
        containerFactory = "springKafkaThreadPoolBatchListenerContainerFactory"
    )
    fun onBatchLifecycle(records: List<ConsumerRecord<String, BatchLifecycleEvent>>) {
        records.forEach(runtime::enqueueBatch)
    }

    @KafkaListener(
        id = "spring-kafka-consumer-cauldron-telemetry",
        idIsGroup = false,
        topics = ["\${demo.topics.cauldron-events}"],
        containerFactory = "springKafkaThreadPoolTelemetryListenerContainerFactory"
    )
    fun onCauldronTelemetry(records: List<ConsumerRecord<String, CauldronTelemetryEvent>>) {
        records.forEach(runtime::enqueueTelemetry)
    }
}
