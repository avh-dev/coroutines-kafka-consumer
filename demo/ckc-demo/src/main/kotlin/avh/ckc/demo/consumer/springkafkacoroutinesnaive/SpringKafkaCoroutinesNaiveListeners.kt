package avh.ckc.demo.consumer.springkafkacoroutinesnaive

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
@Profile("spring-kafka-coroutines-naive")
@ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
class SpringKafkaCoroutinesNaiveListeners(
    private val runtime: SpringKafkaCoroutinesNaiveRuntime
) {
    @KafkaListener(
        id = "spring-kafka-coroutines-naive-order-lifecycle",
        topics = ["\${demo.topics.order-events}"],
        containerFactory = "springKafkaCoroutinesNaiveOrderListenerContainerFactory"
    )
    fun onOrderLifecycle(records: List<ConsumerRecord<String, OrderLifecycleEvent>>) {
        records.forEach(runtime::enqueueOrder)
    }

    @KafkaListener(
        id = "spring-kafka-coroutines-naive-batch-lifecycle",
        topics = ["\${demo.topics.batch-events}"],
        containerFactory = "springKafkaCoroutinesNaiveBatchListenerContainerFactory"
    )
    fun onBatchLifecycle(records: List<ConsumerRecord<String, BatchLifecycleEvent>>) {
        records.forEach(runtime::enqueueBatch)
    }

    @KafkaListener(
        id = "spring-kafka-coroutines-naive-cauldron-telemetry",
        topics = ["\${demo.topics.cauldron-events}"],
        containerFactory = "springKafkaCoroutinesNaiveTelemetryListenerContainerFactory"
    )
    fun onCauldronTelemetry(records: List<ConsumerRecord<String, CauldronTelemetryEvent>>) {
        records.forEach(runtime::enqueueTelemetry)
    }
}
