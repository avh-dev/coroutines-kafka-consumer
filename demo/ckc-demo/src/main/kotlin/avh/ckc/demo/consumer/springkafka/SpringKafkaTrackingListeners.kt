package avh.ckc.demo.consumer.springkafka

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.DemoConsumerRecordContext
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
@Profile("spring-kafka")
@ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
class SpringKafkaTrackingListeners(
    private val trackingService: SpringKafkaTrackingService
) {
    @KafkaListener(
        id = "spring-kafka-consumer-order-lifecycle",
        topics = ["\${demo.topics.order-events}"],
        containerFactory = "orderLifecycleListenerContainerFactory"
    )
    fun onOrderLifecycle(
        @Header(KafkaHeaders.RECEIVED_KEY, required = false) key: String?,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        event: OrderLifecycleEvent
    ) {
        trackingService.processOrderLifecycle(
            DemoConsumerRecordContext(key, topic, partition, offset, timestamp),
            event
        )
    }

    @KafkaListener(
        id = "spring-kafka-consumer-batch-lifecycle",
        topics = ["\${demo.topics.batch-events}"],
        containerFactory = "batchLifecycleListenerContainerFactory"
    )
    fun onBatchLifecycle(
        @Header(KafkaHeaders.RECEIVED_KEY, required = false) key: String?,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        event: BatchLifecycleEvent
    ) {
        trackingService.processBatchLifecycle(
            DemoConsumerRecordContext(key, topic, partition, offset, timestamp),
            event
        )
    }

    @KafkaListener(
        id = "spring-kafka-consumer-cauldron-telemetry",
        topics = ["\${demo.topics.cauldron-events}"],
        containerFactory = "cauldronTelemetryListenerContainerFactory"
    )
    fun onCauldronTelemetry(
        @Header(KafkaHeaders.RECEIVED_KEY, required = false) key: String?,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        event: CauldronTelemetryEvent
    ) {
        trackingService.processCauldronTelemetry(
            DemoConsumerRecordContext(key, topic, partition, offset, timestamp),
            event
        )
    }
}
