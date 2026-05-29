package avh.ckc.demo.consumer.springkafka

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@Configuration(proxyBeanMethods = false)
@Profile("spring-kafka")
@ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
class SpringKafkaProfileConfiguration {
    @Bean
    fun orderOrderConsumerFactory(properties: DemoApplicationProperties): ConsumerFactory<String, OrderLifecycleEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.orderGroupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java
            )
        )

    @Bean
    fun orderLifecycleListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent> =
        ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent>().apply {
            this.consumerFactory = consumerFactory
            setConcurrency(properties.consumers.order.pollLoopConcurrency)
        }

    @Bean
    fun batchOrderConsumerFactory(properties: DemoApplicationProperties): ConsumerFactory<String, BatchLifecycleEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.batchGroupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BatchLifecycleEventDeserializer::class.java
            )
        )

    @Bean
    fun batchLifecycleListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, BatchLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, BatchLifecycleEvent> =
        ConcurrentKafkaListenerContainerFactory<String, BatchLifecycleEvent>().apply {
            this.consumerFactory = consumerFactory
            setConcurrency(properties.consumers.batch.pollLoopConcurrency)
        }

    @Bean
    fun cauldronTelemetryConsumerFactory(properties: DemoApplicationProperties): ConsumerFactory<String, CauldronTelemetryEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java
            )
        )

    @Bean
    fun cauldronTelemetryListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, CauldronTelemetryEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, CauldronTelemetryEvent> =
        ConcurrentKafkaListenerContainerFactory<String, CauldronTelemetryEvent>().apply {
            this.consumerFactory = consumerFactory
            setConcurrency(properties.consumers.telemetry.pollLoopConcurrency)
        }

    private fun commonConsumerProperties(properties: DemoApplicationProperties): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    ) + properties.kafka.consumerProperties()

    private fun DemoApplicationProperties.Kafka.consumerProperties(): Map<String, Any> = mapOf(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG to consumer.fetchMinBytes,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to consumer.fetchMaxWaitMs,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to consumer.maxPollRecords
    )
}
