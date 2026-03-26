package avh.ckc.demo.config

import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
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
    fun orderLifecycleConsumerFactory(properties: DemoApplicationProperties): ConsumerFactory<String, OrderLifecycleEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.lifecycleGroupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java
            )
        )

    @Bean
    fun orderLifecycleListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderLifecycleEvent>
    ): ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent> =
        ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent>().apply {
            this.consumerFactory = consumerFactory
        }

    @Bean
    fun cauldronTelemetryConsumerFactory(properties: DemoApplicationProperties): ConsumerFactory<String, CauldronTelemetryEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.telemetryGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java
            )
        )

    @Bean
    fun cauldronTelemetryListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, CauldronTelemetryEvent>
    ): ConcurrentKafkaListenerContainerFactory<String, CauldronTelemetryEvent> =
        ConcurrentKafkaListenerContainerFactory<String, CauldronTelemetryEvent>().apply {
            this.consumerFactory = consumerFactory
        }

    private fun commonConsumerProperties(properties: DemoApplicationProperties): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    )
}
