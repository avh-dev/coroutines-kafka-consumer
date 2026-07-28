package avh.ckc.demo.consumer.springkafkathreadpool

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.AuditDropReasons
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.logDropped
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import avh.ckc.demo.service.batch.SyncBatchLifecycleService
import avh.ckc.demo.service.cauldron.SyncCauldronTelemetryService
import avh.ckc.demo.service.order.SyncOrderLifecycleService
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration(proxyBeanMethods = false)
@Profile("spring-kafka-thread-pool")
class SpringKafkaThreadPoolProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun springKafkaThreadPoolRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SyncOrderLifecycleService,
        batchLifecycleService: SyncBatchLifecycleService,
        cauldronTelemetryService: SyncCauldronTelemetryService,
        @Qualifier("springKafkaOrderConsumerMetrics")
        orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("springKafkaBatchConsumerMetrics")
        batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
        @Qualifier("springKafkaConsumerMetrics")
        telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>
    ): SpringKafkaThreadPoolRuntime =
        SpringKafkaThreadPoolRuntime(
            properties = properties,
            orderConsumerMetrics = orderConsumerMetrics,
            batchConsumerMetrics = batchConsumerMetrics,
            telemetryConsumerMetrics = telemetryConsumerMetrics,
            orderHandler = { orderLifecycleService.apply(it) },
            batchHandler = { batchLifecycleService.apply(it) },
            telemetryHandler = { cauldronTelemetryService.recalculate(it) }
        )

    @Bean
    fun springKafkaThreadPoolOrderConsumerFactory(
        properties: DemoApplicationProperties
    ): ConsumerFactory<String, OrderLifecycleEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties, properties.consumers.order) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.orderGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java
            )
        )

    @Bean
    fun springKafkaThreadPoolOrderListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent> =
        batchListenerContainerFactory(consumerFactory, properties.consumers.order, properties).apply {
            configureTimeBasedCommits(properties)
        }

    @Bean
    fun springKafkaThreadPoolBatchConsumerFactory(
        properties: DemoApplicationProperties
    ): ConsumerFactory<String, BatchLifecycleEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties, properties.consumers.batch) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.batchGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BatchLifecycleEventDeserializer::class.java
            )
        )

    @Bean
    fun springKafkaThreadPoolBatchListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, BatchLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, BatchLifecycleEvent> =
        batchListenerContainerFactory(consumerFactory, properties.consumers.batch, properties).apply {
            configureTimeBasedCommits(properties)
        }

    @Bean
    fun springKafkaThreadPoolTelemetryConsumerFactory(
        properties: DemoApplicationProperties
    ): ConsumerFactory<String, CauldronTelemetryEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties, properties.consumers.telemetry) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java
            )
        )

    @Bean
    fun springKafkaThreadPoolTelemetryListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, CauldronTelemetryEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, CauldronTelemetryEvent> =
        batchListenerContainerFactory(consumerFactory, properties.consumers.telemetry, properties)

    private fun <V> batchListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, V>,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, V> =
        ConcurrentKafkaListenerContainerFactory<String, V>().apply {
            runtime.processingMode.requireSupportedBySpringKafkaThreadPool()
            this.consumerFactory = consumerFactory
            setBatchListener(true)
            setConcurrency(runtime.pollLoopConcurrency)
            setCommonErrorHandler(threadPoolAdmissionErrorHandler(properties))
            containerProperties.isStopImmediate = true
            containerProperties.shutdownTimeout = 5_000L
            setAutoStartup(true)
        }

    private fun threadPoolAdmissionErrorHandler(properties: DemoApplicationProperties): DefaultErrorHandler =
        DefaultErrorHandler(
            { record: ConsumerRecord<*, *>, _ -> logDropped(record, properties.audit, AuditDropReasons.ADMISSION_FAILED) },
            FixedBackOff(0L, 0L)
        )

    private fun commonConsumerProperties(
        properties: DemoApplicationProperties,
        runtime: DemoApplicationProperties.ConsumerRuntime
    ): Map<String, Any> {
        runtime.processingMode.requireSupportedBySpringKafkaThreadPool()
        return mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        ) + properties.kafka.consumerProperties()
    }

    private fun DemoApplicationProperties.Kafka.consumerProperties(): Map<String, Any> = mapOf(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG to consumer.fetchMinBytes,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to consumer.fetchMaxWaitMs,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to consumer.maxPollRecords,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to consumer.commitIntervalMs
    )

    private fun ConcurrentKafkaListenerContainerFactory<*, *>.configureTimeBasedCommits(
        properties: DemoApplicationProperties
    ) {
        containerProperties.ackMode = ContainerProperties.AckMode.TIME
        containerProperties.ackTime = properties.kafka.consumer.commitIntervalMs.toLong()
    }
}
