package avh.ckc.demo.consumer.springkafkacoroutinesnaive

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
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
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
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Configuration(proxyBeanMethods = false)
@Profile("spring-kafka-coroutines-naive")
class SpringKafkaCoroutinesNaiveProfileConfiguration {
    @Bean(destroyMethod = "close")
    fun springKafkaCoroutinesNaiveWorkerDispatcher(
        properties: DemoApplicationProperties
    ): ExecutorCoroutineDispatcher {
        val threads = properties.consumers.workerDispatcherThreads
        require(threads > 0) {
            "demo.consumers.worker-dispatcher-threads must be > 0 for spring-kafka-coroutines-naive"
        }
        val threadNumber = AtomicInteger()
        return Executors.newFixedThreadPool(threads) { runnable ->
            Thread(runnable, "spring-kafka-coroutines-naive-worker-${threadNumber.incrementAndGet()}").apply {
                isDaemon = true
            }
        }.asCoroutineDispatcher()
    }

    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun springKafkaCoroutinesNaiveRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SuspendOrderLifecycleService,
        batchLifecycleService: SuspendBatchLifecycleService,
        cauldronTelemetryService: SuspendCauldronTelemetryService,
        @Qualifier("springKafkaCoroutinesNaiveConsumerMetrics")
        telemetryConsumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("springKafkaCoroutinesNaiveOrderConsumerMetrics")
        orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("springKafkaCoroutinesNaiveBatchConsumerMetrics")
        batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
        @Qualifier("springKafkaCoroutinesNaiveWorkerDispatcher")
        workerDispatcher: ExecutorCoroutineDispatcher
    ): SpringKafkaCoroutinesNaiveRuntime =
        SpringKafkaCoroutinesNaiveRuntime(
            properties = properties,
            orderConsumerMetrics = orderConsumerMetrics,
            batchConsumerMetrics = batchConsumerMetrics,
            telemetryConsumerMetrics = telemetryConsumerMetrics,
            workerDispatcher = workerDispatcher,
            orderHandler = { orderLifecycleService.apply(it) },
            batchHandler = { batchLifecycleService.apply(it) },
            telemetryHandler = { cauldronTelemetryService.recalculate(it) }
        )

    @Bean
    fun springKafkaCoroutinesNaiveOrderConsumerFactory(
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
    fun springKafkaCoroutinesNaiveOrderListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, OrderLifecycleEvent> =
        batchListenerContainerFactory(consumerFactory, properties.consumers.order, properties)

    @Bean
    fun springKafkaCoroutinesNaiveBatchConsumerFactory(
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
    fun springKafkaCoroutinesNaiveBatchListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, BatchLifecycleEvent>,
        properties: DemoApplicationProperties
    ): ConcurrentKafkaListenerContainerFactory<String, BatchLifecycleEvent> =
        batchListenerContainerFactory(consumerFactory, properties.consumers.batch, properties)

    @Bean
    fun springKafkaCoroutinesNaiveTelemetryConsumerFactory(
        properties: DemoApplicationProperties
    ): ConsumerFactory<String, CauldronTelemetryEvent> =
        DefaultKafkaConsumerFactory(
            commonConsumerProperties(properties, properties.consumers.telemetry) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java
            )
        )

    @Bean
    fun springKafkaCoroutinesNaiveTelemetryListenerContainerFactory(
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
            runtime.processingMode.requireSupportedBySpringKafkaCoroutinesNaive()
            this.consumerFactory = consumerFactory
            setBatchListener(true)
            setConcurrency(runtime.pollLoopConcurrency)
            setCommonErrorHandler(naiveBatchAdmissionErrorHandler(properties))
            containerProperties.isStopImmediate = true
            containerProperties.shutdownTimeout = 5_000L
            setAutoStartup(true)
        }

    private fun naiveBatchAdmissionErrorHandler(properties: DemoApplicationProperties): DefaultErrorHandler =
        DefaultErrorHandler(
            { record: ConsumerRecord<*, *>, _ -> logDropped(record, properties.audit, AuditDropReasons.ADMISSION_FAILED) },
            FixedBackOff(0L, 0L)
        )

    private fun commonConsumerProperties(
        properties: DemoApplicationProperties,
        runtime: DemoApplicationProperties.ConsumerRuntime
    ): Map<String, Any> {
        runtime.processingMode.requireSupportedBySpringKafkaCoroutinesNaive()
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
}
