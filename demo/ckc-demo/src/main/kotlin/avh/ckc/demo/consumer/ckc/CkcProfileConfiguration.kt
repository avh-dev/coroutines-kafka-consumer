package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConsumerProperties
import avh.ckc.demo.consumer.DemoProcessingDispatcher
import avh.ckc.demo.consumer.DemoProcessingDispatcherFactory
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SyncBatchLifecycleService
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SyncCauldronTelemetryService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SyncOrderLifecycleService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("ckc")
class CkcProfileConfiguration {
    @Bean(destroyMethod = "close")
    fun ckcWorkerDispatcher(properties: DemoApplicationProperties): DemoProcessingDispatcher =
        DemoProcessingDispatcherFactory.create(
            properties,
            dispatcherName = "ckc-worker",
            defaultType = DemoApplicationProperties.ProcessingDispatcherType.FIXED,
            allowedTypes = setOf(
                DemoApplicationProperties.ProcessingDispatcherType.DEFAULT,
                DemoApplicationProperties.ProcessingDispatcherType.FIXED,
                DemoApplicationProperties.ProcessingDispatcherType.IO,
                DemoApplicationProperties.ProcessingDispatcherType.VIRTUAL
            )
        )

    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcConsumerRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SuspendOrderLifecycleService,
        batchLifecycleService: SuspendBatchLifecycleService,
        cauldronTelemetryService: SuspendCauldronTelemetryService,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("orderConsumerMetrics") orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("batchConsumerMetrics") batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
        @Qualifier("ckcWorkerDispatcher") workerDispatcher: CoroutineDispatcher
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            workerDispatcher
        )
}

@Configuration(proxyBeanMethods = false)
@Profile("ckc-sync")
class CkcSyncProfileConfiguration {
    @Bean("ckcSyncProcessingDispatcher", destroyMethod = "close")
    fun ckcSyncProcessingDispatcher(properties: DemoApplicationProperties): DemoProcessingDispatcher =
        DemoProcessingDispatcherFactory.create(
            properties,
            dispatcherName = "ckc-sync-worker",
            defaultType = DemoApplicationProperties.ProcessingDispatcherType.IO,
            allowedTypes = setOf(
                DemoApplicationProperties.ProcessingDispatcherType.IO,
                DemoApplicationProperties.ProcessingDispatcherType.VIRTUAL
            )
        )

    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcSyncConsumerRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SyncOrderLifecycleService,
        batchLifecycleService: SyncBatchLifecycleService,
        cauldronTelemetryService: SyncCauldronTelemetryService,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("orderConsumerMetrics") orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("batchConsumerMetrics") batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
        @Qualifier("ckcSyncProcessingDispatcher") processingDispatcher: CoroutineDispatcher
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            processingDispatcher
        )
}

private class CkcConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val orderHandler: suspend (String?, OrderLifecycleEvent) -> Unit,
    private val batchHandler: suspend (String?, BatchLifecycleEvent) -> Unit,
    private val telemetryHandler: suspend (String?, CauldronTelemetryEvent) -> Unit,
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>,
    private val processingDispatcher: CoroutineDispatcher
) : SmartLifecycle {
    private lateinit var orderConsumer: CoroutinesKafkaConsumer<String, OrderLifecycleEvent>
    private lateinit var batchConsumer: CoroutinesKafkaConsumer<String, BatchLifecycleEvent>
    private lateinit var telemetryConsumer: CoroutinesKafkaConsumer<String, CauldronTelemetryEvent>
    @Volatile
    private var running = false

    override fun start() {
        val commonProperties = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

        orderConsumer = DemoConsumers.orderConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.order) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            orderConsumerMetrics,
            properties.audit,
            properties.consumers.order,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> orderHandler(key, event) }

        batchConsumer = DemoConsumers.batchConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.batch) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            batchConsumerMetrics,
            properties.audit,
            properties.consumers.batch,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> batchHandler(key, event) }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.telemetry) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            consumerMetrics,
            properties.audit,
            properties.consumers.telemetry,
            properties.consumers.freshnessFirstMaxRecordAgeSeconds,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, telemetry -> telemetryHandler(key, telemetry) }

        orderConsumer.start()
        batchConsumer.start()
        telemetryConsumer.start()
        running = true
    }

    override fun stop() {
        if (!running) {
            return
        }
        try {
            runBlocking {
                telemetryConsumer.stop()
                batchConsumer.stop()
                orderConsumer.stop()
            }
        } finally {
            running = false
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true
}
