package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Configuration(proxyBeanMethods = false)
@Profile("ckc")
class CkcProfileConfiguration {
    @Bean(destroyMethod = "close")
    fun ckcWorkerDispatcher(properties: DemoApplicationProperties): ExecutorCoroutineDispatcher {
        val threads = properties.consumers.workerDispatcherThreads
        require(threads > 0) {
            "demo.consumers.worker-dispatcher-threads must be > 0 for ckc"
        }
        val threadNumber = AtomicInteger()
        return Executors.newFixedThreadPool(threads) { runnable ->
            Thread(runnable, "ckc-worker-${threadNumber.incrementAndGet()}").apply {
                isDaemon = true
            }
        }.asCoroutineDispatcher()
    }

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
@Profile("ckc-sync", "ckc-sync-loom")
class CkcSyncProfileConfiguration {
    @Bean("ckcSyncProcessingDispatcher", destroyMethod = "")
    @Profile("ckc-sync & !ckc-sync-loom")
    fun ckcSyncIoDispatcher(): CoroutineDispatcher =
        Dispatchers.IO

    @Bean("ckcSyncProcessingDispatcher", destroyMethod = "close")
    @Profile("ckc-sync-loom")
    fun ckcSyncLoomDispatcher(
        @Value("\${demo.consumers.virtual-thread-name-prefix:ckc-sync-loom-worker-}")
        threadNamePrefix: String
    ): ExecutorCoroutineDispatcher {
        val threadNumber = AtomicInteger()
        return Executors.newThreadPerTaskExecutor { runnable ->
            Thread.ofVirtual()
                .name(threadNamePrefix, threadNumber.incrementAndGet().toLong())
                .factory()
                .newThread(runnable)
        }.asCoroutineDispatcher()
    }

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
        val commonProperties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        ) + properties.kafka.consumerProperties()

        orderConsumer = DemoConsumers.orderConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.orderGroupId),
            orderConsumerMetrics,
            properties.audit,
            properties.consumers.order,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> orderHandler(key, event) }

        batchConsumer = DemoConsumers.batchConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.batchGroupId),
            batchConsumerMetrics,
            properties.audit,
            properties.consumers.batch,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> batchHandler(key, event) }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId),
            consumerMetrics,
            properties.audit,
            properties.consumers.telemetry,
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

private fun DemoApplicationProperties.Kafka.consumerProperties(): Map<String, Any> = mapOf(
    ConsumerConfig.FETCH_MIN_BYTES_CONFIG to consumer.fetchMinBytes,
    ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to consumer.fetchMaxWaitMs,
    ConsumerConfig.MAX_POLL_RECORDS_CONFIG to consumer.maxPollRecords
)
