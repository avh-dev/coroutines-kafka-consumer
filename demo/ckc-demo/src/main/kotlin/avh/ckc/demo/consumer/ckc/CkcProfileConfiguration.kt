package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.AuditLog
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
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.beans.factory.annotation.Qualifier

@Configuration(proxyBeanMethods = false)
@Profile("ckc")
class CkcProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcConsumerRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SuspendOrderLifecycleService,
        batchLifecycleService: SuspendBatchLifecycleService,
        cauldronTelemetryService: SuspendCauldronTelemetryService,
        auditLog: AuditLog,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("orderConsumerMetrics") orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("batchConsumerMetrics") batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            auditLog,
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            Dispatchers.Default
        )
}

@Configuration(proxyBeanMethods = false)
@Profile("ckc-sync")
class CkcSyncProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcSyncConsumerRuntime(
        properties: DemoApplicationProperties,
        orderLifecycleService: SyncOrderLifecycleService,
        batchLifecycleService: SyncBatchLifecycleService,
        cauldronTelemetryService: SyncCauldronTelemetryService,
        auditLog: AuditLog,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("orderConsumerMetrics") orderConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("batchConsumerMetrics") batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            auditLog,
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            Dispatchers.IO
        )
}

private class CkcConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val orderHandler: suspend (String?, OrderLifecycleEvent) -> Unit,
    private val batchHandler: suspend (String?, BatchLifecycleEvent) -> Unit,
    private val telemetryHandler: suspend (String?, CauldronTelemetryEvent) -> Unit,
    private val auditLog: AuditLog,
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
            auditLog,
            properties.consumers.order,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> orderHandler(key, event) }

        batchConsumer = DemoConsumers.batchConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.batchGroupId),
            batchConsumerMetrics,
            auditLog,
            properties.consumers.batch,
            processingDispatcher,
            properties.consumers.processingEnabled
        ) { key, event -> batchHandler(key, event) }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId),
            consumerMetrics,
            auditLog,
            properties.consumers.telemetry,
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
