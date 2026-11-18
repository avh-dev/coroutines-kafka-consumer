package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ConsumerContainer
import avh.ckc.core.consumerContainer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.warmup.trafficWarmupContext
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConnectionProperties
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
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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
    private val log = LoggerFactory.getLogger(javaClass)
    private var lifecycle: CkcConsumersLifecycle? = null

    override fun start() {
        check(lifecycle == null) { "CKC demo consumers cannot be restarted" }
        val commonProperties = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        val trafficWarmup = createTrafficWarmup()

        val orderConsumer = DemoConsumers.orderConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.order) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            orderConsumerMetrics,
            properties.audit,
            properties.consumers.order,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup
        ) { key, event -> orderHandler(key, event) }

        val batchConsumer = DemoConsumers.batchConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.batch) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            batchConsumerMetrics,
            properties.audit,
            properties.consumers.batch,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup
        ) { key, event -> batchHandler(key, event) }

        val telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + properties.kafkaConsumerProperties(properties.consumers.telemetry) +
                mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId),
            consumerMetrics,
            properties.audit,
            properties.consumers.telemetry,
            properties.consumers.freshnessFirstMaxRecordAgeSeconds,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup
        ) { key, telemetry -> telemetryHandler(key, telemetry) }

        CkcConsumersLifecycle(
            consumerContainer(orderConsumer, batchConsumer, telemetryConsumer)
        ).also {
            lifecycle = it
            it.start()
        }
    }

    override fun stop() = lifecycle?.stop() ?: Unit

    override fun isRunning(): Boolean = lifecycle?.isRunning ?: false

    override fun isAutoStartup(): Boolean = true

    private fun createTrafficWarmup(): DemoConsumers.TrafficWarmup? {
        val warmup = properties.trafficWarmup
        if (!warmup.enabled) return null

        val controlProperties = warmup.controlKafka.kafkaConnectionProperties()
        val destinationProperties = warmup.destinationKafka.kafkaConnectionProperties()
        val context = trafficWarmupContext {
            instanceId = warmup.instanceId
            namespace = warmup.namespace
            controlTopic = warmup.controlTopic
            controlProducerProperties = controlProperties
            controlConsumerProperties = controlProperties
            warmupDuration = warmup.durationSeconds.seconds
            offerWindow = warmup.offerWindowMs.milliseconds
            participantStartTimeout = warmup.participantStartTimeoutSeconds.seconds
            preparationTimeout = warmup.preparationTimeoutSeconds.seconds
            completionTimeout = warmup.completionTimeoutSeconds.seconds
            delegationTimeout = warmup.delegationTimeoutSeconds.seconds
            maxDonorSessions = warmup.maxDonorSessions
            maxAggregateQuotaPerSecond = warmup.maxAggregateQuotaPerSecond
            onLifecycleEvent { event ->
                log.info(
                    "Warm-up lifecycle: phase={}, participantId={}, sessionId={}, reasonCode={}",
                    event.phase,
                    event.participantId,
                    event.sessionId,
                    event.reasonCode
                )
            }
        }
        return DemoConsumers.TrafficWarmup(
            context = context,
            consumerProperties = destinationProperties,
            producerProperties = destinationProperties + mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.LINGER_MS_CONFIG to warmup.producerLingerMs,
                ProducerConfig.BATCH_SIZE_CONFIG to warmup.producerBatchSize
            ),
            configuration = warmup
        )
    }
}

/** Adapts the shared core lifecycle to Spring without reimplementing joint cleanup. */
internal class CkcConsumersLifecycle(
    private val consumers: ConsumerContainer
) : SmartLifecycle {
    @Volatile
    private var running = false

    override fun start() {
        // A failed partial start still owns resources and must remain eligible for Spring shutdown.
        running = true
        consumers.start()
    }

    override fun stop() {
        try {
            runBlocking { consumers.stop() }
        } finally {
            running = consumers.status().participants.any { !it.stopComplete }
        }
    }

    override fun isRunning(): Boolean = running
}
