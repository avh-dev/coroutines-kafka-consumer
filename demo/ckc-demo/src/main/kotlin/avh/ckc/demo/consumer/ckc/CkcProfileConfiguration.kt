package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
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
        @Qualifier("ckcWorkerDispatcher") workerDispatcher: CoroutineDispatcher,
        trafficWarmupRecipientReadiness: TrafficWarmupRecipientReadiness,
        meterRegistry: MeterRegistry
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            workerDispatcher,
            trafficWarmupRecipientReadiness,
            meterRegistry
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
        @Qualifier("ckcSyncProcessingDispatcher") processingDispatcher: CoroutineDispatcher,
        trafficWarmupRecipientReadiness: TrafficWarmupRecipientReadiness,
        meterRegistry: MeterRegistry
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderHandler = { _, event -> orderLifecycleService.apply(event) },
            batchHandler = { _, event -> batchLifecycleService.apply(event) },
            telemetryHandler = { _, telemetry -> cauldronTelemetryService.recalculate(telemetry) },
            consumerMetrics,
            orderConsumerMetrics,
            batchConsumerMetrics,
            processingDispatcher,
            trafficWarmupRecipientReadiness,
            meterRegistry
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
    private val processingDispatcher: CoroutineDispatcher,
    private val trafficWarmupRecipientReadiness: TrafficWarmupRecipientReadiness,
    private val meterRegistry: MeterRegistry
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val failureCleanupScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private lateinit var orderConsumer: CoroutinesKafkaConsumer<String, OrderLifecycleEvent>
    private lateinit var batchConsumer: CoroutinesKafkaConsumer<String, BatchLifecycleEvent>
    private lateinit var telemetryConsumer: CoroutinesKafkaConsumer<String, CauldronTelemetryEvent>
    private val recipientConsumers = mutableListOf<CoroutinesKafkaConsumer<*, *>>()
    private var lifecycleMonitor: CkcConsumerLifecycleMonitor? = null
    @Volatile
    private var running = false
    @Volatile
    private var lifecycleStarted = false

    override fun start() {
        val trafficWarmup = StaticTrafficWarmupPlan.from(properties)
        val automaticTrafficWarmup = AutomaticTrafficWarmupPlan.from(
            properties,
            processCpuLoadScore(meterRegistry)
        )
        val commonProperties = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

        orderConsumer = DemoConsumers.orderConsumer(
            consumerProperties(commonProperties, properties.consumers.order, trafficWarmup.order),
            orderConsumerMetrics,
            properties.audit,
            properties.consumers.order,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup.order.route,
            automaticTrafficWarmup?.order
        ) { key, event -> orderHandler(key, event) }

        batchConsumer = DemoConsumers.batchConsumer(
            consumerProperties(commonProperties, properties.consumers.batch, trafficWarmup.batch),
            batchConsumerMetrics,
            properties.audit,
            properties.consumers.batch,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup.batch.route,
            automaticTrafficWarmup?.batch
        ) { key, event -> batchHandler(key, event) }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            consumerProperties(commonProperties, properties.consumers.telemetry, trafficWarmup.telemetry),
            consumerMetrics,
            properties.audit,
            properties.consumers.telemetry,
            properties.consumers.freshnessFirstMaxRecordAgeSeconds,
            properties.consumers.retry,
            processingDispatcher,
            properties.consumers.processingEnabled,
            trafficWarmup.telemetry.route,
            automaticTrafficWarmup?.telemetry
        ) { key, telemetry -> telemetryHandler(key, telemetry) }

        val startedConsumers = mutableListOf<CoroutinesKafkaConsumer<*, *>>()
        try {
            orderConsumer.start()
            startedConsumers += orderConsumer
            batchConsumer.start()
            startedConsumers += batchConsumer
            telemetryConsumer.start()
            startedConsumers += telemetryConsumer
            recipientConsumers.clear()
            if (trafficWarmup.order.recipient) recipientConsumers += orderConsumer
            if (trafficWarmup.batch.recipient) recipientConsumers += batchConsumer
            if (trafficWarmup.telemetry.recipient) recipientConsumers += telemetryConsumer
            runBlocking {
                recipientConsumers.forEach { it.awaitReady() }
            }
            if (automaticTrafficWarmup == null) {
                trafficWarmupRecipientReadiness.markReady()
            }
            lifecycleStarted = true
            running = true
            lifecycleMonitor = createLifecycleMonitor(
                listOf(
                    "order" to orderConsumer,
                    "batch" to batchConsumer,
                    "telemetry" to telemetryConsumer
                ),
                observeReadiness = automaticTrafficWarmup != null
            ).also(CkcConsumerLifecycleMonitor::start)
        } catch (startupFailure: Throwable) {
            lifecycleMonitor?.stop()
            lifecycleMonitor = null
            running = false
            lifecycleStarted = false
            trafficWarmupRecipientReadiness.markFailed()
            runBlocking {
                startedConsumers.asReversed().forEach { consumer ->
                    runCatching { consumer.stop() }
                        .exceptionOrNull()
                        ?.let(startupFailure::addSuppressed)
                }
            }
            recipientConsumers.clear()
            throw startupFailure
        }
    }

    override fun stop() {
        if (!lifecycleStarted) {
            return
        }
        lifecycleMonitor?.stop()
        lifecycleMonitor = null
        trafficWarmupRecipientReadiness.markStopped()
        try {
            runBlocking {
                telemetryConsumer.stop()
                batchConsumer.stop()
                orderConsumer.stop()
            }
        } finally {
            running = false
            lifecycleStarted = false
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    private fun createLifecycleMonitor(
        consumers: List<Pair<String, CoroutinesKafkaConsumer<*, *>>>,
        observeReadiness: Boolean
    ): CkcConsumerLifecycleMonitor = CkcConsumerLifecycleMonitor(
        consumers = consumers.map { (name, consumer) ->
            MonitoredCkcConsumer(name, consumer::awaitReady, consumer::awaitTermination)
        },
        observeReadiness = observeReadiness,
        onReady = trafficWarmupRecipientReadiness::markReady,
        onTerminalFailure = ::handleTerminalFailure
    )

    private fun handleTerminalFailure(failure: Throwable) {
        logger.error("CKC consumer reached terminal failure", failure)
        running = false
        trafficWarmupRecipientReadiness.markFailed()
        failureCleanupScope.launch {
            listOf(telemetryConsumer, batchConsumer, orderConsumer).forEach { consumer ->
                runCatching { consumer.stop() }
                    .onFailure { cleanupFailure ->
                        if (cleanupFailure !== failure) failure.addSuppressed(cleanupFailure)
                    }
            }
        }
    }

    private fun consumerProperties(
        commonProperties: Map<String, Any>,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        participant: StaticTrafficWarmupParticipantPlan
    ): Map<String, Any> {
        val bootstrapServers = if (participant.recipient) {
            properties.trafficWarmup.kafkaBootstrapServers.ifBlank { properties.kafka.bootstrapServers }
        } else {
            properties.kafka.bootstrapServers
        }
        val groupId = if (participant.recipient) {
            "${properties.trafficWarmup.recipientGroupIdPrefix}-${properties.trafficWarmup.sessionId}-" +
                participant.consumerId.value
        } else {
            runtime.groupIdOr(properties.kafka.groupId)
        }
        return commonProperties +
            properties.kafkaConsumerProperties(runtime) +
            properties.kafkaConnectionProperties(bootstrapServers) +
            mapOf(ConsumerConfig.GROUP_ID_CONFIG to groupId)
    }
}
