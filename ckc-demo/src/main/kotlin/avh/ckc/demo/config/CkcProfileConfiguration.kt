package avh.ckc.demo.config

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.ConsumerMetrics
import avh.ckc.demo.DemoConsumers
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.service.BrewingLifecycleService
import avh.ckc.demo.service.EtaRecalculationService
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.beans.factory.annotation.Qualifier
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Configuration(proxyBeanMethods = false)
@Profile("ckc")
class CkcProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun ckcConsumerRuntime(
        properties: DemoApplicationProperties,
        brewingStateRepository: BrewingStateRepository,
        brewingLifecycleService: BrewingLifecycleService,
        etaRecalculationService: EtaRecalculationService,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("lifecycleConsumerMetrics") lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            brewingStateRepository,
            brewingLifecycleService,
            etaRecalculationService,
            consumerMetrics,
            lifecycleConsumerMetrics
        )
}

private class CkcConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val brewingStateRepository: BrewingStateRepository,
    private val brewingLifecycleService: BrewingLifecycleService,
    private val etaRecalculationService: EtaRecalculationService,
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var lifecycleConsumer: CoroutinesKafkaConsumer<String, OrderLifecycleEvent>
    private lateinit var telemetryConsumer: CoroutinesKafkaConsumer<String, CauldronTelemetryEvent>
    private lateinit var deserializationDispatcher: DemoDeserializationDispatcher
    @Volatile
    private var running = false

    override fun start() {
        deserializationDispatcher = newDemoDeserializationDispatcher(properties.consumers.deserializationDispatcher)
        val commonProperties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

        lifecycleConsumer = DemoConsumers.lifecycleConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.lifecycleGroupId),
            lifecycleConsumerMetrics,
            properties.audit.enabled,
            properties.consumers.lifecycle,
            deserializationDispatcher.dispatcher,
            properties.consumers.processingEnabled
        ) { _, event ->
            try {
                brewingLifecycleService.applyLifecycleEvent(event).await()
            } catch (error: Throwable) {
                logger.error(
                    "CKC lifecycle processing failed for orderId={}, eventType={}",
                    event.orderId,
                    event.eventType.name,
                    error
                )
                throw error
            }
        }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.telemetryGroupId),
            consumerMetrics,
            properties.audit.enabled,
            properties.consumers.telemetry,
            deserializationDispatcher.dispatcher,
            properties.consumers.processingEnabled
        ) { _, telemetry ->
            try {
                val batchId = telemetry.batchId.ifBlank { brewingStateRepository.findActiveBatchId(telemetry.cauldronId).await() ?: "" }
                if (batchId.isBlank()) {
                    return@telemetryConsumer
                }
                val batchState = brewingStateRepository.findBatch(batchId).await() ?: return@telemetryConsumer
                val estimate = etaRecalculationService.recalculate(batchState, telemetry).await()
                logger.info(
                    "CKC ETA recalculated for batch={}, cauldron={}, etaSeconds={}",
                    estimate.batchId,
                    estimate.cauldronId,
                    estimate.etaSeconds
                )
            } catch (error: Throwable) {
                logger.error(
                    "CKC telemetry processing failed for cauldronId={}, batchId={}, occurredAt={}",
                    telemetry.cauldronId,
                    telemetry.batchId,
                    telemetry.metadata.occurredAt,
                    error
                )
                throw error
            }
        }

        lifecycleConsumer.start()
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
                lifecycleConsumer.stop()
            }
        } finally {
            deserializationDispatcher.close()
            running = false
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true
}

private data class DemoDeserializationDispatcher(
    val dispatcher: CoroutineDispatcher,
    private val closeAction: () -> Unit = {}
) {
    fun close() = closeAction.invoke()
}

private fun newDemoDeserializationDispatcher(
    properties: DemoApplicationProperties.DeserializationDispatcher
): DemoDeserializationDispatcher =
    when (properties.mode) {
        DemoApplicationProperties.DeserializationDispatcherMode.DEFAULT ->
            DemoDeserializationDispatcher(Dispatchers.Default)
        DemoApplicationProperties.DeserializationDispatcherMode.IO ->
            DemoDeserializationDispatcher(Dispatchers.IO)
        DemoApplicationProperties.DeserializationDispatcherMode.CUSTOM_THREAD_POOL -> {
            require(properties.customThreadPoolSize > 0) {
                "demo.consumers.deserialization-dispatcher.custom-thread-pool-size must be > 0"
            }
            val dispatcher = newCustomDemoDeserializationDispatcher(properties)
            DemoDeserializationDispatcher(dispatcher, dispatcher::close)
        }
    }

private fun newCustomDemoDeserializationDispatcher(
    properties: DemoApplicationProperties.DeserializationDispatcher
): ExecutorCoroutineDispatcher {
    val threadCounter = AtomicInteger(1)
    return Executors.newFixedThreadPool(properties.customThreadPoolSize) { runnable ->
        Thread(runnable, "${properties.customThreadNamePrefix}-${threadCounter.getAndIncrement()}").apply {
            isDaemon = true
        }
    }.asCoroutineDispatcher()
}
