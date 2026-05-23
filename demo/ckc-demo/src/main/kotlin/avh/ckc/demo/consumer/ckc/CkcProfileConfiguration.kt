package avh.ckc.demo.consumer.ckc

import avh.ckc.core.CoroutinesKafkaConsumer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.service.batch.SuspendBatchLifecycleService
import avh.ckc.demo.service.cauldron.SuspendCauldronTelemetryService
import avh.ckc.demo.service.order.SuspendOrderLifecycleService
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
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
        orderLifecycleService: SuspendOrderLifecycleService,
        batchLifecycleService: SuspendBatchLifecycleService,
        cauldronTelemetryService: SuspendCauldronTelemetryService,
        @Qualifier("consumerMetrics") consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
        @Qualifier("lifecycleConsumerMetrics") lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
        @Qualifier("batchConsumerMetrics") batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>
    ): SmartLifecycle =
        CkcConsumerRuntime(
            properties,
            orderLifecycleService,
            batchLifecycleService,
            cauldronTelemetryService,
            consumerMetrics,
            lifecycleConsumerMetrics,
            batchConsumerMetrics
        )
}

private class CkcConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val orderLifecycleService: SuspendOrderLifecycleService,
    private val batchLifecycleService: SuspendBatchLifecycleService,
    private val cauldronTelemetryService: SuspendCauldronTelemetryService,
    private val consumerMetrics: ConsumerMetrics<String, CauldronTelemetryEvent>,
    private val lifecycleConsumerMetrics: ConsumerMetrics<String, OrderLifecycleEvent>,
    private val batchConsumerMetrics: ConsumerMetrics<String, BatchLifecycleEvent>
) : SmartLifecycle {
    private lateinit var lifecycleConsumer: CoroutinesKafkaConsumer<String, OrderLifecycleEvent>
    private lateinit var batchConsumer: CoroutinesKafkaConsumer<String, BatchLifecycleEvent>
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
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.orderGroupId),
            lifecycleConsumerMetrics,
            properties.audit.enabled,
            properties.consumers.lifecycle,
            deserializationDispatcher.dispatcher,
            properties.consumers.processingEnabled
        ) { _, event ->
            orderLifecycleService.apply(event)
        }

        batchConsumer = DemoConsumers.batchConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.batchGroupId),
            batchConsumerMetrics,
            properties.audit.enabled,
            properties.consumers.batch,
            deserializationDispatcher.dispatcher,
            properties.consumers.processingEnabled
        ) { _, event ->
            batchLifecycleService.apply(event)
        }

        telemetryConsumer = DemoConsumers.telemetryConsumer(
            commonProperties + mapOf(ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.cauldronGroupId),
            consumerMetrics,
            properties.audit.enabled,
            properties.consumers.telemetry,
            deserializationDispatcher.dispatcher,
            properties.consumers.processingEnabled
        ) { _, telemetry ->
            cauldronTelemetryService.recalculate(telemetry)
        }

        lifecycleConsumer.start()
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
