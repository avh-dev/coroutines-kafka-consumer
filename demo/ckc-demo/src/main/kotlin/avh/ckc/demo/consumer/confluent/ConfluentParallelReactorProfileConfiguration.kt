package avh.ckc.demo.consumer.confluent

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConsumerProperties
import avh.ckc.demo.consumer.DemoProcessingDispatcher
import avh.ckc.demo.consumer.DemoProcessingDispatcherFactory
import avh.ckc.demo.consumer.toConfluentProcessingOrder
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.RecordContext
import io.confluent.parallelconsumer.reactor.ReactorProcessor
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import reactor.core.scheduler.Schedulers

@Configuration(proxyBeanMethods = false)
@Profile("confluent-parallel-reactor")
class ConfluentParallelReactorProfileConfiguration {
    @Bean(destroyMethod = "close")
    fun confluentParallelReactorWorkerDispatcher(
        properties: DemoApplicationProperties
    ): DemoProcessingDispatcher =
        DemoProcessingDispatcherFactory.create(
            properties,
            dispatcherName = "pc-reactor-worker",
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
    fun confluentParallelReactorConsumerRuntime(
        properties: DemoApplicationProperties,
        trackingService: ConfluentParallelReactorTrackingService,
        meterRegistry: MeterRegistry
    ): SmartLifecycle =
        ConfluentParallelReactorConsumerRuntime(properties, trackingService, meterRegistry)
}

private class ConfluentParallelReactorConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val trackingService: ConfluentParallelReactorTrackingService,
    private val meterRegistry: MeterRegistry
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var processors: List<ManagedReactorProcessor>
    @Volatile
    private var running = false

    override fun start() {
        processors = orderProcessors() + batchProcessors() + telemetryProcessors()
        running = true
        processors.forEach { it.thread.start() }
    }

    override fun stop() {
        if (!running) {
            return
        }
        running = false
        try {
            processors.forEach { it.processor.close() }
            processors.forEach { it.kafkaClientMetrics.close() }
            processors.forEach { it.thread.join(STOP_JOIN_TIMEOUT_MILLIS) }
        } finally {
            logger.info("Confluent Parallel Consumer Reactor runtime stopped")
        }
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true

    private fun commonConsumerProperties(runtime: DemoApplicationProperties.ConsumerRuntime): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
    ) + properties.kafkaConsumerProperties(runtime)

    private fun orderProcessors(): List<ManagedReactorProcessor> =
        newManagedProcessors(
            name = "order-lifecycle",
            topic = properties.topics.orderEvents,
            consumerProperties = commonConsumerProperties(properties.consumers.order) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to OrderLifecycleEventDeserializer::class.java
            ),
            runtime = properties.consumers.order,
            handler = trackingService::processOrderLifecycle
        )

    private fun batchProcessors(): List<ManagedReactorProcessor> =
        newManagedProcessors(
            name = "batch-lifecycle",
            topic = properties.topics.batchEvents,
            consumerProperties = commonConsumerProperties(properties.consumers.batch) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to BatchLifecycleEventDeserializer::class.java
            ),
            runtime = properties.consumers.batch,
            handler = trackingService::processBatchLifecycle
        )

    private fun telemetryProcessors(): List<ManagedReactorProcessor> =
        newManagedProcessors(
            name = "cauldron-telemetry",
            topic = properties.topics.cauldronEvents,
            consumerProperties = commonConsumerProperties(properties.consumers.telemetry) + mapOf(
                ConsumerConfig.GROUP_ID_CONFIG to properties.kafka.groupId,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to CauldronTelemetryEventDeserializer::class.java
            ),
            runtime = properties.consumers.telemetry,
            handler = trackingService::processCauldronTelemetry
        )

    private fun <V> newManagedProcessors(
        name: String,
        topic: String,
        consumerProperties: Map<String, Any>,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        handler: (RecordContext<String, V>) -> Publisher<*>
    ): List<ManagedReactorProcessor> {
        require(runtime.pollLoopConcurrency > 0) {
            "demo.consumers.*.poll-loop-concurrency must be > 0 for confluent-parallel-reactor"
        }
        return (1..runtime.pollLoopConcurrency).map { index ->
            val processorName = "confluent-parallel-reactor-$name-$index"
            val consumer = KafkaConsumer<String, V>(
                consumerProperties + mapOf(ConsumerConfig.CLIENT_ID_CONFIG to processorName)
            )
            val kafkaClientMetrics = KafkaClientMetrics(consumer).apply {
                bindTo(meterRegistry)
            }
            val processor = newProcessor(
                consumer = consumer,
                runtime = runtime,
                processorName = processorName
            )
            processor.subscribe(listOf(topic))
            ManagedReactorProcessor(
                processor = processor,
                kafkaClientMetrics = kafkaClientMetrics,
                thread = newPollThread(processorName) {
                    processor.react { context ->
                        handler(context.getSingleRecord())
                    }
                }
            )
        }
    }

    private fun <V> newProcessor(
        consumer: KafkaConsumer<String, V>,
        runtime: DemoApplicationProperties.ConsumerRuntime,
        processorName: String
    ): ReactorProcessor<String, V> {
        require(runtime.workerConcurrency > 0) {
            "demo.consumers.*.worker-concurrency must be > 0 for confluent-parallel-reactor"
        }
        val options = ParallelConsumerOptions.builder<String, V>()
            .ordering(runtime.processingMode.toConfluentProcessingOrder())
            .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
            .maxConcurrency(runtime.workerConcurrency)
            .consumer(consumer)
            .build()
        return ReactorProcessor(options, Schedulers::immediate)
    }

    private fun newPollThread(name: String, pollAction: () -> Unit): Thread =
        Thread {
            try {
                pollAction()
            } catch (error: Throwable) {
                if (running) {
                    logger.error("Confluent Parallel Consumer Reactor poll loop failed: {}", name, error)
                }
            }
        }.apply {
            this.name = name
            isDaemon = true
        }

    companion object {
        private const val STOP_JOIN_TIMEOUT_MILLIS = 10_000L
    }
}

private data class ManagedReactorProcessor(
    val processor: ReactorProcessor<*, *>,
    val kafkaClientMetrics: KafkaClientMetrics,
    val thread: Thread
)
