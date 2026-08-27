package avh.ckc.demo.consumer.confluent

import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.config.kafkaConsumerProperties
import avh.ckc.demo.consumer.toConfluentProcessingOrder
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.BatchLifecycleEventDeserializer
import avh.ckc.demo.serialization.CauldronTelemetryEventDeserializer
import avh.ckc.demo.serialization.OrderLifecycleEventDeserializer
import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.confluent.parallelconsumer.RecordContext
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration(proxyBeanMethods = false)
@Profile("confluent-parallel")
class ConfluentParallelProfileConfiguration {
    @Bean
    @ConditionalOnProperty(prefix = "demo.kafka", name = ["enabled"], havingValue = "true")
    fun confluentParallelConsumerRuntime(
        properties: DemoApplicationProperties,
        trackingService: ConfluentParallelTrackingService,
        meterRegistry: MeterRegistry
    ): SmartLifecycle =
        ConfluentParallelConsumerRuntime(properties, trackingService, meterRegistry)
}

private class ConfluentParallelConsumerRuntime(
    private val properties: DemoApplicationProperties,
    private val trackingService: ConfluentParallelTrackingService,
    private val meterRegistry: MeterRegistry
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var processors: List<ManagedProcessor>
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
            logger.info("Confluent Parallel Consumer runtime stopped")
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

    private fun orderProcessors(): List<ManagedProcessor> =
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

    private fun batchProcessors(): List<ManagedProcessor> =
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

    private fun telemetryProcessors(): List<ManagedProcessor> =
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
        handler: (RecordContext<String, V>) -> Unit
    ): List<ManagedProcessor> {
        require(runtime.pollLoopConcurrency > 0) {
            "demo.consumers.*.poll-loop-concurrency must be > 0 for confluent-parallel"
        }
        return (1..runtime.pollLoopConcurrency).map { index ->
            val processorName = "confluent-parallel-$name-$index"
            val consumer = KafkaConsumer<String, V>(
                consumerProperties + mapOf(ConsumerConfig.CLIENT_ID_CONFIG to processorName)
            )
            val kafkaClientMetrics = KafkaClientMetrics(consumer).apply {
                bindTo(meterRegistry)
            }
            val processor = newProcessor<V>(
                consumer = consumer,
                runtime = runtime,
                processorName = processorName
            )
            processor.subscribe(listOf(topic))
            ManagedProcessor(
                processor = processor,
                kafkaClientMetrics = kafkaClientMetrics,
                thread = newPollThread(processorName) {
                    processor.poll { context ->
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
    ): ParallelStreamProcessor<String, V> {
        require(runtime.workerConcurrency > 0) {
            "demo.consumers.*.worker-concurrency must be > 0 for confluent-parallel"
        }
        val options = ParallelConsumerOptions.builder<String, V>()
            .ordering(runtime.processingMode.toConfluentProcessingOrder())
            .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
            .maxConcurrency(runtime.workerConcurrency)
            .consumer(consumer)
            .build()
        return ParallelStreamProcessor.createEosStreamProcessor(options)
    }

    private fun newPollThread(name: String, pollAction: () -> Unit): Thread =
        Thread {
            try {
                pollAction()
            } catch (error: Throwable) {
                if (running) {
                    logger.error("Confluent Parallel Consumer poll loop failed: {}", name, error)
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

private data class ManagedProcessor(
    val processor: ParallelStreamProcessor<*, *>,
    val kafkaClientMetrics: KafkaClientMetrics,
    val thread: Thread
)
