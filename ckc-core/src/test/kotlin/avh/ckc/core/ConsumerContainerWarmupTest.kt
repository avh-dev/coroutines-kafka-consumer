package avh.ckc.core

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.warmup.TrafficWarmupContext
import avh.ckc.core.warmup.trafficWarmupContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

class ConsumerContainerWarmupTest {
    @Test
    fun `container accepts plain consumers and participants from shared or independent warmup contexts`() = runBlocking {
        val shared = context("shared")
        val independentFirst = context("independent-first")
        val independentSecond = context("independent-second")
        val consumers = listOf(
            plainConsumer("plain"),
            warmupConsumer(shared, "shared-a", "source-a", "warmup-a"),
            warmupConsumer(shared, "shared-b", "source-b", "warmup-b"),
            warmupConsumer(independentFirst, "independent-a", "source-c", "warmup-c"),
            warmupConsumer(independentSecond, "independent-b", "source-d", "warmup-d")
        )
        val container = consumerContainer(*consumers.toTypedArray())

        container.stop()

        assertTrue(consumers.all(CoroutinesKafkaConsumer<*, *>::isLifecycleStopComplete))
        consumers.forEach { consumer ->
            withTimeout(1.seconds) { consumer.awaitShutdownCompletion() }
        }
    }

    @Test
    fun `stop before start closes prepared warmup runtime and releases parent jobs`() = runBlocking {
        val parent = Job()
        val parentContext = parent + Dispatchers.Default
        val context = context("prepared", parentContext)
        val consumer = warmupConsumer(
            context = context,
            participantId = "prepared",
            sourceTopic = "source",
            warmupTopic = "warmup",
            parentContext = parentContext
        )
        val container = consumerContainer(consumer)

        container.stop()

        withTimeout(1.seconds) {
            consumer.awaitShutdownCompletion()
            while (parent.children.any()) delay(10)
        }
        assertTrue(consumer.stateSnapshot().stopped)
        assertTrue(parent.children.none())
    }

    private fun context(
        id: String,
        lifecycleContext: CoroutineContext = Dispatchers.Default
    ): TrafficWarmupContext = trafficWarmupContext {
        instanceId = id
        namespace = "global5-v6-$id"
        controlTopic = "control-$id"
        controlProducerProperties = connectionProperties()
        controlConsumerProperties = connectionProperties()
        parentContext = lifecycleContext
    }

    private fun plainConsumer(topic: String): CoroutinesKafkaConsumer<String, String> =
        coroutinesKafkaConsumer(mainProperties()) {
            topics(topic)
            @Suppress("UNCHECKED_CAST")
            metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
            handle { }
        }

    private fun warmupConsumer(
        context: TrafficWarmupContext,
        participantId: String,
        sourceTopic: String,
        warmupTopic: String,
        parentContext: CoroutineContext = Dispatchers.Default
    ): CoroutinesKafkaConsumer<String, String> = coroutinesKafkaConsumer(mainProperties()) {
        topics(sourceTopic)
        this.parentContext = parentContext
        @Suppress("UNCHECKED_CAST")
        metrics = ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
        handle { }
        trafficWarmup(context, participantId) {
            mapTopic(sourceTopic, warmupTopic)
            warmupConsumerProperties = connectionProperties()
            forwardingProducerProperties = connectionProperties() + mapOf(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        }
    }

    private fun mainProperties(): Map<String, Any?> = connectionProperties() + mapOf(
        ConsumerConfig.GROUP_ID_CONFIG to "container-test",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    )

    private fun connectionProperties(): Map<String, Any?> = mapOf(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "unused:9092"
    )
}
