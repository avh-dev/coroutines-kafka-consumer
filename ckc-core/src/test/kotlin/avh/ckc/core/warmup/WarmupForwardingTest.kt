package avh.ckc.core.warmup

import avh.ckc.core.awaitFor
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import java.util.Optional
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class WarmupForwardingTest {
    @Test
    fun `forwarding configuration validates ownership producer and concurrency and snapshots inputs`() {
        val consumerId = WarmupConsumerId("orders")
        val assignments = mutableListOf(
            WarmupPartitionAssignment(consumerId, "source-orders", "warmup-orders", 0)
        )
        val producerProperties = mutableMapOf<String, Any?>(
            org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )
        val configuration = WarmupForwardingConfiguration(
            sessionId = WarmupSessionId("session-1"),
            consumerId = consumerId,
            assignments = assignments,
            profile = WarmupForwardingProfile(),
            producerProperties = producerProperties
        )
        assignments += assignments.single().copy(sourceTopic = "source-batches")
        producerProperties.clear()

        assertEquals(1, configuration.assignments.size)
        assertEquals(2, configuration.producerProperties.size)
        assertFailsWith<UnsupportedOperationException> {
            (configuration.producerProperties as MutableMap)["changed"] = true
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingConfiguration(
                sessionId = WarmupSessionId("session-1"),
                consumerId = WarmupConsumerId("other"),
                assignments = configuration.assignments,
                profile = WarmupForwardingProfile(),
                producerProperties = configuration.producerProperties
            )
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingConfiguration(
                sessionId = WarmupSessionId("session-1"),
                consumerId = consumerId,
                assignments = configuration.assignments,
                profile = WarmupForwardingProfile(),
                producerProperties = emptyMap()
            )
        }
        assertFailsWith<IllegalArgumentException> {
            WarmupForwardingConfiguration(
                sessionId = WarmupSessionId("session-1"),
                consumerId = consumerId,
                assignments = configuration.assignments,
                profile = WarmupForwardingProfile(),
                producerProperties = configuration.producerProperties,
                maxInFlightSends = 0
            )
        }
    }

    @Test
    fun `acknowledged forwarding preserves record data and reports source to destination link`() = runBlocking {
        val producer = producer(autoComplete = false)
        val acknowledgements = mutableListOf<WarmupForwardingAcknowledgement>()
        val forwarding = forwarding(
            producer = producer,
            observer = WarmupForwardingObserver { acknowledgements += it }
        )
        val source = sourceRecord()

        val result = async { forwarding.forwardIfSelected(source) }
        awaitFor(2_000L, 10L) { producer.history().singleOrNull() }
        assertFalse(result.isCompleted)
        assertTrue(producer.completeNext())
        assertTrue(result.await())

        val sent = producer.history().single()
        assertEquals("warmup-orders", sent.topic())
        assertEquals(3, sent.partition())
        assertEquals(1_234L, sent.timestamp())
        assertEquals("key-7", sent.key())
        assertEquals("payload-7", sent.value())
        assertEquals("trace-7", sent.headers().lastHeader("trace-id").value().decodeToString())
        assertEquals("source-orders", source.topic())
        assertEquals("trace-7", source.headers().lastHeader("trace-id").value().decodeToString())
        assertEquals(
            WarmupForwardingAcknowledgement(
                sessionId = WarmupSessionId("session-1"),
                consumerId = WarmupConsumerId("orders"),
                sourceTopicPartition = org.apache.kafka.common.TopicPartition("source-orders", 2),
                sourceOffset = 7L,
                warmupTopicPartition = org.apache.kafka.common.TopicPartition("warmup-orders", 3),
                warmupOffset = 0L
            ),
            acknowledgements.single()
        )
        forwarding.close()
    }

    @Test
    fun `send failure is exposed without a forwarding acknowledgement`() = runBlocking {
        val producer = producer(autoComplete = false)
        val acknowledgements = mutableListOf<WarmupForwardingAcknowledgement>()
        val forwarding = forwarding(
            producer = producer,
            observer = WarmupForwardingObserver { acknowledgements += it }
        )
        val error = kotlinx.coroutines.supervisorScope {
            val result = async { forwarding.forwardIfSelected(sourceRecord()) }
            awaitFor(2_000L, 10L) { producer.history().singleOrNull() }
            assertTrue(producer.errorNext(org.apache.kafka.common.KafkaException("warm-up send failed")))
            assertFailsWith<org.apache.kafka.common.KafkaException> { result.await() }
        }

        assertEquals("warm-up send failed", error.message)
        assertTrue(acknowledgements.isEmpty())
        forwarding.close()
    }

    @Test
    fun `in-flight send limit delays the next producer send until acknowledgement`() = runBlocking {
        val producer = producer(autoComplete = false)
        val forwarding = forwarding(producer = producer, maxInFlightSends = 1)

        val first = async { forwarding.forwardIfSelected(sourceRecord(offset = 7L)) }
        awaitFor(2_000L, 10L) { producer.history().singleOrNull() }
        val second = async { forwarding.forwardIfSelected(sourceRecord(offset = 8L)) }
        kotlinx.coroutines.delay(50)
        assertEquals(1, producer.history().size)

        assertTrue(producer.completeNext())
        awaitFor(2_000L, 10L) { producer.history().takeIf { it.size == 2 } }
        assertTrue(producer.completeNext())
        assertTrue(first.await())
        assertTrue(second.await())
        forwarding.close()
    }

    @Test
    fun `unselected and unmapped records stay on the local path`() = runBlocking {
        val producer = producer(autoComplete = true)
        val neverSelected = forwarding(
            producer = producer,
            profile = WarmupForwardingProfile(initialFraction = 0.0, targetFraction = 0.0)
        )

        assertFalse(neverSelected.forwardIfSelected(sourceRecord()))
        assertFalse(neverSelected.forwardIfSelected(sourceRecord(topic = "other-topic")))
        assertTrue(producer.history().isEmpty())
        neverSelected.close()
    }

    @Test
    fun `selection fraction ramps from initial to target over traffic time`() {
        var now = 100L
        val selector = WarmupForwardingSelector(
            profile = WarmupForwardingProfile(
                initialFraction = 0.1,
                targetFraction = 0.9,
                rampUpDuration = 100.seconds
            ),
            nanoTime = { now },
            randomDouble = { 0.49 }
        )

        assertFalse(selector.shouldForward())
        now += 50.seconds.inWholeNanoseconds
        assertTrue(selector.shouldForward())
        now += 100.seconds.inWholeNanoseconds
        assertTrue(selector.shouldForward())
    }

    private fun forwarding(
        producer: MockProducer<String, String>,
        observer: WarmupForwardingObserver = WarmupForwardingObserver.NOOP,
        profile: WarmupForwardingProfile = WarmupForwardingProfile(
            initialFraction = 1.0,
            targetFraction = 1.0,
            rampUpDuration = kotlin.time.Duration.ZERO
        ),
        maxInFlightSends: Int = 32
    ): KafkaWarmupRecordForwarding<String, String> = KafkaWarmupRecordForwarding(
        configuration = configuration(profile, maxInFlightSends),
        observer = observer,
        producerFactory = { producer }
    )

    private fun producer(autoComplete: Boolean): MockProducer<String, String> {
        val node = Node(0, "localhost", 9092)
        val partitions = (0..3).map { partition ->
            PartitionInfo("warmup-orders", partition, node, arrayOf(node), arrayOf(node))
        }
        return MockProducer(
            Cluster("test", listOf(node), partitions, emptySet(), emptySet()),
            autoComplete,
            StringSerializer(),
            StringSerializer()
        )
    }

    private fun configuration(
        profile: WarmupForwardingProfile,
        maxInFlightSends: Int
    ) = WarmupForwardingConfiguration(
        sessionId = WarmupSessionId("session-1"),
        consumerId = WarmupConsumerId("orders"),
        assignments = listOf(
            WarmupPartitionAssignment(
                consumerId = WarmupConsumerId("orders"),
                sourceTopic = "source-orders",
                warmupTopic = "warmup-orders",
                warmupPartition = 3
            )
        ),
        profile = profile,
        producerProperties = mapOf(
            org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        ),
        maxInFlightSends = maxInFlightSends
    )

    @Suppress("DEPRECATION")
    private fun sourceRecord(
        topic: String = "source-orders",
        offset: Long = 7L
    ): ConsumerRecord<String, String> = ConsumerRecord(
        topic,
        2,
        offset,
        1_234L,
        TimestampType.CREATE_TIME,
        5,
        9,
        "key-$offset",
        "payload-$offset",
        RecordHeaders().add("trace-id", "trace-$offset".encodeToByteArray()),
        Optional.empty()
    )
}
