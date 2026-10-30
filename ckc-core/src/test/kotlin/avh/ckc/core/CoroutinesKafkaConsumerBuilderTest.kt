package avh.ckc.core

import avh.ckc.core.warmup.WarmupConsumerId
import avh.ckc.core.warmup.WarmupForwardingConfiguration
import avh.ckc.core.warmup.WarmupForwardingProfile
import avh.ckc.core.warmup.WarmupPartitionAssignment
import avh.ckc.core.warmup.WarmupSessionId
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.regex.Pattern
import kotlin.time.Duration.Companion.seconds

class CoroutinesKafkaConsumerBuilderTest {

    @Test
    fun `when builder is configured with topics then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topics("topic-a")
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when builder is configured with topics pattern then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topicsPattern(Pattern.compile("topic-.*"))
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when builder is configured with assigned warm-up partitions then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            assignedPartitionsAtEnd(TopicPartition("warmup-a", 1))
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when unordered builder has warm-up forwarding then donor consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topics("source-orders")
            warmupForwarding(forwardingConfiguration())
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when ordered builder has warm-up forwarding then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                processingMode = ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING
                topics("source-orders")
                warmupForwarding(forwardingConfiguration())
                handle { }
            }
        }

        assertEquals("Warm-up forwarding supports only AT_LEAST_ONCE_NO_ORDERING", error.message)
    }

    @Test
    fun `when assigned warm-up partitions are empty then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                assignedPartitionsAtEnd(emptySet())
                handle { }
            }
        }

        assertEquals("assignedPartitionsAtEnd must not be empty", error.message)
    }

    @Test
    fun `when assigned warm-up partitions use ordered processing then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                processingMode = ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING
                assignedPartitionsAtEnd(TopicPartition("warmup-a", 0))
                handle { }
            }
        }

        assertEquals("Explicit warm-up assignment supports only AT_LEAST_ONCE_NO_ORDERING", error.message)
    }

    @Test
    fun `when assigned warm-up partitions use multiple poll loops then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                consumerPollLoopConcurrency = 2
                assignedPartitionsAtEnd(TopicPartition("warmup-a", 0))
                handle { }
            }
        }

        assertEquals("Explicit warm-up assignment requires consumerPollLoopConcurrency = 1", error.message)
    }

    @Test
    fun `when builder does not define handler then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                topics("topic-a")
            }
        }

        assertEquals("Kafka record handler must be specified", error.message)
    }

    @Test
    fun `when freshness max record age is configured for at least once mode then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                processingMode = ProcessingMode.AT_LEAST_ONCE_NO_ORDERING
                freshnessMaxRecordAge = 10.seconds
                topics("topic-a")
                handle { }
            }
        }

        assertEquals(
            "freshnessMaxRecordAge is supported only for freshness-first processing modes",
            error.message
        )
    }

    @Test
    fun `when freshness max record age is not positive then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
                freshnessMaxRecordAge = 0.seconds
                topics("topic-a")
                handle { }
            }
        }

        assertEquals("freshnessMaxRecordAge must be > 0", error.message)
    }

    @Test
    fun `when commit records threshold is not positive then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                commitRecordsThreshold = 0
                topics("topic-a")
                handle { }
            }
        }

        assertEquals("commitRecordsThreshold must be > 0", error.message)
    }

    @Test
    fun `when maximum tracked offsets is not a supported ring size then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
                maxTrackedOffsetsPerPartition = 1_000
                topics("topic-a")
                handle { }
            }
        }

        assertEquals("maxTrackedOffsetsPerPartition must be a power of two and >= 128", error.message)
    }

    @Test
    fun `when freshness-first mode has auto commit disabled then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(
            stringSerdeProperties() + (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false")
        ) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            topics("topic-a")
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when freshness-first mode explicitly enables auto commit then build fails`() {
        assertAutoCommitRejected(ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST)
    }

    @Test
    fun `when at-least-once mode explicitly enables auto commit then build fails`() {
        assertAutoCommitRejected(ProcessingMode.AT_LEAST_ONCE_NO_ORDERING)
    }

    private fun assertAutoCommitRejected(processingMode: ProcessingMode) {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(
                stringSerdeProperties() + mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
                )
            ) {
                this.processingMode = processingMode
                topics("topic-a")
                handle { }
            }
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}' must not be true; CKC manages commits explicitly",
            error.message
        )
    }

    private fun forwardingConfiguration(): WarmupForwardingConfiguration = WarmupForwardingConfiguration(
        sessionId = WarmupSessionId("session-1"),
        consumerId = WarmupConsumerId("orders"),
        assignments = listOf(
            WarmupPartitionAssignment(
                consumerId = WarmupConsumerId("orders"),
                sourceTopic = "source-orders",
                warmupTopic = "warmup-orders",
                warmupPartition = 0
            )
        ),
        profile = WarmupForwardingProfile(),
        producerProperties = mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )
    )
}
