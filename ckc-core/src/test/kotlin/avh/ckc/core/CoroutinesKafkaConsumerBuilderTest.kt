package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig
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
    fun `when processing mode is FRESHNESS_FIRST_DROP_OLDEST and auto commit disabled then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(
                stringSerdeProperties() + mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                )
            ) {
                processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
                topics("topic-a")
                handle { }
            }
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}' must be true when processingMode=FRESHNESS_FIRST_DROP_OLDEST",
            error.message
        )
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_DROP_OLDEST and auto commit enabled then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(
            stringSerdeProperties() + mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
            )
        ) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST
            topics("topic-a")
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY and auto commit disabled then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(
                stringSerdeProperties() + mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                )
            ) {
                processingMode = ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
                topics("topic-a")
                handle { }
            }
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}' must be true when processingMode=FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY",
            error.message
        )
    }

    @Test
    fun `when processing mode is FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY and auto commit enabled then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(
            stringSerdeProperties() + mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
            )
        ) {
            processingMode = ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
            topics("topic-a")
            handle { }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }
}
