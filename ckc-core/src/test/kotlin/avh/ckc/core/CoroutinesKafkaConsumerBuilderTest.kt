package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.regex.Pattern

class CoroutinesKafkaConsumerBuilderTest {

    @Test
    fun `when builder is configured with topics then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topics("topic-a")
            handle { _, _, _ -> }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }

    @Test
    fun `when builder is configured with topics pattern then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(stringSerdeProperties()) {
            topicsPattern(Pattern.compile("topic-.*"))
            handle { _, _, _ -> }
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
    fun `when delivery strategy is lossy and auto commit disabled then build fails`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            coroutinesKafkaConsumer<String, String>(
                stringSerdeProperties() + mapOf(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                )
            ) {
                deliveryStrategy = DeliveryStrategy.LOSSY
                topics("topic-a")
                handle { _, _, _ -> }
            }
        }

        assertEquals(
            "Kafka property '${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}' must be true when deliveryStrategy=LOSSY",
            error.message
        )
    }

    @Test
    fun `when delivery strategy is lossy and auto commit enabled then consumer is created`() {
        val consumer = coroutinesKafkaConsumer<String, String>(
            stringSerdeProperties() + mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
            )
        ) {
            deliveryStrategy = DeliveryStrategy.LOSSY
            topics("topic-a")
            handle { _, _, _ -> }
        }

        assertEquals(CoroutinesKafkaConsumer::class, consumer::class)
    }
}
