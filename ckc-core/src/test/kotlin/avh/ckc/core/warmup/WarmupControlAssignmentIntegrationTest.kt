package avh.ckc.core.warmup

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class WarmupControlAssignmentIntegrationTest {
    @Test
    fun `initial Kafka group delay does not recreate the control consumer`() = runBlocking {
        val topic = "control-assignment-${UUID.randomUUID()}"
        AdminClient.create(connection()).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1))).all().get(10, TimeUnit.SECONDS)
        }
        val consumerCreations = AtomicInteger()
        val transport = WarmupControlTransport(
            configuration = WarmupControlTransportConfiguration(
                instanceId = WarmupInstanceId("delayed-assignment"),
                namespace = WarmupControlNamespace("global5-v6-assignment-test"),
                topic = topic,
                producerProperties = connection(),
                consumerProperties = connection(),
                pollTimeout = 20.milliseconds,
                metadataTimeout = 5.seconds,
                assignmentTimeout = 8.seconds,
                recoveryCheckInterval = 100.milliseconds,
                recoveryFailureThreshold = 2
            ),
            handler = WarmupControlMessageHandler { },
            consumerFactory = { properties ->
                consumerCreations.incrementAndGet()
                KafkaConsumer(properties)
            }
        )

        try {
            val startedAt = System.nanoTime()
            transport.start()
            withTimeout(10.seconds) { transport.awaitReady() }
            val elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000

            assertTrue(elapsedMillis >= 2_000, "Kafka group join was not delayed: ${elapsedMillis}ms")
            assertEquals(1, consumerCreations.get())
        } finally {
            withTimeout(5.seconds) { transport.stop() }
        }
    }

    private fun connection(): Map<String, Any?> = mapOf("bootstrap.servers" to kafka.bootstrapServers)

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "3000")
    }
}
