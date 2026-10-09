package avh.ckc.demo.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaConsumerPropertiesTest {
    @Test
    fun `shared assignment strategy is passed to Kafka`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.assignmentStrategy = "org.apache.kafka.clients.consumer.RoundRobinAssignor"
        }

        val resolved = properties.kafkaConsumerProperties(properties.consumers.order)

        assertEquals(
            "org.apache.kafka.clients.consumer.RoundRobinAssignor",
            resolved[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG]
        )
    }

    @Test
    fun `blank assignment strategy preserves Kafka defaults`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.assignmentStrategy = "  "
        }

        val resolved = properties.kafkaConsumerProperties(properties.consumers.order)

        assertEquals(false, resolved.containsKey(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))
    }

    @Test
    fun `topic overrides replace only selected shared consumer settings`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.fetchMinBytes = 8192
            kafka.consumer.fetchMaxWaitMs = 250
            kafka.consumer.maxPollRecords = 500
            kafka.consumer.maxPollIntervalMs = 1_800_000
            kafka.consumer.fetchMaxBytes = 32 * 1024 * 1024
            kafka.consumer.maxPartitionFetchBytes = 1024 * 1024
            consumers.telemetry.kafka.fetchMaxWaitMs = 50
            consumers.telemetry.kafka.maxPollRecords = 200
            consumers.telemetry.kafka.maxPartitionFetchBytes = 2 * 1024 * 1024
        }

        val resolved = properties.kafkaConsumerProperties(properties.consumers.telemetry)

        assertEquals(8192, resolved[ConsumerConfig.FETCH_MIN_BYTES_CONFIG])
        assertEquals(50, resolved[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
        assertEquals(200, resolved[ConsumerConfig.MAX_POLL_RECORDS_CONFIG])
        assertEquals(1_800_000, resolved[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG])
        assertEquals(32 * 1024 * 1024, resolved[ConsumerConfig.FETCH_MAX_BYTES_CONFIG])
        assertEquals(2 * 1024 * 1024, resolved[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG])
    }
}
