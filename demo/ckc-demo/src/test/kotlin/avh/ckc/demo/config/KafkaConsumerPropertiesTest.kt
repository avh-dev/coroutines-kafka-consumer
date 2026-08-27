package avh.ckc.demo.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaConsumerPropertiesTest {
    @Test
    fun `topic overrides replace only selected shared consumer settings`() {
        val properties = DemoApplicationProperties().apply {
            kafka.consumer.fetchMinBytes = 8192
            kafka.consumer.fetchMaxWaitMs = 250
            kafka.consumer.maxPollRecords = 500
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
        assertEquals(32 * 1024 * 1024, resolved[ConsumerConfig.FETCH_MAX_BYTES_CONFIG])
        assertEquals(2 * 1024 * 1024, resolved[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG])
    }
}
