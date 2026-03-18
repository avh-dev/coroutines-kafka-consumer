package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.Properties

class CoroutineKafkaConsumerConfigTest {

    @Test
    fun `when kafka int property is configured then parsed value is returned`() {
        val config = configWith(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "42"
        )

        assertEquals(42, config.getKafkaPropertyInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    @Test
    fun `when kafka int property is missing then kafka default is returned`() {
        val config = configWith()

        assertEquals(500, config.getKafkaPropertyInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    @Test
    fun `when kafka int property is not a number then null is returned`() {
        val config = configWith(
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "not-a-number"
        )

        assertNull(config.getKafkaPropertyInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    private fun configWith(vararg extraProperties: Pair<String, String>) =
        CoroutineKafkaConsumerConfig(
            overflowStrategy = OverflowStrategy.BACKPRESSURE,
            workerConcurrency = 1,
            consumerPollLoopConcurrency = 1,
            commitIntervalMs = 1_000L,
            kafkaProperties = Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
                put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
                extraProperties.forEach { (key, value) -> put(key, value) }
            }
        )
}
