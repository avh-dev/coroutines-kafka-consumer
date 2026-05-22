package avh.ckc.core

import avh.ckc.core.kafka.KafkaConsumerConfigAdapter
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class KafkaConsumerConfigAdapterTest {

    @Test
    fun `when kafka int property is configured then parsed value is returned`() {
        val adapter = KafkaConsumerConfigAdapter(
            testConsumerProperties(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "42"
            )
        )

        assertEquals(42, adapter.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    @Test
    fun `when kafka int property is missing then kafka default is returned`() {
        val adapter = KafkaConsumerConfigAdapter(
            testConsumerProperties()
                .filterKeys { it != ConsumerConfig.MAX_POLL_RECORDS_CONFIG }
        )

        assertEquals(500, adapter.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    @Test
    fun `when kafka int property is not a number then null is returned`() {
        val adapter = KafkaConsumerConfigAdapter(
            testConsumerProperties(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "not-a-number"
            )
        )

        assertNull(adapter.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
    }

    @Test
    fun `when kafka boolean property is configured then parsed value is returned`() {
        val adapter = KafkaConsumerConfigAdapter(
            testConsumerProperties(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
            )
        )

        assertTrue(adapter.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == true)
    }
}
