package avh.ckc.core.warmup

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test

class WarmupKafkaClientPropertiesTest {
    @Test
    fun `missing settings receive client specific defaults`() {
        assertEquals(0, buildControlProducerProperties(emptyMap(), emptyMap())[ProducerConfig.LINGER_MS_CONFIG])
        assertEquals(5, buildWarmupForwardingProducerProperties(emptyMap())[ProducerConfig.LINGER_MS_CONFIG])
        listOf(
            buildControlConsumerProperties(emptyMap(), emptyMap()),
            buildWarmupRecipientConsumerProperties(emptyMap(), emptyMap())
        ).forEach { properties ->
            assertEquals(1, properties[ConsumerConfig.FETCH_MIN_BYTES_CONFIG])
            assertEquals(3_000, properties[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
        }
    }

    @Test
    fun `explicit client settings win over defaults`() {
        assertEquals(
            17,
            buildControlProducerProperties(
                mapOf(ProducerConfig.LINGER_MS_CONFIG to 17),
                emptyMap()
            )[ProducerConfig.LINGER_MS_CONFIG]
        )
        assertEquals(
            0,
            buildWarmupForwardingProducerProperties(
                mapOf(ProducerConfig.LINGER_MS_CONFIG to 0)
            )[ProducerConfig.LINGER_MS_CONFIG]
        )
        val recipient = buildWarmupRecipientConsumerProperties(
            userProperties = mapOf(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 8,
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 250
            ),
            requiredProperties = emptyMap()
        )
        assertEquals(8, recipient[ConsumerConfig.FETCH_MIN_BYTES_CONFIG])
        assertEquals(250, recipient[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
    }

    @Test
    fun `complete connection maps retain unrelated security serde and routing settings`() {
        val user = linkedMapOf<String, Any?>(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "warmup:9092",
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SASL_SSL",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "key.serializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "value.serializer",
            ProducerConfig.BATCH_SIZE_CONFIG to 32_768,
            ProducerConfig.LINGER_MS_CONFIG to 9
        )
        val original = LinkedHashMap(user)

        val result = buildWarmupForwardingProducerProperties(user)

        user.forEach { (name, value) -> assertEquals(value, result[name]) }
        assertEquals(original, user)
    }

    @Test
    fun `required protocol properties keep final priority and static membership is removed`() {
        val controlProducer = buildControlProducerProperties(
            userProperties = mapOf(ProducerConfig.ACKS_CONFIG to "0"),
            requiredProperties = mapOf(ProducerConfig.ACKS_CONFIG to "all")
        )
        val controlConsumer = buildControlConsumerProperties(
            userProperties = mapOf(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true),
            requiredProperties = mapOf(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false)
        )
        val recipient = buildWarmupRecipientConsumerProperties(
            userProperties = mapOf(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG to "static-member",
                ConsumerConfig.GROUP_ID_CONFIG to "user-group"
            ),
            requiredProperties = mapOf(ConsumerConfig.GROUP_ID_CONFIG to "required-group")
        )

        assertEquals("all", controlProducer[ProducerConfig.ACKS_CONFIG])
        assertEquals(false, controlConsumer[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals("required-group", recipient[ConsumerConfig.GROUP_ID_CONFIG])
        assertFalse(recipient.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG))
    }

    @Test
    fun `property composition does not mutate source maps`() {
        val user = linkedMapOf<String, Any?>(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 77)
        val required = linkedMapOf<String, Any?>(ConsumerConfig.GROUP_ID_CONFIG to "required")
        val userBefore = LinkedHashMap(user)
        val requiredBefore = LinkedHashMap(required)

        buildWarmupRecipientConsumerProperties(user, required)
        buildControlConsumerProperties(user, required)
        buildControlProducerProperties(user, required)
        buildWarmupForwardingProducerProperties(user)

        assertEquals(userBefore, user)
        assertEquals(requiredBefore, required)
    }
}
