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
        assertEquals(
            0,
            buildControlProducerProperties(emptyMap(), emptyMap())[ProducerConfig.LINGER_MS_CONFIG]
        )
        assertEquals(
            5,
            buildWarmupForwardingProducerProperties(emptyMap())[ProducerConfig.LINGER_MS_CONFIG]
        )
        listOf(
            buildControlConsumerProperties(emptyMap(), emptyMap()),
            buildWarmupRecipientConsumerProperties(emptyMap(), emptyMap(), emptyMap())
        ).forEach { properties ->
            assertEquals(1, properties[ConsumerConfig.FETCH_MIN_BYTES_CONFIG])
            assertEquals(3_000, properties[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
        }
    }

    @Test
    fun `explicit producer linger including zero wins over defaults`() {
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
    }

    @Test
    fun `recipient defaults replace inherited main fetch settings without changing main properties`() {
        val main = linkedMapOf<String, Any?>(
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 64,
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 100,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 42
        )
        val original = LinkedHashMap(main)

        val recipient = buildWarmupRecipientConsumerProperties(main, emptyMap(), emptyMap())

        assertEquals(1, recipient[ConsumerConfig.FETCH_MIN_BYTES_CONFIG])
        assertEquals(3_000, recipient[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
        assertEquals(42, recipient[ConsumerConfig.MAX_POLL_RECORDS_CONFIG])
        assertEquals(original, main)
    }

    @Test
    fun `explicit recipient fetch settings win over defaults`() {
        val recipient = buildWarmupRecipientConsumerProperties(
            mainConsumerProperties = mapOf(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 64,
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 100
            ),
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
    fun `complete user maps retain unrelated routing security and serde settings`() {
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
    fun `required protocol properties keep final priority`() {
        val controlProducer = buildControlProducerProperties(
            userProperties = mapOf(
                ProducerConfig.ACKS_CONFIG to "0",
                ProducerConfig.CLIENT_ID_CONFIG to "user-producer"
            ),
            requiredProperties = mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.CLIENT_ID_CONFIG to "required-producer"
            )
        )
        val controlConsumer = buildControlConsumerProperties(
            userProperties = mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
                ConsumerConfig.GROUP_ID_CONFIG to "user-group"
            ),
            requiredProperties = mapOf(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.GROUP_ID_CONFIG to "required-group"
            )
        )
        val recipient = buildWarmupRecipientConsumerProperties(
            mainConsumerProperties = emptyMap(),
            userProperties = mapOf(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG to "static-member",
                ConsumerConfig.GROUP_ID_CONFIG to "user-group"
            ),
            requiredProperties = mapOf(ConsumerConfig.GROUP_ID_CONFIG to "required-group")
        )

        assertEquals("all", controlProducer[ProducerConfig.ACKS_CONFIG])
        assertEquals("required-producer", controlProducer[ProducerConfig.CLIENT_ID_CONFIG])
        assertEquals(false, controlConsumer[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals("required-group", controlConsumer[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals("required-group", recipient[ConsumerConfig.GROUP_ID_CONFIG])
        assertFalse(recipient.containsKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG))
    }

    @Test
    fun `property composition does not mutate any source map`() {
        val main = linkedMapOf<String, Any?>(ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 99)
        val user = linkedMapOf<String, Any?>(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 77)
        val required = linkedMapOf<String, Any?>(ConsumerConfig.GROUP_ID_CONFIG to "required")
        val mainBefore = LinkedHashMap(main)
        val userBefore = LinkedHashMap(user)
        val requiredBefore = LinkedHashMap(required)

        buildWarmupRecipientConsumerProperties(main, user, required)
        buildControlConsumerProperties(user, required)
        buildControlProducerProperties(user, required)
        buildWarmupForwardingProducerProperties(user)

        assertEquals(mainBefore, main)
        assertEquals(userBefore, user)
        assertEquals(requiredBefore, required)
    }
}
