package avh.ckc.core.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
 * Thin adapter over Kafka's [ConsumerConfig].
 *
 * Builds a single resolved [ConsumerConfig] instance from raw consumer properties
 * so internal code can read Kafka defaults and typed values without reparsing
 * the configuration on each access.
 */
internal class KafkaConsumerConfigAdapter(
    consumerProperties: Map<String, Any?>
) {
    private val consumerConfig: ConsumerConfig by lazy {
        ConsumerConfig(
            consumerProperties + mapOf(
                KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
            )
        )
    }

    /**
     * Returns the Kafka configuration property as an [Int].
     *
     * If the property is missing or cannot be parsed as an integer,
     * null is returned.
     */
    fun getInt(key: String): Int? =
        try {
            consumerConfig.getInt(key)
        } catch (_: Exception) {
            null
        }

    /**
     * Returns the Kafka configuration property as a [Boolean].
     *
     * If the property is missing or cannot be parsed as a boolean,
     * null is returned.
     */
    fun getBoolean(key: String): Boolean? =
        try {
            consumerConfig.getBoolean(key)
        } catch (_: Exception) {
            null
        }
}
