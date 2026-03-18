package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.Properties

/**
 * Configuration container for [CoroutineKafkaConsumer] and [ConsumerPollLoop].
 *
 * Combines coroutine-specific settings (concurrency, overflow handling, commit cadence)
 * with the raw Kafka client configuration supplied via [kafkaProperties].
 *
 * The Kafka properties are wrapped into a single [ConsumerConfig] instance, which applies
 * Kafka defaults and performs type conversion once. This avoids repeated parsing during
 * the poll loop.
 */
class CoroutineKafkaConsumerConfig(

    /**
     * Strategy used when downstream workers cannot keep up with incoming records.
     *
     * - [OverflowStrategy.BACKPRESSURE] — pause/resume Kafka intake and buffer records locally.
     * - [OverflowStrategy.THROTTLING] — suspend on channel send and rely on channel behaviour
     *   (e.g. dropping) and Kafka auto-commit.
     */
    val overflowStrategy: OverflowStrategy,

    /**
     * Number of worker coroutines processing records from the dispatch channel.
     */
    val workerConcurrency: Int,

    /**
     * Number of independent poll loops (each with its own KafkaConsumer).
     *
     * Increasing this value allows consuming multiple partitions in parallel
     * when a single poll thread becomes a bottleneck.
     */
    val consumerPollLoopConcurrency: Int,

    /**
     * Interval for periodic best-effort offset commits (milliseconds).
     *
     * Applies only to the BACKPRESSURE strategy where commits are managed explicitly.
     */
    val commitIntervalMs: Long,

    /**
     * Raw Kafka consumer properties passed to the underlying Kafka client.
     *
     * These correspond to the standard Kafka configuration keys such as
     * `bootstrap.servers`, `group.id`, `max.poll.records`, etc.
     */
    val kafkaProperties: Properties,
) {

    /**
     * Lazily constructed Kafka [ConsumerConfig].
     *
     * Built once to allow Kafka's configuration parser to apply defaults
     */
    private val consumerConfig: ConsumerConfig by lazy {
        // ConsumerConfig accepts Map<String, *>
        @Suppress("UNCHECKED_CAST")
        val map = kafkaProperties.entries.associate { (k, v) -> k.toString() to v } +
                mapOf(
                    KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
                )
        ConsumerConfig(map)
    }

    /**
     * Returns the Kafka configuration property as an Int.
     *
     * If the property is missing or cannot be parsed as an integer,
     * null is returned.
     */
    fun getKafkaPropertyInt(key: String): Int? =
        try {
            consumerConfig.getInt(key)
        } catch (_: Exception) {
            null
        }
}