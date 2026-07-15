package avh.ckc.spring

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Spring-facing CKC consumer contract.
 *
 * Implement this interface on a Spring bean annotated with [CkcKafkaConsumer].
 * Runtime settings are supplied by `ckc.consumers.<name>` application properties.
 */
interface CkcConsumer<K, V> {
    /**
     * Handles a Kafka record after the starter-managed CKC runtime has polled it.
     */
    suspend fun process(record: ConsumerRecord<K, V>)

    /**
     * Handles a terminal processing failure after the configured retry policy is exhausted.
     *
     * The default implementation treats the failure as handled so CKC can continue according
     * to its normal offset tracking rules.
     */
    suspend fun handleFailure(record: ConsumerRecord<K, V>, reason: Throwable) = Unit
}
