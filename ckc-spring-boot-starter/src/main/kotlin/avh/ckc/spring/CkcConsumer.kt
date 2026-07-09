package avh.ckc.spring

import avh.ckc.micrometer.RecordDrivenTagValues
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Spring-facing CKC consumer contract.
 *
 * Implement this interface on a Spring bean annotated with [CkcKafkaConsumer].
 * Runtime settings are supplied by `ckc.consumers.<name>` application properties.
 */
interface CkcConsumer<K, V> {
    suspend fun process(record: ConsumerRecord<K, V>)

    suspend fun handleFailure(record: ConsumerRecord<K, V>, reason: Throwable) = Unit

    fun metricsCustomizer(): CkcMetricsCustomizer<K, V> =
        CkcMetricsCustomizer.none()
}

fun interface CkcMetricsCustomizer<K, V> {
    fun recordDrivenTagValues(): RecordDrivenTagValues<K, V>

    companion object {
        fun <K, V> none(): CkcMetricsCustomizer<K, V> =
            CkcMetricsCustomizer { RecordDrivenTagValues.none() }
    }
}
