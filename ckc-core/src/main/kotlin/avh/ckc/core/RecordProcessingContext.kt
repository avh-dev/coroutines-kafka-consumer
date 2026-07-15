package avh.ckc.core

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Optional context wrapper applied around user record processing callbacks.
 *
 * Implementations can attach logging, tracing, or other coroutine-local context for the lifetime of
 * a single record. Passing `null` to the consumer runtime keeps the hot path direct.
 */
fun interface RecordProcessingContext<K, V> {
    suspend fun withRecordContext(record: ConsumerRecord<K, V>, block: suspend () -> Unit)
}
