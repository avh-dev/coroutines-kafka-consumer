package avh.ckc.core.warmup

import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.ProcessedRecordTracker
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Drops records for every session except [sessionId] before they enter a processing runtime.
 *
 * The poll loop has already registered the record's offset and assignment generation when this
 * sink is called. A rejected record therefore closes its own tracked offset and then releases its
 * lease bookkeeping. It deliberately emits no business, retry, terminal, or ordinary record
 * metric callback; Kafka poll metrics remain the poll loop's responsibility.
 */
internal class WarmupRecipientSessionFilteringSink<K, V>(
    sessionId: WarmupSessionId,
    private val delegate: PolledRecordSink<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker
) : PolledRecordSink<K, V> {
    private val expectedSessionHeader = sessionId.value.toByteArray(Charsets.UTF_8)

    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (belongsToRecipientSession(record)) return delegate.tryEmit(record)

        processedRecordTracker.markProcessed(record)
        processedRecordTracker.discard(record)
        return true
    }

    private fun belongsToRecipientSession(record: ConsumerRecord<K, V>): Boolean {
        val headers = record.headers().headers(WARMUP_SESSION_HEADER).iterator()
        if (!headers.hasNext()) return false
        val value = headers.next().value()
        return !headers.hasNext() && value != null && value.contentEquals(expectedSessionHeader)
    }
}
