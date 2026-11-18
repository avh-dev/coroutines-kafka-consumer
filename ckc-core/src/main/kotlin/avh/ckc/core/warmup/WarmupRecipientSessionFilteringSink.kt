package avh.ckc.core.warmup

import avh.ckc.core.processing.PolledRecordSink
import avh.ckc.core.processing.ProcessedRecordTracker
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Drops records for every session except [sessionId] before they enter a processing runtime.
 *
 * For tracked modes, the poll loop has already registered the record's offset and assignment
 * generation: rejection marks that offset processed and releases its lease bookkeeping. For
 * freshness modes, poll admission already records the accepted position, so rejection only
 * releases ownership bookkeeping. Neither path emits business, retry, terminal, or ordinary
 * record metrics; Kafka poll metrics remain the poll loop's responsibility.
 */
internal class WarmupRecipientSessionFilteringSink<K, V>(
    sessionId: WarmupSessionId,
    private val delegate: PolledRecordSink<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker,
    private val resultAdmission: WarmupResultAdmission? = null
) : PolledRecordSink<K, V> {
    private val expectedSessionHeader = sessionId.value.toByteArray(Charsets.UTF_8)

    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (discardIfFiltered(record)) return true
        val admission = resultAdmission
        if (admission != null && !admission.tryAcquire(record)) return false
        return try {
            delegate.tryEmit(record).also { accepted ->
                if (!accepted) admission?.release(record)
            }
        } catch (error: Throwable) {
            admission?.release(record)
            throw error
        }
    }

    override fun discardIfFiltered(record: ConsumerRecord<K, V>): Boolean {
        if (belongsToRecipientSession(record)) return false

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
