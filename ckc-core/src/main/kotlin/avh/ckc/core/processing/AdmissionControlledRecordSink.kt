package avh.ckc.core.processing

import avh.ckc.core.extension.RecordAdmission
import org.apache.kafka.clients.consumer.ConsumerRecord

/** Transfers admission reservations to the processing queue only after acceptance. */
internal class AdmissionControlledRecordSink<K, V>(
    private val delegate: PolledRecordSink<K, V>,
    private val processedRecordTracker: ProcessedRecordTracker,
    private val admission: RecordAdmission<K, V>
) : PolledRecordSink<K, V> {
    override fun tryEmit(record: ConsumerRecord<K, V>): Boolean {
        if (discardIfFiltered(record)) return true
        if (!admission.tryAcquire(record)) return false
        var accepted = false
        try {
            accepted = delegate.tryEmit(record)
            return accepted
        } finally {
            if (!accepted) admission.release(record)
        }
    }

    override fun discardIfFiltered(record: ConsumerRecord<K, V>): Boolean {
        if (!admission.shouldSkip(record)) return false

        // Tracked modes advance the processed frontier. Freshness modes have already counted
        // the accepted position in the poll loop; both release the record's ownership bookkeeping.
        processedRecordTracker.markProcessed(record)
        processedRecordTracker.discard(record)
        return true
    }
}
