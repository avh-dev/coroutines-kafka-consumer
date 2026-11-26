package avh.ckc.core.extension

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Controls admission before a record enters the processing queue. Callbacks run on the poll loop
 * and may be called concurrently by different poll loops. Keep them thread-safe and non-blocking.
 * The extension owns reservations that were transferred to the processing runtime.
 */
fun interface RecordAdmission<K, V> {
    /** Skips a record without business processing or terminal record metrics. */
    fun shouldSkip(record: ConsumerRecord<K, V>): Boolean

    /**
     * Reserves capacity for a non-skipped record. False leaves it pending for a later admission
     * attempt. A successful reservation is transferred to the runtime if it accepts the record,
     * and the extension must retire it on the record's terminal or handoff path.
     */
    fun tryAcquire(record: ConsumerRecord<K, V>): Boolean = true

    /** Rolls back a successful reservation if the queue did not accept the record. */
    fun release(record: ConsumerRecord<K, V>) = Unit
}
