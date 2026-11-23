package avh.ckc.core.extension

import org.apache.kafka.clients.consumer.ConsumerRecord

/** Controls admission before a polled record enters the processing queue. */
fun interface RecordAdmission<K, V> {
    /** Skips a record without business processing or terminal record metrics. */
    fun shouldSkip(record: ConsumerRecord<K, V>): Boolean

    /** Reserves capacity for a non-skipped record. False keeps it pending in the poll loop. */
    fun tryAcquire(record: ConsumerRecord<K, V>): Boolean = true

    /** Releases a reservation if the queue could not accept the record. */
    fun release(record: ConsumerRecord<K, V>) = Unit
}
