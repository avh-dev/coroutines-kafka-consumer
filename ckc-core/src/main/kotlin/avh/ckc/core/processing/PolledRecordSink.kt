package avh.ckc.core.processing

import org.apache.kafka.clients.consumer.ConsumerRecord

internal interface PolledRecordSink<K, V> {
    fun tryEmit(record: ConsumerRecord<K, V>): Boolean

    /** Returns true after silently releasing a record rejected before runtime admission. */
    fun discardIfFiltered(record: ConsumerRecord<K, V>): Boolean = false
}
